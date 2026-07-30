/// @file
/// @brief HFT control closeout / force-flat 执行路径实现。
///
/// 本文件承接 `HftCloseoutGate.h` 暴露的 gate 与强平入口。
/// gate 判定保持纯函数，便于单测 session、force window、TD readiness 和节流语义。
/// snapshot 收集、撤在途单和强平下单留在本文件内。
/// TD 恢复中不触发强平，避免 recovery/reconcile 与人工平仓互相覆盖。

#include "xt_live/hft/control/HftCloseoutGate.h"
#include "xt_live/hft/control/HftRunnerControl.h"
#include "xt_risk/config/XtRiskCfg.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "XtLogger.h"
#include "XtLiveOrderGatewayCfg.h"
#include "api/XtRunnerSignals.h"
#include "xt_util/time/XtSteadyClock.h"
#include "XtFixedPoint.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/api/AccountSnapshot.h"
#include "state/AccountStateStore.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "api/ITradeGateway.h"
#include "xt_identity/OrderAttributionStore.h"
#include "xt_core/session/XtSessionFilter.h"
#include "xt_util/time/XtTime.h"
#include "xt_core/order/XtOrderTypes.h"
#include "api/ExecFlags.h"
#include "CloseoutPricing.h"
#include "xt_config/catalog/XtCommodityCatalog.h"

namespace {

ITradeGateway* load_td_for_runner_force_close(ITradeGateway* td, const std::atomic<bool>& trading_ready) {
    if (!td) return nullptr;
    if (!trading_ready.load(std::memory_order_acquire)) return nullptr;
    if (td->is_recovering()) return nullptr;
    return td;
}

// 单合约强平方向上的 account snapshot（mark / qty / closeable）。
struct CloseoutAccountSideSnap {
    int64_t mark_fp = 0;
    int64_t long_qty_fp = 0;
    int64_t short_qty_fp = 0;
    xt_account::CloseableView long_close;
    xt_account::CloseableView short_close;
    int64_t price_tick_fp = 0;
};

void collect_closeout_order_side(
    xt_runtime::order::XtOrderRuntimeBook& book,
    std::vector<uint64_t>& cancel_ids,
    std::vector<std::string>& candidate_codes) {
    cancel_ids.clear();
    candidate_codes.clear();

    std::vector<std::string> tracked;
    book.collect_tracked_codes(tracked);

    std::vector<uint64_t> per_code_ids;
    per_code_ids.reserve(16);
    for (const auto& code : tracked) {
        if (code.empty()) continue;
        per_code_ids.clear();
        book.collect_working_ids_for_code(code.c_str(), per_code_ids);
        for (uint64_t id : per_code_ids) cancel_ids.push_back(id);
    }
    if (!cancel_ids.empty()) return;

    candidate_codes.reserve(tracked.size());
    for (auto& code : tracked) {
        if (code.empty()) continue;
        if (book.is_force_flat_pending(code.c_str())) continue;
        candidate_codes.push_back(std::move(code));
    }
}

void snapshot_closeout_account_side(
    const xt_account::AccountStateStore& account,
    const std::vector<std::string>& candidate_codes,
    std::vector<CloseoutAccountSideSnap>& snaps,
    const XtCommodityCatalog* catalog) {
    snaps.clear();
    snaps.reserve(candidate_codes.size());
    for (const auto& code : candidate_codes) {
        CloseoutAccountSideSnap s;
        const auto pos = account.snapshot_position(code);
        s.mark_fp = pos.mark_price;
        s.long_qty_fp = pos.long_qty;
        s.short_qty_fp = pos.short_qty;
        if (catalog) {
            const auto* info = catalog->find(code.c_str());
            if (info && info->price_tick > 0.0) {
                s.price_tick_fp = order_exec_to_fp(info->price_tick);
            }
        }
        if (s.long_qty_fp > 0) {
            s.long_close = xt_account::closeable_view(pos,  false);
        }
        if (s.short_qty_fp > 0) {
            s.short_close = xt_account::closeable_view(pos,  true);
        }
        snaps.push_back(std::move(s));
    }
}

void build_force_flat_orders_unlocked(const std::vector<std::string>& candidate_codes,
                                      const std::vector<CloseoutAccountSideSnap>& snaps,
                                      std::vector<xt_core::XtOrder>& force_flat_orders,
                                      bool escalate,
                                      int  buffer_bps) {
    force_flat_orders.clear();
    if (candidate_codes.size() != snaps.size()) return;

    const auto append_force_req = [&](const std::string& code,
                                       bool is_buy,
                                       int64_t qty_fp,
                                       int64_t mark_fp,
                                       int64_t price_tick_fp,
                                       XtOffset close_offset) {
        if (qty_fp <= 0 || mark_fp <= 0) return;
        int64_t       price_fp = mark_fp;
        XtTimeInForce tif      = XtTimeInForce::GTC;
        if (escalate) {

            price_fp = xt_live::control::legal_close_price_fp(
                code,
                xt_live::control::aggressive_close_price_fp(mark_fp, is_buy, buffer_bps),
                is_buy, price_tick_fp);
            tif = XtTimeInForce::IOC;
        }
        if (price_fp <= 0) return;
        xt_core::XtOrder req;
        req.set_code(code.c_str());
        req.is_buy = is_buy;
        req.price_fp = price_fp;
        req.qty_fp = qty_fp;
        req.tif = tif;
        req.close_offset = close_offset;
        force_flat_orders.push_back(std::move(req));
    };

    for (size_t i = 0; i < candidate_codes.size(); ++i) {
        const std::string& code = candidate_codes[i];
        const CloseoutAccountSideSnap& s = snaps[i];
        if (s.mark_fp <= 0) continue;
        if (s.long_qty_fp > 0) {
            append_force_req(code, false, s.long_close.today_fp,     s.mark_fp, s.price_tick_fp, XtOffset::CloseToday);
            append_force_req(code, false, s.long_close.yesterday_fp, s.mark_fp, s.price_tick_fp, XtOffset::CloseYesterday);
        }
        if (s.short_qty_fp > 0) {
            append_force_req(code, true, s.short_close.today_fp,     s.mark_fp, s.price_tick_fp, XtOffset::CloseToday);
            append_force_req(code, true, s.short_close.yesterday_fp, s.mark_fp, s.price_tick_fp, XtOffset::CloseYesterday);
        }
    }
}

CloseoutGateInput build_closeout_gate_input(const XtLiveOrderGatewayCfg* cfg,
                                            const XtHftRunnerControlContext& ctx,
                                            int64_t closeout_last_try_ms,
                                            std::vector<int>& force_list_out) {
    const uint32_t tick_tm  = ctx.runtime_shared_state.last_tick_action_time.load(std::memory_order_relaxed);
    const int      tick_hhmm  = tick_tm == 0
                                    ? -1
                                    : static_cast<int>(xt_time::action_time_to_hhmm(tick_tm));
    const int      local_hhmm = xt_time::local_hhmm_now();
    const int64_t  tnow       = XtSteadyClock::steady_now_ms();

    force_list_out.clear();
    if (cfg) {
        force_list_out.reserve(cfg->closeout_windows.size());
        for (const auto& w : cfg->closeout_windows) force_list_out.push_back(w.force_hhmm);
    }

    const bool td_avail = load_td_for_runner_force_close(ctx.td_gateway,
                                                         ctx.runtime_shared_state.trading_ready) != nullptr;

    CloseoutGateInput gate_in;
    gate_in.cfg_enabled = cfg && cfg->closeout_enabled;
    gate_in.has_windows = cfg && !cfg->closeout_windows.empty();
    gate_in.session_gate_enabled = cfg && cfg->session_gate_enabled;
    gate_in.session_gate_sections = cfg ? &cfg->session_gate_sections : nullptr;
    gate_in.force_hhmm_list = &force_list_out;
    gate_in.local_hhmm = local_hhmm;
    gate_in.tick_hhmm = tick_hhmm;
    gate_in.td_available = td_avail;
    gate_in.tnow_ms = tnow;
    gate_in.closeout_last_try_ms = closeout_last_try_ms;
    gate_in.closeout_retry_ms = cfg ? cfg->closeout_retry_ms : 1000;
    return gate_in;
}

void collect_closeout_phases(const XtHftRunnerControlContext& ctx,
                             std::vector<uint64_t>& cancel_ids_out,
                             std::vector<xt_core::XtOrder>& force_flat_orders_out,
                             bool escalate,
                             int  buffer_bps) {
    if (!ctx.order_book || !ctx.account_store) return;
    std::vector<std::string> candidate_codes;
    collect_closeout_order_side(*ctx.order_book, cancel_ids_out, candidate_codes);
    if (!cancel_ids_out.empty() || candidate_codes.empty()) return;
    std::vector<CloseoutAccountSideSnap> snaps;
    snapshot_closeout_account_side(*ctx.account_store, candidate_codes, snaps, ctx.catalog);
    build_force_flat_orders_unlocked(candidate_codes, snaps, force_flat_orders_out, escalate, buffer_bps);
}

void submit_force_flat_request(ITradeGateway* td,
                               xt_runtime::order::XtOrderRuntimeBook* book,
                               const xt_core::XtOrder& req,
                               int64_t max_order_qty_fp,
                               size_t& submit_cnt) {
    if (!book) return;
    int64_t remaining_fp = req.qty_fp;
    while (remaining_fp > 0) {
        const int64_t send_fp = (max_order_qty_fp > 0)
                                    ? std::min(remaining_fp, max_order_qty_fp)
                                    : remaining_fp;
        if (send_fp <= 0) break;

        xt_identity::OrderAttributionStore::set_thread_submit_strategy_sid(StrategySid::legacy());
        const uint64_t oid = td->submit_order(req.code,
                                              req.is_buy,
                                              req.price_fp,
                                              send_fp,
                                              req.tif,
                                              "runner_force_flat",
                                              req.close_offset,
                                              exec_none());
        xt_identity::OrderAttributionStore::set_thread_submit_strategy_sid(StrategySid::legacy());
        if (oid == 0) {
            XtLogger::warn_m("RT",
                             "closeout force-flat submit failed code={} side={} send_fp={} "
                             "remaining_fp={} offset={}",
                             req.code,
                             req.is_buy ? "buy" : "sell",
                             send_fp,
                             remaining_fp,
                             static_cast<int>(req.close_offset));
            break;
        }

        book->record_force_flat_order(oid, req.code, req.is_buy, send_fp,
                                      req.close_offset, StrategySid::legacy());
        ++submit_cnt;
        remaining_fp -= send_fp;
    }
}

}

bool closeout_should_escalate(int64_t escalate_after_ms,
                              int64_t force_active_since_ms,
                              int64_t now_ms) noexcept {
    if (escalate_after_ms <= 0) return true;
    if (force_active_since_ms == 0) return false;
    return now_ms - force_active_since_ms >= escalate_after_ms;
}

ForceCloseResult try_runner_force_close(XtHftRunnerControlContext& ctx,
                                        int64_t& closeout_last_try_ms,
                                        int64_t& closeout_force_active_since_ms) {
    const XtLiveOrderGatewayCfg* cfg = ctx.order_gateway_cfg;
    std::vector<int> force_list;
    const auto gate_in = build_closeout_gate_input(cfg, ctx, closeout_last_try_ms, force_list);

    if (auto gated = decide_closeout_gate(gate_in); gated.has_value()) {

        if (*gated == ForceCloseResult::kNotNeeded || *gated == ForceCloseResult::kSessionGated) {
            closeout_force_active_since_ms = 0;
        }
        return *gated;
    }

    closeout_last_try_ms = gate_in.tnow_ms;
    ITradeGateway* td = load_td_for_runner_force_close(ctx.td_gateway,
                                                       ctx.runtime_shared_state.trading_ready);
    if (!td) return ForceCloseResult::kGatewayDown;

    const int  buffer_bps = cfg ? cfg->closeout_aggressive_buffer_bps : 10;
    const bool escalate   = closeout_should_escalate(
        cfg ? cfg->closeout_escalate_after_ms : 0, closeout_force_active_since_ms, gate_in.tnow_ms);

    std::vector<uint64_t> cancel_ids;
    std::vector<xt_core::XtOrder> force_flat_orders;
    collect_closeout_phases(ctx, cancel_ids, force_flat_orders, escalate, buffer_bps);

    if (!cancel_ids.empty()) {
        for (uint64_t id : cancel_ids) {
            td->cancel_order(id);
        }
        XtLogger::warn_m("RT", "closeout cancel outstanding orders count={}", cancel_ids.size());
        return ForceCloseResult::kCancelledFirst;
    }

    if (force_flat_orders.empty()) {
        closeout_force_active_since_ms = 0;
        return ForceCloseResult::kExecuted;
    }
    if (closeout_force_active_since_ms == 0) {
        closeout_force_active_since_ms = gate_in.tnow_ms;
    }

    size_t submit_cnt = 0;
    for (const auto& r : force_flat_orders) {
        submit_force_flat_request(td, ctx.order_book, r, xt_fp::to_fp(cfg->risk.max_order_qty),
                                  submit_cnt);
    }
    XtLogger::warn_m("RT", "closeout force-flat req_count={} submit_count={} mode={}",
                     force_flat_orders.size(), submit_cnt,
                     escalate ? "ioc_aggressive" : "gtc_mark");
    return ForceCloseResult::kExecuted;
}

std::optional<ForceCloseResult> decide_closeout_gate(const CloseoutGateInput& in) {
    if (!in.cfg_enabled || !in.has_windows) return ForceCloseResult::kNotNeeded;

    if (in.session_gate_enabled && in.session_gate_sections && !in.session_gate_sections->empty()) {
        const bool local_in =
            xt_core::in_hhmm_sections_halfopen(in.local_hhmm, *in.session_gate_sections);
        const bool tick_in =
            xt_core::in_hhmm_sections_halfopen(in.tick_hhmm, *in.session_gate_sections);
        if (!local_in && !tick_in) {
            return ForceCloseResult::kSessionGated;
        }
    }

    bool force_phase = false;
    if (in.force_hhmm_list) {
        const std::vector<std::pair<int, int>> empty_sections;
        const auto& sec_ref = in.session_gate_sections ? *in.session_gate_sections : empty_sections;
        for (int force_hhmm : *in.force_hhmm_list) {
            if (xt_core::hhmm_reaches_mark_in_sections(in.local_hhmm, force_hhmm, sec_ref) ||
                xt_core::hhmm_reaches_mark_in_sections(in.tick_hhmm, force_hhmm, sec_ref)) {
                force_phase = true;
                break;
            }
        }
    }
    if (!force_phase) return ForceCloseResult::kNotNeeded;

    if (in.closeout_last_try_ms != 0 && in.closeout_retry_ms != 0 &&
        (in.tnow_ms - in.closeout_last_try_ms < in.closeout_retry_ms)) {
        return ForceCloseResult::kThrottled;
    }

    if (!in.td_available) return ForceCloseResult::kGatewayDown;

    return std::nullopt;
}

bool closeout_result_trips_kill_switch(ForceCloseResult r) noexcept {

    return r == ForceCloseResult::kRejectedAmbiguousScope;
}

bool closeout_latch_auto_release_decision(bool kill_switch_active,
                                          uint8_t source,
                                          bool account_hydrated,
                                          bool account_flat) noexcept {
    if (!kill_switch_active) return false;

    if (!kill_switch_source_auto_releasable(source)) return false;
    if (!account_hydrated) return false;
    return account_flat;
}

bool kill_switch_source_auto_releasable(uint8_t source) noexcept {

    return source == XtRunnerSignals::kKillSwitchSourceAutoCloseoutBlock ||
           source == XtRunnerSignals::kKillSwitchSourceUnmatchedBudget;
}

bool closeout_boundary_trap(const std::vector<int>& force_hhmm_list,
                            const std::vector<std::pair<int, int>>& sections) {
    if (sections.empty()) return false;
    for (int force_hhmm : force_hhmm_list) {
        if (!xt_hhmm::valid_hhmm(force_hhmm)) continue;
        if (!xt_core::in_hhmm_sections_halfopen(force_hhmm, sections)) {
            return true;
        }
    }
    return false;
}

