// TraderCTP recovery 分片：登录后查询超时守卫、recovery pipeline entry 与 reverse-sync。
// 本文件只编排 readiness gate 前的 account/position/order/trade 查询结果和 alive in-flight 灌入。
// query 发送和 retry 控制在 TraderCTPQuery.cpp；订单/成交行的 CTP 解析在 TraderCTP.cpp/OrderOps 中完成。
// timeout token 使用 per-kind 原子序号失效过期回调，保证 stop/reset/recovery restart 后不会消费过期 timer。

#include "ctp/td/TraderCTP.h"

#include "ctp/common/CTPUtil.h"
#include "ctp/recovery/CtpRecoveryRearm.h"
#include "xt_live/gateway/common/ReverseSyncHelper.h"
#include "XtLogger.h"
#include "common/XtLiveRecoveryError.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

#include <algorithm>
#include <cstdio>

namespace {

const char* rc(XtLiveRecoveryError code) { return live_recovery_error_str(code); }

}

void TraderCTP::clear_recovery_query_timeout(QueryKind kind) {
    auto& q = qry_[qry_idx(kind)];
    q.timeout_token.fetch_add(1, std::memory_order_acq_rel);
    q.timeout_count.store(0, std::memory_order_release);
}

void TraderCTP::clear_recovery_query_timeout_states() {
    for (auto& q : qry_) {
        q.timeout_token.fetch_add(1, std::memory_order_acq_rel);
        q.timeout_count.store(0, std::memory_order_release);
    }
}

void TraderCTP::set_recovery_wedged_(bool wedged) {
    recovery_wedged_.store(wedged, std::memory_order_release);
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().set_td_recovery_wedged("ctp", wedged);
#endif
}

void TraderCTP::reset_query_ctrl_for_rearm_(QueryKind kind) {

    auto& q = qry_[qry_idx(kind)];
    q.inflight.store(false, std::memory_order_release);

    q.active_request_id.store(kQueryRequestIdInvalidated, std::memory_order_release);

    query_session_gen_.fetch_add(1, std::memory_order_acq_rel);
    q.dirty.store(false, std::memory_order_release);
    q.next_allowed_ms.store(0, std::memory_order_release);
    q.backoff_level.store(0, std::memory_order_release);
    q.retry_pending.store(false, std::memory_order_release);
    clear_recovery_query_timeout(kind);
}

void TraderCTP::maybe_schedule_recovery_rearm_(QueryKind kind) {
    if (recovery_rearm_backoff_ms_ <= 0) return;
    if (stopping_.load(std::memory_order_acquire)) return;
    if (!connected_.load(std::memory_order_acquire)) return;
    if (!recovery_.is_recovering()) return;

    int      round;
    int      backoff_ms;
    uint64_t token;
    {
        std::lock_guard<std::mutex> lk(recovery_rearm_mu_);
        if (recovery_rearm_pending_) return;
        recovery_rearm_pending_ = true;
        round = ++recovery_rearm_round_;
        const auto d = ctp_recovery::recovery_rearm_decision(
            round, recovery_rearm_alert_rounds_, recovery_rearm_backoff_ms_,
            ctp_recovery::kRecoveryRearmBackoffMaxMs);
        backoff_ms = d.backoff_ms;
        token      = recovery_rearm_token_.load(std::memory_order_acquire);
        if (d.raise_alert) set_recovery_wedged_(true);
        recovery_rearm_scheduled_total_.fetch_add(1, std::memory_order_acq_rel);
    }
    const QueryMeta meta = query_meta(kind);
    XtLogger::warn_m("TD",
                     "event=recovery_rearm_scheduled stage={} round={} alert_rounds={} backoff_ms={}",
                     meta.name, round, recovery_rearm_alert_rounds_, backoff_ms);

    schedule_after_ms(backoff_ms, [this, kind, token]() {
        if (stopping_.load(std::memory_order_acquire)) return;
        {
            std::lock_guard<std::mutex> lk(recovery_rearm_mu_);

            if (recovery_rearm_token_.load(std::memory_order_acquire) != token) return;
            recovery_rearm_pending_ = false;
        }
        if (stopping_.load(std::memory_order_acquire)) return;
        if (!connected_.load(std::memory_order_acquire)) return;
        if (!recovery_.is_recovering()) return;
        const QueryMeta m = query_meta(kind);
        XtLogger::warn_m("TD", "event=recovery_rearm_fire stage={} re-issuing query", m.name);
        reset_query_ctrl_for_rearm_(kind);
        query_once(kind);
    });
}

void TraderCTP::arm_recovery_query_timeout(QueryKind kind) {
    if (recovery_query_timeout_ms_ <= 0) return;
    if (!recovery_.is_recovering()) return;

    auto&          q         = qry_[qry_idx(kind)];
    const uint64_t token     = q.timeout_token.fetch_add(1, std::memory_order_acq_rel) + 1;
    const int      timeout_ms = recovery_query_timeout_ms_;

    schedule_after_ms(timeout_ms, [this, kind, token, timeout_ms]() {
        if (!is_session_alive()) return;
        if (!recovery_.is_recovering()) return;
        auto& q = qry_[qry_idx(kind)];
        if (q.timeout_token.load(std::memory_order_acquire) != token) return;
        const int timeout_hit = q.timeout_count.fetch_add(1, std::memory_order_acq_rel) + 1;
        const QueryMeta meta  = query_meta(kind);

        if (kind == QueryKind::Position || kind == QueryKind::AccountState) {
            if (timeout_hit >= recovery_query_max_timeouts_) {
                XtLogger::error_m("TD",
                                 "event=recovery_query_timeout stage={} reason_code={} "
                                 "timeout_ms={} hit={}/{} fail closed",
                                 meta.name, rc(XtLiveRecoveryError::RecoveryQueryTimeout),
                                 timeout_ms, timeout_hit, recovery_query_max_timeouts_);
                if (kind == QueryKind::Position) {
                    readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Position,
                                         "ctp position query timeout");
                } else {
                    readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                                         "ctp account query timeout");
                }
                maybe_schedule_recovery_rearm_(kind);
                clear_recovery_query_timeout(kind);
                return;
            }
            XtLogger::warn_m("TD",
                             "event=recovery_query_timeout stage={} reason_code={} "
                             "timeout_ms={} hit={}/{} retry query",
                             meta.name, rc(XtLiveRecoveryError::RecoveryQueryTimeout),
                             timeout_ms, timeout_hit, recovery_query_max_timeouts_);
            const uint64_t prev = q.timeout_token.load(std::memory_order_acquire);
            query_once(kind);

            if (q.timeout_token.load(std::memory_order_acquire) == prev) {
                arm_recovery_query_timeout(kind);
            }
            return;
        }

        XtLogger::error_m("TD",
                         "event=recovery_query_timeout stage={} reason_code={} "
                         "timeout_ms={} hit={} fail closed",
                         meta.name, rc(XtLiveRecoveryError::RecoveryQueryTimeout),
                         timeout_ms, timeout_hit);
        if (kind == QueryKind::XtOrder) {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::OpenOrders,
                                 "ctp order query timeout");
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                                 "ctp order query timeout before reverse-sync");
        } else if (kind == QueryKind::Trade) {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                                 "ctp trade query timeout before recovery ready");
        }

        maybe_schedule_recovery_rearm_(kind);
        clear_recovery_query_timeout(kind);
    });
}

void TraderCTP::enter_recovery_pipeline(bool settlement_confirmed) {

    if (identity_mismatch_latched_.load(std::memory_order_acquire)) {
        XtLogger::error_m("TD",
                          "enter_recovery_pipeline blocked: identity mismatch latched "
                          "(expected_investor_id={})", expected_investor_id_);
        return;
    }
    settlement_confirmed_ = settlement_confirmed;
    clear_recovery_query_timeout_states();
    reset_query_control_states();

    {
        std::lock_guard<std::mutex> lk(recovery_alive_mu_);
        recovery_alive_orders_.clear();
        recovery_alive_routing_.clear();
    }

    {
        std::lock_guard<std::mutex> lk(recovery_staged_pos_mu_);
        recovery_staged_positions_.clear();
        recovery_staged_pos_valid_ = false;
    }
    recovery_position_anchor_us_.store(0, std::memory_order_release);
    recovery_skipped_foreign_ = 0;

    if (recovery_.recovery_enabled()) {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        recovery_window_trades_.clear();
        recovery_window_gate_closed_ = true;
        recovery_window_overflow_ = 0;
    }
    set_session_phase(SessionPhase::Recovering);
    recovery_.on_login();
}

void TraderCTP::rebuild_alive_inflight_and_emit(bool is_cold_start) {
    std::vector<AliveInflightOrder> orders;
    std::unordered_map<uint64_t, RecoveredCancelRouting> routing;
    uint64_t skipped_foreign = 0;
    {
        std::lock_guard<std::mutex> lk(recovery_alive_mu_);
        orders.swap(recovery_alive_orders_);
        routing.swap(recovery_alive_routing_);
    }
    skipped_foreign = recovery_skipped_foreign_;
    recovery_skipped_foreign_ = 0;

    for (const auto& a : orders) {
        char ref_buf[24];
        std::snprintf(ref_buf, sizeof(ref_buf), "%llu",
                      static_cast<unsigned long long>(a.local_id));
        if (!state_.has_local_order_ref(ref_buf)) {
            const bool is_close = (a.close_offset != XtOffset::Open);
            const bool is_today = (a.close_offset == XtOffset::CloseToday);
            const double price = order_exec_from_fp(a.price_fp);

            state_.register_order(a.local_id, ref_buf, a.code.c_str(),
                                  a.is_buy, price, a.total_qty_fp,
                                    "",
                                  is_close, is_today,
                                    0,
                                    0,
                                    a.filled_qty_fp);
        }

        const auto rit = routing.find(a.local_id);
        if (rit != routing.end()) {
            state_.backfill_cancel_routing(a.local_id,
                                           rit->second.exchange_id,
                                           rit->second.order_sys_id,
                                           rit->second.front_id,
                                           rit->second.session_id);
        }
    }

#ifdef XT_HAVE_METRICS
    for (uint64_t i = 0; i < skipped_foreign; ++i) {
        ct::metrics::XtMetricRegistry::instance()
            .inc_recovery_rebuild_alive_skipped_foreign_total("ctp");
    }
#endif

    XtLogger::info_m("TD",
                     "event=recovery_rebuild_alive_emit venue=ctp orders={} skipped_foreign={} is_cold_start={}",
                     orders.size(), skipped_foreign, is_cold_start ? 1 : 0);

    if (!callbacks_.on_reverse_sync_batch) {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "ctp reverse-sync handler not bound");
        return;
    }
    xyybtrader::recovery::emit_reverse_sync_batch_with_metric(
        std::move(orders), is_cold_start, "ctp", callbacks_.on_reverse_sync_batch);
    readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::ReverseSync,
                            "ctp recovery alive-order batch emitted");

}

void TraderCTP::stage_recovery_positions_(std::vector<PositionSnapshot> out) {

    recovery_position_anchor_us_.store(wall_now_epoch_us_(), std::memory_order_release);
    std::lock_guard<std::mutex> lk(recovery_staged_pos_mu_);
    recovery_staged_positions_ = std::move(out);
    recovery_staged_pos_valid_ = true;
}

void TraderCTP::emit_staged_recovery_positions_(bool is_cold_start) {
    std::vector<PositionSnapshot> out;
    bool valid = false;
    {
        std::lock_guard<std::mutex> lk(recovery_staged_pos_mu_);
        valid = recovery_staged_pos_valid_;
        out.swap(recovery_staged_positions_);
        recovery_staged_pos_valid_ = false;
    }
    if (!valid) return;

    if (callbacks_.on_recovery_position) {
        callbacks_.on_recovery_position(out, is_cold_start);
    } else if (callbacks_.on_position) {
        callbacks_.on_position(out);
    }
}

void TraderCTP::reopen_recovery_window_gate_and_drain_() {

    std::vector<XtTradeRecord> residual;
    {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        recovery_window_gate_closed_ = false;
        residual.swap(recovery_window_trades_);
    }
    if (!callbacks_.on_trade) return;

    for (auto& tr : residual) callbacks_.on_trade(tr, XtLatencyEligibility::kIneligible);
}

void TraderCTP::emit_recovery_finish_sequence_(bool is_cold_start) {

    std::vector<XtTradeRecord> pending;
    uint64_t overflow = 0;
    {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        pending.swap(recovery_window_trades_);
        overflow = recovery_window_overflow_;
        recovery_window_overflow_ = 0;
    }
    if (!callbacks_.on_reverse_sync_batch) {

        emit_staged_recovery_positions_(is_cold_start);
        rebuild_alive_inflight_and_emit(is_cold_start);
        {
            std::lock_guard<std::mutex> lk(recovery_window_mu_);
            recovery_window_gate_closed_ = false;

            recovery_window_trades_.clear();
        }
        if (!pending.empty() || overflow > 0) {
            XtLogger::error_m("TD",
                "event=recovery_window_trades_dropped reason=reverse_sync_not_ready "
                "dropped={} overflow={}",
                pending.size(), overflow);
        }
        return;
    }

    const int64_t anchor =
        recovery_position_anchor_us_.load(std::memory_order_acquire);
    std::vector<XtTradeRecord> pre;
    std::vector<XtTradeRecord> post;
    pre.reserve(pending.size());
    for (auto& tr : pending) {
        const auto fill_us = static_cast<int64_t>(tr.fill_ts_us);
        if (anchor > 0 && fill_us > anchor + kFillSealMarginUs) {
            post.push_back(std::move(tr));
        } else {
            pre.push_back(std::move(tr));
        }
    }

    std::stable_sort(pre.begin(), pre.end(),
                     [](const XtTradeRecord& a, const XtTradeRecord& b) {
                         return a.fill_ts_us < b.fill_ts_us;
                     });

    if (callbacks_.on_trade) {
        for (auto& tr : pre) callbacks_.on_trade(tr, XtLatencyEligibility::kIneligible);
    }
    emit_staged_recovery_positions_(is_cold_start);
    rebuild_alive_inflight_and_emit(is_cold_start);
    if (callbacks_.on_trade) {
        for (auto& tr : post) callbacks_.on_trade(tr, XtLatencyEligibility::kIneligible);
    }
    reopen_recovery_window_gate_and_drain_();
    flush_recovery_pending_terminals_();
    if (!pre.empty() || !post.empty() || overflow > 0) {
        XtLogger::info_m("TD",
            "event=recovery_window_trades_flush pre={} post={} dropped_overflow={}",
            pre.size(), post.size(), overflow);
    }
}

void TraderCTP::flush_recovery_pending_terminals_() {
    std::unordered_map<uint64_t, RecoveryPendingTerminal> pending;
    {
        std::lock_guard<std::mutex> lk(recovery_term_mu_);
        pending.swap(recovery_pending_terminals_);
    }
    if (pending.empty()) return;
    for (auto& [lid, entry] : pending) {

        XtLogger::warn_m("TD",
                         "event=td_recovery_terminal_flush local_id={} status={}",
                         lid, static_cast<int>(entry.upd.status));
        if (callbacks_.on_order_update)
            callbacks_.on_order_update(entry.upd, entry.elig);
    }
}
