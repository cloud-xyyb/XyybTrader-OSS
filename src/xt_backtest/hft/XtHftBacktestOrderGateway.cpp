#include "XtHftBacktestOrderGateway.h"

#include <algorithm>
#include <cassert>
#include <limits>

#include "XtLogger.h"
#include "XtFixedPoint.h"
#include "xt_util/time/XtTime.h"
#include "xt_engine/sim/XtLatencyArith.h"
#include "xt_core/order/XtOrderTypes.h"
#include "CloseOffsetCore.h"
#include "xt_core/order/XtOffsetRouter.h"
#include "xt_risk/engine/RiskActionDispatch.h"
#include "api/IRejectSink.h"
#include "backtest/XtBacktestRiskGuardContext.h"
#include "xt_risk/engine/AnnualLatchArm.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_account/state/AccountEvent.h"
#include "xt_core/order/XtOrderLifecycle.h"
#include "xt_backtest/common/XtBtTimeEncode.h"
#include "xt_runtime/market/XtRuntimeMarketCache.h"

using xt_runtime::SubmitResult;

static XtRiskResult make_reject(XtRiskReasonCode code, XtRiskStage stage, const char* text = nullptr) {
    XtRiskResult out;
    out.passed = false;
    out.reason_code = code;
    out.stage = stage;
    out.reason_text = (text && text[0] != '\0') ? text : risk_reason_text(code);
    return out;
}

void XtHftBacktestOrderGateway::init(InitParams p) {
    match_engine_ = XtHftMatchEngine(p.match);
    max_order_qty_fp_ = p.max_order_qty_fp;
    price_limit_enabled_ = p.match.price_limit_enabled;
    self_match_check_ = p.self_match_check;
    margin_call_enabled_ = p.margin_call_enabled;
    maintenance_ratio_fp_ = xt_fp::to_fp(p.maintenance_ratio);
    in_liquidation_ = false;
    margin_call_count_ = 0;
    daily_loss_check_enabled_ = p.risk.max_daily_loss > 0.0;
    risk_max_order_qty_fp_ = p.risk.max_order_qty > 0.0 ? xt_fp::to_fp(p.risk.max_order_qty) : 0;

    obs_max_pos_qty_fp_ = p.risk.max_pos_qty > 0.0 ? xt_fp::to_fp(p.risk.max_pos_qty) : 0;
    risk_obs_ = {};
    fill_net_fp_.clear();
    breach_state_.clear();

    account_     = p.account;
    commodities_ = p.catalog;
    order_book_  = p.order_book;

    fee_book_ = XtFeeRuleBook(p.fees);
    risk_engine_  = std::make_unique<XtRiskEngine>(
        p.risk, XtRiskEngine::InitDeps{&fee_book_, commodities_});
    risk_policy_  = p.risk_policy;
    risk_gw_.bind(*risk_engine_, risk_policy_);
    cost_applier_.init(std::move(p.cost), &fee_book_, commodities_);
    account_view_ = std::make_unique<xt_account::AccountStoreView>(*account_);

    match_engine_.reset_volume_cache();

    order_book_->reset_for_recovery({},  true);

    tick_idx_     = 0;
    last_date_    = 0;
    last_time_ms_ = 0;
    last_event_ns_ = 0;

    event_forward_spike_count_ = 0;

    eof_closing_ = false;
    delivered_proj_.clear();

    trades_buf_.reserve(8);
    updates_buf_.reserve(16);

    initialized_ = true;
}

void XtHftBacktestOrderGateway::notify_order_terminal(uint64_t order_id) {
    if (!self_match_check_) return;

    stp_checker_.on_order_terminal(order_id);
}

xt_runtime::IAccountView* XtHftBacktestOrderGateway::account_view() {
    return account_view_.get();
}

xt_runtime::order::XtOrderRuntimeBook* XtHftBacktestOrderGateway::order_book() {
    return order_book_;
}

const xt_runtime::order::XtOrderRuntimeBook* XtHftBacktestOrderGateway::order_book() const {
    return order_book_;
}

const XtCommodityCatalog* XtHftBacktestOrderGateway::commodity_catalog() const {
    return commodities_;
}

int64_t XtHftBacktestOrderGateway::get_cash_fp() const {
    return account_ ? account_->snapshot_account().cash : 0;
}

int64_t XtHftBacktestOrderGateway::get_equity_fp() const {
    return account_ ? account_->snapshot_account().equity : 0;
}

int64_t XtHftBacktestOrderGateway::get_unrealized_fp() const {
    return account_ ? account_->snapshot_account().unrealized_pnl : 0;
}

int64_t XtHftBacktestOrderGateway::get_frozen_fp() const {
    return account_ ? account_->snapshot_account().frozen_cash : 0;
}

size_t XtHftBacktestOrderGateway::live_orders() const {
    return match_engine_.live_orders();
}

int64_t XtHftBacktestOrderGateway::current_ts() const {

    return xt_time::packed_ts(last_action_date_ != 0 ? last_action_date_ : last_date_,
                              last_time_ms_);
}

void XtHftBacktestOrderGateway::advance_clock(uint32_t action_date, uint32_t action_time_ms,
                                              uint32_t trading_date) {
    ++tick_idx_;

    last_date_        = trading_date;
    last_time_ms_     = action_time_ms;
    last_action_date_ = (action_date != 0) ? action_date : trading_date;

    const int64_t event_ns = xt_time::to_ns(last_action_date_, action_time_ms);
    note_event_forward_spike_(event_ns);
    if (event_ns > last_event_ns_) last_event_ns_ = event_ns;

    match_engine_.advance_clock(tick_idx_, event_ns, trading_date, action_time_ms);
}

void XtHftBacktestOrderGateway::advance_clock_only(uint32_t action_date, uint32_t action_time_ms,
                                                   uint32_t trading_date) {

    last_date_        = trading_date;
    last_time_ms_     = action_time_ms;
    last_action_date_ = (action_date != 0) ? action_date : trading_date;
    const int64_t event_ns = xt_time::to_ns(last_action_date_, action_time_ms);
    note_event_forward_spike_(event_ns);
    if (event_ns > last_event_ns_) last_event_ns_ = event_ns;
    match_engine_.advance_clock_to(event_ns, trading_date, action_time_ms);

    release_due_updates_(last_event_ns_);
    match_engine_.drain_updates(updates_buf_);
}

void XtHftBacktestOrderGateway::release_due_updates_(int64_t now_ns) {

    if (pending_updates_.empty()) return;
    size_t w = 0;
    for (size_t i = 0; i < pending_updates_.size(); ++i) {
        if (pending_updates_[i].visible_at_ns <= now_ns) {
            updates_buf_.push_back(std::move(pending_updates_[i].update));
        } else {
            if (w != i) pending_updates_[w] = std::move(pending_updates_[i]);
            ++w;
        }
    }
    pending_updates_.resize(w);
}

namespace {

constexpr int64_t kEventForwardSpikeNs = 366LL * 86400LL * 1'000'000'000LL;
}

void XtHftBacktestOrderGateway::note_event_forward_spike_(int64_t event_ns) noexcept {
    if (last_event_ns_ == 0) return;
    if (event_ns <= last_event_ns_) return;

    if (static_cast<__int128>(event_ns) - static_cast<__int128>(last_event_ns_)
        > kEventForwardSpikeNs) {
        if (event_forward_spike_count_++ == 0) {
            XtLogger::warn_m("BT",
                "event clock forward spike: event_ns={} last_event_ns_={} "
                "(jump > 366d, 疑脏未来日期 tick 毒化 event 轴; TTL/ACK 立即到期 + "
                "fill 滞留 EOF); 后续同类只计数不重复告警",
                event_ns, last_event_ns_);
        }
    }
}

void XtHftBacktestOrderGateway::set_dispatch_clock(uint32_t date, uint32_t time_ms) {

    pending_dispatch_date_    = date;
    pending_dispatch_time_ms_ = time_ms;
}

void XtHftBacktestOrderGateway::process_tick(const XtTick& tick) {
    trades_buf_.clear();
    if (pending_dispatch_date_ != 0) {
        match_engine_.set_dispatch_clock(pending_dispatch_date_, pending_dispatch_time_ms_);
        pending_dispatch_date_    = 0;
        pending_dispatch_time_ms_ = 0;
    }
    match_engine_.on_tick(tick, trades_buf_);
    cost_applier_.apply(trades_buf_);

    const bool use_model_response = match_engine_.has_latency_model();

    thread_local std::vector<std::pair<uint64_t, int64_t>> fill_visible;
    fill_visible.clear();
    for (auto& tr : trades_buf_) {

        const int64_t resp_ns  = (use_model_response && tr.sim_response_latency_ns >= 0)
                                     ? tr.sim_response_latency_ns
                                     : fill_latency_ns_;
        const int64_t visible_at_ns = xt_latency::checked_add_event_ns(last_event_ns_, resp_ns);
        bool found = false;
        for (auto& [oid, vis] : fill_visible) {
            if (oid == tr.order_id) {
                if (visible_at_ns > vis) vis = visible_at_ns;
                found = true;
                break;
            }
        }
        if (!found) fill_visible.emplace_back(tr.order_id, visible_at_ns);

        tr.sim_visible_at_ns = visible_at_ns;
        pending_fills_.push_back({std::move(tr), visible_at_ns});
    }
    trades_buf_.clear();

    updates_buf_.clear();

    release_due_updates_(last_event_ns_);
    const size_t new_upd_begin = updates_buf_.size();
    match_engine_.drain_updates(updates_buf_);

    if (!fill_visible.empty() && updates_buf_.size() > new_upd_begin) {
        const int64_t now_ns = last_event_ns_;
        size_t w = new_upd_begin;
        for (size_t i = new_upd_begin; i < updates_buf_.size(); ++i) {
            XtOrderExecUpdate& upd = updates_buf_[i];

            int64_t vis = -1;
            const bool defer_status =
                upd.status == XtOrderStatus::Filled ||
                upd.status == XtOrderStatus::PartiallyFilled ||
                order_status_is_final(upd.status);
            if (defer_status) {
                for (const auto& [oid, v] : fill_visible) {
                    if (oid == upd.id) { vis = v; break; }
                }
            }
            if (vis > now_ns) {
                pending_updates_.push_back({std::move(upd), vis});
            } else {
                if (w != i) updates_buf_[w] = std::move(updates_buf_[i]);
                ++w;
            }
        }
        updates_buf_.resize(w);
    }

    if (margin_call_enabled_) check_margin_call_();
}

size_t XtHftBacktestOrderGateway::drain_fills(uint32_t date, uint32_t time_ms,
                                              std::vector<XtTradeRecord>& out) {
    if (pending_fills_.empty()) return 0;

    const int64_t now_ns = xt_time::to_ns(date, time_ms);
    const size_t before = out.size();
    {
        size_t w = 0;
        for (size_t i = 0; i < pending_fills_.size(); ++i) {
            if (pending_fills_[i].visible_at_ns <= now_ns) {
                out.push_back(std::move(pending_fills_[i].trade));
            } else {
                if (w != i) pending_fills_[w] = std::move(pending_fills_[i]);
                ++w;
            }
        }
        pending_fills_.resize(w);
    }
    const size_t drained = out.size() - before;

    for (size_t i = before; i < out.size(); ++i) {
        XtTradeRecord& tr = out[i];
        const auto it = order_strategy_.find(tr.order_id);
        if (it != order_strategy_.end()) {
            if (tr.strategy_id.empty()) tr.strategy_id = it->second.sid;
            if (tr.sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
                tr.sid_id = it->second.sid_id;
            }
        }

        if (auto dp = delivered_proj_.find(tr.order_id); dp != delivered_proj_.end()) {
            dp->second.delivered_filled_fp += tr.qty_fp;

            if (dp->second.terminal_delivered &&
                dp->second.delivered_filled_fp >= dp->second.claimed_filled_fp) {
                delivered_proj_.erase(dp);
            }
        }
        note_fill_for_risk_obs_(tr);
        for (auto* obs : observers_) {
            obs->on_trade(tr);
        }
    }
    return drained;
}

void XtHftBacktestOrderGateway::note_fill_for_risk_obs_(const XtTradeRecord& tr) {
    int64_t& net = fill_net_fp_[tr.code];
    net += tr.is_buy ? tr.qty_fp : -tr.qty_fp;
    const int64_t abs_net = net < 0 ? -net : net;
    if (abs_net > risk_obs_.peak_abs_position_fp) {
        risk_obs_.peak_abs_position_fp = abs_net;
    }
    if (obs_max_pos_qty_fp_ <= 0) return;
    bool& in_breach = breach_state_[tr.code];
    if (abs_net > obs_max_pos_qty_fp_) {
        if (!in_breach) {
            in_breach = true;
            ++risk_obs_.position_breach_count;
            XtLogger::warn_m("BT_RISK_BREACH",
                "post-fill |net position| exceeds max_pos_qty: code={} net_fp={} "
                "cap_fp={} (breach #{}) — pre-trade max_pos_qty 为 confirmed 净仓"
                "语义(resting 不计入), 检查策略敞口口径(strategy_sdk_guide §12)",
                tr.code, net, obs_max_pos_qty_fp_,
                risk_obs_.position_breach_count);
        }
    } else {
        in_breach = false;
    }
}

size_t XtHftBacktestOrderGateway::drop_pending_fills() {

    const size_t dropped = pending_fills_.size();
    pending_fills_.clear();
    pending_updates_.clear();
    return dropped;
}

std::size_t XtHftBacktestOrderGateway::synthesize_eof_delivered_cancels() {
    if (delivered_proj_.empty()) return 0;

    const uint64_t cb_ns = static_cast<uint64_t>(last_event_ns_ < 0 ? 0 : last_event_ns_);
    std::size_t n = 0;
    for (const auto& [id, st] : delivered_proj_) {

        assert(!st.terminal_delivered &&
               "terminal_delivered projection must be completed+erased before EOF synthesis");
        if (st.terminal_delivered) continue;
        XtOrderExecUpdate u;
        u.id           = id;
        u.is_buy       = st.is_buy;
        u.close_offset = st.close_offset;
        u.status       = XtOrderStatus::Canceled;
        u.price_fp     = st.price_fp;
        u.qty_fp       = st.qty_fp;
        u.filled_fp    = st.delivered_filled_fp;
        u.leaves_fp    = st.qty_fp - st.delivered_filled_fp;
        u.last_fill_fp = 0;
        u.local_cb_ns  = cb_ns;

        updates_buf_.push_back(u);
        ++n;
    }

    match_engine_.discard_eof_live_orders();

    return n;
}

std::size_t XtHftBacktestOrderGateway::complete_eof_delivered_terminal_fills(
        std::vector<XtTradeRecord>& out) {
    if (delivered_proj_.empty() || pending_fills_.empty()) return 0;
    const size_t before = out.size();

    {
        size_t w = 0;
        for (size_t i = 0; i < pending_fills_.size(); ++i) {
            const uint64_t oid = pending_fills_[i].trade.order_id;
            const auto dp = delivered_proj_.find(oid);
            if (dp != delivered_proj_.end() && dp->second.terminal_delivered) {
                out.push_back(std::move(pending_fills_[i].trade));
            } else {
                if (w != i) pending_fills_[w] = std::move(pending_fills_[i]);
                ++w;
            }
        }
        pending_fills_.resize(w);
    }
    const size_t completed = out.size() - before;

    for (size_t i = before; i < out.size(); ++i) {
        XtTradeRecord& tr = out[i];
        const auto it = order_strategy_.find(tr.order_id);
        if (it != order_strategy_.end()) {
            if (tr.strategy_id.empty()) tr.strategy_id = it->second.sid;
            if (tr.sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
                tr.sid_id = it->second.sid_id;
            }
        }
        note_fill_for_risk_obs_(tr);
        for (auto* obs : observers_) {
            obs->on_trade(tr);
        }
    }

    for (auto it = delivered_proj_.begin(); it != delivered_proj_.end();) {
        if (it->second.terminal_delivered) it = delivered_proj_.erase(it);
        else ++it;
    }
    return completed;
}

void XtHftBacktestOrderGateway::on_session_begin(uint32_t trading_date) {
    if (account_) account_->apply(XtAccountEvent::session_begin());
    if (account_) {

        account_->on_session_begin_hook(get_equity_fp());
        session_begin_realized_fp_ = account_->snapshot_account().realized_pnl;
    }
    if (risk_engine_) {

        xt_risk::arm_annual_loss_on_session_begin(
            *risk_engine_, trading_date, get_equity_fp(),
            [this](int64_t& realized_fp, int64_t& fees_fp, int64_t& unrealized_fp) {
                if (!account_) return;
                const auto a = account_->snapshot_account();
                realized_fp   = a.realized_pnl;
                fees_fp       = a.fees;
                unrealized_fp = a.unrealized_pnl;
            });
        risk_engine_->on_session_begin(trading_date);
    }

    if (current_trading_date_ != 0 && current_trading_date_ != trading_date) {
        std::size_t pruned = 0;
        for (auto it = order_strategy_.begin(); it != order_strategy_.end(); ) {
            if (it->second.terminal_at_date != 0
                && it->second.terminal_at_date < trading_date) {
                it = order_strategy_.erase(it);
                ++pruned;
            } else {
                ++it;
            }
        }
        if (pruned > 0) {
            XtLogger::info_m("BT", "prune order_strategy_ sidecar: removed={} remaining={} (cross trading_date {} → {})",
                              pruned, order_strategy_.size(),
                              current_trading_date_, trading_date);
        }
    }
    current_trading_date_ = trading_date;
}

size_t XtHftBacktestOrderGateway::drain_order_updates(std::vector<XtOrderExecUpdate>& out) {
    if (updates_buf_.empty()) return 0;
    const size_t before = out.size();

    for (XtOrderExecUpdate& upd : updates_buf_) {
        const bool upd_final = order_status_is_final(upd.status);

        if (eof_closing_ && upd_final && order_has_unvisible_pending_fill_(upd.id)) {
            continue;
        }
        const auto it = order_strategy_.find(upd.id);
        if (it != order_strategy_.end()) {
            if (upd.sid_id == xt_identity::StrategySidRegistry::kNoSidId) {

                upd.sid_id = it->second.sid_id;
            }
            if (upd_final && it->second.terminal_at_date == 0) {
                it->second.terminal_at_date = current_trading_date_;
            }
        }

        if (upd_final) {
            if (auto dp = delivered_proj_.find(upd.id); dp != delivered_proj_.end()) {
                if (!pending_fills_.empty() && order_has_unvisible_pending_fill_(upd.id)) {
                    dp->second.terminal_delivered = true;
                    dp->second.claimed_filled_fp  = upd.filled_fp;
                } else {
                    delivered_proj_.erase(dp);
                }
            }
        }
        for (auto* obs : observers_) {
            obs->on_order_update(upd);
        }
        out.push_back(std::move(upd));
    }
    updates_buf_.clear();
    return out.size() - before;
}

bool XtHftBacktestOrderGateway::order_has_unvisible_pending_fill_(uint64_t id) const {

    for (const auto& pf : pending_fills_) {
        if (pf.trade.order_id == id && pf.visible_at_ns > last_event_ns_) return true;
    }
    return false;
}

SubmitResult XtHftBacktestOrderGateway::submit(const char* code,
                                               bool is_buy,
                                               int64_t price_fp,
                                               int64_t qty_fp,
                                               XtTimeInForce tif,
                                               std::string_view user_tag,
                                               XtOffset offset,
                                               ExecFlags  ,
                                               std::string_view strategy_id) {

    if (eof_closing_) {
        return SubmitResult{0, xt_runtime::XtRejectCode::BacktestEofClosing};
    }

    if (offset == XtOffset::Auto) {
        offset = xt_offset_router::resolve_submit_offset(
                     offset, xt_core::XtOrderSource::Strategy)
                     .value_or(XtOffset::Open);
    }
    return submit_single_(code, is_buy, price_fp, qty_fp, tif, user_tag, offset, strategy_id);
}

using FinalSlice_ = close_offset_core::CloseSlice;
static constexpr int kMaxFinalSlices_ = 8;

static int build_final_slices_(const char* code, bool is_buy, int64_t qty_fp,
                                XtOffset offset, int64_t max_order_qty_fp,
                                xt_account::AccountStateStore* account,
                                const XtCommodityCatalog* catalog,
                                xt_runtime::order::XtOrderRuntimeBook* order_book,
                                FinalSlice_* out, bool& out_truncated,
                                bool& out_deferred) {
    out_truncated = false;
    out_deferred = false;

    FinalSlice_ routed[3];
    int routed_count = 1;
    routed[0] = {offset, qty_fp};

    if (offset == XtOffset::Open && account && catalog && order_book &&
        catalog->effective_offset_policy(code) == XtOffsetPolicy::CloseFirst) {
        xt_offset_router::OffsetRouteInput in;
        in.code = code;
        in.policy = XtOffsetPolicy::CloseFirst;
        in.req_qty_fp = qty_fp;
        const auto cv = xt_account::closeable_view(
            account->snapshot_position(code ? code : ""), is_buy);
        in.opposite_closeable_today_fp = cv.today_fp;
        in.opposite_closeable_yesterday_fp = cv.yesterday_fp;
        order_book->accumulate_close_outstanding_fp(
            code, is_buy, in.opposite_close_outstanding_today_fp,
            in.opposite_close_outstanding_yesterday_fp);

        in.opposite_open_outstanding_fp =
            order_book->open_no_trade_qty_fp(code, !is_buy);

        const auto d = xt_offset_router::route_open_intent(in);
        using Action = xt_offset_router::OffsetRouteDecision::Action;
        if (d.action == Action::kDefer) {
            out_deferred = true;
            return 0;
        }
        if (d.action == Action::kRewrite) {
            routed_count = 0;
            for (int i = 0; i < d.close_slice_count; ++i) routed[routed_count++] = d.close_slices[i];
            if (d.open_remainder_fp > 0) routed[routed_count++] = {XtOffset::Open, d.open_remainder_fp};
        }

    } else if (offset == XtOffset::Close) {
        const auto closeable =
            xt_account::closeable_view(account->snapshot_position(code ? code : ""), is_buy);
        close_offset_core::CloseSlice resolved[2];
        const int n = close_offset_core::resolve_close_slices(
            code, qty_fp, closeable.today_fp, closeable.yesterday_fp,  false, resolved);
        if (n > 0) {
            routed_count = n;
            for (int i = 0; i < n; ++i) routed[i] = resolved[i];
        }

    }

    return close_offset_core::chunk_slices_by_max_qty(
        routed, routed_count, max_order_qty_fp, out, kMaxFinalSlices_, out_truncated);
}

SubmitResult XtHftBacktestOrderGateway::submit_single_(const char* code,
                                                       bool is_buy,
                                                       int64_t price_fp,
                                                       int64_t qty_fp,
                                                       XtTimeInForce tif,
                                                       std::string_view user_tag,
                                                       XtOffset offset,
                                                       std::string_view strategy_id,
                                                       bool is_forced_close) {

    if (in_liquidation_ && !is_forced_close) {
        return SubmitResult{0, xt_runtime::XtRejectCode::MarginCallLiquidation};
    }

    const int64_t now_ms = bt_risk_now_ms(last_date_, last_time_ms_);

    xt_core::XtOrder exec_req;
    exec_req.set_code(code);
    exec_req.is_buy       = is_buy;
    exec_req.price_fp     = price_fp;
    exec_req.qty_fp       = qty_fp;
    exec_req.tif          = tif;
    exec_req.close_offset = offset;

    if (qty_fp <= 0) {
        if (reject_sink_) {
            risk_action::dispatch_reject(
                *reject_sink_, exec_req,
                make_reject(XtRiskReasonCode::InvalidPriceOrQty, XtRiskStage::InputGuard,
                            "invalid close qty after routing"),
                now_ms);
        }
        return {};
    }

    int64_t limit_up_fp = 0;
    int64_t limit_down_fp = 0;
    if (price_limit_enabled_ && market_cache_) {
        if (const XtTick* t = market_cache_->last_tick(code)) {
            limit_up_fp   = t->upper_limit_fp;
            limit_down_fp = t->lower_limit_fp;
        }
    }

    if (self_match_check_ && !is_forced_close) stp_checker_.register_symbol(std::string(code));

    const int64_t today_pnl_fp = daily_loss_check_enabled_
        ? account_->snapshot_account().realized_pnl - session_begin_realized_fp_
        : 0;

    const XtBacktestRiskGuardContext data_ctx(
        *account_, order_book_, commodities_, limit_up_fp, limit_down_fp,
        (self_match_check_ && !is_forced_close) ? &stp_checker_ : nullptr,
        today_pnl_fp);
    const XtRiskEvalContext eval_ctx{
        .now_ms           = now_ms,
        .run_input_guards = true,
        .is_forced_close  = is_forced_close,
    };
    const XtRiskResult risk_dec = risk_gw_.evaluate(exec_req, data_ctx, eval_ctx);
    if (!risk_dec.passed) {
        if (reject_sink_) {
            risk_action::dispatch_reject(*reject_sink_, exec_req, risk_dec, now_ms);
        }

        if (risk_dec.reason_code == XtRiskReasonCode::PriceOutOfDailyLimit) {
            return SubmitResult{0, xt_runtime::XtRejectCode::PriceExceedsLimit};
        }
        if (risk_dec.reason_code == XtRiskReasonCode::SelfTradeDetected) {
            return SubmitResult{0, xt_runtime::XtRejectCode::SelfTradeDetected};
        }
        if (risk_dec.reason_code == XtRiskReasonCode::DailyLossExceeded) {
            return SubmitResult{0, xt_runtime::XtRejectCode::DailyLossExceeded};
        }
        return {};
    }

    bool slices_truncated = false;
    bool slices_deferred = false;
    FinalSlice_ final_slices[kMaxFinalSlices_];
    const int final_count = build_final_slices_(
        code, is_buy, qty_fp, offset, max_order_qty_fp_, account_, commodities_,
        order_book_, final_slices, slices_truncated, slices_deferred);

    if (slices_deferred) {
        return SubmitResult{0, xt_runtime::XtRejectCode::OppositeOpenPending};
    }

    if (slices_truncated) {
        XtLogger::warn_m("MATCH",
            "max_slices({}) exceeded -> reject whole order code={} qty_fp={} is_buy={}",
            kMaxFinalSlices_, code ? code : "", qty_fp, is_buy);
        if (reject_sink_) {
            risk_action::dispatch_reject(
                *reject_sink_, exec_req,
                make_reject(XtRiskReasonCode::MaxOrderQty, XtRiskStage::InputGuard,
                            "qty exceeds max_order_qty x kMaxFinalSlices budget"),
                now_ms);
        }
        return {};
    }
    if (final_count == 0) return {};

    const auto closeable_for_book = (offset != XtOffset::Open)
        ? xt_account::closeable_view(account_->snapshot_position(code ? code : ""), is_buy)
        : xt_account::CloseableView{};

    uint32_t sid_id = xt_identity::StrategySidRegistry::kNoSidId;
    if (!strategy_id.empty()) {
        if (auto* reg = xt_identity::StrategySidRegistry::global()) {
            sid_id = reg->lookup(std::string(strategy_id));
        }
    }

    uint64_t first_id = 0;
    for (int i = 0; i < final_count; ++i) {
        const uint64_t id = submit_one_slice_(
            code, is_buy, price_fp, tif, user_tag,
            final_slices[i].offset, final_slices[i].qty_fp,
            closeable_for_book, StrategySid::from_raw(sid_id));
        if (id == 0) {

            if (first_id == 0) return {};
            XtLogger::warn_m("MATCH",
                "partial_slice_failure code={} slice={}/{}",
                code ? code : "", i, final_count);
            break;
        }
        if (i == 0) first_id = id;

        if (final_slices[i].offset == XtOffset::Open && risk_dec.per_unit_margin_fp > 0) {
            account_->apply(XtAccountEvent::order_accepted(
                id, xt_fp::mul(risk_dec.per_unit_margin_fp, final_slices[i].qty_fp),
                risk_dec.per_unit_margin_fp));
        }

        if (self_match_check_ && !is_forced_close) {
            stp_checker_.on_order_submitted(std::string(code), id, is_buy, price_fp);
        }

        if (!strategy_id.empty()) {
            order_strategy_[id] = OrderStrategyEntry{std::string(strategy_id), sid_id, 0};
        }
    }

    if (first_id != 0) {
        const size_t live = match_engine_.live_orders();
        if (live > risk_obs_.peak_live_orders) risk_obs_.peak_live_orders = live;
    }

    SubmitResult out;
    out.order_id = first_id;
    return out;
}

uint64_t XtHftBacktestOrderGateway::submit_one_slice_(
    const char* code, bool is_buy, int64_t price_fp,
    XtTimeInForce tif, std::string_view user_tag,
    XtOffset sl_offset, int64_t sl_qty,
    const xt_account::CloseableView& closeable_for_book,
    StrategySid sid) {

    const uint64_t oid = match_engine_.alloc_order_id();
    const uint64_t id = match_engine_.submit(
        oid, code, is_buy, price_fp, sl_qty, tif,
        last_date_, last_time_ms_, user_tag, sl_offset);
    XtLogger::trace_m(
        "MATCH", "submit {} {} {} @{} offset={} id={}", is_buy ? "buy" : "sell", code,
        xt_fp::from_fp(sl_qty), xt_fp::from_fp(price_fp), static_cast<int>(sl_offset), id);

    const bool is_close = (sl_offset != XtOffset::Open);
    const bool is_today = (sl_offset == XtOffset::CloseToday);
    if (id != 0) {

        delivered_proj_[id] = DeliveredOrderState{is_buy, sl_offset, price_fp, sl_qty, 0};
    }
    if (id != 0) {

        (void)order_book_->on_order_submitted(
            id, code, is_buy, sl_qty, price_fp, sl_offset,
            is_close ? closeable_for_book : xt_account::CloseableView{}, sid);
    }

    const int64_t ts = current_ts();
    for (auto* obs : observers_) {
        obs->on_order_submit(ts, id, code, is_buy, price_fp, sl_qty,
                             user_tag, is_close, is_today);
    }
    return id;
}

bool XtHftBacktestOrderGateway::cancel(uint64_t id) {
    return match_engine_.cancel(id);
}

int64_t XtHftBacktestOrderGateway::compute_maintenance_fp_() const {
    int64_t gross_margin_fp = 0;
    for (const auto& code : account_->snapshot_tracked_codes()) {
        const auto pos = account_->snapshot_position(code);
        const int64_t gross_qty = pos.long_qty + pos.short_qty;
        if (gross_qty <= 0 || pos.mark_price <= 0) continue;
        const XtCommodityInfo* info = commodities_->find(code.c_str());

        const int64_t per_unit = commodities_->per_unit_margin_fp(info, code.c_str(), pos.mark_price);
        if (per_unit <= 0) continue;
        gross_margin_fp += xt_fp::mul(gross_qty, per_unit);
    }
    return xt_fp::mul(gross_margin_fp, maintenance_ratio_fp_);
}

void XtHftBacktestOrderGateway::check_margin_call_() {
    if (!account_ || !commodities_) return;
    if (in_liquidation_) {

        if (match_engine_.live_orders() != 0 || !pending_fills_.empty()) return;
        if (positions_flat_()) {
            in_liquidation_ = false;
            return;
        }
        execute_forced_liquidation_();
        return;
    }

    if (!pending_fills_.empty()) return;
    const int64_t maint_fp = compute_maintenance_fp_();
    if (maint_fp <= 0) return;
    const auto acct = account_->snapshot_account();
    if (acct.equity >= maint_fp) return;
    ++margin_call_count_;
    XtLogger::warn_m("BT_RISK", "margin call #{}: equity={} < maintenance={} → forced liquidation",
                     margin_call_count_, xt_fp::from_fp(acct.equity), xt_fp::from_fp(maint_fp));
    in_liquidation_ = true;
    execute_forced_liquidation_();
}

bool XtHftBacktestOrderGateway::positions_flat_() const {
    for (const auto& code : account_->snapshot_tracked_codes()) {
        const auto pos = account_->snapshot_position(code);
        if (pos.long_qty > 0 || pos.short_qty > 0) return false;
    }
    return true;
}

void XtHftBacktestOrderGateway::execute_forced_liquidation_() {

    std::vector<uint64_t> ids;
    ids.reserve(match_engine_.live_orders());
    for (const auto& o : match_engine_.live_orders_view()) ids.push_back(o.id);

    match_engine_.collect_in_transit_order_ids(ids);
    for (uint64_t id : ids) match_engine_.cancel(id);

    int64_t chunk_cap = std::numeric_limits<int64_t>::max();
    if (max_order_qty_fp_ > 0) chunk_cap = max_order_qty_fp_;
    if (risk_max_order_qty_fp_ > 0) chunk_cap = std::min(chunk_cap, risk_max_order_qty_fp_);
    auto close_side = [&](const std::string& code, bool is_buy, int64_t total_qty, int64_t px) {
        int64_t remaining = total_qty;
        while (remaining > 0) {
            const int64_t q = std::min(remaining, chunk_cap);
            const auto r = submit_single_(code.c_str(), is_buy, px, q,
                                          XtTimeInForce::IOC, "margin_call", XtOffset::Close,
                                           "",  true);
            if (r.order_id == 0) {

                XtLogger::warn_m("BT_RISK",
                    "margin call close rejected code={} is_buy={} qty={} reject={}",
                    code, is_buy, xt_fp::from_fp(q), static_cast<int>(r.reject_reason));
                break;
            }
            remaining -= q;
        }
    };
    for (const auto& code : account_->snapshot_tracked_codes()) {
        const auto pos = account_->snapshot_position(code);
        if (pos.long_qty <= 0 && pos.short_qty <= 0) continue;
        const XtTick* t = market_cache_ ? market_cache_->last_tick(code.c_str()) : nullptr;
        const XtCommodityInfo* info = commodities_->find(code.c_str());
        const int64_t tick_fp = (info && info->price_tick > 0.0) ? xt_fp::to_fp(info->price_tick) : 0;
        if (pos.long_qty > 0) {
            int64_t px = (t && t->bid_price_fp[0] > 0) ? t->bid_price_fp[0]
                                                       : pos.mark_price - pos.mark_price / 100;
            if (tick_fp > 0 && px > 0) px -= px % tick_fp;
            if (px <= 0) {

                XtLogger::warn_m("BT_RISK", "margin call skip long {}: no book and mark<=0", code);
            } else {
                close_side(code,  false, pos.long_qty, px);
            }
        }
        if (pos.short_qty > 0) {
            int64_t px = (t && t->ask_price_fp[0] > 0) ? t->ask_price_fp[0]
                                                       : pos.mark_price + pos.mark_price / 100;
            if (tick_fp > 0 && px % tick_fp != 0) px += tick_fp - px % tick_fp;
            if (px <= 0) {
                XtLogger::warn_m("BT_RISK", "margin call skip short {}: no book and mark<=0", code);
            } else {
                close_side(code,  true, pos.short_qty, px);
            }
        }
    }
}
