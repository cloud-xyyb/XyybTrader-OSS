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

// ── 初始化 ───────────────────────────────────────────────────────────────────

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
    // breach 观测阈值口径与 risk engine 同式（>0 才启用）。
    obs_max_pos_qty_fp_ = p.risk.max_pos_qty > 0.0 ? xt_fp::to_fp(p.risk.max_pos_qty) : 0;
    risk_obs_ = {};
    fill_net_fp_.clear();
    breach_state_.clear();

    // 所有 domain 组件 Runner 持有，gateway 仅引用
    account_     = p.account;
    commodities_ = p.catalog;
    order_book_  = p.order_book;

    // fee_book_ 由 gateway 持有，cost_applier_/risk_engine_ 仅引用其 IFeeRuleProvider 接口。
    fee_book_ = XtFeeRuleBook(p.fees);
    risk_engine_  = std::make_unique<XtRiskEngine>(
        p.risk, XtRiskEngine::InitDeps{&fee_book_, commodities_});
    risk_policy_  = p.risk_policy;
    risk_gw_.bind(*risk_engine_, risk_policy_);
    cost_applier_.init(std::move(p.cost), &fee_book_, commodities_);
    account_view_ = std::make_unique<xt_account::AccountStoreView>(*account_);

    match_engine_.reset_volume_cache();
    // backtest 走 cold-start 路径 (每次 init 都从空 book 开始,is_cold_start=true)
    order_book_->reset_for_recovery({}, /*is_cold_start=*/true);

    tick_idx_     = 0;
    last_date_    = 0;
    last_time_ms_ = 0;
    last_event_ns_ = 0;
    // 观测计数随 event 轴复位（warn-once 语义 = 每次 run 一次，非每对象一次）。
    event_forward_spike_count_ = 0;

    // EOF 收尾状态：栅栏复位 + delivered 投影清空（init 契约：每次从 cold-start 重建）。
    eof_closing_ = false;
    delivered_proj_.clear();

    trades_buf_.reserve(8);
    updates_buf_.reserve(16);

    initialized_ = true;
}

void XtHftBacktestOrderGateway::notify_order_terminal(uint64_t order_id) {
    if (!self_match_check_) return;
    // id-keyed：按 id erase active entry 并重算该品种边界（对齐 live）。BT 单线程，
    // 提交/终态全经本 gateway 线性流过 → active-set 重建与旧 book 扫描等价。
    stp_checker_.on_order_terminal(order_id);
}

// ── 视图访问 ──────────────────────────────────────────────────────────────────

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

// ── 报告与统计 ─────────────────────────────────────────────────────────────

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
    // RejectSink / observer 使用 packed timestamp，与 reporter helper 保持同一编码。
    //
    // 事件轴取 last_action_date_（自然日），不取 last_date_（交易日／账务归属轴）：
    // 夜盘 22:xx 与次晨 09:xx 属同一交易日，用交易日拼 packed ts 会让同一订单出现
    // submit 晚于其 fill 近一日的时间倒退（fill/update 侧走的是自然日轴）。账务归属
    // 由 trading_day 字段单独承载，不参与 wall-clock 构造。见 advance_clock 的两轴分离。
    return xt_time::packed_ts(last_action_date_ != 0 ? last_action_date_ : last_date_,
                              last_time_ms_);
}

// ── 热路径 ────────────────────────────────────────────────────────────────────

void XtHftBacktestOrderGateway::advance_clock(uint32_t action_date, uint32_t action_time_ms,
                                              uint32_t trading_date) {
    ++tick_idx_;
    // report/current_ts 戳保持 trading 轴；action 轴另存供 EOF/event 重建。
    last_date_        = trading_date;
    last_time_ms_     = action_time_ms;
    last_action_date_ = (action_date != 0) ? action_date : trading_date;
    // pending 可见性/visible base 同挂 event 轴；max-clamp 冻结倒流脏数据
    //（方向安全：只晚释不早释），与 OrderManager 入口 clamp 同款语义。
    const int64_t event_ns = xt_time::to_ns(last_action_date_, action_time_ms);
    note_event_forward_spike_(event_ns);  // 观测巨幅前跳（脏未来日期毒化 event 轴）
    if (event_ns > last_event_ns_) last_event_ns_ = event_ns;
    // 撮合域（deadline/TTL/watermark）走真 event 轴；trading 仅供成交/回报戳。
    match_engine_.advance_clock(tick_idx_, event_ns, trading_date, action_time_ms);
}

void XtHftBacktestOrderGateway::advance_clock_only(uint32_t action_date, uint32_t action_time_ms,
                                                   uint32_t trading_date) {
    // grid clock / EOF flush 只推进 wall-clock；tick_idx_ 保持在最近一个 market tick。
    // match engine 的 no-tick overload 用当前 tick index 处理 time-only TTL/ACK 到期。
    last_date_        = trading_date;
    last_time_ms_     = action_time_ms;
    last_action_date_ = (action_date != 0) ? action_date : trading_date;
    const int64_t event_ns = xt_time::to_ns(last_action_date_, action_time_ms);
    note_event_forward_spike_(event_ns);  // 观测巨幅前跳（脏未来日期毒化 event 轴）
    if (event_ns > last_event_ns_) last_event_ns_ = event_ns;
    match_engine_.advance_clock_to(event_ns, trading_date, action_time_ms);
    // 先释放到期的 fill-driven update（产生时间更早），再抽本刻 grid clock
    // 新产的终态（TTL/ACK 到期 cancel）——与 process_tick 同序（旧先新后）；
    // driver 紧随其后 drain（EOF 无后续 tick 时不再滞留 manager）。
    release_due_updates_(last_event_ns_);
    match_engine_.drain_updates(updates_buf_);
}

void XtHftBacktestOrderGateway::release_due_updates_(int64_t now_ns) {
    // 保序 compaction（同 CTA promote_due_ N17 模式）：同 deadline 保 emit 序，
    // 不同 deadline 允许后 match 先可见——队头 while 的 HOL-blocking（BT-R4）根除。
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
// event 轴前跳阈值：> 366 自然日折 ns。夜盘/节假日跳空最多数日，日内数据远达不到
// 年尺度；命中即脏未来日期 tick（如 2099 混入 2022 数据 → last_event_ns_ 被毒化、
// TTL/ACK 立即到期 + fill 滞留 EOF）。纯观测。
// 注：合法但极稀疏的多年窗口（相邻两 tick 跨 >366d）也会命中——阈值只作噪声护栏，
// 命中不改行为（只计数 + 首次 warn），不误当脏数据处理。
constexpr int64_t kEventForwardSpikeNs = 366LL * 86400LL * 1'000'000'000LL;
}  // namespace

void XtHftBacktestOrderGateway::note_event_forward_spike_(int64_t event_ns) noexcept {
    if (last_event_ns_ == 0) return;  // 首个 event 无单调基准，前跳无意义
    if (event_ns <= last_event_ns_) return;  // 非前跳（含倒流）：先判方向，避开有符号减法 UB
    // 已知 event_ns > last_event_ns_；__int128 差值防两个各自可表示但跨度超 INT64_MAX
    // 的 epoch-ns 相减触发 UB（纯观测逻辑不得引入 UB）。
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
    // 暂存至成员，等 process_tick 时再下发给 match_engine，避免未匹配 process_tick
    // 的时钟泄露到下次撮合。
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

    // 成交已由 match_engine 产生；这里只按 response latency 设置 fill 回报可见时间。
    // response 取随单存的 submit 时刻配对采样值（paired CDF 语义：与 entry 同 percentile）；
    // 多在途单交错成交序下 per-fill 重采会错配 percentile，故不在此采样。
    // 无模型/未盖章（stamp < 0）回退固定 fill_latency_ns_。
    const bool use_model_response = match_engine_.has_latency_model();
    // 同单多笔 fill 取最大 visible_at（update 反映的是该单累计状态）。
    thread_local std::vector<std::pair<uint64_t, int64_t>> fill_visible;
    fill_visible.clear();
    for (auto& tr : trades_buf_) {
        // visible base = 当前 match event ns（cur 推进后的 last_event_ns_），
        // 不从 trade 戳反推——trade_date/time 承载 trading/report 语义（可能是
        // dispatch tick 归属），不是时长基准。
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
        // 导出载体：record 内瞬态字段带走 per-fill 可见时刻（sink 侧 trade/latency
        // 行 append visible_at）；drain gate 继续用 PendingFill.visible_at_ns。
        tr.sim_visible_at_ns = visible_at_ns;
        pending_fills_.push_back({std::move(tr), visible_at_ns});
    }
    trades_buf_.clear();

    // 订单状态更新默认本 tick 即可见；本 tick 有 fill 的单例外：其同 tick
    // 全部 update（Filled/PartiallyFilled 及同帧的 Canceled 余量撤）与对应
    // fill 同 visible_at 释放——只 defer 部分 status 会让 IOC 部分成交的
    // Canceled 终态先于 PartiallyFilled/fill 到达（回报倒序 + 分裂态）。
    // resp=0 时 visible_at == 本 tick → 保持即时（零延迟基线结构性不变）。
    updates_buf_.clear();
    // 早先挂起、现已到期的 update 先释（产生时间更早，排新 update 之前）。
    release_due_updates_(last_event_ns_);
    const size_t new_upd_begin = updates_buf_.size();
    match_engine_.drain_updates(updates_buf_);
    // 分区只扫本 tick 新产段：已释放的旧 update 属早先 fill，不得被同单
    // 本 tick 新 fill 的 visible_at 重新挂起。
    if (!fill_visible.empty() && updates_buf_.size() > new_upd_begin) {
        const int64_t now_ns = last_event_ns_;
        size_t w = new_upd_begin;
        for (size_t i = new_upd_begin; i < updates_buf_.size(); ++i) {
            XtOrderExecUpdate& upd = updates_buf_[i];
            // fill 态（Filled/PartiallyFilled）与同帧终态（IOC 余量撤的
            // Canceled 等）都属同一回程批，按 id 命中即 defer——只 defer
            // fill 态会让 Canceled 先于 PartiallyFilled/fill 到达（倒序）。
            // Submitted/Accepted（提交期回报）保持即时，不动冻结记账时序。
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

    // 爆仓检查放 tick 尾：本 tick 成交/mark 已归账（engine 在 prepare_cycle_ 里
    // 先写 account mark 再调 process_tick），强平单下一 tick 撮合 = 自然 match_delay。
    if (margin_call_enabled_) check_margin_call_();
}

size_t XtHftBacktestOrderGateway::drain_fills(uint32_t date, uint32_t time_ms,
                                              std::vector<XtTradeRecord>& out) {
    if (pending_fills_.empty()) return 0;
    // date/time = 自然日 event 对（与 visible_at 同轴）；保序 compaction 同
    // release_due_updates_：同 deadline 保 match 序，不同 deadline 后 match 可先可见。
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
    // Backtest multi-strategy routing — fill 从 order_strategy_ sidecar 回填
    // strategy_id + sid_id（mirror live gateway 同源 stamp）。sidecar miss 时保持
    // 空/0，engine 走恢复链或 single-mode fallback exec_sinks_[0]。
    for (size_t i = before; i < out.size(); ++i) {
        XtTradeRecord& tr = out[i];
        const auto it = order_strategy_.find(tr.order_id);
        if (it != order_strategy_.end()) {
            if (tr.strategy_id.empty()) tr.strategy_id = it->second.sid;
            if (tr.sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
                tr.sid_id = it->second.sid_id;
            }
        }
        // delivered-view 记账：fill 真正派发（drain + observer fan）时累计，供 EOF
        // 收尾按 delivered 口径合成终态；已 delivered 终态摘除投影后此处 find miss（no-op）。
        if (auto dp = delivered_proj_.find(tr.order_id); dp != delivered_proj_.end()) {
            dp->second.delivered_filled_fp += tr.qty_fp;
            // 机制 2（盘中自然闭合）：终态已盘中派发的单其隐藏 fill 现已可见交付，
            // delivered 追平 claim → 投影使命完成即 erase（run 继续时无需 EOF 补交付）。
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
    // Strict EOF 只保留在 EOF clock 前已可见的 fill；剩余 pending fill
    // 直接丢弃，不触发 observer，也不进入 strategy/P&L。未可见的
    // fill-driven update 同丢（避免半终态：update 到而 fill 永不到）。
    const size_t dropped = pending_fills_.size();
    pending_fills_.clear();
    pending_updates_.clear();
    return dropped;
}

std::size_t XtHftBacktestOrderGateway::synthesize_eof_delivered_cancels() {
    if (delivered_proj_.empty()) return 0;
    // 合成回调 epoch ns 用 EOF 事件轴时刻（flush 的 advance_clock_only 已对齐）；
    // 非 golden 字段，取当前 event-ns 即可。
    const uint64_t cb_ns = static_cast<uint64_t>(last_event_ns_ < 0 ? 0 : last_event_ns_);
    std::size_t n = 0;
    for (const auto& [id, st] : delivered_proj_) {
        // 与机制 3 互斥：terminal_delivered 投影必由 complete_eof_delivered_terminal_fills
        // 补交付并 erase；此处若仍见到 = 双终态 mutant（先派内部终态又合成第二条），跳过
        // 防重复合成（assert 提前暴露实现错误）。hold-back 的单未标 terminal_delivered，正常进本合成。
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
        u.filled_fp    = st.delivered_filled_fp;              // delivered 口径（strict-drop fill 不计）
        u.leaves_fp    = st.qty_fp - st.delivered_filled_fp;  // qty − delivered
        u.last_fill_fp = 0;
        u.local_cb_ns  = cb_ns;
        // sid_id 保持 kNoSidId，drain_order_updates 从 order_strategy_ sidecar 回填。
        updates_buf_.push_back(u);
        ++n;
    }
    // 收 sim 撮合簿：清理仍 alive 的 resting 在途单（archive-only，不 emit——delivered
    // 终态已在上方按投影合成），使 EOF 后 live_orders()==0。撮合簿 resting 单
    // ⊆ delivered 投影，故不产生漏账。
    match_engine_.discard_eof_live_orders();
    // 投影不在此清：合成终态经 drain_order_updates 派发时按 upd_final 统一摘除（幂等）。
    return n;
}

std::size_t XtHftBacktestOrderGateway::complete_eof_delivered_terminal_fills(
        std::vector<XtTradeRecord>& out) {
    if (delivered_proj_.empty() || pending_fills_.empty()) return 0;
    const size_t before = out.size();
    // 机制 3（EOF 补交付）：盘中已派终态（terminal_delivered）的单，其在 pending_fills_
    // 内仍未可见的 fill 越过 visible_at 一次性补交付（终态后不再有新 fill，claim == 交付累计，
    // 账本闭合）。这些 fill 从随后 drop_pending_fills 的 strict-drop 中救回；其余 fill（非
    // terminal_delivered 单 / 投影已摘的干净终态单）保留，交 drop/synthesize 原路处理。
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
    // 补交付 fill 的交付语义 mirror drain_fills 尾部：sid 回填 + 风险观测 + observer fan。
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
    // 补交付完成 → erase 所有 terminal_delivered 投影（synthesize 不再触及它们，单终态恰一次）。
    for (auto it = delivered_proj_.begin(); it != delivered_proj_.end();) {
        if (it->second.terminal_delivered) it = delivered_proj_.erase(it);
        else ++it;
    }
    return completed;
}

void XtHftBacktestOrderGateway::on_session_begin(uint32_t trading_date) {
    if (account_) account_->apply(XtAccountEvent::session_begin());
    if (account_) {
        // 对齐 live XtRiskMonitor 冻结口径：starting_balance = 交易日初 equity；
        // daily_loss 的 realized-delta 基线同刻冻结（realized 是 lifetime 累计不按日复位）。
        account_->on_session_begin_hook(get_equity_fp());
        session_begin_realized_fp_ = account_->snapshot_account().realized_pnl;
    }
    if (risk_engine_) {
        // BT risk 使用统一 annual-latch arm；account 快照提供 realized/fees/unrealized。
        // account_ 为空时 callable 不写输出，helper 按默认值处理。
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

    // 跨 trading_date 时清理上一日已终态 sidecar；active orders 与当日终态记录保留。
    // backtest 单线程运行，清理只在 session_begin 边界执行。
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

// ── 回报链 ────────────────────────────────────────────────────────────────────

size_t XtHftBacktestOrderGateway::drain_order_updates(std::vector<XtOrderExecUpdate>& out) {
    if (updates_buf_.empty()) return 0;
    const size_t before = out.size();
    // order_update 无 visibility delay；drain 时回填 sid_id，并在终态记录 terminal_at_date。
    for (XtOrderExecUpdate& upd : updates_buf_) {
        const bool upd_final = order_status_is_final(upd.status);
        // EOF 收尾 hold-back：flush 期若某单的终态（TTL 到期 / 延迟撤单在
        // advance_clock_only 合成）与其一笔仍未可见的 pending fill 并存，该 fill 随后被
        // drop_pending_fills 丢弃、从策略/账户视角从未发生；派发携撮合层内部累计 filled
        // 的终态会与 delivered 口径分裂（filled=内部量 vs delivered=已派发量）。此处不
        // 派发、不摘投影，该终态随 pending 通道 drop 一并消失，投影留给
        // synthesize_eof_delivered_cancels 按 delivered 口径闭账。盘中（eof_closing_
        // 为假）drain 行为逐位不变；drop_pending_fills 之后 pending_fills_ 已空，后续
        // lost_intransit / resting 合成产物无 pending fill，天然不命中本 gate。
        if (eof_closing_ && upd_final && order_has_unvisible_pending_fill_(upd.id)) {
            continue;
        }
        const auto it = order_strategy_.find(upd.id);
        if (it != order_strategy_.end()) {
            if (upd.sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
                // submit 期已 intern；未注册时仍为 kNoSidId，engine fallback exec_sinks_[0]。
                upd.sid_id = it->second.sid_id;
            }
            if (upd_final && it->second.terminal_at_date == 0) {
                it->second.terminal_at_date = current_trading_date_;
            }
        }
        // 机制 1（投影摘除延迟）：终态派发时若该单仍有一笔未可见的 pending fill（撮合层
        // 内部先出终态、fill 后到的真实回报倒序），不 erase 投影，改标 terminal_delivered
        // + 记 claim（终态携带的内部 filled）；待 fill 可见时 drain_fills 自然闭合、或 EOF
        // complete_eof_delivered_terminal_fills 补交付，避免终态携内部 filled 却无对应
        // fill/仓位（delivered-view 分裂）。无隐藏 fill 才立即 erase（原行为）；pending_fills_
        // 为空（常态）时短路直接 erase，热路径 O(1) 逐位不变。
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
    // 未可见 = visible_at > 当前 event 轴时刻（drain_order_updates 的 flush 调用点上
    // last_event_ns_ 已被 advance_clock_only 推到 EOF cutoff）；visible_at == cutoff 的
    // fill 会被 drain_fills 派发，不算未可见（与 drop 口径一致，取严 `>`）。
    for (const auto& pf : pending_fills_) {
        if (pf.trade.order_id == id && pf.visible_at_ns > last_event_ns_) return true;
    }
    return false;
}

// ── IExecutionGateway ─────────────────────────────────────────────────────────

SubmitResult XtHftBacktestOrderGateway::submit(const char* code,
                                               bool is_buy,
                                               int64_t price_fp,
                                               int64_t qty_fp,
                                               XtTimeInForce tif,
                                               std::string_view user_tag,
                                               XtOffset offset,
                                               ExecFlags /*exec_flags*/,
                                               std::string_view strategy_id) {
    // EOF 收尾栅栏：flush_pending 期合成 Canceled 终态经 on_order 可触发策略重挂，
    // 无 tick 可撮合的新单会重新冻结并在 flush 返回时留活单 → 本地拒断链。
    if (eof_closing_) {
        return SubmitResult{0, xt_runtime::XtRejectCode::BacktestEofClosing};
    }
    // strategy_id 下沉到 submit_single_，由每个成功切片写入 sidecar。
    // ExecFlags 在期货 BT 撮合里没有额外语义，当前保持忽略。
    // Reject 不触发 on_order_submit；拒单通过 reject_sink_ 落地。
    // XtOffset::Auto 提交入口解析（BT 提交全为策略源）：Auto → Open 意图，
    // close_first 品种由 build_final_slices_ 内 offset 路由改写 —— 与 live 同路径，
    // Auto 永不进风控/撮合/回报 payload。
    if (offset == XtOffset::Auto) {
        offset = xt_offset_router::resolve_submit_offset(
                     offset, xt_core::XtOrderSource::Strategy)
                     .value_or(XtOffset::Open);
    }
    return submit_single_(code, is_buy, price_fp, qty_fp, tif, user_tag, offset, strategy_id);
}

// 切片结构（inline，仅 submit_single_ 使用）
using FinalSlice_ = close_offset_core::CloseSlice;
static constexpr int kMaxFinalSlices_ = 8;

/// @brief 构建最终提交切片（offset 路由 + close offset 路由 + max_order_qty）。
/// @details
/// - 无副作用，仅依赖输入参数，因此保留为 file-local helper。
/// - `out_deferred=true` 表示 close-first 对侧在途 Open 未决，caller 应拒 `OppositeOpenPending`。
/// @return 最终切片数；0 表示 defer、无可提交切片或输入被路由为空。
static int build_final_slices_(const char* code, bool is_buy, int64_t qty_fp,
                                XtOffset offset, int64_t max_order_qty_fp,
                                xt_account::AccountStateStore* account,
                                const XtCommodityCatalog* catalog,
                                xt_runtime::order::XtOrderRuntimeBook* order_book,
                                FinalSlice_* out, bool& out_truncated,
                                bool& out_deferred) {
    out_truncated = false;
    out_deferred = false;
    // Open 意图按 close-first policy 检查对侧可平与在途开仓；
    // XtOffset::Close 按今昨仓量解析为实际 close slice。
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
        // pending 对侧 Open 会占用可平窗口，close-first 路由需要按在途量 defer。
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
        // kPassOpen 保持 routed = {Open, qty}。
    } else if (offset == XtOffset::Close) {
        const auto closeable =
            xt_account::closeable_view(account->snapshot_position(code ? code : ""), is_buy);
        close_offset_core::CloseSlice resolved[2];
        const int n = close_offset_core::resolve_close_slices(
            code, qty_fp, closeable.today_fp, closeable.yesterday_fp, /*prefer_today=*/false, resolved);
        if (n > 0) {
            routed_count = n;
            for (int i = 0; i < n; ++i) routed[i] = resolved[i];
        }
        // n==0 表示 qty > closeable；风控通常已拦截，保留原始切片交给撮合兜底。
    }

    // max_order_qty 对开仓和平仓切片统一生效，预算不足时通过 out_truncated 通知 caller。
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
    // 爆仓接管期：账户被强平流程接管，非强平单一律拒（防清仓中开新仓 /
    // 策略平仓与强平叠加 over-close）；也保证闩期 live_orders 只含强平单，
    // check_margin_call_ 的重试判据（在途活动结清）才成立。
    if (in_liquidation_ && !is_forced_close) {
        return SubmitResult{0, xt_runtime::XtRejectCode::MarginCallLiquidation};
    }
    // 构建风控请求与提交时间戳；last_date_/last_time_ms_ 已由 advance_clock() 对齐到当前 tick。
    const int64_t now_ms = bt_risk_now_ms(last_date_, last_time_ms_);

    xt_core::XtOrder exec_req;
    exec_req.set_code(code);
    exec_req.is_buy       = is_buy;
    exec_req.price_fp     = price_fp;
    exec_req.qty_fp       = qty_fp;
    exec_req.tif          = tif;
    exec_req.close_offset = offset;

    // 输入 guard。
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

    // risk guard。price_limit_enabled 时从 market cache 取当前 tick 涨跌停价
    // （dispatch 前 cache 已持当前 tick）；off/缺 cache/首 tick 前 → 0 = engine
    // PriceOutOfDailyLimit 检查 bypass（fail-open）。
    int64_t limit_up_fp = 0;
    int64_t limit_down_fp = 0;
    if (price_limit_enabled_ && market_cache_) {
        if (const XtTick* t = market_cache_->last_tick(code)) {
            limit_up_fp   = t->upper_limit_fp;
            limit_down_fp = t->lower_limit_fp;
        }
    }
    // STP 自成交预防：惰性注册 symbol（BT 单线程，live 的运行期注册阻塞警告不适用）。
    if (self_match_check_ && !is_forced_close) stp_checker_.register_symbol(std::string(code));
    // daily_loss：realized-delta 口径（对齐 live venue close_profit 语义）；未配
    // max_daily_loss 时不算不读（免 per-submit snapshot 拷贝）。
    const int64_t today_pnl_fp = daily_loss_check_enabled_
        ? account_->snapshot_account().realized_pnl - session_begin_realized_fp_
        : 0;
    // 强平单免 STP：己方挂单刚被 execute_forced_liquidation_ 全撤，但终态 update
    // 要到下一次 drain 才重建边界 —— 同 tick 提交的强平单必撞陈旧边界被误拒。
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
        // 越板拒对策略可见（last_reject_code）；其余 risk reason 维持 NoError 返回，
        // 通用 reason→XtRejectCode 映射另行评审。
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

    // close offset 路由与 max_order_qty 切片。
    bool slices_truncated = false;
    bool slices_deferred = false;
    FinalSlice_ final_slices[kMaxFinalSlices_];
    const int final_count = build_final_slices_(
        code, is_buy, qty_fp, offset, max_order_qty_fp_, account_, commodities_,
        order_book_, final_slices, slices_truncated, slices_deferred);
    // close-first 品种对侧在途 Open 未决时，本地拒单并把拒因返回给策略。
    if (slices_deferred) {
        return SubmitResult{0, xt_runtime::XtRejectCode::OppositeOpenPending};
    }
    // 切片预算耗尽仍有余量未覆盖时拒整笔，避免只发前缀切片却返回 accepted。
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

    // 逐切片提交到撮合引擎。
    // 持仓快照在循环外读一次：submit 期间 account_ 不变，各切片共享相同快照。
    const auto closeable_for_book = (offset != XtOffset::Open)
        ? xt_account::closeable_view(account_->snapshot_position(code ? code : ""), is_buy)
        : xt_account::CloseableView{};

    // sid 在 submit 期 lookup（只读查表）一次；drain 走 sidecar 内的 sid_id，不做逐 update 字符串查表。
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
            // 首切片失败表示整笔未提交；中途失败保留已提交切片并停止后续切片。
            if (first_id == 0) return {};
            XtLogger::warn_m("MATCH",
                "partial_slice_failure code={} slice={}/{}",
                code ? code : "", i, final_count);
            break;
        }
        if (i == 0) first_id = id;
        // Open 切片 accept 冻结（在途资金预占；release 走 order update 事件链：
        // drain → order_event codec → AccountState::on_order_update 增量/终态释放）。
        // per_unit 来源：期货 = guard 的 futures margin 门禁；stock t+1 = core 的资金占用
        // 估算（notional+fee，经 gateway merge 透传）。Close 切片 per_unit=0 不冻。
        // 不变量：freeze 落账（无条件，按 Open+margin）与 book slot 注册（受
        // should_skip_submit gate，见 submit_one_slice_）各自独立；release 由 account
        // order_slots_ 事件链独立担保，不依赖 book slot 存在。
        if (final_slices[i].offset == XtOffset::Open && risk_dec.per_unit_margin_fp > 0) {
            account_->apply(XtAccountEvent::order_accepted(
                id, xt_fp::mul(risk_dec.per_unit_margin_fp, final_slices[i].qty_fp),
                risk_dec.per_unit_margin_fp));
        }
        // STP 边界更新：成功挂单进入活跃集合后维护己方价格边界（对齐 live，id-keyed）。
        if (self_match_check_ && !is_forced_close) {
            stp_checker_.on_order_submitted(std::string(code), id, is_buy, price_fp);
        }
        // 每个成功切片都记录 sidecar；fill/update 后续按各自 order id 回填 sid。
        if (!strategy_id.empty()) {
            order_strategy_[id] = OrderStrategyEntry{std::string(strategy_id), sid_id, 0};
        }
    }

    // B2 峰值挂单观测：submit 成功时刻是在途数的局部峰值点。
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

    // 提前分配 order id，使 match engine、order book 和 observer 使用同一个 id。
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
        // delivered 投影无条件登记（与冻结落账同口径，独立于下方 should_skip_submit
        // gate 的 book slot 注册）；EOF 收尾据此为无终态在途单按 delivered 口径闭账，
        // delivered 终态派发后由 drain_order_updates 摘除。
        delivered_proj_[id] = DeliveredOrderState{is_buy, sl_offset, price_fp, sl_qty, 0};
    }
    if (id != 0) {
        // slot.sid_id 在 submit-to-ack 窗口立即可见，用于 project_inflight 按 strategy 过滤。
        // 终态 tombstone 消费收在注册入口内部（返回 kSkippedTerminal），BT 不需分派返回值：
        // 冻结走 delivered 投影与 account 侧独立口径，与本地 slot 是否建成无关。
        (void)order_book_->on_order_submitted(
            id, code, is_buy, sl_qty, price_fp, sl_offset,
            is_close ? closeable_for_book : xt_account::CloseableView{}, sid);
    }
    // on_order_submit 使用当前 packed timestamp，与 reject/report timestamp 口径一致。
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


// ── 爆仓模拟（margin call）──────────────────────────────────────

int64_t XtHftBacktestOrderGateway::compute_maintenance_fp_() const {
    int64_t gross_margin_fp = 0;
    for (const auto& code : account_->snapshot_tracked_codes()) {
        const auto pos = account_->snapshot_position(code);
        const int64_t gross_qty = pos.long_qty + pos.short_qty;  // 两侧均非负；锁仓双计
        if (gross_qty <= 0 || pos.mark_price <= 0) continue;
        const XtCommodityInfo* info = commodities_->find(code.c_str());
        // 与 submit 风控同源公式（catalog per_unit_margin），价格用当前 mark。
        const int64_t per_unit = commodities_->per_unit_margin_fp(info, code.c_str(), pos.mark_price);
        if (per_unit <= 0) continue;
        gross_margin_fp += xt_fp::mul(gross_qty, per_unit);
    }
    return xt_fp::mul(gross_margin_fp, maintenance_ratio_fp_);
}

void XtHftBacktestOrderGateway::check_margin_call_() {
    if (!account_ || !commodities_) return;
    if (in_liquidation_) {
        // 闩期：在途活动（挂单/未归账 fill）结清前不动作；闩期策略单被拒 →
        // live_orders 只可能是强平单。结清后残仓（零成交/部分成交/拒单）→ 重试。
        if (match_engine_.live_orders() != 0 || !pending_fills_.empty()) return;
        if (positions_flat_()) {
            in_liquidation_ = false;
            return;
        }
        execute_forced_liquidation_();
        return;
    }
    // 本 tick 成交未归账（fill 走 pending + drain 异步）时快照偏旧：此时触发会
    // 误判 equity/仓位且可能对已平仓位 over-close → 延后到归账后的 tick 评估。
    if (!pending_fills_.empty()) return;
    const int64_t maint_fp = compute_maintenance_fp_();
    if (maint_fp <= 0) return;  // 无持仓
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
    // (a) 撤全部在途挂单；先收集 id 再撤（cancel 会改 live 集合，view 引用失效）。
    std::vector<uint64_t> ids;
    ids.reserve(match_engine_.live_orders());
    for (const auto& o : match_engine_.live_orders_view()) ids.push_back(o.id);
    // wheel 内在途单一并撤（N07 后 cancel 可达 wheel；漏撤的 delayed Open
    // 会在强平后 flush+成交 → 瞬时反向/超平）。
    match_engine_.collect_in_transit_order_ids(ids);
    for (uint64_t id : ids) match_engine_.cancel(id);

    // (b) 对手价 IOC 平全部持仓；缺盘口时 mark±1% 兜底，按 price_tick 向不利方向
    // snap 保成交（sell 向下 / buy 向上）。预分块 cap 取 engine.max_order_qty 与
    // risk.max_order_qty 两值严：风控 MaxOrderQty 在切片前按整笔 qty 预检（读
    // risk 值），只按 engine 值分块在两配置错位时仍会被整笔拒。
    int64_t chunk_cap = std::numeric_limits<int64_t>::max();
    if (max_order_qty_fp_ > 0) chunk_cap = max_order_qty_fp_;
    if (risk_max_order_qty_fp_ > 0) chunk_cap = std::min(chunk_cap, risk_max_order_qty_fp_);
    auto close_side = [&](const std::string& code, bool is_buy, int64_t total_qty, int64_t px) {
        int64_t remaining = total_qty;
        while (remaining > 0) {
            const int64_t q = std::min(remaining, chunk_cap);
            const auto r = submit_single_(code.c_str(), is_buy, px, q,
                                          XtTimeInForce::IOC, "margin_call", XtOffset::Close,
                                          /*strategy_id=*/"", /*is_forced_close=*/true);
            if (r.order_id == 0) {
                // 拒单可观测；残仓由 check_margin_call_ 闩期重试收敛。
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
                // 无盘口且 mark<=0（几不可达：有仓必有过 tick）：skip，闩期重试。
                XtLogger::warn_m("BT_RISK", "margin call skip long {}: no book and mark<=0", code);
            } else {
                close_side(code, /*is_buy=*/false, pos.long_qty, px);
            }
        }
        if (pos.short_qty > 0) {
            int64_t px = (t && t->ask_price_fp[0] > 0) ? t->ask_price_fp[0]
                                                       : pos.mark_price + pos.mark_price / 100;
            if (tick_fp > 0 && px % tick_fp != 0) px += tick_fp - px % tick_fp;
            if (px <= 0) {
                XtLogger::warn_m("BT_RISK", "margin call skip short {}: no book and mark<=0", code);
            } else {
                close_side(code, /*is_buy=*/true, pos.short_qty, px);
            }
        }
    }
}
