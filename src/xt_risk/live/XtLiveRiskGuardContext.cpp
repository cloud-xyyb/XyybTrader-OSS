#include "live/XtLiveRiskGuardContext.h"
#include "live/XtPerSidTodayPnl.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_metrics/XtStrategyPnlAggregator.h"
#include "xt_metrics/XtStrategyPnlBaseline.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "XtFixedPoint.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_risk/engine/XtRiskMonitorState.h"
#include "xt_risk/engine/XtRiskProjection.h"
#include "xt_util/time/XtSteadyClock.h"
#include "XtLogger.h"
#include "xt_util/time/CtHHMM.h"
#include "xt_core/session/XtSessionFilter.h"
#include "xt_util/time/XtTime.h"

namespace {

using xt_hhmm::valid_hhmm;

int64_t pick_closeable_by_offset(XtOffset close_offset, int64_t today_fp, int64_t yesterday_fp) {
    switch (close_offset) {
    case XtOffset::CloseToday:
        return today_fp;
    case XtOffset::CloseYesterday:
        return yesterday_fp;
    case XtOffset::Close:
    default:
        return today_fp + yesterday_fp;
    }
}

XtRiskResult pass_precheck(XtRiskStage stage) {
    XtRiskResult out;
    out.passed = true;
    out.reason_code = XtRiskReasonCode::None;
    out.stage = stage;
    out.reason_text = risk_reason_text(XtRiskReasonCode::None);
    return out;
}

XtRiskResult reject_precheck(XtRiskReasonCode code, XtRiskStage stage, const char* text = nullptr) {
    XtRiskResult out;
    out.passed = false;
    out.reason_code = code;
    out.stage = stage;
    out.reason_text = (text && text[0] != '\0') ? text : risk_reason_text(code);
    return out;
}

XtRiskResult reject_precheck(XtRiskReasonCode code, XtRiskStage stage, std::string text) {
    XtRiskResult out;
    out.passed = false;
    out.reason_code = code;
    out.stage = stage;
    out.reason_text = text.empty() ? risk_reason_text(code) : std::move(text);
    return out;
}

// 冻结后的 symbol fence：开仓面按 DeclaredUniverse 判，平仓面按 declared ∪ slot 判。
// 恢复快照带入的外来仓（未 declared 但已取得 position slot）必须能平，否则那笔仓位没有
// 平仓通道；但它不构成在该合约上开新敞口的许可——两面共用 is_registered_symbol（declared ∪
// slot）会让未声明合约的 Open 直接穿过本 fence。Auto 与 Close* 同归平仓侧，与本文件
// 其它判据（required_margin / close quota / kill-switch 穿透）的 `== XtOffset::Open` 口径一致。
std::optional<XtRiskResult> guard_symbol_fence(const xt_account::AccountStateStore& store,
                                               const xt_core::XtOrder& req) {
    if (!store.symbols_frozen()) return std::nullopt;
    const std::string_view code = req.code ? std::string_view(req.code) : std::string_view{};
    if (req.close_offset == XtOffset::Open) {
        if (store.is_declared_symbol(code)) return std::nullopt;
        return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard,
                               std::string("undeclared symbol open blocked: ") + std::string(code));
    }
    if (store.is_registered_symbol(code)) return std::nullopt;
    return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard,
                           std::string("unregistered symbol after freeze: ") + std::string(code));
}

// guard_before_submit 内部聚合账户、持仓和在途平仓资源；数量和金额均为 fixed-point。
struct SubmitResourceSnapshot {
    int64_t per_unit_margin_fp          = 0;  ///< 单位数量开仓所需保证金。
    int64_t available_cash_fp           = 0;  ///< 账户可用现金快照。
    bool    position_snapshot_fresh     = true;  ///< false 表示持仓快照超过 stale_guard 窗口。
    int64_t closeable_today_fp          = 0;
    int64_t closeable_yesterday_fp      = 0;
    int64_t outstanding_today_fp        = 0;  ///< 本进程已提交但未终结的平今数量。
    int64_t outstanding_yesterday_fp    = 0;  ///< 本进程已提交但未终结的平昨数量。
};

}  // namespace

// ── Construction and wiring ───────────────────────────────────────────────

XtLiveRiskGuardContext::XtLiveRiskGuardContext(Config cfg,
                                           std::atomic<bool>* trading_ready,
                                           std::atomic<uint32_t>* last_tick_action_time,
                                           std::atomic<bool>* td_position_ready,
                                           const std::atomic<ITradeGateway*>* td,
                                           const std::atomic<IMdGateway*>* md,
                                           const std::atomic<int64_t>* block_until_ms,
                                           OrderRateLimitFn order_rate_limit_fn)
    : cfg_(std::move(cfg)),
      trading_ready_(trading_ready),
      last_tick_action_time_(last_tick_action_time),
      td_position_ready_(td_position_ready),
      td_(td),
      md_(md),
      block_until_ms_(block_until_ms),
      order_rate_limit_fn_(std::move(order_rate_limit_fn)) {}

void XtLiveRiskGuardContext::set_strategy_pnl_aggregator(
    const ct::metrics::XtStrategyPnlAggregator* aggr) noexcept {
    // genuinely-late dep：消费线程已在跑，store-release 与 make_snapshot 的 acquire-load 配对。
    strategy_pnl_aggr_.store(aggr, std::memory_order_release);
}

void XtLiveRiskGuardContext::set_strategy_pnl_baseline(
    const ct::metrics::XtStrategyPnlBaseline* baseline) noexcept {
    // genuinely-late dep：与 aggregator 同发布语义，store-release 配 make_snapshot 的 acquire-load。
    strategy_pnl_baseline_.store(baseline, std::memory_order_release);
}

void XtLiveRiskGuardContext::set_per_strategy_risk_monitor_state(
    const xt_risk::XtRiskMonitorState* monitor_state) noexcept {
    per_strategy_monitor_state_.store(monitor_state, std::memory_order_release);
}

void XtLiveRiskGuardContext::wire(GuardDeps deps) {
    // 单发 misuse-guard：重复 wire 忽略并告警（非 fence；见 header @note）。
    if (wired_) {
        XtLogger::warn_m("RT", "XtLiveRiskGuardContext::wire() called twice; ignoring re-wire");
        return;
    }
    kill_switch_               = deps.kill_switch;
    stp_checker_               = deps.stp_checker;
    today_pnl_fn_              = std::move(deps.today_pnl_fn);
    velocity_limiter_fn_       = std::move(deps.velocity_limiter_fn);
    exchange_time_fn_          = std::move(deps.exchange_time_fn);
    catalog_                   = deps.catalog;
    account_store_submit_      = deps.account_store;
    order_book_submit_         = deps.order_book;
    last_position_snapshot_ms_ = deps.last_position_snapshot_ms;
    stale_guard_enabled_       = deps.stale_guard_enabled;
    stale_ms_                  = deps.stale_ms;
    wired_                     = true;
}

// ── Lifecycle and validation ──────────────────────────────────────────────

std::string XtLiveRiskGuardContext::validate(const XtRiskCfg& risk_cfg) const {
    if (risk_cfg.max_daily_loss > 0.0 && !today_pnl_fn_) {
        return "max_daily_loss > 0 but today_pnl_fn is not injected: "
               "DailyLossExceeded check will never trigger (silent bypass). "
               "Wire GuardDeps.today_pnl_fn (see XtLiveOrderGateway::inject_components).";
    }
    // 配置了 per-strategy DLL 时，PnL provider 是必需依赖；缺失必须启动失败，
    // 避免风控规则在 production 中静默失效。
    if (!risk_cfg.per_strategy_daily_loss_pct.empty() &&
        !strategy_pnl_aggr_.load(std::memory_order_acquire)) {
        return "per_strategy_daily_loss_pct configured but XtStrategyPnlAggregator "
               "is not injected: StrategyDailyLossExceeded check will never trigger "
               "(silent bypass). Call set_strategy_pnl_aggregator() before trading starts.";
    }
    // 启用 session gate 但未提供任何交易时段时，不能退化为 allow-all。
    if (cfg_.session_gate_enabled && cfg_.session_gate_sections.empty()) {
        return "session_gate_enabled=true but session_gate_sections is empty: "
               "all opening orders will pass the session gate (silent allow-all). "
               "Either add session_gate_sections or disable session_gate_enabled.";
    }
    return {};
}

// ── Snapshot assembly ────────────────────────────────────────────────────

XtRiskSnapshot XtLiveRiskGuardContext::make_snapshot(const xt_core::XtOrder& req,
                                                    const XtRiskEvalContext& eval_ctx) const {
    // 账户/持仓派生字段由同帧读 + 风控投影得到；caller-builds 字段在本函数内按 provider 补齐。
    XtRiskSnapshot out{};
    if (account_store_submit_) {
        const auto frame = account_store_submit_->snapshot_account_and_position(req.code);
        out = xt_risk::project_risk_snapshot(frame.account, frame.position);
        // starting_balance_fp 不进 published snapshot，必须直接读 store-frozen atomic。
        out.starting_balance_fp = account_store_submit_->starting_balance_fp();
    }
    if (today_pnl_fn_) out.today_pnl_fp = today_pnl_fn_();
    // Aggregator 以 sid string 为 key；eval_ctx 只携带 sid_id，因此这里做一次反查。
    // 缺少 aggregator 或缺少有效 sid 时字段保持 0；validate() 负责拦截已配置 DLL 的误接线。
    const auto* aggr = strategy_pnl_aggr_.load(std::memory_order_acquire);
    if (aggr && eval_ctx.strategy_id != 0) {
        if (auto* reg = xt_identity::StrategySidRegistry::global()) {
            const std::string& sid_str = reg->name(eval_ctx.strategy_id);
            if (!sid_str.empty()) {
                // today realized = lifetime realized − 日初 baseline；reject 层与 auto-liquidation
                // trigger 共用 per_sid_today_realized_fp helper 防两层口径分歧（baseline 缺失退化 lifetime）。
                const auto* baseline = strategy_pnl_baseline_.load(std::memory_order_acquire);
                out.per_strategy_today_pnl_fp =
                    xt_risk::live::per_sid_today_realized_fp(aggr, baseline, sid_str);
            }
        }
    }
    // per-strategy kill/watchdog 是 runtime atomic 状态，按 sid_id 直读，不能经账户快照发布。
    const auto* mon = per_strategy_monitor_state_.load(std::memory_order_acquire);
    if (mon && eval_ctx.strategy_id != 0) {
        out.strategy_killed = mon->per_strategy.is_strategy_killed(eval_ctx.strategy_id);
    }
    return out;
}

// ── Guard pipeline ────────────────────────────────────────────────────────

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_kill_switch_(
    const xt_core::XtOrder& req, const XtRiskEvalContext& eval_ctx) const {
    // Kill switch 是绝对门禁，不受 dry-run 或 run_input_guards 影响。
    if (kill_switch_ && kill_switch_->load(std::memory_order_acquire)) {
        // 强平平仓单必须能穿透 kill switch，否则熔断后无法降仓。
        if (!eval_ctx.is_forced_close || req.close_offset == XtOffset::Open)
            return reject_precheck(XtRiskReasonCode::KillSwitchActive, XtRiskStage::InputGuard);
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_session_and_throttle_(
    const xt_core::XtOrder& req, const XtRiskEvalContext& eval_ctx) const {
    // session gate 覆盖所有 submit 方向；closeout gate 只限制开仓。
    if (cfg_.session_gate_enabled && !cfg_.session_gate_sections.empty()) {
        const int gate_hhmm = resolve_gate_hhmm();
        if (!xt_core::in_hhmm_sections_halfopen(gate_hhmm, cfg_.session_gate_sections))
            return reject_precheck(XtRiskReasonCode::SessionClosed, XtRiskStage::InputGuard);
    }

    // 强制平仓（is_forced_close）和预判（dry_run）跳过消耗型限速，不跳过合法性校验。
    const bool skip_throttle = eval_ctx.is_forced_close || eval_ctx.dry_run;
    if (!skip_throttle) {
        if (block_until_ms_ && block_until_ms_->load(std::memory_order_acquire) > eval_ctx.now_ms)
            return reject_precheck(XtRiskReasonCode::Cooldown, XtRiskStage::InputGuard);
        if (order_rate_limit_fn_ && !order_rate_limit_fn_(cfg_.max_orders_per_sec, eval_ctx.now_ms))
            return reject_precheck(XtRiskReasonCode::RateLimited, XtRiskStage::InputGuard);
        // Velocity 先于字段合法性执行，确保非法请求也计入失控算法的速率窗口。
        if (velocity_limiter_fn_ && !velocity_limiter_fn_(req.code, req.qty_fp, eval_ctx.now_ms))
            return reject_precheck(XtRiskReasonCode::VelocityLimitExceeded, XtRiskStage::InputGuard);
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_input_validity_(
    const xt_core::XtOrder& req) const {
    // XtOrder::code 进入 char[16] 兼容路径前必须保持非空、无空白且 <= 15 字节。
    auto invalid_input = []() {
        return reject_precheck(XtRiskReasonCode::InvalidPriceOrQty, XtRiskStage::InputGuard);
    };
    if (req.code[0] == '\0' || std::strlen(req.code) > 15)    return invalid_input();
    if (std::strpbrk(req.code, " \t\n\r") != nullptr)         return invalid_input();
    if (req.price_fp <= 0 || req.qty_fp <= 0)                 return invalid_input();
    return std::nullopt;
}

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_trading_ready_and_tick_(
    const xt_core::XtOrder& req) const {
    // 必须先确认 trading_ready，再按 catalog 校验 price_tick；CTP 合约信息在 TD 查询完成后才可信。
    if (!catalog_)
        return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard, "runtime missing");
    if (!trading_ready_ || !trading_ready_->load(std::memory_order_acquire))
        return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard);
    if (account_store_submit_) {
        if (auto r = guard_symbol_fence(*account_store_submit_, req)) return *r;
    }
    {
        // catalog_ 启动后只读；price_tick 对齐检查不需要额外同步。
        const double price = xt_fp::from_fp(req.price_fp);
        if (!std::isfinite(price) || price <= 0.0)
            return reject_precheck(XtRiskReasonCode::PriceTickMismatch, XtRiskStage::InputGuard);
        const XtCommodityInfo* info = catalog_->find(req.code);
        const double tick = (info && std::isfinite(info->price_tick) && info->price_tick > 0.0)
                                ? info->price_tick : 0.0;
        if (tick > 0.0) {
            const double units = price / tick;
            if (std::fabs(units - std::round(units)) > 1e-6)
                return reject_precheck(XtRiskReasonCode::PriceTickMismatch, XtRiskStage::InputGuard);
        }
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtLiveRiskGuardContext::guard_td_and_closeout_(
    const xt_core::XtOrder& req) const {
    // Open/Close 均须等本 session 持仓查询 ready；否则 broker 分腿仍可能读到 0，
    // 策略会把未知持仓误判为空仓并开出反向腿。
    if (td_position_ready_ && !td_position_ready_->load(std::memory_order_acquire))
        return reject_precheck(XtRiskReasonCode::TdRecovering, XtRiskStage::InputGuard);

    // closeout gate 仅限制开仓；时间源与 session gate 共用 resolve_gate_hhmm()。
    if (cfg_.closeout_enabled && req.close_offset == XtOffset::Open) {
        const int gate_hhmm = resolve_gate_hhmm();
        if (cfg_.session_gate_enabled && !cfg_.session_gate_sections.empty()
            && !xt_core::in_hhmm_sections_halfopen(gate_hhmm, cfg_.session_gate_sections))
            return reject_precheck(XtRiskReasonCode::CloseoutNoOpen, XtRiskStage::InputGuard);
        if (xt_core::in_closeout_no_open_window_closed(gate_hhmm, cfg_.closeout_windows))
            return reject_precheck(XtRiskReasonCode::CloseoutNoOpen, XtRiskStage::InputGuard);
    }

    if (td_) {
        const ITradeGateway* td = td_->load(std::memory_order_acquire);
        if (td && td->is_recovering())
            return reject_precheck(XtRiskReasonCode::TdRecovering, XtRiskStage::InputGuard);
    }
    return std::nullopt;
}

XtRiskResult XtLiveRiskGuardContext::precheck_input(const xt_core::XtOrder& req,
                                                     const XtRiskEvalContext& eval_ctx) const {
    // input guard reject pipeline；kill switch 不受 run_input_guards 控制 (绝对门禁)。
    if (auto r = guard_kill_switch_(req, eval_ctx)) return *r;
    if (!eval_ctx.run_input_guards) return pass_precheck(XtRiskStage::InputGuard);
    if (auto r = guard_session_and_throttle_(req, eval_ctx)) return *r;
    if (auto r = guard_input_validity_(req)) return *r;
    if (auto r = guard_trading_ready_and_tick_(req)) return *r;
    if (auto r = guard_td_and_closeout_(req)) return *r;
    // STP 必须保持 O(1) 读侧检查；不得在 submit 热路径遍历 open orders。
    // @note STP 有意 gate 平仓 / force-close 单：self-match 防护 offset-agnostic（穿越己方
    //       对侧挂单不区分开平）。瞬时拒由终态重建释放边界 + PanelForceExitLatch 重试自愈。
    if (stp_checker_ && stp_checker_->would_self_trade(req))
        return reject_precheck(XtRiskReasonCode::SelfTradeDetected, XtRiskStage::InputGuard);
    return pass_precheck(XtRiskStage::InputGuard);
}

XtRiskResult XtLiveRiskGuardContext::precheck_capability(const xt_core::XtOrder& req,
                                                           const XtRiskEvalContext& /*eval_ctx*/) const {
    ITradeGateway* td = td_ ? td_->load(std::memory_order_acquire) : nullptr;
    if (!td) {
        return reject_precheck(XtRiskReasonCode::TdNotInitialized, XtRiskStage::CapabilityGuard);
    }

    const ITradeGateway::Capabilities td_caps = td->capabilities();
    if (req.close_offset == XtOffset::CloseToday && !td_caps.supports_close_today) {
        return reject_precheck(XtRiskReasonCode::TdCapCloseTodayUnsupported,
                               XtRiskStage::CapabilityGuard);
    }
    if ((req.tif == XtTimeInForce::IOC || req.tif == XtTimeInForce::FOK) && !td_caps.supports_ioc_fok) {
        return reject_precheck(XtRiskReasonCode::TdCapIocFokUnsupported,
                               XtRiskStage::CapabilityGuard);
    }
    if (cfg_.require_l2_orderflow) {
        // MD capability 是启动后静态属性；并发首次写入同一缓存值是良性竞争。
        int8_t l2 = cached_l2_support_.load(std::memory_order_acquire);
        if (l2 < 0) {
            const IMdGateway* md = md_ ? md_->load(std::memory_order_acquire) : nullptr;
            if (!md) {
                return reject_precheck(XtRiskReasonCode::TdCapL2OrderFlowUnsupported,
                                       XtRiskStage::CapabilityGuard);
            }
            l2 = md->capabilities().supports_l2_orderflow ? int8_t{1} : int8_t{0};
            cached_l2_support_.store(l2, std::memory_order_release);
        }
        if (!l2) {
            return reject_precheck(XtRiskReasonCode::TdCapL2OrderFlowUnsupported,
                                   XtRiskStage::CapabilityGuard);
        }
    }
    return pass_precheck(XtRiskStage::CapabilityGuard);
}

namespace {
/// @brief 平仓单 close quota 检查：avail/raw closeable 双口径区分 CloseSyncPending vs 真不足。
std::optional<XtRiskResult> check_close_quota(const xt_core::XtOrder& req,
                                              const SubmitResourceSnapshot& snap) {
    auto avail = [](int64_t total, int64_t outstanding) noexcept -> int64_t {
        return total > outstanding ? total - outstanding : int64_t{0};
    };
    const int64_t avail_today_fp     = avail(snap.closeable_today_fp, snap.outstanding_today_fp);
    const int64_t avail_yest_fp      = avail(snap.closeable_yesterday_fp, snap.outstanding_yesterday_fp);
    const int64_t raw_closeable_fp   = pick_closeable_by_offset(req.close_offset, snap.closeable_today_fp, snap.closeable_yesterday_fp);
    const int64_t avail_closeable_fp = pick_closeable_by_offset(req.close_offset, avail_today_fp, avail_yest_fp);

    if (req.qty_fp > avail_closeable_fp + 1) {
        if (req.qty_fp <= raw_closeable_fp + 1)
            return reject_precheck(XtRiskReasonCode::CloseSyncPending, XtRiskStage::PositionPrecheck);
        if (req.close_offset == XtOffset::CloseToday)
            return reject_precheck(XtRiskReasonCode::InsufficientCloseToday, XtRiskStage::PositionPrecheck);
        if (req.close_offset == XtOffset::CloseYesterday)
            return reject_precheck(XtRiskReasonCode::InsufficientCloseYesterday, XtRiskStage::PositionPrecheck);
        return reject_precheck(XtRiskReasonCode::InsufficientAvailablePosition, XtRiskStage::PositionPrecheck);
    }
    return std::nullopt;
}
}  // namespace

XtRiskResult XtLiveRiskGuardContext::guard_before_submit(const xt_core::XtOrder& req,
                                                          const XtRiskEvalContext& /*eval_ctx*/) const {
    if (!account_store_submit_ || !order_book_submit_ || !catalog_)
        return reject_precheck(XtRiskReasonCode::TradingNotReady, XtRiskStage::InputGuard, "runtime missing");
    // account_store_submit_ 非空已在上一条判据确认。
    if (auto r = guard_symbol_fence(*account_store_submit_, req)) return *r;

    const uint64_t now_ms = stale_guard_enabled_
                                ? static_cast<uint64_t>(XtSteadyClock::steady_now_ms())
                                : 0;

    // closeable 与 available_cash 是同一次提交判据的两半，必须同帧：分别调
    // snapshot_position() + snapshot_account() 会横跨一次发布，在持仓快照重建
    // 窗口里读到「重建前的持仓 + 重建后的资金」这种不存在的组合。
    // now_ms 先于本次读采样，与 pos.broker_frozen_last_update_ms 的 staleness
    // 比较方向保持原样（读到的时间戳不会晚于 now_ms）。
    const auto frame =
        account_store_submit_->snapshot_account_and_position(req.code ? req.code : "");

    const XtCommodityInfo* info = catalog_->find(req.code);

    // 本函数线性构建资源视图并立即消费，避免在 submit 热路径额外分配。
    // margin 归 catalog (own instrument cost 参数)；guard 只读 per-unit margin，自己按 qty 缩放。
    SubmitResourceSnapshot snap;
    snap.per_unit_margin_fp = catalog_->per_unit_margin_fp(info, req.code, req.price_fp);

    bool snapshot_fresh = true;
    if (stale_guard_enabled_) {
        const uint64_t last_ms = last_position_snapshot_ms_
                                     ? last_position_snapshot_ms_->load(std::memory_order_acquire)
                                     : 0;
        if (last_ms == 0 || now_ms < last_ms) {
            snapshot_fresh = false;
        } else {
            snapshot_fresh = (now_ms - last_ms) <= static_cast<uint64_t>(stale_ms_);
        }
    }
    snap.position_snapshot_fresh = snapshot_fresh;
    // broker_frozen 只在 venue 明确推送该字段且快照未 stale 时参与 close quota 取严。
    // 时效窗常量 = 类常量 kBrokerFrozenStaleMs（config validator 引同一 SSOT）。
    int64_t broker_frozen_fp = 0;
    const bool venue_has_broker_frozen =
        (info && info->has_broker_frozen_push);
    if (snapshot_fresh) {
        const auto& pos = frame.position;
        const auto closeable = xt_account::closeable_view(pos, req.is_buy);
        snap.closeable_today_fp     = closeable.today_fp;
        snap.closeable_yesterday_fp = closeable.yesterday_fp;

        if (!pos.closeable_known) {
            // closeable split 不可信时必须 fail-closed，避免用零值快照误报 InsufficientClose*。
            return reject_precheck(XtRiskReasonCode::PositionStateUnknown,
                                   XtRiskStage::PositionPrecheck);
        }
        if (venue_has_broker_frozen) {
            // close-short 读 short_frozen；close-long 读 long_frozen。
            broker_frozen_fp = req.is_buy ? pos.broker_short_frozen_fp
                                           : pos.broker_long_frozen_fp;
            if (broker_frozen_fp < 0) broker_frozen_fp = 0;
            if (broker_frozen_fp > 0 && stale_guard_enabled_) {
                const auto last_update_ms = static_cast<uint64_t>(pos.broker_frozen_last_update_ms);
                if (last_update_ms == 0 || now_ms < last_update_ms ||
                    (now_ms - last_update_ms) > kBrokerFrozenStaleMs) {
#ifdef XT_HAVE_METRICS
                    // label 语义 = 广义 broker-frozen 源（CTP push + XTP 周期 requery
                    // 派生均计入）；本 context 无 venue 维度可注入，拆 label 需走
                    // /xt-metrics 加维度，见 BACKLOG。
                    ct::metrics::XtMetricRegistry::instance()
                        .inc_broker_frozen_stale_total("ctp");
#endif
                    broker_frozen_fp = 0;
                }
            }
        }
    }
    snap.available_cash_fp = frame.account.available_cash;

    if (snap.position_snapshot_fresh) {
        // 本地 outstanding 覆盖本进程已发 close 单；终态回报负责解扣。
        order_book_submit_->accumulate_close_outstanding_fp(
            req.code ? req.code : "", req.is_buy,
            snap.outstanding_today_fp, snap.outstanding_yesterday_fp);

        // 双源取严：broker_frozen 超出本地 outstanding 的部分按保守冻结量扣减。
        // 可能造成误拒，但避免恢复期本地 book 低估时漏拒超平。
        const int64_t local_total_outstanding =
            snap.outstanding_today_fp + snap.outstanding_yesterday_fp;
        if (broker_frozen_fp > local_total_outstanding) {
            const int64_t delta = broker_frozen_fp - local_total_outstanding;
            // 阈值口径见 XtRiskCfg.h::kFrozenDisagreementEpsilonFp（live/backtest 共用 SSOT）。
            if (delta > kFrozenDisagreementEpsilonFp) {
#ifdef XT_HAVE_METRICS
                ct::metrics::XtMetricRegistry::instance()
                    .inc_close_quota_disagreement_total("ctp");
#endif
            }
            // broker_frozen 不区分今昨；计入 yesterday 对 T+1 约束更保守。
            snap.outstanding_yesterday_fp += delta;
        }
    }
    const int64_t required_margin_fp =
        req.close_offset == XtOffset::Open ? xt_fp::mul(snap.per_unit_margin_fp, req.qty_fp) : 0;

    // 无论通过或拒绝，调用方都需要 margin 字段做资金冻结与拒单输出。
    auto with_margin = [&](XtRiskResult r) noexcept -> XtRiskResult {
        r.required_margin_fp = required_margin_fp;
        r.per_unit_margin_fp = snap.per_unit_margin_fp;
        r.available_cash_fp  = snap.available_cash_fp;
        return r;
    };

    if (!snap.position_snapshot_fresh)
        return reject_precheck(XtRiskReasonCode::PositionSyncStale, XtRiskStage::PositionPrecheck);

    if (req.close_offset != XtOffset::Open) {
        if (auto r = check_close_quota(req, snap)) return *r;
    }

    if (required_margin_fp > 0 && snap.available_cash_fp < required_margin_fp)
        return with_margin(reject_precheck(XtRiskReasonCode::InsufficientMargin, XtRiskStage::MarginPrecheck));

    return with_margin(pass_precheck(XtRiskStage::MarginPrecheck));
}

// ── Private helpers ──────────────────────────────────────────────────────

int XtLiveRiskGuardContext::resolve_gate_hhmm() const {
    if (exchange_time_fn_) {
        return exchange_time_fn_();
    }
    if (last_tick_action_time_) {
        const uint32_t action_tm = last_tick_action_time_->load(std::memory_order_relaxed);
        // 0 或非法 HHMM 表示尚无可信 tick 时间，必须回退 OS 时间。
        const int tick_hhmm = action_tm == 0 ? -1 : static_cast<int>(action_tm / 100000);
        return valid_hhmm(tick_hhmm) ? tick_hhmm : xt_time::local_hhmm_now();
    }
    return xt_time::local_hhmm_now();
}
