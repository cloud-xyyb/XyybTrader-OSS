// PanelHelpers.cpp — cross-engine panel helper implementation.
//
// HFT / CTA / SEL destructive panel RPC 共享这里的 TD readiness、order-book
// 和 risk gate 入口，避免各 runner 复制控制面逻辑。

#include "PanelHelpers.h"

#include <atomic>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>

#include "api/ITradeGateway.h"
#include "xt_identity/OrderAttributionStore.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "XtLiveOrderGateway.h"     // xt_live/execution PUBLIC include path
#include "api/XtRunnerSignals.h"        // xt_live/common PUBLIC include path
#include "XtLiveConfig.h"          // run_overlap_check_for_config
#include "xt_core/order/XtOrderTypes.h"  // XtTimeInForce / XtOffset (via XtOffset.h) / xt_fp::to_fp (via XtFixedPoint.h)
#include "api/ExecFlags.h"        // exec_none()
#include "xt_live/gateway/binance/refdata/BinanceExchangeInfo.h"
#include "xt_live/control/CloseoutPricing.h"  // aggressive_close_price_fp / legal_close_price_fp (shared)
#include "xt_live/control/ForceFlatSubmitFence.h"  // force-flat submit 同步 fence（over-close 缝挡板）
#include "xt_util/time/XtSteadyClock.h"            // fence now（函数内自取，签名不变）
#include "xt_config/catalog/XtCommodityCatalog.h"        // ctx.catalog->find(symbol)->price_tick
#include "xt_risk/engine/XtRiskMonitor.h"        // wire_per_strategy_risk_state
#include "xt_risk/engine/XtRiskMonitorState.h"   // per_strategy.set_strategy_killed (init-failure latch)
#include "XtDeclaredSymbolRegistry.h"        // routing registry for overlap validation
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "XtLogger.h"
#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include <chrono>
#include <thread>

namespace xt_live::control {

namespace {

// TD 可用性检查:恢复中时不触发强平,避免与恢复流程冲突。
// 与 HftCloseoutGate.cpp anon-ns 的 load_td_for_runner_force_close 同实现,
// anon-ns 隔离不冲突。
ITradeGateway* load_td_for_panel_request(ITradeGateway* td,
                                         const std::atomic<bool>& trading_ready) {
    if (!td) return nullptr;
    if (!trading_ready.load(std::memory_order_acquire)) return nullptr;
    if (td->is_recovering()) return nullptr;
    return td;
}

}  // namespace

// ---------------------------------------------------------------------------
// cancel_all_panel_request
// ---------------------------------------------------------------------------
std::size_t cancel_all_panel_request(XtPanelContext& ctx) {
    if (!ctx.td_gateway || !ctx.order_book || !ctx.account_store
        || !ctx.runtime_shared_state) {
        return 0;
    }
    ITradeGateway* td = load_td_for_panel_request(ctx.td_gateway,
        ctx.runtime_shared_state->trading_ready);
    if (!td) return 0;

    std::vector<uint64_t> cancel_ids;
    ctx.order_book->collect_closeout_requests(cancel_ids);
    for (uint64_t id : cancel_ids) td->cancel_order(id);
    return cancel_ids.size();
}

// ---------------------------------------------------------------------------
// flatten_all_panel_request
// ---------------------------------------------------------------------------
FlattenAllPanelResult flatten_all_panel_request(XtPanelContext& ctx) {
    FlattenAllPanelResult out;
    if (!ctx.td_gateway || !ctx.order_book || !ctx.account_store
        || !ctx.runtime_shared_state) {
        out.failed_symbols = 1;
        out.error = "control context incomplete";
        return out;
    }

    ITradeGateway* td = load_td_for_panel_request(ctx.td_gateway,
        ctx.runtime_shared_state->trading_ready);
    if (!td) {
        out.failed_symbols = 1;
        out.error = "td unavailable / recovering / trading_not_ready";
        return out;
    }

    out.cancelled_orders = cancel_all_panel_request(ctx);

    std::unordered_set<std::string> seen;
    for (const auto& code : ctx.account_store->snapshot_tracked_codes()) {
        if (code.empty() || !seen.insert(code).second) {
            continue;
        }
        const auto position = ctx.account_store->snapshot_position(code);
        if (position.long_qty <= 0 && position.short_qty <= 0) {
            continue;
        }

        bool hedge_warn = false;
        uint64_t force_oid = 0;
        const ForceExitOutcome outcome = force_exit_panel_request(
            ctx, code, hedge_warn, &force_oid);
        switch (outcome) {
            case ForceExitOutcome::ForceClosed:
                ++out.closed_positions;
                break;
            case ForceExitOutcome::CancelledOnly:
            case ForceExitOutcome::AlreadyForceFlat:
                ++out.deferred_symbols;
                break;
            case ForceExitOutcome::NoPosition:
                break;
            case ForceExitOutcome::NoTd:
            case ForceExitOutcome::NoMarkPrice:
            case ForceExitOutcome::SubmitFailed:
            default:
                ++out.failed_symbols;
                if (!out.error.empty()) out.error += "; ";
                out.error += code + ": force_exit outcome=" +
                             std::to_string(static_cast<int>(outcome));
                break;
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// cancel_one_panel_request
// ---------------------------------------------------------------------------
std::pair<bool, bool> cancel_one_panel_request(XtPanelContext& ctx,
                                                const std::string& symbol,
                                                const std::string& oid_str,
                                                std::string& err_out) {
    if (!ctx.td_gateway || !ctx.order_book || !ctx.account_store
        || !ctx.runtime_shared_state) {
        err_out = "control context incomplete (td/order_book/account_store null)";
        return {false, false};
    }
    ITradeGateway* td = load_td_for_panel_request(ctx.td_gateway,
        ctx.runtime_shared_state->trading_ready);
    if (!td) {
        err_out = "td unavailable / recovering / trading_not_ready";
        return {false, false};
    }
    if (oid_str.empty()) {
        err_out = "order_id empty";
        return {false, false};
    }

    uint64_t target_oid = 0;
    try {
        target_oid = std::stoull(oid_str);
    } catch (...) {
        err_out = "order_id not parseable as uint64";
        return {false, false};
    }

    std::vector<uint64_t> working_ids;
    ctx.order_book->collect_working_ids_for_code(symbol.c_str(), working_ids);

    bool was_open = false;
    for (uint64_t id : working_ids) {
        if (id == target_oid) {
            was_open = true;
            break;
        }
    }
    if (was_open) {
        td->cancel_order(target_oid);
    }
    return {true, was_open};
}

// ---------------------------------------------------------------------------
// force_exit_panel_request
// ---------------------------------------------------------------------------
ForceExitOutcome force_exit_panel_request(XtPanelContext& ctx,
                                          const std::string& symbol,
                                          bool& hedge_warn_out,
                                          uint64_t* force_order_id_out) {
    return force_exit_panel_request(ctx, symbol, std::string{}, hedge_warn_out, force_order_id_out);
}

ForceExitOutcome force_exit_panel_request(XtPanelContext& ctx,
                                          const std::string& symbol,
                                          const std::string& strategy_id,
                                          bool& hedge_warn_out,
                                          uint64_t* force_order_id_out) {
    if (force_order_id_out) *force_order_id_out = 0;
    hedge_warn_out = false;
    if (!ctx.td_gateway || !ctx.order_book || !ctx.account_store
        || !ctx.runtime_shared_state) {
        return ForceExitOutcome::NoTd;
    }
    ITradeGateway* td = load_td_for_panel_request(ctx.td_gateway,
        ctx.runtime_shared_state->trading_ready);
    if (!td) return ForceExitOutcome::NoTd;

    const bool strategy_scoped = !strategy_id.empty();
    StrategySid scope_sid = StrategySid::legacy();
    if (strategy_scoped) {
        const auto* sid_registry = xt_identity::StrategySidRegistry::global();
        const uint32_t sid_id = sid_registry ? sid_registry->lookup(strategy_id) : 0;
        if (sid_id == xt_identity::StrategySidRegistry::kNoSidId) {
            XtLogger::warn_m("control",
                "panel.force_exit strategy-scoped request rejected: unknown sid={} symbol={}",
                strategy_id, symbol);
            return ForceExitOutcome::SubmitFailed;
        }
        scope_sid = StrategySid::from_raw(sid_id);
    }

    // (1a) 已删（盲审 fold-3）：旧 held 只读早出会在 held 窗口内压制 (2) 段撤单轮
    // ——窗口内新出现的 working order 拖到窗口过期才能撤。原子性由 (4a) 独立承担，
    // held 窗口内的请求照常走撤单轮，仅 submit 被闸。

    // (1b) 真·force-flat 已在途 → 跳过避免双 submit。用窄口径（is_genuine_*）：策略普通平仓
    // 挂单不算 force-flat，故不阻断 operator 接管——落到 (2) cancel-first 撤掉策略 passive
    // 平仓单后下帧强平，与 runner closeout cancel-first 接管语义对称。
    // Strategy-scoped 只与同 sid 或 global 真·force-flat 冲突；sibling sid 仍可平自己的量。
    if (!strategy_scoped) {
        if (ctx.order_book->is_genuine_force_flat_pending(symbol.c_str())) {
            return ForceExitOutcome::AlreadyForceFlat;
        }
    } else {
        if (ctx.order_book->has_genuine_global_force_flat_pending(symbol.c_str())
            || ctx.order_book->is_genuine_force_flat_pending_for_sid(symbol.c_str(), scope_sid)) {
            return ForceExitOutcome::AlreadyForceFlat;
        }
    }

    // (2) 该 symbol 有 working order → 先撤后平, 本帧只撤
    std::vector<uint64_t> cancel_ids;
    if (strategy_scoped) {
        ctx.order_book->collect_working_ids_for_code_and_sid(symbol.c_str(), scope_sid, cancel_ids);
    } else {
        ctx.order_book->collect_working_ids_for_code(symbol.c_str(), cancel_ids);
    }
    if (!cancel_ids.empty()) {
        for (uint64_t id : cancel_ids) td->cancel_order(id);
        return ForceExitOutcome::CancelledOnly;
    }

    // (3) 查 net position — account_store (the single per-symbol truth). Under the
    // hard-enforced 1:1, the account_store per-code position IS the scoped strategy's
    // position, so strategy-scoped and runner-wide force-exit flatten the same quantity
    // (the scoped path still cancels + closeable-clamps per-sid via scope_sid above/below).
    const auto position = ctx.account_store->snapshot_position(symbol);
    int64_t long_fp  = position.long_qty;
    int64_t short_fp = position.short_qty;
    // 定价读 venue 真值，不用帧内 position.mark_price：后者是估值口径（venue 优先、
    // 缺失回退持仓成本价），按成本价挂强平单等于凭空定一个与盘口无关的价格。
    // venue 缺失 → NoMarkPrice 显式拒，把处置交回 operator。
    const int64_t mark_fp = ctx.account_store->venue_mark_price(symbol);
    if (long_fp <= 0 && short_fp <= 0) return ForceExitOutcome::NoPosition;
    if (mark_fp <= 0) return ForceExitOutcome::NoMarkPrice;

    // hedge 模式 (long+short 都 > 0) 警告 — 只关较大边
    if (long_fp > 0 && short_fp > 0) hedge_warn_out = true;

    const bool sell_to_close = (long_fp >= short_fp);
    int64_t qty_fp = sell_to_close ? long_fp : short_fp;
    if (strategy_scoped) {
        const bool buy_to_close = !sell_to_close;
        const auto closeable = xt_account::closeable_view(position, buy_to_close);
        qty_fp = std::min(qty_fp, closeable.total_fp);
    }
    if (qty_fp <= 0) return ForceExitOutcome::NoPosition;

    // (4) IOC@mark + Close offset (跟 try_runner_force_close 同 pattern)
    const bool buy_to_close = !sell_to_close;
    int64_t price_tick_fp = 0;
    if (ctx.catalog) {
        const auto* info = ctx.catalog->find(symbol.c_str());
        if (info && info->price_tick > 0.0) price_tick_fp = order_exec_to_fp(info->price_tick);
    }
    const int64_t close_price_fp = legal_close_price_fp(
        symbol, aggressive_close_price_fp(mark_fp, buy_to_close), buy_to_close, price_tick_fp);
    // (4a) 权威闸：submit 前原子 claim（单锁段 check+set，并发 caller 恰一胜出）。
    // generation 归属：claim 过期后被他方重新 claim 时，先前持有者的 release
    // 不得误删新窗口（盲审 fold-3）。
    uint64_t fence_gen = 0;
    if (ctx.force_flat_fence) {
        fence_gen = ctx.force_flat_fence->try_acquire(symbol, XtSteadyClock::steady_now_ms());
        if (fence_gen == 0) return ForceExitOutcome::AlreadyForceFlat;
    }
    if (strategy_scoped) {
        xt_identity::OrderAttributionStore::set_thread_submit_strategy_sid(scope_sid);
    }
    // 直发 TD 不经 gateway（F02 D5 豁免面）：force-exit 从真实持仓构造硬 Close 单，
    // 不产生 slice、只平已有仓不开新敞口，不破坏「未声明合约不得开仓」不变量；
    // 定价须走 venue-only（D4）。
    const uint64_t oid = td->submit_order(symbol.c_str(),
                                          buy_to_close,
                                          close_price_fp,
                                          qty_fp,
                                          XtTimeInForce::IOC,
                                          "panel_force_exit",
                                          XtOffset::Close,
                                          exec_none());
    if (strategy_scoped) {
        xt_identity::OrderAttributionStore::set_thread_submit_strategy_sid(StrategySid::legacy());
    }
    if (oid == 0) {
        // claim 让出：本次未产生在途 force-flat，窗口不应挡后续重试；
        // 按 generation 归属释放，不碰他方新窗口。
        if (ctx.force_flat_fence) ctx.force_flat_fence->release(symbol, fence_gen);
        return ForceExitOutcome::SubmitFailed;
    }
    if (strategy_scoped) {
        ctx.order_book->record_force_flat_order(oid, symbol.c_str(), buy_to_close,
                                                qty_fp, XtOffset::Close, scope_sid);
    } else {
        // runner-wide emergency scope：显式写出 legacy sid（0 = 对所有 sid 生效）。
        ctx.order_book->record_force_flat_order(oid, symbol.c_str(), buy_to_close,
                                                qty_fp, XtOffset::Close,
                                                StrategySid::legacy());
    }
    if (force_order_id_out) *force_order_id_out = oid;
    return ForceExitOutcome::ForceClosed;
}

// ---------------------------------------------------------------------------
// manual_order_panel_request
// ---------------------------------------------------------------------------
ManualOrderOutcome manual_order_panel_request(XtPanelContext& ctx,
                                              const ManualOrderRequest& req,
                                              std::string& reject_reason_out) {
    reject_reason_out.clear();
    // D8-③：manual 提交前置拒绝的结构化可见性 —— 每个 gateway 之前的早退拒绝分支落
    // keyed WARN（reason text，非纯 code）。侧道日志专用，**不**写 reject_reason_out
    // （守 caller 的 reject.empty() 契约，见 XtRunnerPanelControl.cpp）。
    auto reject_log = [&](ManualOrderOutcome outcome, const char* reason) -> ManualOrderOutcome {
        XtLogger::warn_m("control",
            "panel.manual_order reject: request_id={} venue={} symbol={} reason={}",
            req.request_id, req.venue, req.symbol, reason);
        return outcome;
    };
    if (!ctx.td_gateway || !ctx.account_store || !ctx.live_order_gateway
        || !ctx.runtime_shared_state) {
        return reject_log(ManualOrderOutcome::NoTd, "context_incomplete");
    }
    ITradeGateway* td = load_td_for_panel_request(ctx.td_gateway,
        ctx.runtime_shared_state->trading_ready);
    if (!td) return reject_log(ManualOrderOutcome::NoTd, "td_not_ready");

    // (1) side 解析
    std::string side_upper;
    side_upper.reserve(req.side.size());
    for (char c : req.side) side_upper.push_back(static_cast<char>(::toupper(c)));
    bool is_buy;
    if      (side_upper == "BUY")  is_buy = true;
    else if (side_upper == "SELL") is_buy = false;
    else return reject_log(ManualOrderOutcome::InvalidSide, "invalid_side");

    // (2) type 解析
    std::string type_upper;
    type_upper.reserve(req.type.size());
    for (char c : req.type) type_upper.push_back(static_cast<char>(::toupper(c)));
    const bool is_market = (type_upper == "MARKET");
    if (!is_market && type_upper != "LIMIT")
        return reject_log(ManualOrderOutcome::InvalidType, "invalid_type");

    // (3) qty 解析(double → fp)
    char* end = nullptr;
    const double qty_d = std::strtod(req.qty.c_str(), &end);
    if (end == req.qty.c_str() || qty_d <= 0.0 || !std::isfinite(qty_d)) {
        return reject_log(ManualOrderOutcome::InvalidQty, "invalid_qty");
    }
    const int64_t qty_fp = xt_fp::to_fp(qty_d);
    if (qty_fp <= 0) return reject_log(ManualOrderOutcome::InvalidQty, "invalid_qty_fp");

    // (4) price 解析:MARKET 用 venue 真实行情价;LIMIT 用用户给的
    // 与 force-exit 同一契约：出单价只认 venue 真值，帧内 PositionSnapshot::mark_price
    // 是估值口径（可能是持仓成本价 fallback），拿来开仓会按成本价成交。
    int64_t price_fp = 0;
    XtTimeInForce tif;
    if (is_market) {
        price_fp = ctx.account_store->venue_mark_price(req.symbol);
        if (price_fp <= 0) return reject_log(ManualOrderOutcome::InvalidPrice, "invalid_price_no_mark");
        tif = XtTimeInForce::IOC;
    } else {
        end = nullptr;
        const double price_d = std::strtod(req.price.c_str(), &end);
        if (end == req.price.c_str() || price_d <= 0.0 || !std::isfinite(price_d)) {
            return reject_log(ManualOrderOutcome::InvalidPrice, "invalid_price_parse");
        }
        price_fp = xt_fp::to_fp(price_d);
        if (price_fp <= 0) return reject_log(ManualOrderOutcome::InvalidPrice, "invalid_price_fp");
        tif = XtTimeInForce::GTC;
    }

    // D2 条件 stamp：router 模式（attribution_router_mode）且请求 strategy_id 为空时，
    // 改写归属为保留名 "_manual"（动作归因账簿）；显式非空 strategy_id 保持既有定向归属
    // 语义。非 router 模式不 stamp → 零 delta。
    std::string effective_strategy_id = req.strategy_id;
    if (ctx.attribution_router_mode && effective_strategy_id.empty()) {
        effective_strategy_id = xt_identity::StrategySidRegistry::kManualStrategyId;
    }

    if (!effective_strategy_id.empty()) {
        // 保留名 "_manual" 因 D1 内建 lookup 恒返 kManualSidId（非 kNoSidId），此校验天然通过。
        const auto* sid_registry = xt_identity::StrategySidRegistry::global();
        if (!sid_registry ||
            sid_registry->lookup(effective_strategy_id)
                == xt_identity::StrategySidRegistry::kNoSidId) {
            return reject_log(ManualOrderOutcome::InvalidStrategy, "invalid_strategy");
        }
    }

    // (5) 提交走 full risk gate(critical:绝不 bypass)
    const std::string user_tag = "panel_manual_" + req.request_id;
    xt_core::XtOrderRequest order_req;
    order_req.order.set_code(req.symbol.c_str());
    order_req.order.is_buy = is_buy;
    order_req.order.price_fp = price_fp;
    order_req.order.qty_fp = qty_fp;
    order_req.order.tif = tif;
    order_req.order.set_tag(user_tag.c_str());
    order_req.order.close_offset = XtOffset::Open;  // 新开仓
    order_req.strategy_id = effective_strategy_id;
    order_req.source = xt_core::XtOrderSource::Manual;
    const xt_runtime::SubmitResult r = ctx.live_order_gateway->submit(order_req, exec_none());
    const char* sid_disp = effective_strategy_id.empty() ? "-" : effective_strategy_id.c_str();
    if (!r.accepted()) {
        reject_reason_out = std::to_string(static_cast<int>(r.reject_reason));
        // D8-③：manual 路径提交失败可见性 —— 结构化 journal（request_id + reason text +
        // reject 原因码）。与前置早退分支同 grep key `panel.manual_order reject`。
        XtLogger::warn_m("control",
            "panel.manual_order reject: request_id={} venue={} symbol={} sid={} reason=risk_rejected reason_code={}",
            req.request_id, req.venue, req.symbol, sid_disp, reject_reason_out);
        return ManualOrderOutcome::RiskRejected;
    }
    // 提交回执（server-side lifecycle 起点；终态回执见 engine finish_terminal / D8-④）。
    XtLogger::info_m("control",
        "panel.manual_order submitted: request_id={} venue={} symbol={} sid={} order_id={}",
        req.request_id, req.venue, req.symbol, sid_disp,
        static_cast<unsigned long long>(r.order_id));
    return ManualOrderOutcome::Submitted;
}

// cross-Runner per-strategy risk state wire helper。
// 任一 ptr nullptr → 跳过相关 setter (跟 BT 路径同 fail-safe pattern)。
void wire_per_strategy_risk_state(
    xt_risk::XtRiskMonitor* risk_monitor,
    xt_account::AccountStateStore* account_store,
    XtLiveOrderGateway* order_gateway,
    const xt_risk::XtRiskMonitorState* risk_monitor_state) noexcept {
    // risk_monitor.on_session_begin dedup 块内调
    //   account_store.on_session_begin_hook(equity_fp) freeze starting_balance。
    if (risk_monitor && account_store) {
        risk_monitor->set_account_store(account_store);
    }
    // GuardContext.make_snapshot 走 monitor.per_strategy 反查填
    //   snapshot.strategy_killed/blocked → engine reject StrategyKillSwitchActive/BlockedByWatchdog。
    if (order_gateway && risk_monitor_state) {
        order_gateway->set_per_strategy_risk_monitor_state(risk_monitor_state);
    }
}

// 进程内累计：被 fail-closed disable + kill-latch 的策略 on_init 失败次数（TU-scope）。
namespace {
std::atomic<uint64_t> g_strategy_init_failure_latched_total_{0};
/// 置位后回读没读到 true 的次数（该 sid 的开仓闸实际没上）。
std::atomic<uint64_t> g_strategy_init_failure_latch_unverified_total_{0};
}  // namespace

std::function<void(uint32_t, const std::string&)>
make_strategy_init_failure_handler(
    xt_risk::XtRiskMonitorState* risk_monitor_state) noexcept {
    return [risk_monitor_state](uint32_t sid_id, const std::string& name) {
        const uint64_t total =
            g_strategy_init_failure_latched_total_.fetch_add(1, std::memory_order_relaxed) + 1;
        // per-sid kill latch：拒新开仓，平仓单天然通行（close bypass）。sid_id==0
        // （reserved/no-sid）或 state 缺失 → 仅日志 + counter。
        //
        // set_strategy_killed 对越界 sid 是静默 no-op，而槽位数组在装配期通常还没
        // init（容量 0）——「调过 setter」不等于「闸上了」。故置位后必须回读：只有
        // 读回 true 才敢记「已置位」，否则记 error 并计数，让 runner 侧的 post-init
        // 补置缺失能在日志/counter 上直接暴露，而不是被一条报喜的 critical 掩盖。
        bool attempted = false;
        bool verified  = false;
        if (risk_monitor_state && sid_id != 0) {
            attempted = true;
            risk_monitor_state->per_strategy.set_strategy_killed(sid_id, true);
            verified = risk_monitor_state->per_strategy.is_strategy_killed(sid_id);
        }
        const char* latch_state = !attempted ? "跳过(sid=0 或无 risk state)"
                                             : (verified ? "已置位" : "置位失败(回读为假)");
        // recovery 持仓在本回调触发点（worker 启动 / 装配期）尚未回灌，无法按 sid 读净仓；
        // 折入基础 critical，由 operator 据日志核查该 sid 是否残留恢复持仓（无人管理）。
        XtLogger::critical(
            "[INIT-FAILURE] strategy '{}' (sid_id={}) on_init 失败 → fail-closed disabled + "
            "per-sid kill latch {}（拒新开仓、平仓通行）；若该 sid 有恢复持仓则无人管理，"
            "需 operator 处置。latched_total={}",
            name.c_str(), sid_id, latch_state, total);
        if (attempted && !verified) {
            const uint64_t unverified =
                g_strategy_init_failure_latch_unverified_total_.fetch_add(
                    1, std::memory_order_relaxed) + 1;
            XtLogger::error(
                "[INIT-FAILURE] sid_id={} 的 per-sid kill latch 写后回读为 false —— "
                "per-sid risk state 槽位尚未 init(容量 {})，该 sid 的 gateway/risk 开仓闸"
                "当前未生效，必须由 runner 在 init() 之后补置。unverified_total={}",
                sid_id, risk_monitor_state->per_strategy.size(), unverified);
        }
    };
}

uint64_t strategy_init_failure_latched_total() noexcept {
    return g_strategy_init_failure_latched_total_.load(std::memory_order_relaxed);
}

uint64_t strategy_init_failure_latch_unverified_total() noexcept {
    return g_strategy_init_failure_latch_unverified_total_.load(
        std::memory_order_relaxed);
}

bool wire_risk_state_and_validate(
    xt_risk::XtRiskMonitor* risk_monitor,
    xt_account::AccountStateStore* account_store,
    XtLiveOrderGateway* order_gateway,
    const xt_risk::XtRiskMonitorState* risk_monitor_state,
    const ct::metrics::XtStrategyPnlAggregator* strategy_pnl_aggr,
    const ct::metrics::XtStrategyPnlBaseline* strategy_pnl_baseline) noexcept {
    wire_per_strategy_risk_state(risk_monitor, account_store, order_gateway,
                                 risk_monitor_state);
    // per-strategy daily-loss source = realized-PnL aggregator（HFT 持有；无源
    // engine 的 per_strategy_daily_loss_pct 保持 bypass）。baseline 与 aggregator
    // 配对：gate 用「aggregator 累计 − baseline」得 today realized，否则退化为
    // lifetime 累计。两 setter 必须先于 validate（validate 检查「配 daily-loss
    // 却无 PnL 源」）。
    if (order_gateway && strategy_pnl_aggr) {
        order_gateway->set_strategy_pnl_aggregator(strategy_pnl_aggr);
    }
    if (order_gateway && strategy_pnl_baseline) {
        order_gateway->set_strategy_pnl_baseline(strategy_pnl_baseline);
    }
    // late dep wire 后校验风控配置；misconfig 由 caller fail-exit。
    if (order_gateway) {
        const std::string verr = order_gateway->validate_risk_config();
        if (!verr.empty()) {
            XtLogger::error_m("RT", "risk guard config invalid: {}", verr);
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// cross-strategy symbol overlap validation
// ---------------------------------------------------------------------------
//
// Runner 启动期反查 symbol → sid_set，默认拒绝多 strategy 共仓归因重叠。
bool validate_cross_strategy_symbol_overlap(
    const xt_strategy_engine::XtDeclaredSymbolRegistry* registry,
    const std::vector<std::string>& sid_list,
    bool allow_overlap) noexcept {
    if (sid_list.size() <= 1) {
        return true;
    }
    auto join_sids = [](const std::vector<std::string>& sids) {
        std::string out;
        for (size_t i = 0; i < sids.size(); ++i) {
            if (i > 0) out += ", ";
            out += sids[i];
        }
        return out;
    };
    if (!registry) {
        XtLogger::error_m("router",
            "cross-strategy overlap validation unavailable: tracker=null "
            "for sids=[{}]",
            join_sids(sid_list));
        return false;
    }

    // 反向索引: code → vector<sid> (按 yaml 顺序保稳定)
    std::unordered_map<std::string, std::vector<std::string>> code_to_sids;
    std::vector<std::string> missing_sids;
    for (const auto& sid : sid_list) {
        auto codes = registry->declared_symbols(sid);
        if (codes.empty()) {
            missing_sids.push_back(sid);
            continue;
        }
        for (const auto& code : codes) {
            code_to_sids[code].push_back(sid);
        }
    }
    if (!missing_sids.empty()) {
        XtLogger::error_m("router",
            "cross-strategy overlap validation incomplete: missing "
            "declare_position_interest for sids=[{}]",
            join_sids(missing_sids));
        return false;
    }

    // 扫描重叠
    bool any_overlap = false;
    for (const auto& [code, sids] : code_to_sids) {
        if (sids.size() < 2) continue;
        any_overlap = true;
        const std::string sid_csv = join_sids(sids);
        if (allow_overlap) {
            XtLogger::warn_m("router",
                "cross-strategy overlap (allowed): code={} sids=[{}] — "
                "PnL attribution approximate, ops 注意 strategy mandate 重叠",
                code, sid_csv);
        } else {
            XtLogger::error_m("router",
                "cross-strategy overlap (abort): code={} sids=[{}] — "
                "设 risk.allow_strategy_symbol_overlap=true 显式允许",
                code, sid_csv);
        }
    }

    if (any_overlap && !allow_overlap) {
        return false;  // default: abort
    }
    return true;  // 无重叠 / escape hatch: pass
}

std::vector<std::string> collect_strategy_config_symbols(
    const XtStrategyInitConfig& cfg) {
    std::vector<std::string> out;
    auto add_unique = [&out](const std::string& code) {
        if (code.empty()) return;
        if (std::find(out.begin(), out.end(), code) == out.end()) {
            out.push_back(code);
        }
    };

    add_unique(cfg.get_string("symbol", ""));

    std::map<int, std::string> indexed_symbols;
    const std::string prefix = "symbols.";
    for (const auto& kv : cfg.values) {
        if (kv.first.rfind(prefix, 0) != 0) continue;
        const std::string idx_text = kv.first.substr(prefix.size());
        if (idx_text.empty()) continue;
        char* end = nullptr;
        const long idx = std::strtol(idx_text.c_str(), &end, 10);
        if (!end || *end != '\0' || idx < 0) continue;
        indexed_symbols[static_cast<int>(idx)] = kv.second;
    }
    for (const auto& kv : indexed_symbols) {
        add_unique(kv.second);
    }

    static constexpr const char* kSpreadLegKeys[] = {
        "leg_a_code",
        "leg_b_code",
        "front_code",
        "back_code",
    };
    for (const char* key : kSpreadLegKeys) {
        add_unique(cfg.get_string(key, ""));
    }
    return out;
}

// ---------------------------------------------------------------------------
// run_overlap_check (Runner wrapper)
// ---------------------------------------------------------------------------
// HFT / CTA / SEL runner 共用的 sid_list 校验入口。
// CTA/SEL on_init 在 worker 线程, declare_position_interest 跟主线程 race —
// poll-with-timeout (500ms) 等 tracker.registered_sids 跟 sid_list 全 covered。
// HFT 不需要 (init_all_strategy_plugins_ 同步), wait_for_workers=false。
bool run_overlap_check(
    const xt_strategy_engine::XtDeclaredSymbolRegistry* registry,
    const std::vector<std::string>& sid_list,
    bool allow_overlap,
    const char* engine_tag,
    bool wait_for_workers) noexcept {
    if (!registry) {
        // 多策略无法证明 overlap-safe 时 fail closed; 单策略在 validate 内直接通过。
        return validate_cross_strategy_symbol_overlap(nullptr, sid_list, allow_overlap);
    }
    if (wait_for_workers && !sid_list.empty()) {
        // CTA/SEL on_init worker race: poll tracker.registered_sids 到 fully covered 或 500ms 超时。
        // 实测 on_init 第一次 fire 在 event_loop start 后 ~10-50ms (worker thread bootstrap),
        // 500ms timeout 给 5x margin, 超时 fall-through (log warn 但不 abort, validate 仍跑用部分覆盖)。
        const auto start_ts = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::milliseconds(500);
        while (true) {
            auto registered = registry->registered_sids();
            std::unordered_set<std::string> reg_set(registered.begin(), registered.end());
            bool all_covered = true;
            for (const auto& sid : sid_list) {
                if (reg_set.find(sid) == reg_set.end()) {
                    all_covered = false;
                    break;
                }
            }
            if (all_covered) break;
            if (std::chrono::steady_clock::now() - start_ts >= timeout) {
                XtLogger::warn_m(engine_tag,
                    "overlap validation: on_init race timeout (500ms), "
                    "partial coverage; strict validation will fail if any sid "
                    "still lacks declared symbols");
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return validate_cross_strategy_symbol_overlap(registry, sid_list, allow_overlap);
}

bool run_overlap_check_for_config(
    const xt_strategy_engine::XtDeclaredSymbolRegistry* registry,
    const ::XtLiveConfig& cfg,
    const char* engine_tag) noexcept {
    std::vector<std::string> sid_list;
    sid_list.reserve(cfg.parsed.strategies.size());
    for (const auto& sentry : cfg.parsed.strategies) {
        sid_list.push_back(sentry.factory_cfg.id);
    }
    const bool allow_overlap =
        cfg.bootstrap.order_gateway_cfg.risk.allow_strategy_symbol_overlap;
    if (!run_overlap_check(registry, sid_list, allow_overlap, engine_tag,
                           /*wait_for_workers=*/true)) {
        XtLogger::error_m(engine_tag,
            "cross-strategy symbol overlap detected — abort startup "
            "(set risk.allow_strategy_symbol_overlap=true to bypass)");
        return false;
    }
    return true;
}

}  // namespace xt_live::control
