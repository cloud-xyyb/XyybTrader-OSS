#include "AdvanceRiskFrame.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include "XtLiveOrderGateway.h"
#include "XtLogger.h"
#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#include "api/XtRunnerSignals.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_core/order/XtInstrumentRegistry.h"
#include "xt_metrics/XtMetricRegistry.h"
#include "xt_core/session/XtTradingDay.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_risk/engine/XtRiskMonitor.h"
#include "xt_util/time/XtSteadyClock.h"

namespace xt_live::control {

namespace risk_frame_counters {
std::atomic<uint64_t>& session_begin_deferred() noexcept {
    static std::atomic<uint64_t> counter{0};
    return counter;
}
}  // namespace risk_frame_counters

namespace {

/// session-begin 同步等待的上界：超过则本帧整块推迟（下帧重试）。
/// 取值锚在 1Hz 控制帧上——半个帧周期内拿不到新帧，说明 worker 已明显落后，
/// 继续等只会挤掉本帧其余维护动作。
constexpr int64_t kSessionBeginWaitMs = 500;
/// 等待轮询粒度；停机响应延迟的上界也是它。
constexpr int64_t kSessionBeginPollMs = 2;

// MD 未推 tick 时的交易日 fallback。last_tick_date 仅 HFT channels 写入：HFT 首个
// MD action_date 到达后接管；CTA/SEL 无 writer，本推导是它们的永久日期源。
uint32_t compute_trading_date_for_venue(const std::string& venue,
                                        int64_t now_ns) noexcept {
    xt_core::TimezonePolicy policy = xt_core::TimezonePolicy::ChinaFuturesNightSession;
    if (venue == "binance_u" || venue == "binance") {
        policy = xt_core::TimezonePolicy::UtcNaturalDay;
    }
    return xt_core::derive_trading_day(now_ns / 1'000'000LL, policy);
}

// 每帧一次的兜底发布维护：保证 account worker 上恒有一个「重建+发布」请求在推进。
//
// 行情不触发发布（MarketTick 零发布），帧内 equity/mark 的周期刷新全靠这里。请求
// 编号由 store 的 ticket 合并承担（未 ack 前 `request_publish()` 恒返回同一 ticket），
// 物理闭包的去重由 should_post_publish_closure 承担：同一 ticket 至多一个在途闭包。
void maintain_fallback_publish(const RiskFrameDeps& deps, RiskFrameState& state) {
    if (!deps.post_publish_request) return;
    state.publish_ticket = deps.account_store->request_publish();
    const uint64_t attempts = deps.account_store->publish_request_attempts();
    if (!should_post_publish_closure(state.publish_ticket, state, attempts)) return;
    state.last_posted_ticket = state.publish_ticket;
    state.last_posted_attempts = attempts;
    state.last_post_succeeded = deps.post_publish_request();
    if (!state.last_post_succeeded) {
        xt_account_store_drops::publish_post_failed().fetch_add(1, std::memory_order_relaxed);
    }
}

// session-begin 前的有界等待：本帧登记的发布请求完成才算拿到「不早于此刻」的帧。
// @return true = ticket 已 ack（可以重读 equity）；false = 超时/停机/未接线可用请求。
bool await_publish_ticket(const RiskFrameDeps& deps, const RiskFrameState& state) {
    if (!deps.post_publish_request) {
        // 未接线（单测/无 worker 装配）：没有同步手段，保持「直读当前帧」的旧语义。
        return true;
    }
    if (state.publish_ticket == 0) return false;
    const int64_t deadline_ms = XtSteadyClock::steady_now_ms() + kSessionBeginWaitMs;
    while (!deps.account_store->is_publish_acked(state.publish_ticket)) {
        if (deps.shutdown_flag &&
            deps.shutdown_flag->load(std::memory_order_acquire)) {
            return false;  // 停机：与超时同样处置，本帧整块推迟
        }
        if (XtSteadyClock::steady_now_ms() >= deadline_ms) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(kSessionBeginPollMs));
    }
    return true;
}

// equity 相关的一帧维护（session-begin 双闩 + on_frame drawdown/IDD + integrity）。
// 单独成函数是为了让「session-begin 等待超时 ⇒ 整块推迟」用一个 return 表达，且
// 与 equity 无关的 health check 留在 caller 里不受影响。
void advance_equity_block(const RiskFrameDeps& deps, RiskFrameState& state) {
    // equity 用 fp 直读，risk/annual-latch 全程 fixed-point。
    int64_t equity_fp = deps.account_store->snapshot_account().equity;
    // cashflow 抵消外部入出金对 IDD 的影响；影响时序一致性，用 acquire。
    int64_t cashflow_fp =
        deps.signals ? deps.signals->latest_external_cashflow_today_fp.load(
                           std::memory_order_acquire)
                     : 0;
    // session_begin 按 trading_date 幂等；无 MD tick 时用 venue 日期兜底。
    uint32_t cur_session_date =
        deps.signals ? deps.signals->last_tick_date.load(std::memory_order_acquire)
                     : 0u;
    if (cur_session_date == 0) {
        // 交易日推导依赖 wall-clock；steady clock 无 epoch 语义不可用。
        const int64_t now_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        cur_session_date = compute_trading_date_for_venue(deps.venue_label, now_ns);
    }
    if (cur_session_date != 0 && cur_session_date != state.session_armed_date) {
        // session-begin 帧：日内 peak/starting balance 与 annual 基准两个闩是整个
        // session 的比较基准，必须锁在同一个新鲜 equity 上——先等本帧的发布 ticket
        // 完成，再重读。
        if (!await_publish_ticket(deps, state)) {
            // 超时/停机：整块推迟。两闩不 arm、date 幂等不消费（下帧再进本分支
            // 重试），且本帧的 drawdown/IDD 评估一并跳过——拿上一 session 的 peak
            // 评估当前 equity 会误 latch open_halted。
            risk_frame_counters::session_begin_deferred().fetch_add(
                1, std::memory_order_relaxed);
            XtLogger::warn_m("risk",
                "session_begin deferred: account frame not refreshed within {}ms "
                "(date={}); latches not armed and IDD skipped this frame, retrying",
                kSessionBeginWaitMs, cur_session_date);
            return;
        }
        equity_fp = deps.account_store->snapshot_account().equity;
        cashflow_fp =
            deps.signals ? deps.signals->latest_external_cashflow_today_fp.load(
                               std::memory_order_acquire)
                         : 0;
        deps.risk_monitor->on_session_begin(cur_session_date, equity_fp, cashflow_fp);
        if (deps.order_gateway) {
            deps.order_gateway->arm_annual_loss_on_session_begin(
                deps.account_store, cur_session_date, equity_fp);
        }
        state.session_armed_date = cur_session_date;
    }
    // last_md_ms 与 now_ms 同为 XtSteadyClock，可直接用于 stale 判定。
    const uint32_t now_ms = static_cast<uint32_t>(XtSteadyClock::steady_now_ms());
    const uint32_t last_md_ms =
        deps.signals ? deps.signals->last_tick_steady_ms.load(std::memory_order_relaxed)
                     : 0u;
    deps.risk_monitor->on_frame(equity_fp, last_md_ms, now_ms, cashflow_fp);
    // 每 10 帧校验 sid_registry 与 risk per-sid vector 尺寸，避免 init drift 静默。
    if (++state.integrity_tick >= 10) {
        state.integrity_tick = 0;
        if (deps.sid_registry) {
            (void)deps.risk_monitor->sanity_check_per_sid_state(
                deps.sid_registry->sid_slot_count());
        }
    }
}

// 账户 store 健康面 1Hz emit（F02 P6）：三 runner 共享本帧维护单源，接线只此一处。
// venue_label 空（单测/未配置 metric 身份）时整组不 emit，与
// XtRunnerControlCfg::venue_label 的既有约定一致。counter 类传累计值，delta
// 导出在 registry 内完成。
void emit_store_health_metrics(const RiskFrameDeps& deps) {
    if (deps.venue_label.empty()) return;
    ct::metrics::XtMetricRegistry::AccountStoreHealthSample s;
    s.position_slots_used     = deps.account_store->position_slots_used();
    s.position_slots_capacity = xt_account::AccountStateStore::position_slots_capacity();
    s.position_slot_overflow_total =
        xt_account_store_drops::position_slot_overflow().load(std::memory_order_relaxed);
    s.publish_post_failed_total =
        xt_account_store_drops::publish_post_failed().load(std::memory_order_relaxed);
    const int64_t last_ms = deps.account_store->last_publish_steady_ms();
    // 0 = 尚未发布首帧（生产装配 gen-0 在控制循环前已发布，此分支仅未 freeze
    // 的装配可达）；保持哨兵值跳过 emit，不造假帧龄。
    if (last_ms > 0) {
        const int64_t steady_now = XtSteadyClock::steady_now_ms();
        s.frame_age_ms = steady_now - last_ms;
        if (s.frame_age_ms < 0) s.frame_age_ms = 0;
        // 发布时刻的 wall-clock 换算：freshness alert 用 time()−ts，控制循环冻结
        // 时 ts 停走而告警仍可触发（frame_age gauge 会随 emitter 冻结，只作诊断）。
        const double wall_now_s =
            std::chrono::duration_cast<std::chrono::duration<double>>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        s.last_publish_unix_ts_seconds =
            wall_now_s - static_cast<double>(s.frame_age_ms) / 1000.0;
    }
    s.session_begin_deferred_total =
        risk_frame_counters::session_begin_deferred().load(std::memory_order_relaxed);
    s.intern_overflow_total =
        xt_core::instrument_registry_drops::intern_overflow().load(std::memory_order_relaxed);
    s.slot_reservation_rejected_total =
        xt_live_gateway_counters::slot_reservation_rejected().load(std::memory_order_relaxed);
    ct::metrics::XtMetricRegistry::instance().set_account_store_health(deps.venue_label, s);
}

}  // namespace

void advance_risk_frame(const RiskFrameDeps& deps, RiskFrameState& state) {
    // 兜底发布不受 risk gating 约束：它是帧新鲜度的唯一周期来源，风控未接线的装配
    // （panel / 报表 / 对账读侧）同样要靠它把 equity/mark 刷进已发布帧。
    if (deps.account_store) {
        maintain_fallback_publish(deps, state);
    }

    if (deps.risk_monitor && deps.account_store) {
        advance_equity_block(deps, state);
    }

    // 健康面观测 emit（F02 P6）：不依赖 risk_monitor，只要有 store 就报。放在
    // equity 块之后，session-begin 推迟计数在同帧内即可见。
    if (deps.account_store) {
        emit_store_health_metrics(deps);
    }

    // Gateway 自治健康检查（无条件，不依赖 account_store）；返回 false 时实现已
    // 自行 reconnect/retry，调用方不升级处理（best-effort）。
    {
        const uint32_t health_now_ms =
            static_cast<uint32_t>(XtSteadyClock::steady_now_ms());
        if (deps.md) (void)deps.md->check_health(health_now_ms);
        if (deps.td) (void)deps.td->check_health(health_now_ms);
    }
}

}  // namespace xt_live::control
