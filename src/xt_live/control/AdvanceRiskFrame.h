#pragma once
/// @file
/// @brief AdvanceRiskFrame.h 是 live runner 的“每帧风控维护”共享 helper
///
/// 把 HFT/CTA/SEL 每帧需要做的 risk 检查放在 advance_risk_frame() 统一处理

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>

class IMdGateway;
class ITradeGateway;
class XtLiveOrderGateway;
namespace xt_account {
class AccountStateStore;
}
namespace xt_risk {
class XtRiskMonitor;
}
namespace xt_identity { class StrategySidRegistry; }
struct XtRunnerSignals;

namespace xt_live::control {

/// @brief advance_risk_frame 的非拥有依赖；全部指向 runner composition root 持有对象。
///
/// risk 块（session_begin / arm / on_frame / integrity）要求 `risk_monitor` 与
/// `account_store` 同时非空，否则整块跳过；health check 无条件执行（md/td 各自判空）。
struct RiskFrameDeps {
    xt_risk::XtRiskMonitor* risk_monitor = nullptr;
    const xt_account::AccountStateStore* account_store = nullptr;    ///< equity fp 读侧
    XtRunnerSignals* signals = nullptr;   ///< last_tick_date / last_tick_steady_ms / cashflow 读侧；nullptr 按全 0 处理（date=0 → venue 推导 fallback 仍生效）
    XtLiveOrderGateway* order_gateway = nullptr;                     ///< annual-loss latch arm 入口；nullptr 跳过 arm
    const xt_identity::StrategySidRegistry* sid_registry = nullptr;  ///< integrity check 尺寸源；nullptr 跳过检查
    IMdGateway* md = nullptr;                                        ///< 每帧 check_health
    ITradeGateway* td = nullptr;                                     ///< 每帧 check_health
    /// MD 无 tick（last_tick_date==0）时的交易日兜底口径："binance_u"/"binance" →
    /// UTC 自然日，其余（含空串）→ 中国期货夜盘口径。
    std::string venue_label;
    /// @brief 向 account worker 线程投递一次「执行在册发布请求」闭包
    ///        （`AccountStateStore::run_publish_request`）。
    ///
    /// 控制线程**绝不**自己发布帧（发布线程唯一 = account worker）；本回调是它推动
    /// 帧刷新的唯一手段。返回 false = worker 队列满，本帧投递失败（计数 +
    /// pending 闩保持，下帧重投）。
    /// 未接线（nullptr）时兜底发布与 session-begin 同步一并跳过，帧行为退回
    /// 「只靠事件驱动发布」——单测/无 worker 装配走此路径。
    std::function<bool()> post_publish_request;
    /// @brief 停机谓词；置位时 session-begin 的有界等待立即退出（按超时处置）。
    /// nullptr = 无停机信号，等待只受超时约束。
    const std::atomic<bool>* shutdown_flag = nullptr;
};

/// @brief 跨帧维护状态；每个 frame loop 实例各持一份（可重入，无函数级 static）。
struct RiskFrameState {
    int integrity_tick = 0;  ///< 每 10 帧触发一次 per-sid integrity check 后归零
    /// 本帧登记/合并到的兜底发布请求 ticket；session-begin 等待的就是它。
    /// 合并语义在 store 侧（未 ack 前恒返回同一 ticket），此处只缓存当帧值。
    uint64_t publish_ticket = 0;
    /// 已成功 arm 过 session-begin 双闩的 trading_date；0 = 尚未 arm。
    /// 只用于判定「本帧是否是 session-begin 帧」（决定要不要做同步等待）；
    /// 真正的幂等权威仍是 XtRiskMonitor / annual latch 各自的内部 dedup。
    uint32_t session_armed_date = 0;
    /// 上一次**物理投递**闭包时的 ticket；0 = 尚未投递过。
    uint64_t last_posted_ticket = 0;
    /// 上一次物理投递时 store 的发布尝试计数快照（`publish_request_attempts()`）。
    uint64_t last_posted_attempts = 0;
    /// 上一次物理投递是否成功入队；false = 队列满，闭包根本没进队。
    bool last_post_succeeded = false;
};

/// @brief 本帧是否需要物理投递一个兜底发布闭包（「至多一个在途任务」判据）。
///
/// 同一 ticket 上闭包一经成功入队就不再重投：重投不会让请求更早完成，只会在 worker
/// 已拥塞时继续给队列加压。三种情形必须投：
///   ① ticket 变了——前一个已 ack，本帧是新请求；
///   ② 上次投递失败——队列满，闭包没进队；
///   ③ 上次投递之后跳过计数推进却仍未 ack——闭包已执行完但 pool 耗尽跳过了发布，
///      不重投这个 ticket 就永不完成（session-begin 等待会永久超时）。计数只在尝试
///      完成时推进 ⇒ 闭包排队/执行中的窗口不满足本条，不会双投。
/// @param ticket 本帧 `request_publish()` 的返回值。
/// @param attempts_now `AccountStateStore::publish_request_attempts()` 的当前值。
[[nodiscard]] inline bool should_post_publish_closure(uint64_t ticket,
                                                     const RiskFrameState& state,
                                                     uint64_t attempts_now) noexcept {
    if (ticket != state.last_posted_ticket) return true;
    if (!state.last_post_succeeded) return true;
    return attempts_now != state.last_posted_attempts;
}

/// @brief 每帧风控维护的进程级观测计数（advance_risk_frame 帧尾经健康面 emit 导出 prom）。
namespace risk_frame_counters {

/// @brief session-begin 因帧刷新未在超时内完成而整块推迟的次数。
///
/// prom metric `xt_account_session_begin_deferred_total`。推迟 = 本帧不 arm 日内
/// peak / annual 基准、date 幂等不消费、且本帧 IDD/drawdown 评估一并跳过，下帧重试。
/// 连续增长 = 年度/日内闩迟迟未 arm（fail-open 方向），须告警而不只是 warn 日志。
std::atomic<uint64_t>& session_begin_deferred() noexcept;

}  // namespace risk_frame_counters

/// @brief 推进一帧风控维护。
///
/// @pre 与 risk_monitor 的其它 caller（bootstrap restore 除外）同线程串行调用；
///      本函数不加锁，依赖 frame loop 单线程契约。
/// @note equity 全程 fixed-point 直读（不经 double 往返）；cashflow 从
///       `signals->latest_external_cashflow_today_fp` 无条件接线，无 writer 的
///       runner（CTA/SEL）恒读 0。
/// @note 每帧登记一次兜底发布请求（`post_publish_request`）：行情不触发发布，这是
///       equity 帧新鲜度的周期来源。登记不受 risk gating 约束（只要求 account_store
///       非空）；物理闭包按「至多一个在途」去重，判据见 should_post_publish_closure。
/// @note session-begin 帧（trading_date 相对 `state.session_armed_date` 变化）会
///       先同步等待自己的发布 ticket 完成再重读 equity，让日内 peak 与 annual
///       基准两个闩锁在同一新鲜值上；等待超时（或停机）则**整块推迟**——两闩不
///       arm、date 幂等不消费、本帧 drawdown/IDD 评估一并跳过，下帧重试。
///       与 equity 无关的 MD/TD health check 不受推迟影响。
void advance_risk_frame(const RiskFrameDeps& deps, RiskFrameState& state);

}  // namespace xt_live::control
