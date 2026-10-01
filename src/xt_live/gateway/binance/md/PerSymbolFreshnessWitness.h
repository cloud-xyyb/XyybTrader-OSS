#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "xt_live/gateway/common/XtMdFreshness.h"

namespace binance_u {

/// @brief 每个 Binance symbol 的行情活跃度记录器
///
/// MdBinanceUGateway::check_health() 会逐个 symbol 检查行情是否活跃
/// 如果某个 symbol 太久没有 tick / ready，就认为这个 symbol 的行情可能卡住了，
/// 会触发整条 Binance WS reconnect
class PerSymbolFreshnessWitness {
public:
    PerSymbolFreshnessWitness() = default;

    // 容器 reserve 需要类型可 move（atomic 不可隐式 move）；运行期源对象不会被搬运
    // （vector reserve 后只 index 访问），显式 move 用当前值初始化即可。复制禁止。
    PerSymbolFreshnessWitness(PerSymbolFreshnessWitness&& o) noexcept
        : last_tick_ms_(o.last_tick_ms_.load(std::memory_order_relaxed)),
          last_ready_ms_(o.last_ready_ms_.load(std::memory_order_relaxed)) {}
    PerSymbolFreshnessWitness& operator=(PerSymbolFreshnessWitness&&) = delete;
    PerSymbolFreshnessWitness(const PerSymbolFreshnessWitness&) = delete;
    PerSymbolFreshnessWitness& operator=(const PerSymbolFreshnessWitness&) = delete;

    /// @brief 记录一笔真实 tick 的 steady ms（push_tick 调用）。
    void note_tick(uint32_t steady_ms) noexcept {
        last_tick_ms_.store(steady_ms, std::memory_order_relaxed);
    }

    /// @brief 记录一次 book-ready 的 steady ms（snapshot drain 成功后调用）。
    /// 第二 witness：重连后首笔 tick 到达前，watchdog 以 book-ready 时间为 anchor。
    void note_ready(uint32_t steady_ms) noexcept {
        last_ready_ms_.store(steady_ms, std::memory_order_relaxed);
    }

    uint64_t last_tick_ms() const noexcept {
        return last_tick_ms_.load(std::memory_order_relaxed);
    }
    uint64_t last_ready_ms() const noexcept {
        return last_ready_ms_.load(std::memory_order_relaxed);
    }

    /// @brief 用 PerSymbolFreshnessWitness 自身两个 anchor 走 common 谓词判 freshness。
    xt_live::gateway::detail::MdFreshnessAction evaluate(uint64_t now_ms,
                                                         uint64_t stale_last_close_ms,
                                                         uint32_t stale_reconnect_ms,
                                                         uint32_t throttle_ms) const {
        return xt_live::gateway::detail::evaluate_md_freshness(
            now_ms, last_tick_ms(), last_ready_ms(), stale_last_close_ms,
            stale_reconnect_ms, throttle_ms);
    }

private:
    std::atomic<uint32_t> last_tick_ms_{0};   ///< 最近真实 tick 的 steady ms；0=从未。
    std::atomic<uint32_t> last_ready_ms_{0};  ///< 最近 book-ready 的 steady ms；0=从未。
};

/// @brief 逐 witness 扫描的聚合结论（check_health 决策核的输出）。
struct MdFreshnessSweep {
    bool any_should_reconnect = false;  ///< 任一 witness stale 且 throttle 已过。
    bool any_throttled        = false;  ///< 有 stale 但被 throttle 窗口吞。
    size_t   trigger_index    = 0;      ///< 首个触发 reconnect 的 witness 下标（日志用）。
    uint64_t trigger_last_tick  = 0;    ///< 触发者的 last_tick anchor。
    uint64_t trigger_last_ready = 0;    ///< 触发者的 last_ready anchor。
};

/// @brief 逐 symbol witness 判 freshness 并聚合：任一 stale → 连接级 reconnect
///        一次（N symbol 共用一条 WS，重连即对全部 symbol re-snapshot），只记首个触发者。
/// @param witness_at `witness_at(i)` 返回第 i 个 `const PerSymbolFreshnessWitness&`。
/// @param stale_last_close_ms 连接级 throttle 戳：任一 symbol 触发后整条连接进 throttle
///        窗口，窗口内其它 symbol 的 stale 不重复 reconnect。
template <typename WitnessAt>
MdFreshnessSweep sweep_md_freshness(size_t count, WitnessAt&& witness_at,
                                    uint64_t now_ms, uint64_t stale_last_close_ms,
                                    uint32_t stale_reconnect_ms, uint32_t throttle_ms) {
    using xt_live::gateway::detail::MdFreshnessAction;
    MdFreshnessSweep out;
    for (size_t i = 0; i < count; ++i) {
        const PerSymbolFreshnessWitness& w = witness_at(i);
        const uint64_t last_tick  = w.last_tick_ms();
        const uint64_t last_ready = w.last_ready_ms();
        const auto action = xt_live::gateway::detail::evaluate_md_freshness(
            now_ms, last_tick, last_ready, stale_last_close_ms,
            stale_reconnect_ms, throttle_ms);
        switch (action) {
            case MdFreshnessAction::healthy:
                break;
            case MdFreshnessAction::throttled:
                out.any_throttled = true;
                break;
            case MdFreshnessAction::should_reconnect:
                if (!out.any_should_reconnect) {  // 记首个触发 symbol 即可（reconnect 一次）
                    out.any_should_reconnect = true;
                    out.trigger_index      = i;
                    out.trigger_last_tick  = last_tick;
                    out.trigger_last_ready = last_ready;
                }
                break;
        }
    }
    return out;
}

}  // namespace binance_u
