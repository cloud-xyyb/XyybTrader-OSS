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

    PerSymbolFreshnessWitness(PerSymbolFreshnessWitness&& o) noexcept
        : last_tick_ms_(o.last_tick_ms_.load(std::memory_order_relaxed)),
          last_ready_ms_(o.last_ready_ms_.load(std::memory_order_relaxed)) {}
    PerSymbolFreshnessWitness& operator=(PerSymbolFreshnessWitness&&) = delete;
    PerSymbolFreshnessWitness(const PerSymbolFreshnessWitness&) = delete;
    PerSymbolFreshnessWitness& operator=(const PerSymbolFreshnessWitness&) = delete;

    void note_tick(uint32_t steady_ms) noexcept {
        last_tick_ms_.store(steady_ms, std::memory_order_relaxed);
    }

    void note_ready(uint32_t steady_ms) noexcept {
        last_ready_ms_.store(steady_ms, std::memory_order_relaxed);
    }

    uint64_t last_tick_ms() const noexcept {
        return last_tick_ms_.load(std::memory_order_relaxed);
    }
    uint64_t last_ready_ms() const noexcept {
        return last_ready_ms_.load(std::memory_order_relaxed);
    }

    xt_live::gateway::detail::MdFreshnessAction evaluate(uint64_t now_ms,
                                                         uint64_t stale_last_close_ms,
                                                         uint32_t stale_reconnect_ms,
                                                         uint32_t throttle_ms) const {
        return xt_live::gateway::detail::evaluate_md_freshness(
            now_ms, last_tick_ms(), last_ready_ms(), stale_last_close_ms,
            stale_reconnect_ms, throttle_ms);
    }

private:
    std::atomic<uint32_t> last_tick_ms_{0};
    std::atomic<uint32_t> last_ready_ms_{0};
};

/// @brief 逐 witness 扫描的聚合结论（check_health 决策核的输出）。
struct MdFreshnessSweep {
    bool any_should_reconnect = false;
    bool any_throttled        = false;
    size_t   trigger_index    = 0;
    uint64_t trigger_last_tick  = 0;
    uint64_t trigger_last_ready = 0;
};

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
                if (!out.any_should_reconnect) {
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

}
