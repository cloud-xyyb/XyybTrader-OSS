#pragma once

#include <cstdint>

#include "xt_core/XtLiveTapContext.h"

namespace xt_live {

[[nodiscard]] inline uint64_t select_trigger_anchor_us(uint64_t capture_us,
                                                       uint64_t enqueue_us) noexcept {
    return (capture_us > 0 && capture_us <= enqueue_us) ? capture_us : enqueue_us;
}

/// @brief RAII：作用域内把 tap tick 锚（`xt_core::tap::current_tick_ts_ns`）设为
///        anchor_ns，退出恢复旧值。
///
/// 供 CTA/SEL 事件驱动在策略 dispatch 调用外层包裹：dispatch 内的 gateway submit
/// tap 读取该锚计 `xt_strategy_us`（trigger→submit_begin）。ctor 先存旧值再 set，
/// dtor 恢复旧值 —— 异常 / early-return 安全，且支持嵌套（存旧值而非清 0）。
class TapAnchorScope {
public:
    explicit TapAnchorScope(uint64_t anchor_ns) noexcept
        : prior_(xt_core::tap::current_tick_ts_ns()) {
        xt_core::tap::set_current_tick_ts_ns(anchor_ns);
    }
    ~TapAnchorScope() { xt_core::tap::set_current_tick_ts_ns(prior_); }

    TapAnchorScope(const TapAnchorScope&)            = delete;
    TapAnchorScope& operator=(const TapAnchorScope&) = delete;

private:
    uint64_t prior_;
};

}
