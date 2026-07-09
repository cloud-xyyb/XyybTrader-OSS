#pragma once

#include <cstdint>
#include <optional>

/// @file BinancePlainCloseFallback.h
/// @brief reduceOnly 平仓被交易所拒掉后，决定能否退回普通平仓单的安全 gate
///
/// 普通平仓单没有交易所 reduceOnly 的防翻仓保护，方向和数量必须自己把关。
/// 本文件只放可直接单测的决策规则；拉最新持仓、节流、排队和实际下单归调用方。

namespace binance_u {

/// @brief 通过 no-flip gate 后的普通平仓单参数。
struct PlainCloseFallbackDecision {
    bool    is_buy;
    int64_t qty_fp;
};

inline std::optional<PlainCloseFallbackDecision> decide_plain_close_fallback(
    bool frozen_is_buy, int64_t frozen_qty_fp, int64_t venue_pos_fp) {
    if (frozen_qty_fp <= 0) return std::nullopt;
    constexpr int64_t kFlatEpsFp = 1;
    if (venue_pos_fp <= -kFlatEpsFp) {
        if (!frozen_is_buy) return std::nullopt;
        const int64_t avail = -venue_pos_fp;
        return PlainCloseFallbackDecision{true, frozen_qty_fp < avail ? frozen_qty_fp : avail};
    }
    if (venue_pos_fp >= kFlatEpsFp) {
        if (frozen_is_buy) return std::nullopt;
        return PlainCloseFallbackDecision{false, frozen_qty_fp < venue_pos_fp ? frozen_qty_fp : venue_pos_fp};
    }
    return std::nullopt;
}

}
