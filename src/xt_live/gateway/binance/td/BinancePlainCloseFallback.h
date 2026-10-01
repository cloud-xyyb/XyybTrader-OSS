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
    bool    is_buy; ///< true=BUY 平空，false=SELL 平多。
    int64_t qty_fp; ///< 退化普通平仓单数量，scale 1e6。
};

/// @brief 安全关键 gate：决定 reduceOnly close 被拒后是否允许 plain-close fallback。
/// @note 返回 std::nullopt 表示跳过，不发任何普通单。安全规则：
///       1. 交易所已平仓：跳过，避免普通单开出反向裸仓；
///       2. 最新持仓方向与冻结平仓意图不一致：跳过，交给 reconcile 修正状态；
///       3. 方向一致：发普通平仓单，数量为 min(冻结意图数量, 交易所可平数量)。
///       该函数纯计算、无副作用，便于单元测试覆盖 no-flip sizing。
/// @param frozen_is_buy 原 reduceOnly 平仓意图方向。
/// @param frozen_qty_fp 原 reduceOnly 平仓意图数量，scale 1e6。
/// @param venue_pos_fp 最新交易所持仓，正数=多头，负数=空头，scale 1e6。
inline std::optional<PlainCloseFallbackDecision> decide_plain_close_fallback(
    bool frozen_is_buy, int64_t frozen_qty_fp, int64_t venue_pos_fp) {
    if (frozen_qty_fp <= 0) return std::nullopt;
    constexpr int64_t kFlatEpsFp = 1;  // |pos| < 1 fp unit 视为 flat。
    if (venue_pos_fp <= -kFlatEpsFp) {            // 交易所空头：只能 BUY 平仓。
        if (!frozen_is_buy) return std::nullopt;  // 冻结意图为 SELL，方向不一致。
        const int64_t avail = -venue_pos_fp;
        return PlainCloseFallbackDecision{true, frozen_qty_fp < avail ? frozen_qty_fp : avail};
    }
    if (venue_pos_fp >= kFlatEpsFp) {             // 交易所多头：只能 SELL 平仓。
        if (frozen_is_buy) return std::nullopt;   // 冻结意图为 BUY，方向不一致。
        return PlainCloseFallbackDecision{false, frozen_qty_fp < venue_pos_fp ? frozen_qty_fp : venue_pos_fp};
    }
    return std::nullopt;                           // 交易所已平：跳过，避免 flip。
}

}  // namespace binance_u
