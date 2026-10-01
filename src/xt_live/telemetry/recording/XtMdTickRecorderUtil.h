#pragma once

/// @file
/// @brief MD tick 写入 recorder 前的字段转换 helper。

#include <cstdint>
#include <string>

#include "xt_core/XtFixedPoint.h"
#include "XtLiveRecorder.h"

namespace xt_live_recorder_util {

/// @brief recorder 使用的 fixed-point 转换；保持截断语义。
inline int64_t to_recorder_fp(double value) noexcept {
    return ::to_fp_trunc(value);
}

/// @brief venue 成交量单位判定：binance 是 coin/token 数量（如 1.234），CTP/XTP 是
///        原始整数手数。`normalized_provider` 须已归一化（小写、去空白）。
/// @note shm 多路 tap（GatewayFactory::make_shm_recorder_tap）按 provider 动态构建，
///       无法像三处 per-gateway in-process tap 那样在编译期固定量纲，故显式判定。
inline bool venue_volume_is_coin_quantity(const std::string& normalized_provider) noexcept {
    return normalized_provider == "binance_u";
}

/// @brief 把 `XtRawTick.total_volume` 映射为 recorder 定点 volume 字段。
///
/// 与三处 per-gateway in-process tap 逐值对齐：ParserCTP.cpp / ParserXTP.cpp 用
/// `static_cast<int64_t>` 直传原始整数手数；MdBinanceUGateway.cpp 用 `to_recorder_fp`
/// 对 coin 数量做 ×1e6 定点化。误用整数截断会把 binance 成交量（如 1.234）截断为 1，
/// 丢失精度（JSONL 与 tick.fbs 双错）。
inline int64_t to_recorder_volume_fp(const std::string& normalized_provider,
                                     double total_volume) noexcept {
    return venue_volume_is_coin_quantity(normalized_provider)
        ? to_recorder_fp(total_volume)
        : static_cast<int64_t>(total_volume);
}

/// @brief 把 gateway 原始深度数组转换为 recorder 固定 20 档数组，缺失档补 0。
inline void fill_md_depth_fp(const double* bid_prices,
                             const double* ask_prices,
                             const double* bid_qty,
                             const double* ask_qty,
                             int depth_levels,
                             int64_t bid_prices_fp[XtLiveRecorder::kMdDepth],
                             int64_t ask_prices_fp[XtLiveRecorder::kMdDepth],
                             int64_t bid_qty_fp[XtLiveRecorder::kMdDepth],
                             int64_t ask_qty_fp[XtLiveRecorder::kMdDepth]) noexcept {
    for (int i = 0; i < XtLiveRecorder::kMdDepth; ++i) {
        bid_prices_fp[i] = 0;
        ask_prices_fp[i] = 0;
        bid_qty_fp[i] = 0;
        ask_qty_fp[i] = 0;
    }
    if (!bid_prices || !ask_prices || !bid_qty || !ask_qty || depth_levels <= 0) return;

    const int n = depth_levels < XtLiveRecorder::kMdDepth
                    ? depth_levels
                    : XtLiveRecorder::kMdDepth;
    for (int i = 0; i < n; ++i) {
        bid_prices_fp[i] = to_recorder_fp(bid_prices[i]);
        ask_prices_fp[i] = to_recorder_fp(ask_prices[i]);
        bid_qty_fp[i] = to_recorder_fp(bid_qty[i]);
        ask_qty_fp[i] = to_recorder_fp(ask_qty[i]);
    }
}

} // namespace xt_live_recorder_util
