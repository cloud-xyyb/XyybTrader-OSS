#pragma once

/// @file
/// @brief MD tick 写入 recorder 前的字段转换 helper。

#include <cstdint>

#include "xt_core/XtFixedPoint.h"
#include "XtLiveRecorder.h"

namespace xt_live_recorder_util {

inline int64_t to_recorder_fp(double value) noexcept {
    return ::to_fp_trunc(value);
}

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

}
