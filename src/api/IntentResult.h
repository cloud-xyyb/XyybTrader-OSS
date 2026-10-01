#pragma once

#include "api/XtRejectCode.h"

#include <cstdint>

namespace xt_runtime {

/// @brief intent 执行的同步受理结果。
///
/// `accepted_delta_fp` 是实际受理的 signed delta；符号与请求 delta 一致，
/// 绝对值不超过请求量。engine / context 用它推进或回滚 virtual position，
/// 避免本地 reject 或部分受理后 logical position 漂移。
///
/// 调用方需结合 requested diff 判断结果：
/// - `accepted_delta_fp == requested_diff_fp` 表示完全受理。
/// - `abs(accepted_delta_fp) < abs(requested_diff_fp)` 表示部分受理或完全拒绝。
///
/// `reject_reason == NoError` 表示完全受理；部分或完全拒绝时记录第一个拒单原因。
struct IntentResult {
    int64_t accepted_delta_fp = 0;
    XtRejectCode reject_reason = XtRejectCode::NoError;

    bool fully_accepted() const noexcept {
        return reject_reason == XtRejectCode::NoError;
    }
};

} // namespace xt_runtime
