#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <unordered_map>
#include <unordered_set>

#include "XtOrderTypes.h"

/// @file
/// @brief venue 无关的订单生命周期判定与回报去重。
///
/// 不同交易所推来的订单回报格式各异，但生命周期语义相同：一单从报出走到终态，中途可能被
/// 重复推送、或推来进度倒退的旧回报。本文件把这些判定收成一处——算有效剩余量、判是否终态、
/// 比两条回报是否同进度——外加一个去重器：吞掉终态后的重复回报、进度回退和原地重复的更新，
/// 只把真正推进的回报放行给下游。

namespace order_lifecycle {

int64_t effective_remaining_fp(const XtOrderExecUpdate& upd);

bool is_final_update(const XtOrderExecUpdate& upd);

bool same_update_progress(const XtOrderExecUpdate& lhs,
                          const XtOrderExecUpdate& rhs);

bool apply_open_order_update(std::unordered_map<uint64_t, XtOrderExecUpdate>& open_orders,
                             const XtOrderExecUpdate& upd);

/// @brief 订单更新去重器（protocol-agnostic deduplication）。
///
/// 基于 fixed-point 域做进度比较，无 double epsilon。
/// 去重规则：
/// - 已终结订单（final_seen_）再次收到 update 返回 false
/// - 进度回退（filled 减小或 remaining 增大）的更新返回 false
/// - 与上次更新相同进度的更新返回 false
///
/// @note 非线程安全；调用方应在单 worker/callback 序列内使用。
class XtOrderUpdateDeduper {
public:
    using StatusRankFn = int (*)(XtOrderStatus);

    explicit XtOrderUpdateDeduper(size_t final_seen_cap = 200000)
        : final_seen_cap_(final_seen_cap) {}

    bool should_emit(uint64_t order_id, const XtOrderExecUpdate& upd, StatusRankFn rank_fn = nullptr);

    void forget(uint64_t order_id);

    bool last_update(uint64_t order_id, XtOrderExecUpdate& out) const;

    void clear();

    size_t size() const;

private:
    void remember_final(uint64_t order_id);

    size_t final_seen_cap_ = 200000;
    std::unordered_map<uint64_t, XtOrderExecUpdate> last_update_;
    std::unordered_set<uint64_t> final_seen_;
    std::deque<uint64_t> final_seen_fifo_;
};

}
