#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <unordered_map>
#include <unordered_set>

#include "XtOrderTypes.h"

/// @file
/// @brief Protocol-agnostic 订单生命周期 helper 与去重器。

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
