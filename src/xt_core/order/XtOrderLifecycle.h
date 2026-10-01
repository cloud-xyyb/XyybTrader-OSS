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

/// @brief 有效剩余量（fixed-point 域，无 epsilon）。
/// leaves_fp 为正时直接返回；否则 fallback 到 qty_fp - filled_fp；最终 clamp 到 0。
int64_t effective_remaining_fp(const XtOrderExecUpdate& upd);

/// @brief 判断是否为终态更新（fixed-point 域）。
/// 终态条件：status 是终态（Filled/Canceled/Rejected）或 effective_remaining_fp <= 0。
bool is_final_update(const XtOrderExecUpdate& upd);

/// @brief 判断两个 update 是否代表相同进度（fixed-point 域，整数精确比较）。
bool same_update_progress(const XtOrderExecUpdate& lhs,
                          const XtOrderExecUpdate& rhs);

/// @brief 维护 open_orders 表（终态时 erase，非终态时 upsert）。
///
/// @return true 表示订单仍在 open 表内；false 表示终态更新已移除订单。
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

    /// @param final_seen_cap 终态订单 ID 的去重窗口容量；0 表示不主动淘汰。
    explicit XtOrderUpdateDeduper(size_t final_seen_cap = 200000)
        : final_seen_cap_(final_seen_cap) {}

    /// @brief 判断该 update 是否应向下游 emit。
    ///
    /// `rank_fn` 可选，用于 venue-specific 终态/非终态 rank 防回退。`order_id=0` 是调用方错误，
    /// debug 下 assert，release 下 abort。
    ///
    /// @return true 表示该 update 通过去重与单调性检查。
    bool should_emit(uint64_t order_id, const XtOrderExecUpdate& upd, StatusRankFn rank_fn = nullptr);

    /// @brief 移除某订单的 last/update final_seen 记录，供重建或测试重置单笔状态。
    void forget(uint64_t order_id);

    /// @brief 读取某订单最近一次非终态观测（终态后条目已清，返回 false）。
    bool last_update(uint64_t order_id, XtOrderExecUpdate& out) const;

    /// @brief 清空全部去重状态。
    void clear();

    /// @brief 返回 last_update_ 与 final_seen_ 的合计观测规模。
    size_t size() const;

private:
    void remember_final(uint64_t order_id);

    // final_seen_ 防止终态后重复回报；FIFO 只用于按容量淘汰，forget() 后的 stale ID 惰性跳过。
    size_t final_seen_cap_ = 200000;
    std::unordered_map<uint64_t, XtOrderExecUpdate> last_update_;
    std::unordered_set<uint64_t> final_seen_;
    std::deque<uint64_t> final_seen_fifo_;
};

} // namespace order_lifecycle
