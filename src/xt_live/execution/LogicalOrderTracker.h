#pragma once

#include <array>
#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrderTypes.h"
#include "api/IOrderIdTranslator.h"

/// @brief 多切片订单的逻辑 ID 聚合器。
///
/// 单切片订单不注册，策略看到的 id 就是物理 id；
/// 多切片订单需要注册 clord_id，后续物理回报会被聚合成同一个逻辑订单视图。
///
/// - LogicalOrderTracker 会被 gateway/control path 和 engine worker 同时访问，
///   内部用 mutex 保护映射和聚合状态。
/// - 不在 tick 行情处理热路径上。
class LogicalOrderTracker : public xt_runtime::IOrderIdTranslator {
public:
    /// @brief 与 XtLiveOrderGateway::kMaxSubmitSlices 对齐。
    static constexpr uint8_t kMaxSlices = 4;

    /// @brief 逻辑订单 ID 起始域，避开 CTP order_ref 数值范围。
    static constexpr uint64_t kClordIdBase = (uint64_t)0xFFFF0000ULL << 16;

    /// @brief 单个逻辑订单的聚合状态。
    struct Entry {
        uint64_t      clord_id                    = 0;
        uint64_t      phys_ids[kMaxSlices]         = {};  // 物理切片 ID（0 表示无效槽）
        int64_t       slice_qty_fp[kMaxSlices]     = {};  // 每切片原始报单量（fixed-point）
        int64_t       slice_filled_fp[kMaxSlices]  = {};  // 每切片累计成交量（动态更新）
        XtOrderStatus slice_status[kMaxSlices]     = {};  // 每切片当前状态（动态更新）
        int64_t       total_qty_fp                 = 0;   // 逻辑总量（= sum of slice_qty_fp，守恒）
        uint8_t       slice_count                  = 0;   // 有效切片数

        // 首切片的非数量字段，用于合成逻辑 XtOrderExecUpdate。
        bool      is_buy       = true;
        int64_t   price_fp     = 0;
        XtOffset  close_offset = XtOffset::Open; // 取首切片（策略原始意图）
        uint64_t  symbol_id    = 0;
        uint64_t  register_ms  = 0;              ///< 注册时 steady_clock ms（stale 清理用）
        uint64_t  all_done_since_ms = 0;         ///< 首次观察到全终态的 steady ms；0 = 仍有活切片

        /// @brief 所有物理切片均已进入终态。
        bool all_done() const noexcept;

        /// @brief 当前全部切片的累计成交量之和。
        int64_t total_filled_fp() const noexcept;

        /// @brief 活切片的未成交量之和（FIX LeavesQty 语义：终态切片不计入）。
        int64_t working_leaves_fp() const noexcept;
    };

    /// @brief 注册多切片订单，并返回策略侧 clord_id。
    ///
    /// 仅在 `slice_count >= 2` 且物理切片均已成功提交后调用；调用方保证
    /// `phys_ids` 非零且不重复。
    uint64_t register_multi_slice(const uint64_t* phys_ids,
                                   const int64_t*  slice_qty_fps,
                                   uint8_t         slice_count,
                                   int64_t         total_qty_fp,
                                   bool            is_buy,
                                   int64_t         price_fp,
                                   XtOffset        close_offset,
                                   uint64_t        symbol_id = 0);

    /// @brief 将物理切片回报翻译为逻辑聚合视图。
    ///
    /// 未命中映射时直接透传；命中时输出 `id=clord_id`，`filled/leaves/status`
    /// 使用全切片聚合值，`last_fill_fp` 保留本次物理回报增量。
    void translate(const XtOrderExecUpdate& phys_upd, XtOrderExecUpdate& out) override;

    /// @brief 将 clord_id 展开为物理切片 ID；返回 0 表示按单切片物理 ID 处理。
    uint8_t expand_to_phys(uint64_t  clord_id,
                            uint64_t* phys_out,
                            uint8_t   max_count) const;

    /// @brief 全终态条目的延迟回收窗口（ms）。
    ///
    /// venue 对最后一个切片先投递终态 order rtn、后投递该笔成交的 trade
    /// （CTP OnRtnOrder→OnRtnTrade；binance UDS 同帧 update→trade）。全终态后
    /// 立即回收会让尾随 fill 的 lookup_clord 落空、策略收到物理 id；窗口内
    /// 映射保持可查，到期由后续任意 cleanup_done 调用回收。
    static constexpr uint64_t kDoneEntryGraceMs = 3000;

    /// @brief 清理全终态且超出 kDoneEntryGraceMs 的逻辑订单及其反查映射。
    void cleanup_done() override;

    /// @brief cleanup_done 的注入时钟版本；返回本次回收的条目数。
    size_t cleanup_done_at(uint64_t now_ms);

    /// @brief 把 book 物理 open-order 行合并为逻辑行（与 on_order 翻译同口径）。
    ///
    /// rows[0..n) 为 get_open_orders 的物理切片行；同一 clord 的多行合并为
    /// 单一逻辑行（id=clord，qty/filled/leaves/status/意图字段取 Entry 聚合），
    /// 未注册的行原样保留，相对顺序不变。返回合并后的行数（<= n)。
    /// @note 进程重启后恢复的多切片单不在 tracker 注册，保持物理行透传。
    /// @note 合并行的 leaves 取 Entry working 口径，可能为 0（如全终态条目在
    ///   grace 窗口内仍被 book 行触发合成）；消费者按 leaves<=0 跳过即可。
    size_t translate_open_orders(XtOrderExecUpdate* rows, size_t n) const;

    /// @brief 物理切片 id → clord_id 只读反查；未命中返回 0（单切片直发单）。
    uint64_t lookup_clord(uint64_t phys_id) const override {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = phys_to_clord_.find(phys_id);
        return it == phys_to_clord_.end() ? 0 : it->second;
    }

    /// @brief 物理切片所属逻辑订单已全终态时返回 clord_id；否则返回 0。
    /// @note 用于 gateway sidecar grace：不能在首个物理切片终态时 mark clord，
    ///       否则尚有活切片的逻辑单会被提前 prune。
    uint64_t lookup_done_clord(uint64_t phys_id) const;

    /// @brief 清理超时未终态的逻辑订单，防止异常场景下映射表无限增长。
    size_t cleanup_stale(uint64_t now_ms, uint64_t stale_threshold_ms);

    /// @brief 当前活跃的多切片逻辑订单数。
    size_t active_count() const {
        std::lock_guard<std::mutex> lock(mu_);
        return entries_.size();
    }

private:
    /// @brief 根据各切片状态聚合逻辑订单状态。
    static XtOrderStatus aggregate_status(const Entry& e,
                                           int64_t total_filled_fp) noexcept;

    /// @brief 判断 status 是否为终态。
    static bool is_terminal(XtOrderStatus s) noexcept;

    /// 串行化全部公开方法；translate 会写 Entry 动态字段，shared lock 无收益。
    mutable std::mutex mu_;

    /// phys_id → clord_id：on_order_update 回调时从物理 ID 反查逻辑 ID。
    std::unordered_map<uint64_t, uint64_t> phys_to_clord_;

    /// clord_id → Entry：维护每个逻辑订单的完整聚合状态。
    std::unordered_map<uint64_t, Entry>    entries_;

    /// 单调自增的逻辑 ID 分配器；从 kClordIdBase+1 起步。
    uint64_t next_clord_id_ = kClordIdBase + 1;
};
