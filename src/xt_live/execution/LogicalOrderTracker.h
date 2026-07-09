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

    static constexpr uint8_t kMaxSlices = 4;

    static constexpr uint64_t kClordIdBase = (uint64_t)0xFFFF0000ULL << 16;

    /// @brief 单个逻辑订单的聚合状态。
    struct Entry {
        uint64_t      clord_id                    = 0;
        uint64_t      phys_ids[kMaxSlices]         = {};
        int64_t       slice_qty_fp[kMaxSlices]     = {};
        int64_t       slice_filled_fp[kMaxSlices]  = {};
        XtOrderStatus slice_status[kMaxSlices]     = {};
        int64_t       total_qty_fp                 = 0;
        uint8_t       slice_count                  = 0;

        bool      is_buy       = true;
        int64_t   price_fp     = 0;
        XtOffset  close_offset = XtOffset::Open;
        uint64_t  symbol_id    = 0;
        uint64_t  register_ms  = 0;
        uint64_t  all_done_since_ms = 0;

        bool all_done() const noexcept;

        int64_t total_filled_fp() const noexcept;

        int64_t working_leaves_fp() const noexcept;
    };

    uint64_t register_multi_slice(const uint64_t* phys_ids,
                                   const int64_t*  slice_qty_fps,
                                   uint8_t         slice_count,
                                   int64_t         total_qty_fp,
                                   bool            is_buy,
                                   int64_t         price_fp,
                                   XtOffset        close_offset,
                                   uint64_t        symbol_id = 0);

    void translate(const XtOrderExecUpdate& phys_upd, XtOrderExecUpdate& out) override;

    uint8_t expand_to_phys(uint64_t  clord_id,
                            uint64_t* phys_out,
                            uint8_t   max_count) const;

    static constexpr uint64_t kDoneEntryGraceMs = 3000;

    void cleanup_done() override;

    size_t cleanup_done_at(uint64_t now_ms);

    size_t translate_open_orders(XtOrderExecUpdate* rows, size_t n) const;

    uint64_t lookup_clord(uint64_t phys_id) const override {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = phys_to_clord_.find(phys_id);
        return it == phys_to_clord_.end() ? 0 : it->second;
    }

    uint64_t lookup_done_clord(uint64_t phys_id) const;

    size_t cleanup_stale(uint64_t now_ms, uint64_t stale_threshold_ms);

    size_t active_count() const {
        std::lock_guard<std::mutex> lock(mu_);
        return entries_.size();
    }

private:

    static XtOrderStatus aggregate_status(const Entry& e,
                                           int64_t total_filled_fp) noexcept;

    static bool is_terminal(XtOrderStatus s) noexcept;

    mutable std::mutex mu_;

    std::unordered_map<uint64_t, uint64_t> phys_to_clord_;

    std::unordered_map<uint64_t, Entry>    entries_;

    uint64_t next_clord_id_ = kClordIdBase + 1;
};
