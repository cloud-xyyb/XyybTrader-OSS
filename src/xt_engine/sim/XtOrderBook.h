#pragma once
#include <array>
#include <cstdint>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "xt_core/market/XtRawTick.h"
#include "XtSimOrder.h"

/// @file XtOrderBook.h
/// @brief HFT 回测本地订单簿与容量 drop 计数接口。
namespace xt_engine_sim_book_drops {

uint64_t orders_per_level_drops() noexcept;

uint64_t price_levels_full_drops() noexcept;
}

/// @brief 提供 HFT 回测本地订单簿撮合。
///
/// `add_order()` / `remove_order()` 维护本地订单 id 所在价位，
/// `match_tick()` 按当前盘口输出成交、完成和撤单结果。
/// 订单记录由 XtOrderManager 持有；调用方通过 lookup callback 提供当前订单。
///
/// 同一实例必须串行访问。`match_tick()` 返回内部复用结果；调用方必须在下一次
/// `match_tick()` 前消费完。
class XtOrderBook {
public:

    static constexpr double kPriceFpScale = 1e6;

    static constexpr int kMaxPriceLevels = 512;

    static constexpr int kMaxOrdersPerLevel = 32;

    struct Fill {
        uint64_t id        = 0;
        int64_t  qty_fp    = 0;
        int64_t  price_fp  = 0;
    };

    struct MatchResult {
        std::vector<Fill>     fills;
        std::vector<uint64_t> done;
        std::vector<uint64_t> canceled;
    };

    [[nodiscard]] bool add_order(const XtSimOrder& ord);

    void remove_order(const XtSimOrder& ord);

    const MatchResult& match_tick(const char* code,
                                  const int64_t* bid_px_fp,
                                  const int64_t* ask_px_fp,
                                  const int64_t* bid_qty_fp,
                                  const int64_t* ask_qty_fp,
                                  int depth,
                                  double max_fill_qty,
                                  double liquidity_ratio,
                                  XtSimOrder& (*lookup_fn)(void*, uint64_t),
                                  void* lookup_ctx,
                                  int64_t upper_limit_fp = 0,
                                  int64_t lower_limit_fp = 0);

private:

    /// @brief 单价格层级的订单 ID 队列。
    ///
    /// `push()` / `pop_front()` / `remove()` 共同维护同价位 FIFO：撤单后
    /// 剩余订单保持入队序（venue 时间优先语义）。
    struct PriceLevel {
        int64_t  price_fp                  = 0;
        uint64_t ids[kMaxOrdersPerLevel]   = {};
        int32_t  count                     = 0;

        bool empty() const noexcept { return count == 0; }

        bool push(uint64_t id) noexcept {
            if (count >= kMaxOrdersPerLevel) return false;
            ids[count++] = id;
            return true;
        }

        bool remove(uint64_t id) noexcept {
            for (int i = 0; i < count; ++i) {
                if (ids[i] == id) {
                    for (int j = i; j < count - 1; ++j) ids[j] = ids[j + 1];
                    --count;
                    return true;
                }
            }
            return false;
        }

        uint64_t pop_front() noexcept {
            if (count == 0) return 0;
            const uint64_t id = ids[0];
            for (int i = 0; i < count - 1; ++i) ids[i] = ids[i + 1];
            --count;
            return id;
        }

        uint64_t front() const noexcept {
            return (count > 0) ? ids[0] : 0;
        }
    };

    /// @brief 单侧有序价格层级数组。
    ///
    /// `Descending=true` 用于 bids，价格降序；`false` 用于 asks，价格升序。
    /// 插入和删除通过数组位移保持排序，容量固定为 kMaxPriceLevels。
    template <bool Descending>
    struct FlatLevels {
        std::array<PriceLevel, kMaxPriceLevels> levels{};
        int size = 0;

        bool empty() const noexcept { return size == 0; }

        PriceLevel* find(int64_t price_fp) noexcept {
            for (int i = 0; i < size; ++i) {
                if (levels[i].price_fp == price_fp) return &levels[i];
            }
            return nullptr;
        }

        PriceLevel* get_or_insert(int64_t price_fp) noexcept {
            for (int i = 0; i < size; ++i) {
                if (levels[i].price_fp == price_fp) return &levels[i];
            }
            if (size >= kMaxPriceLevels) return nullptr;

            int pos = size;
            for (int i = 0; i < size; ++i) {
                const bool before = Descending
                    ? (price_fp > levels[i].price_fp)
                    : (price_fp < levels[i].price_fp);
                if (before) { pos = i; break; }
            }

            for (int i = size; i > pos; --i) levels[i] = levels[i - 1];
            levels[pos] = PriceLevel{};
            levels[pos].price_fp = price_fp;
            ++size;
            return &levels[pos];
        }

        void erase_at(int idx) noexcept {
            for (int i = idx; i < size - 1; ++i) levels[i] = levels[i + 1];
            --size;
        }
    };

    using BidLevels = FlatLevels<true>;
    using AskLevels = FlatLevels<false>;

    struct Book {
        BidLevels bids;
        AskLevels asks;
    };

    template <bool IsBuy>
    static void match_one_side(
        typename std::conditional<IsBuy, BidLevels, AskLevels>::type& side_levels,
        const int64_t* bid_px_fp,
        const int64_t* ask_px_fp,
        double* bid_left,
        double* ask_left,
        int depth,
        double liq_factor,
        double& remaining_capacity,
        MatchResult& result,
        XtSimOrder& (*lookup_fn)(void*, uint64_t),
        void* lookup_ctx);

    template <bool Descending>
    static void sweep_ioc_fok_(FlatLevels<Descending>& side_levels,
                               MatchResult& result,
                               XtSimOrder& (*lookup_fn)(void*, uint64_t),
                               void* lookup_ctx);

    std::unordered_map<std::string, Book> books_;

    MatchResult match_result_;

};
