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
/// @brief 进程级 PriceLevel slot 溢出计数。
uint64_t orders_per_level_drops() noexcept;
/// @brief 进程级 price level 容量溢出计数。
uint64_t price_levels_full_drops() noexcept;
}  // namespace xt_engine_sim_book_drops

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
    // ── Constants ───────────────────────────────────────────────────

    /// 价格 fixed-point 缩放因子；与 `kOrderExecFpScale` 保持一致。
    static constexpr double kPriceFpScale = 1e6;

    /// @brief 单 symbol 单方向最大价格层级数；超出时 add_order() 计入 drop counter。
    static constexpr int kMaxPriceLevels = 512;

    /// @brief 单价格层级最大订单数；超出时 add_order() 计入 drop counter。
    static constexpr int kMaxOrdersPerLevel = 32;

    // ── Result types ────────────────────────────────────────────────

    struct Fill {
        uint64_t id        = 0;
        int64_t  qty_fp    = 0;   ///< 成交量 fixed-point
        int64_t  price_fp  = 0;   ///< 成交价 fixed-point
    };

    struct MatchResult {
        std::vector<Fill>     fills;    ///< 本次撮合产生的成交
        std::vector<uint64_t> done;     ///< 已全部成交的订单 ID
        std::vector<uint64_t> canceled; ///< 被撮合规则撤销的订单 ID
    };

    // ── Book mutation ───────────────────────────────────────────────

    /// @brief 将 accepted 订单插入对应 symbol 的价格层级。
    ///
    /// @return false = 未入簿（`ord.code` 空 / `price_fp <= 0` / `qty_fp <= 0` /
    ///         价格层级或层内订单达上限，容量类另递增 drop counter）。
    ///         caller 必须对 false 合成终态回报——否则订单停留 Accepted
    ///         却永不参与撮合，frozen 永不释放。
    [[nodiscard]] bool add_order(const XtSimOrder& ord);

    /// @brief 从价格层级移除订单；未知 symbol、price 或 id 时无副作用。
    void remove_order(const XtSimOrder& ord);

    // ── Matching ────────────────────────────────────────────────────

    /// @brief 基于当前盘口撮合指定 symbol 的在途订单。
    ///
    /// `lookup_fn(lookup_ctx, id)` 必须返回有效的 live order 引用；订单簿会更新
    /// `queue_ahead` 并按 FIFO 处理同价位头部订单。`liquidity_ratio` 会 clamp 到
    /// `[0,1]`，每档可成交量为 `qty * (1 - liquidity_ratio)`。
    ///
    /// @param max_fill_qty 本 tick 全局成交量上限；<0 表示不限制，0 表示本 tick
    ///        无可成交量（GTC 留簿，IOC/FOK 即时撤销）。
    /// @param upper_limit_fp 涨停价（fixed-point；0 = 无限制）。买一顶板时整侧
    ///        冻结 buy taker 撮合（订单留簿，与 level_blocked 语义一致）。
    /// @param lower_limit_fp 跌停价，对称冻结 sell taker；0 = 无限制。
    /// @return 内部复用结果缓冲；下一次 match_tick() 会清空该引用指向的内容。
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
    // ── Internal storage ────────────────────────────────────────────

    /// @brief 单价格层级的订单 ID 队列。
    ///
    /// `push()` / `pop_front()` / `remove()` 共同维护同价位 FIFO：撤单后
    /// 剩余订单保持入队序（venue 时间优先语义）。
    struct PriceLevel {
        int64_t  price_fp                  = 0;   ///< 价格层级 fixed-point key
        uint64_t ids[kMaxOrdersPerLevel]   = {};
        int32_t  count                     = 0;   ///< 当前有效订单数

        bool empty() const noexcept { return count == 0; }

        /// @return false 表示层内订单数已达 kMaxOrdersPerLevel。
        bool push(uint64_t id) noexcept {
            if (count >= kMaxOrdersPerLevel) return false;
            ids[count++] = id;
            return true;
        }

        /// @brief 移除指定 id；保序前移，剩余订单维持 FIFO。
        /// swap-with-last 会把末单提到被撤单位置，破坏时间优先；
        /// 32 上限小数组的 O(n) 前移成本可忽。
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

        /// @brief 弹出同价位队首订单；空层级返回 0。
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
        int size = 0;  ///< 当前有效层级数 [0, kMaxPriceLevels)

        bool empty() const noexcept { return size == 0; }

        /// @brief 按 price_fp 线性查找层级。
        PriceLevel* find(int64_t price_fp) noexcept {
            for (int i = 0; i < size; ++i) {
                if (levels[i].price_fp == price_fp) return &levels[i];
            }
            return nullptr;
        }

        /// @brief 返回已有层级，或按排序不变量插入新层级。
        /// @return 容量已满时返回 nullptr。
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

        /// @brief 按下标移除层级；向左位移以保持排序。
        void erase_at(int idx) noexcept {
            for (int i = idx; i < size - 1; ++i) levels[i] = levels[i + 1];
            --size;
        }
    };

    using BidLevels = FlatLevels<true>;   ///< 买盘：price_fp 降序
    using AskLevels = FlatLevels<false>;  ///< 卖盘：price_fp 升序

    struct Book {
        BidLevels bids;
        AskLevels asks;
    };

    // ── Matching helper ─────────────────────────────────────────────

    /// @brief 单侧撮合主循环；IsBuy 编译期选择买/卖方向。
    ///
    /// Buy 侧消耗 ask depth，Sell 侧消耗 bid depth；价格穿透方向和深度数组
    /// 通过 `if constexpr` 选择。
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

    /// @brief 扫簿撤 IOC/FOK：本 tick 已获撮合机会仍未终态的 IOC/FOK 一律
    ///        进 result.canceled（IOC/FOK 永不留簿）。覆盖未穿价 head、GTC
    ///        身后、深档与冻结侧——旧行为退化 GTC 留簿等 TTL，而 shipped
    ///        主流 order_timeout_ticks: 0。
    template <bool Descending>
    static void sweep_ioc_fok_(FlatLevels<Descending>& side_levels,
                               MatchResult& result,
                               XtSimOrder& (*lookup_fn)(void*, uint64_t),
                               void* lookup_ctx);

    // ── State ───────────────────────────────────────────────────────

    /// 按 symbol 索引的订单簿；每个 match_tick 只做一次 symbol lookup。
    std::unordered_map<std::string, Book> books_;

    /// match_tick() 复用的结果缓冲；返回引用在下一次 match_tick() 后失效。
    MatchResult match_result_;

};
