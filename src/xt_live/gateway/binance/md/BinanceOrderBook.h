#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <utility>
#include <vector>

namespace binance_u {

/// @brief Binance 本地 L2 盘口
///
/// 单线程对象；调用方负责在同一个 io_context 线程内串行调用 reset/apply/read。
/// BinanceOrderBook 维护 book 和校验 depth diff 连续性；
///
/// - REST snapshot 初始化完整盘口，然后用 WS depth diff 持续更新盘口
///   如果 diff 的 update id 接不上，就返回失败，让上层重新拉 snapshot。
/// - gateway 组装行情 tick 时，从 BinanceOrderBook 取 top N 档 bid/ask。
class BinanceOrderBook {
public:
    /// @brief 价格 key 的量化因子（BTCUSDT tickSize=0.1，1e4 有冗余精度）。
    static constexpr int64_t kPriceScale = 10000;

    BinanceOrderBook() = default;

    /// @brief 从 REST depth snapshot 重置 full book；清空旧 bids/asks。
    /// @param lastUpdateId Binance snapshot 的 `lastUpdateId` 字段。
    /// @param bids/asks `<price, qty>` 对；qty<=0 的行会被忽略。
    void apply_snapshot(uint64_t lastUpdateId,
                        const std::vector<std::pair<double, double>>& bids,
                        const std::vector<std::pair<double, double>>& asks);

    /// @brief 应用一条 futures `@depth` diff；返回 false 表示 sequence gap。
    /// @param U  event 的 first update id。
    /// @param u  event 的 final update id。
    /// @param pu futures `depthUpdate.pu`；首条 snapshot 后 diff 可传 0。
    /// @param bid_updates/ask_updates `<price, qty>`；qty==0 删除档位，qty>0 覆盖绝对数量。
    /// @return true = 已应用或丢弃 stale diff；false = gap，调用方必须 re-snapshot。
    bool apply_diff(uint64_t U, uint64_t u, uint64_t pu,
                    const std::vector<std::pair<double, double>>& bid_updates,
                    const std::vector<std::pair<double, double>>& ask_updates);

    /// @brief 取 top N 档填入输出数组；不足 N 档尾部置 0。
    /// @param n                   请求档位数（调用方保证 n <= 输出数组容量）
    /// @param out_bid_prices      输出买价数组（降序）
    /// @param out_bid_qtys        输出买量数组
    /// @param out_ask_prices      输出卖价数组（升序）
    /// @param out_ask_qtys        输出卖量数组
    void top_n(std::size_t n,
               double* out_bid_prices, double* out_bid_qtys,
               double* out_ask_prices, double* out_ask_qtys) const;

    /// @brief 是否已成功 apply snapshot，并可接受 diff。
    bool is_ready() const noexcept { return has_snapshot_; }

    /// @brief 当前本地 book 对应的最新 Binance update id。
    uint64_t last_update_id() const noexcept { return last_update_id_; }

    /// @brief 清空 book，回到未初始化状态（用于 reconnect + re-snapshot 前的重置）。
    void reset() noexcept;

    /// @brief 盘口深度统计（测试/诊断用）。
    std::size_t bid_levels() const noexcept { return bids_.size(); }
    std::size_t ask_levels() const noexcept { return asks_.size(); }

private:
    /// @brief 将 Binance price 量化为内部 price_key，用于 book map 稳定排序。
    static int64_t price_to_key(double price) noexcept;

    /// @brief 将内部 price_key 还原为 top_n 输出价格。
    static double  key_to_price(int64_t key) noexcept;

    /// @brief 把 Binance depth diff 的一侧更新写入本地 book。
    /// @tparam LevelsMap `bids_` 或 `asks_` 的 map 类型；key 为内部 price_key，value 为绝对 qty。
    /// @param levels 目标侧盘口容器。
    /// @param updates Binance diff 的 `[price, qty]` 列表；`qty > 0` 覆盖绝对数量，`qty == 0` 删除价位。
    template <typename LevelsMap>
    static void apply_updates(LevelsMap& levels,
                              const std::vector<std::pair<double, double>>& updates);

    std::map<int64_t, double, std::greater<int64_t>> bids_;  ///< price_key 降序，best bid 在 begin。
    std::map<int64_t, double>                         asks_; ///< price_key 升序，best ask 在 begin。
    uint64_t last_update_id_     = 0;  ///< 本地 book 对应的最新 update id。
    uint64_t prev_diff_u_        = 0;  ///< 上一条 diff 的 u，用于 futures `pu` 连续性校验。
    bool     has_snapshot_       = false; ///< 是否已经加载 REST snapshot。
    bool     first_after_snap_   = true;  ///< snapshot 后首条 diff 走 lastUpdateId 覆盖校验。
};

}  // namespace binance_u
