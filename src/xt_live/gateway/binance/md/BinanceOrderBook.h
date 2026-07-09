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

    static constexpr int64_t kPriceScale = 10000;

    BinanceOrderBook() = default;

    void apply_snapshot(uint64_t lastUpdateId,
                        const std::vector<std::pair<double, double>>& bids,
                        const std::vector<std::pair<double, double>>& asks);

    bool apply_diff(uint64_t U, uint64_t u, uint64_t pu,
                    const std::vector<std::pair<double, double>>& bid_updates,
                    const std::vector<std::pair<double, double>>& ask_updates);

    void top_n(std::size_t n,
               double* out_bid_prices, double* out_bid_qtys,
               double* out_ask_prices, double* out_ask_qtys) const;

    bool is_ready() const noexcept { return has_snapshot_; }

    uint64_t last_update_id() const noexcept { return last_update_id_; }

    void reset() noexcept;

    std::size_t bid_levels() const noexcept { return bids_.size(); }
    std::size_t ask_levels() const noexcept { return asks_.size(); }

private:

    static int64_t price_to_key(double price) noexcept;

    static double  key_to_price(int64_t key) noexcept;

    template <typename LevelsMap>
    static void apply_updates(LevelsMap& levels,
                              const std::vector<std::pair<double, double>>& updates);

    std::map<int64_t, double, std::greater<int64_t>> bids_;
    std::map<int64_t, double>                         asks_;
    uint64_t last_update_id_     = 0;
    uint64_t prev_diff_u_        = 0;
    bool     has_snapshot_       = false;
    bool     first_after_snap_   = true;
};

}
