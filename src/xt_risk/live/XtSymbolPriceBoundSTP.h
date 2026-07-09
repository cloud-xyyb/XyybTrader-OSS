#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "api/ISTPChecker.h"
#include "xt_core/order/XtOrder.h"
#include "xt_core/sync/XtTransparentStringHash.h"

/// @brief 基于己方最优买卖边界的轻量 STP 检查器。
///
/// 每个已注册品种只维护两条边界：己方最高买价（max_bid_fp）和最低卖价
///（min_ask_fp）。新买单若价格不低于己方最低卖价，或新卖单若价格不高于
/// 己方最高买价，即判定会穿越己方反向挂单并拒绝。
///
/// 该实现不维护完整 open-order book；订单终态后必须由调用方提供同品种剩余活跃
/// 买/卖价格重建边界。未注册品种返回 false，等价于该品种未启用本检查。
///
/// @warning 不是 lock-free。shared_mutex 只保护 bounds_ 的 key set 与 iterator
/// 生命周期；单品种 price bound 通过 atomic 发布，读侧不会遍历订单簿。
/// @note register_symbol() 应在启动期完成，运行期注册会短暂阻塞 submit 检查路径。
class XtSymbolPriceBoundSTP : public ISTPChecker {
public:
    XtSymbolPriceBoundSTP() = default;

    void register_symbol(const std::string& code) {
        std::unique_lock lock(map_mu_);
        if (bounds_.find(code) == bounds_.end()) {
            bounds_.emplace(std::piecewise_construct,
                            std::forward_as_tuple(code),
                            std::forward_as_tuple());
        }
    }

    bool would_self_trade(const xt_core::XtOrder& req) const override {
        std::shared_lock lock(map_mu_);
        auto it = bounds_.find(req.code);
        if (it == bounds_.end()) return false;

        const SymbolBounds& b = it->second;
        if (req.is_buy) {
            const int64_t min_ask = b.min_ask_fp.load(std::memory_order_acquire);
            return min_ask != kNoAsk && req.price_fp >= min_ask;
        } else {
            const int64_t max_bid = b.max_bid_fp.load(std::memory_order_acquire);
            return max_bid != kNoBid && req.price_fp <= max_bid;
        }
    }

    void on_order_submitted(const std::string& code, bool is_buy, int64_t price_fp) {
        std::shared_lock lock(map_mu_);
        auto it = bounds_.find(code);
        if (it == bounds_.end()) return;

        SymbolBounds& b = it->second;
        if (is_buy) {
            int64_t cur = b.max_bid_fp.load(std::memory_order_relaxed);
            while (price_fp > cur &&
                   !b.max_bid_fp.compare_exchange_weak(cur, price_fp,
                                                        std::memory_order_release,
                                                        std::memory_order_relaxed)) {}
        } else {
            int64_t cur = b.min_ask_fp.load(std::memory_order_relaxed);
            while (price_fp < cur &&
                   !b.min_ask_fp.compare_exchange_weak(cur, price_fp,
                                                        std::memory_order_release,
                                                        std::memory_order_relaxed)) {}
        }
    }

    void on_order_terminal(const std::string& code,
                           const int64_t* active_bids_fp, size_t bid_count,
                           const int64_t* active_asks_fp, size_t ask_count) {
        std::shared_lock lock(map_mu_);
        auto it = bounds_.find(code);
        if (it == bounds_.end()) return;

        SymbolBounds& b = it->second;

        int64_t new_max_bid = kNoBid;
        for (size_t i = 0; i < bid_count; ++i) {
            if (active_bids_fp[i] > new_max_bid) new_max_bid = active_bids_fp[i];
        }

        int64_t new_min_ask = kNoAsk;
        for (size_t i = 0; i < ask_count; ++i) {
            if (active_asks_fp[i] < new_min_ask) new_min_ask = active_asks_fp[i];
        }

        b.max_bid_fp.store(new_max_bid, std::memory_order_release);
        b.min_ask_fp.store(new_min_ask, std::memory_order_release);
    }

private:
    static constexpr int64_t kNoAsk = std::numeric_limits<int64_t>::max();
    static constexpr int64_t kNoBid = std::numeric_limits<int64_t>::min();

    /// @brief 单品种己方最优反向挂单边界。
    struct SymbolBounds {
        std::atomic<int64_t> min_ask_fp{kNoAsk};
        std::atomic<int64_t> max_bid_fp{kNoBid};
    };

    mutable std::shared_mutex map_mu_;

    xt_core::sync::TransparentStringMap<SymbolBounds> bounds_;
};
