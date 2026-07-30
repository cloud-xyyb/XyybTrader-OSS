#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "xt_core/sync/XtTransparentStringHash.h"

/// @brief 按品种的窗口内累计下单量限制
///
/// 与 order rate limit 的区别：rate limit 控制下单笔数，velocity limit 控制
/// qty/window，防止少量大单造成仓位快速突变。
///
/// @note max_qty_per_window_fp <= 0 或品种未注册时 bypass。
/// @note check_and_record() 先记录本次 qty，再返回是否超限；超限尝试也计入窗口。
/// @warning 检查路径使用 shared_lock + map lookup + per-symbol atomics；不是 lock-free。
class XtVelocityLimiter {
public:
    struct Config {
        int64_t window_ms = 100;
        int64_t max_qty_per_window_fp = 0;
    };

    explicit XtVelocityLimiter(Config cfg) : cfg_(cfg) {}

    void register_symbol(const std::string& code) {
        std::unique_lock lock(map_mu_);
        windows_.emplace(std::piecewise_construct,
                         std::forward_as_tuple(code),
                         std::forward_as_tuple());
    }

    bool is_registered(std::string_view code) const {
        std::shared_lock lock(map_mu_);
        return windows_.find(std::string(code)) != windows_.end();
    }

    bool check_and_record(std::string_view code, int64_t qty_fp, int64_t now_ms) {
        if (cfg_.max_qty_per_window_fp <= 0) return true;

        std::shared_lock lock(map_mu_);
        auto it = windows_.find(std::string(code));
        if (it == windows_.end()) return true;

        WindowState& w = it->second;
        int64_t expected_start = w.window_start_ms.load(std::memory_order_relaxed);

        if (now_ms >= expected_start + cfg_.window_ms) {

            if (w.window_start_ms.compare_exchange_strong(
                    expected_start, now_ms, std::memory_order_relaxed)) {
                w.cumulative_qty_fp.store(qty_fp, std::memory_order_relaxed);
                return qty_fp <= cfg_.max_qty_per_window_fp;
            }
        }

        const int64_t new_cum =
            w.cumulative_qty_fp.fetch_add(qty_fp, std::memory_order_relaxed) + qty_fp;
        return new_cum <= cfg_.max_qty_per_window_fp;
    }

private:
    /// @brief 单品种滑动窗口状态。
    /// @note window_start_ms 与 cumulative_qty_fp 不做复合原子一致性保证；
    ///       窗口滚动并发时可能多计少量 qty，属于保守拒单。
    struct WindowState {
        std::atomic<int64_t> window_start_ms{0};
        std::atomic<int64_t> cumulative_qty_fp{0};
    };

    Config cfg_;
    mutable std::shared_mutex map_mu_;

    xt_core::sync::TransparentStringMap<WindowState> windows_;
};
