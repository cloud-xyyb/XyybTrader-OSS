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
        int64_t window_ms = 100;            ///< 滑动时间窗口长度 ms
        int64_t max_qty_per_window_fp = 0;  ///< 窗口内最大累计下单量；0 = 不启用
    };

    explicit XtVelocityLimiter(Config cfg) : cfg_(cfg) {}

    /// @brief 启动期预注册品种窗口状态。
    /// @warning 运行期注册会阻塞 check_and_record() 的 shared_lock 热路径。
    void register_symbol(const std::string& code) {
        std::unique_lock lock(map_mu_);
        windows_.emplace(std::piecewise_construct,
                         std::forward_as_tuple(code),
                         std::forward_as_tuple());
    }

    /// @brief 品种是否已注册窗口状态。
    /// @note shared_lock 快查，供 submit 热路径「先查后注册」兜底用；未命中才走
    ///       register_symbol 的 unique_lock（每品种一次性）。
    bool is_registered(std::string_view code) const {
        std::shared_lock lock(map_mu_);
        return windows_.find(std::string(code)) != windows_.end();
    }

    /// @brief 检查并记录本次下单量。
    /// @param code 品种代码；未注册时返回 true。
    /// @param qty_fp 本次下单量（fixed-point）。
    /// @param now_ms 评估时间戳 ms。
    /// @return true 允许；false 表示窗口累计量已超限。
    /// @note 并发窗口滚动失败的线程会走累加路径，语义偏保守。
    bool check_and_record(std::string_view code, int64_t qty_fp, int64_t now_ms) {
        if (cfg_.max_qty_per_window_fp <= 0) return true;

        std::shared_lock lock(map_mu_);
        auto it = windows_.find(std::string(code));
        if (it == windows_.end()) return true;

        WindowState& w = it->second;
        int64_t expected_start = w.window_start_ms.load(std::memory_order_relaxed);

        if (now_ms >= expected_start + cfg_.window_ms) {
            // 窗口过期：CAS 尝试滚动。成功时本线程重置窗口；失败时走累加路径。
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
        std::atomic<int64_t> window_start_ms{0};     ///< 当前窗口起始时间 ms
        std::atomic<int64_t> cumulative_qty_fp{0};   ///< 窗口内累计下单量（fixed-point）
    };

    Config cfg_;
    mutable std::shared_mutex map_mu_;  ///< 保护 windows_ key set 和 iterator 生命周期
    // TransparentStringHash 与全仓纪律一致；C++20 后 check_and_record(string_view)
    // 异构查找免临时 string（C++17 下行为不变）。
    xt_core::sync::TransparentStringMap<WindowState> windows_;  ///< code -> window state
};
