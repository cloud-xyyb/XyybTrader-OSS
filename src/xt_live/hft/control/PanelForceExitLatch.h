#pragma once

/// @file
/// @brief panel force-exit 的 cancel-first 续帧 latch（HFT control frame 驱动）。
///
/// panel ForceExit 是一次性 RPC：symbol 有 working order 时
/// `force_exit_panel_request` 本帧只撤单返 `CancelledOnly`，「下帧再平」没有
/// 载体——活跃策略持续补挂单时 operator 手动重触发永远追不上，(3)/(4) close
/// 阶段结构性不可达。本 latch 把 `CancelledOnly` 挂账，control frame 每轮复用
/// `force_exit_panel_request`（cancel-first → flatten）重试，直到 flat（Done）
/// 或 TTL 到期（Expired）。actuation 模式镜像 PerSidLiquidator。
///
/// 线程契约：`arm()`/`active_count()` 可从 gRPC RPC 线程调用；`step()`/
/// `step_panel()` 只在 control frame 线程调用；内部单 `std::mutex` 序列化。
/// `arm`/`active_count` 为 header-inline——xt_panel（XtRunnerPanelControl）
/// 只引用 inline 成员，不对本 TU 产生链接依赖。

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "PanelHelpers.h"
#include "XtLogger.h"

struct XtHftRunnerControlContext;

namespace xt_live {
namespace control {

enum class LatchStep : uint8_t {
    Keep,
    Done,
    Expired,
};

inline LatchStep panel_force_exit_latch_step(ForceExitOutcome out,
                                             int64_t age_ms,
                                             int64_t ttl_ms) noexcept {
    if (out == ForceExitOutcome::NoPosition) return LatchStep::Done;
    if (ttl_ms > 0 && age_ms >= ttl_ms) return LatchStep::Expired;
    return LatchStep::Keep;
}

/// @brief panel force-exit 续帧 latch registry（HFT-only；CTA/SEL 不装配 = 行为不变）。
class PanelForceExitLatch {
public:
    /// @brief 挂账条目；(symbol, strategy_id) 唯一。
    struct Entry {
        std::string symbol;
        std::string strategy_id;
        int64_t     armed_ms     = 0;
        int64_t     last_outcome = 0;
        uint64_t    frames       = 0;
    };

    explicit PanelForceExitLatch(int64_t ttl_ms = 30'000) : ttl_ms_(ttl_ms) {}

    void arm(const std::string& symbol, const std::string& strategy_id, int64_t now_ms) {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto& e : entries_) {
            if (e.symbol == symbol && e.strategy_id == strategy_id) {
                e.armed_ms = now_ms;
                XtLogger::info_m("control",
                    "panel force_exit latch RE-ARMED symbol={} scope={}",
                    symbol, strategy_id.empty() ? "_global" : strategy_id);
                return;
            }
        }
        Entry e;
        e.symbol      = symbol;
        e.strategy_id = strategy_id;
        e.armed_ms    = now_ms;
        entries_.push_back(std::move(e));
        XtLogger::info_m("control",
            "panel force_exit latch ARMED symbol={} scope={}",
            symbol, strategy_id.empty() ? "_global" : strategy_id);
    }

    void step(XtHftRunnerControlContext& ctx, int64_t now_ms);

    void step_panel(XtPanelContext& panel, int64_t now_ms);

    std::size_t active_count() const {
        std::lock_guard<std::mutex> lk(mu_);
        return entries_.size();
    }

private:
    mutable std::mutex mu_;
    std::vector<Entry> entries_;
    int64_t ttl_ms_ = 30'000;
};

}
}
