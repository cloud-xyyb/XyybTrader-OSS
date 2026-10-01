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

#include "PanelHelpers.h"  // ForceExitOutcome / XtPanelContext
#include "XtLogger.h"

struct XtHftRunnerControlContext;  // global ns（HftRunnerControl.h，XtRunner 直编单元）

namespace xt_live {
namespace control {

/// @brief 单帧 step 对一条 latch 条目的处置。
enum class LatchStep : uint8_t {
    Keep,     ///< 保 latch 下帧重试（CancelledOnly / ForceClosed / NoTd / ...）
    Done,     ///< flat（NoPosition）→ 清 latch
    Expired,  ///< TTL 到期仍未 flat → warn + 清 latch（operator 需重新 ForceExit）
};

/// @brief latch 条目处置纯决策（可测）。
/// @note NoPosition 优先于 TTL（同帧 flat 即 Done）；`ttl_ms<=0` 禁用过期。
/// @note NoTd（td down/recovering）同样消耗 TTL——长时间 TD 不可用会走到
///       Expired 清账（已接受姿态：operator 在 TD 恢复后重新触发即可）。
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
        std::string strategy_id;      ///< 空 = runner-wide force-exit
        int64_t     armed_ms     = 0; ///< steady ms（与 control frame now_ms 同源）
        int64_t     last_outcome = 0; ///< 最近一帧 ForceExitOutcome（诊断）
        uint64_t    frames       = 0; ///< 已重试帧数（诊断）
    };

    /// @param ttl_ms 续帧重试 TTL；<=0 = 永不过期。
    explicit PanelForceExitLatch(int64_t ttl_ms = 30'000) : ttl_ms_(ttl_ms) {}

    /// @brief RPC 线程：CancelledOnly 后挂账。(symbol, strategy_id) 去重，
    ///        重复 arm 只刷新时间戳不重复添加。
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

    /// @brief control frame 每轮：投影 panel 依赖视图后逐条目重试 force_exit。
    /// @pre control frame 线程调用；now_ms 与 arm 同为 steady ms。
    void step(XtHftRunnerControlContext& ctx, int64_t now_ms);

    /// @brief step 可测核心：panel 关键依赖缺失 → fail-safe no-op（保 latch 不崩）。
    void step_panel(XtPanelContext& panel, int64_t now_ms);

    /// @brief 当前挂账条目数（诊断/测试）。
    std::size_t active_count() const {
        std::lock_guard<std::mutex> lk(mu_);
        return entries_.size();
    }

private:
    mutable std::mutex mu_;      ///< 序列化 RPC 线程 arm × frame 线程 step
    std::vector<Entry> entries_; ///< (symbol, strategy_id) 去重；panel 操作量级小，线性扫
    int64_t ttl_ms_ = 30'000;    ///< <=0 = 永不过期
};

}  // namespace control
}  // namespace xt_live
