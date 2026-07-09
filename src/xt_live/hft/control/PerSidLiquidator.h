#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "xt_core/XtFixedPoint.h"

struct XtHftRunnerControlContext;

namespace xt_live {
namespace control {

/// @brief auto-liquidation 触发判定输入（纯数值，可测）。
struct LiquidateTriggerInput {
    bool    killed              = false;
    int64_t today_realized_fp   = 0;
    int64_t unrealized_fp       = 0;
    int64_t starting_balance_fp = 0;
    int64_t liquidate_pct_fp    = 0;
    int64_t mark_price_fp       = 0;
};

inline bool liquidate_breach(const LiquidateTriggerInput& in) noexcept {
    if (in.killed) return true;
    if (in.liquidate_pct_fp <= 0) return false;
    if (in.starting_balance_fp <= 0) return false;
    if (in.mark_price_fp <= 0) return false;
    const int64_t trigger_pnl = in.today_realized_fp + in.unrealized_fp;
    const int64_t threshold   = xt_fp::mul(in.liquidate_pct_fp, in.starting_balance_fp);
    return trigger_pnl < -threshold;
}

/// @brief PerSidLiquidator 配置。Runner bootstrap 装配，control 线程只读。
struct PerSidLiquidatorCfg {
    bool enabled = false;
    bool dry_run = false;
    std::unordered_map<uint32_t, int64_t> liquidate_pct_fp;
};

/// @brief per-strategy 日内亏损超限的自动强平器。
///
/// PerSidLiquidator 在控制环每轮检查每个策略：per-sid kill switch 已置位、
/// 或当日亏损超过配置比例，就对该策略的持仓调 force_exit 强平——
/// 只平这个策略，不影响同进程其他策略。
/// 强平复用 panel force-exit 的同一条路径（激进 IOC，每帧重发即追价），
/// 自己不维护仓位状态；防超平由 force_exit 内部护栏保证。
///
/// @note 控制线程单线程调用；未启用时整段 no-op。
class PerSidLiquidator {
public:
    void configure(PerSidLiquidatorCfg cfg) { cfg_ = std::move(cfg); }
    bool enabled() const noexcept { return cfg_.enabled; }

    void step(XtHftRunnerControlContext& ctx);

private:
    PerSidLiquidatorCfg cfg_;
    std::unordered_set<std::string> warned_1to1_;
};

}
}
