#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "xt_account/state/AccountStateStore.h"
#include "xt_core/XtFixedPoint.h"

struct XtHftRunnerControlContext;  // global ns（HftRunnerControl.h，XtRunner 直编单元）

namespace xt_live {
namespace control {

/// @brief auto-liquidation 触发判定输入（纯数值，可测）。
struct LiquidateTriggerInput {
    bool    killed              = false; ///< per-sid kill_switch 已置 → 直接触发。
    int64_t today_realized_fp   = 0;     ///< aggregator − 日初 baseline。
    int64_t unrealized_fp       = 0;     ///< snapshot_position(code).unrealized_pnl（含 vol_scale）。
    int64_t starting_balance_fp = 0;     ///< account session 起始权益。
    int64_t liquidate_pct_fp    = 0;     ///< to_fp(per_strategy_liquidate_pct[sid])；0 = 未配置。
    int64_t mark_price_fp       = 0;     ///< venue 真值（`venue_mark_price`）；<=0 → 跳过 daily-loss 评估。
};

/// @brief 是否应 arm per-sid auto-liquidation。
///
/// kill_switch 直接触发；否则 daily-loss：trigger_pnl < -(pct × starting_balance)。
/// 判据与 reject 层 per-strategy daily-loss（XtRiskEngine::eval_per_strategy_）同口径：
/// strict `<` + sid/pct/starting_balance bypass guard；本层额外 mark<=0 skip
/// （防 mark 不可用时按假亏误 latch）。bypass 任一不满足返回 false。
/// mark 闸的口径必须与 actuation 一致（venue 真值）：force_exit 缺 venue 即拒单，
/// 按估值口径开闸只会 arm 一个执行不了的 latch。
inline bool liquidate_breach(const LiquidateTriggerInput& in) noexcept {
    if (in.killed) return true;                    // kill_switch source
    if (in.liquidate_pct_fp <= 0) return false;    // 未配置 liquidate 阈值
    if (in.starting_balance_fp <= 0) return false; // session 起始权益未冻结
    if (in.mark_price_fp <= 0) return false;       // mark 不可用 → 不按假亏触发
    const int64_t trigger_pnl = in.today_realized_fp + in.unrealized_fp;
    const int64_t threshold   = xt_fp::mul(in.liquidate_pct_fp, in.starting_balance_fp);
    return trigger_pnl < -threshold;               // strict <
}

/// @brief 从账户读侧组装 daily-loss 触发输入。
///
/// mark 取 `venue_mark_price()`（执行口径）而不是帧内 `PositionSnapshot::mark_price`
/// （估值口径，venue 缺失时回退持仓成本价）：actuation 走的 force_exit 缺 venue 即返
/// `NoMarkPrice`，按估值口径开闸只会 arm 一个执行不了的 latch——该 sid 被 kill、每帧
/// 重试却永远平不掉。
/// @param store 账户状态存储（venue mark 与 session 起始权益的读点）。
/// @param code 该 sid 的唯一 declared symbol。
/// @param killed per-sid kill_switch 是否已置位。
/// @param today_realized_fp aggregator − 日初 baseline。
/// @param unrealized_fp 帧内持仓浮盈（与 trigger gauge 同一次快照读，口径不分叉）。
/// @param liquidate_pct_fp to_fp(per_strategy_liquidate_pct[sid])；0 = 未配置。
[[nodiscard]] inline LiquidateTriggerInput make_liquidate_trigger_input(
        const xt_account::AccountStateStore& store,
        const std::string& code,
        bool killed,
        int64_t today_realized_fp,
        int64_t unrealized_fp,
        int64_t liquidate_pct_fp) {
    LiquidateTriggerInput in;
    in.killed              = killed;
    in.starting_balance_fp = store.starting_balance_fp();
    in.mark_price_fp       = store.venue_mark_price(code);
    in.unrealized_fp       = unrealized_fp;
    in.liquidate_pct_fp    = liquidate_pct_fp;
    // pct<=0 时 today_realized 不参与（liquidate_breach 在 pct guard 早返）。
    in.today_realized_fp   = (liquidate_pct_fp > 0) ? today_realized_fp : 0;
    return in;
}

/// @brief PerSidLiquidator 配置。Runner bootstrap 装配，control 线程只读。
struct PerSidLiquidatorCfg {
    bool enabled = false;                                   ///< 总开关（runtime.risk.per_strategy_auto_liquidate）。
    bool dry_run = false;                                   ///< true: 评估+latch+log，不调 force_exit（无订单/无 book 变更）。
    std::unordered_map<uint32_t, int64_t> liquidate_pct_fp; ///< sid_id → to_fp(pct)；flatten 硬阈值。
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

    /// @brief 每帧：评估 per-sid 突破 → edge-latch → 对 latched sid 调 force_exit。
    /// @pre 单线程（control frame loop）。enabled()==false 时立即返回。
    void step(XtHftRunnerControlContext& ctx);

private:
    PerSidLiquidatorCfg cfg_;
    std::unordered_set<std::string> warned_1to1_; ///< 1:1 violation warn-once。
};

}  // namespace control
}  // namespace xt_live
