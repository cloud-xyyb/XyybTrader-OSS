#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "XtSelRunnerConfig.h"

class XtStrategyBase;

namespace xt_bt_sel {

/// @brief SEL backtest runner 的一次性执行入口。
///
/// 按 trading date 驱动 `XtSelEngine` 和 `SelTaskScheduler`，把目标权重变化转成
/// CTA backtest gateway 的 submit/risk/account/fill 链路，并复用 shared backtest
/// report row 写出结果。调用方通过 `XtSelRunnerConfig` 提供策略、日历和价格输入。
class XtSelBacktestRunner {
public:
    /// @brief 装配 + 跑完整 backtest（一次性）。
    /// @param external_strategy 非空时 cfg.strategies[0] 不走 plugin factory，
    ///        直接用该实例（caller 保生命周期跨 run；init/on_init/add_context
    ///        与 plugin 路径同流，所有权不接管）。仅支持单 strategy
    ///        （cfg.strategies.size()>1 时报错）。
    /// @return 0 = 成功；非 0 = 失败（1=data/tradability/CA load 或 CA 输入数据错误
    ///         （每股现金分红 ≥ T-1 参考价 → 除息后参考价非正）, 3=strategy 装配
    ///         失败（register 失败/so 空且无 external/external 多 strategy）,
    ///         4=create, 5=add_context, 6=task 不匹配, 8=首日无 T-1 close 且
    ///         策略当日请求 sizing（拒绝 T 日 close lookahead 兜底，需提供
    ///         warmup close 数据或前移数据起点；报告不写出）,
    ///         9=报告写出失败（回测跑完但产物不完整/不可信）,
    ///         10=现金落负、11=权益会计恒等破裂（两者同级账本不变量破裂，报告不写出））
    /// @note rc=7 已退役（原 equity_after 回填对齐守卫）：报告行改在 account apply
    ///       同帧构造，回填错位这一失败模式不复存在。该值不再复用。
    /// @note rc=64 已退役（原「CA 含配股行」拒跑）：配股已建模（`selector.rights_policy`
    ///       缴款认购 / 弃权两口径），该失败模式不复存在。该值不再复用。
    ///       退役不 bump pybind ABI —— 签名/type/layout 未变，且单边 bump 会拉起
    ///       standalone xtpy 的 import 护栏（见 src/xt_pybind/README.md）。
    [[nodiscard]] static int run(const XtSelRunnerConfig& cfg,
                                 XtStrategyBase* external_strategy = nullptr);
};

}  // namespace xt_bt_sel
