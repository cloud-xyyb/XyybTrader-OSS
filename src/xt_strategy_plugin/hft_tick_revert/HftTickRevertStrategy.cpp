// HFT ABI 导出（策略类定义在 HftTickRevertStrategy.h）
//
// HFT 反转策略通过当前统一 ABI 导出。
// 与 cta_sma_xover 同 ABI（XtStrategyBase 已统一支持 HFT/CTA），
// 仅 override on_tick 而非 on_bar。

#include "HftTickRevertStrategy.h"
#include "xt_strategy_engine/StrategyABI.h"

XT_STRATEGY_ABI_EXPORTS(HftTickRevertStrategy)
