// HFT ABI 导出（策略类定义在 HftMarketMakingStrategy.h）
//
// 经典 HFT 双边挂单做市策略通过当前统一 ABI 导出。
// 与 hft_tick_revert 同 ABI（XtStrategyBase 已统一支持 HFT/CTA），
// 仅 override on_tick / on_fill / on_order / on_roll，仓位走主动 pull，使用 GTC 限价单维护
// 双边 maker quotes。

#include "HftMarketMakingStrategy.h"
#include "xt_strategy_engine/StrategyABI.h"

XT_STRATEGY_ABI_EXPORTS(HftMarketMakingStrategy)
