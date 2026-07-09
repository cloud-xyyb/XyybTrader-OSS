// HFT ABI 导出（策略类定义在 HftMarketVerifyStrategy.h）。
//
// market_order_support Stage 4 一次性市价单实盘验收插件。与 hft_tick_revert 同 ABI
// （XtStrategyBase 已统一支持 HFT/CTA），仅 override on_tick/on_fill。
#include "HftMarketVerifyStrategy.h"
#include "xt_strategy_engine/StrategyABI.h"

XT_STRATEGY_ABI_EXPORTS(HftMarketVerifyStrategy)
