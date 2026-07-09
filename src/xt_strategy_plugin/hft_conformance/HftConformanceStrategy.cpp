// HFT ABI 导出（策略类定义在 HftConformanceStrategy.h）。
//
// HFT 全功能高压自检策略（conformance / stress oracle）。与 hft_tick_revert 同 ABI
// （XtStrategyBase 已统一支持 HFT/CTA），override on_tick/on_order/on_fill/on_account
// 及生命周期回调，一套 .so 在 backtest / CTP / binance 三路同跑。
#include "HftConformanceStrategy.h"
#include "xt_strategy_engine/StrategyABI.h"

XT_STRATEGY_ABI_EXPORTS(HftConformanceStrategy)
