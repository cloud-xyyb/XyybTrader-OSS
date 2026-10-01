#pragma once

#include <cstdint>

#include "XtStrategyBase.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"

/// @file
/// @brief 策略 plugin 与 runner 之间的 C ABI 边界。
///
/// runner 在创建策略实例前校验版本号和 hot-path layout manifest；任一不匹配
/// 都必须 fail-fast，避免 header-only 策略按不匹配布局读写订单回报 buffer。
/// 修改插件可见接口、虚函数表或 hot-path DTO layout 时必须提升版本号并重编策略。

/// @brief 当前策略接口 ABI 版本号。
/// v17：XtTick/XtRawTick DEPTH 10→20（sizeof 512→832，hot-path DTO layout 变更；
///      插件 .so 必须与 runner 同批重编部署）。
constexpr uint32_t XT_STRATEGY_ABI_VERSION = 17;

/// @brief `XtOrderExecUpdate` size manifest；插件和 runner 必须完全一致。
constexpr uint64_t XT_ABI_SIZEOF_CT_ORDER_EXEC_UPDATE =
    static_cast<uint64_t>(sizeof(XtOrderExecUpdate));
/// @brief `XtOrderExecUpdate` alignment manifest；插件和 runner 必须完全一致。
constexpr uint64_t XT_ABI_ALIGNOF_CT_ORDER_EXEC_UPDATE =
    static_cast<uint64_t>(alignof(XtOrderExecUpdate));
/// @brief `XtTick` size manifest；插件和 runner 必须完全一致（on_tick /
///        get_last_tick / get_ticks 跨 .so 传递的 hot-path DTO）。
constexpr uint64_t XT_ABI_SIZEOF_CT_TICK =
    static_cast<uint64_t>(sizeof(XtTick));

/// @name 统一插件导出符号（unified plugin contract）
///
/// 每个策略插件必须导出 `create_strategy` / `destroy_strategy` /
/// `xt_abi_version` / `xt_sizeof_CtOrderExecUpdate` /
/// `xt_alignof_CtOrderExecUpdate` / `xt_sizeof_XtTick` 六个 `extern "C"` 符号。
/// `XT_STRATEGY_ABI_EXPORTS(StrategyType)` 是插件侧标准实现入口。
/// @{

/// @brief `create_strategy` symbol：创建策略实例，ownership 转交 runner。
using FnCreateStrategy  = XtStrategyBase* (*)();
/// @brief `destroy_strategy` symbol：销毁由同一插件创建的策略实例。
using FnDestroyStrategy = void             (*)(XtStrategyBase*);
/// @brief `xt_abi_version` symbol：返回插件编译时 ABI 版本。
using FnCtAbiVersion    = uint32_t         (*)();
/// @brief layout manifest symbols：返回插件编译时 hot-path DTO layout。
using FnCtAbiLayout     = uint64_t         (*)();

#define XT_STRATEGY_ABI_EXPORTS(StrategyType)                                      \
    extern "C" XtStrategyBase* create_strategy() { return new StrategyType(); }     \
    extern "C" void destroy_strategy(XtStrategyBase* s) { delete s; }              \
    extern "C" uint32_t xt_abi_version() { return XT_STRATEGY_ABI_VERSION; }        \
    extern "C" uint64_t xt_sizeof_CtOrderExecUpdate() {                            \
        return XT_ABI_SIZEOF_CT_ORDER_EXEC_UPDATE;                                  \
    }                                                                               \
    extern "C" uint64_t xt_alignof_CtOrderExecUpdate() {                           \
        return XT_ABI_ALIGNOF_CT_ORDER_EXEC_UPDATE;                                 \
    }                                                                               \
    extern "C" uint64_t xt_sizeof_XtTick() { return XT_ABI_SIZEOF_CT_TICK; }

/// @}
