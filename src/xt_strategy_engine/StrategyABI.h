#pragma once

#include <cstdint>

#include "XtStrategyBase.h"
#include "xt_core/order/XtOrderTypes.h"

/// @file
/// @brief 策略 plugin 与 runner 之间的 C ABI 边界。
///
/// runner 在创建策略实例前校验版本号和 hot-path layout manifest；任一不匹配
/// 都必须 fail-fast，避免 header-only 策略按不匹配布局读写订单回报 buffer。
/// 修改插件可见接口、虚函数表或 hot-path DTO layout 时必须提升版本号并重编策略。

constexpr uint32_t XT_STRATEGY_ABI_VERSION = 16;

constexpr uint64_t XT_ABI_SIZEOF_CT_ORDER_EXEC_UPDATE =
    static_cast<uint64_t>(sizeof(XtOrderExecUpdate));

constexpr uint64_t XT_ABI_ALIGNOF_CT_ORDER_EXEC_UPDATE =
    static_cast<uint64_t>(alignof(XtOrderExecUpdate));

using FnCreateStrategy  = XtStrategyBase* (*)();

using FnDestroyStrategy = void             (*)(XtStrategyBase*);

using FnCtAbiVersion    = uint32_t         (*)();

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
    }

