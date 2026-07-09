#pragma once

/// @file XtStrategyBase.inl
/// @brief XtStrategyBase 方向下单 helper 的 inline 定义。
///
/// 调用 `enter_long` / `exit_long` / `enter_short` / `exit_short` 的翻译单元
/// 显式 include 本文件；只声明或继承 `XtStrategyBase` 的消费者不需要 include。
/// 这样 `XtStrategyBase.h` 可以保持轻依赖，避免强制拉入 `IStrategyContext` 完整定义。

#include "api/IStrategyContext.h"
#include "xt_strategy_engine/XtStrategyBase.h"

inline uint64_t XtStrategyBase::enter_long(IStrategyContext* ctx,
                                           const char* code,
                                           int64_t price_fp,
                                           int64_t qty_fp,
                                           const char* tag) {
    return ctx->buy_fp(code, price_fp, qty_fp,
                       IStrategyContext::TIF_GTC,
                       tag ? tag : "",
                       XtOffset::Open);
}

inline uint64_t XtStrategyBase::exit_long(IStrategyContext* ctx,
                                          const char* code,
                                          int64_t price_fp,
                                          int64_t qty_fp,
                                          const char* tag) {
    return ctx->sell_fp(code, price_fp, qty_fp,
                        IStrategyContext::TIF_GTC,
                        tag ? tag : "",
                        XtOffset::Close);
}

inline uint64_t XtStrategyBase::enter_short(IStrategyContext* ctx,
                                            const char* code,
                                            int64_t price_fp,
                                            int64_t qty_fp,
                                            const char* tag) {
    return ctx->sell_fp(code, price_fp, qty_fp,
                        IStrategyContext::TIF_GTC,
                        tag ? tag : "",
                        XtOffset::Open);
}

inline uint64_t XtStrategyBase::exit_short(IStrategyContext* ctx,
                                           const char* code,
                                           int64_t price_fp,
                                           int64_t qty_fp,
                                           const char* tag) {
    return ctx->buy_fp(code, price_fp, qty_fp,
                       IStrategyContext::TIF_GTC,
                       tag ? tag : "",
                       XtOffset::Close);
}
