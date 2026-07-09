#pragma once

#include <cstdint>
#include <string>

#include "xt_core/order/XtOrder.h"

namespace xt_core {

enum class XtOrderSource : uint8_t {
    Strategy = 0,
    Manual,
    Closeout,
    Recovery,
};

inline const char* to_cstr(XtOrderSource s) noexcept {
    switch (s) {
        case XtOrderSource::Strategy: return "strategy";
        case XtOrderSource::Manual:   return "manual";
        case XtOrderSource::Closeout: return "closeout";
        case XtOrderSource::Recovery: return "recovery";
    }
    return "unknown";
}

/**
 * @brief 进程内订单请求
 *
 * 包含 `XtOrder` 订单意图以及 submit-time metadata。该结构只在进程内传递；
 * risk/gateway 层读取 source、sid、trace、dry-run、forced-close 和时间字段后，
 * 构造各自模块的执行上下文。
 *
 * @note 该 wrapper 含 `std::string`，不是 hot-path POD wire type。
 */
struct XtOrderRequest {

    XtOrder order{};

    std::string strategy_id;

    XtOrderSource source = XtOrderSource::Strategy;

    uint64_t audit_trace_id = 0;

    uint32_t sid_id = 0;

    bool dry_run = false;

    bool is_forced_close = false;

    int64_t now_ms = 0;
};

}
