#pragma once

/// @file
/// @brief CTP 原始字段的清洗与转换工具。
///
/// CTP 回调给的数据比较原始：OrderRef 是字符串、无效价格用 DBL_MAX 当哨兵、
/// 开平和方向是单字符枚举。本文件收集这些字段级转换工具
/// 只做格式转换，不发请求、不持状态。

#include <cstdint>
#include <cmath>
#include <limits>

#include "xt_core/CloseOffsetCore.h"
#include "xt_core/order/XtOrderTypes.h"
#include "ThostFtdcTraderApi.h"

namespace ctp_util {

inline uint64_t parse_local_id(const char* order_ref) {
    if (!order_ref || order_ref[0] == '\0') return 0;
    auto is_space = [](char c) {
        return c == ' ' || c == '\t' || c == '\r' || c == '\n';
    };
    const char* p = order_ref;
    while (*p != '\0' && is_space(*p)) ++p;
    if (*p == '\0') return 0;

    uint64_t value = 0;
    bool has_digit = false;
    constexpr uint64_t kU64Max = std::numeric_limits<uint64_t>::max();
    constexpr uint64_t kU64MaxDiv10 = kU64Max / 10;
    constexpr uint64_t kU64MaxMod10 = kU64Max % 10;

    for (; *p != '\0'; ++p) {
        const char ch = *p;
        if (ch >= '0' && ch <= '9') {
            has_digit = true;
            const uint64_t digit = static_cast<uint64_t>(ch - '0');
            if (value > kU64MaxDiv10 || (value == kU64MaxDiv10 && digit > kU64MaxMod10)) {
                return 0;
            }
            value = value * 10 + digit;
            continue;
        }
        if (!is_space(ch)) return 0;
        while (*p != '\0' && is_space(*p)) ++p;
        return (*p == '\0' && has_digit) ? value : 0;
    }
    return has_digit ? value : 0;
}

inline double normalize_price(double v) noexcept {
    if (!std::isfinite(v)) return 0.0;
    if (v <= 0.0) return 0.0;
    if (v > 1e20) return 0.0;
    return v;
}

inline TThostFtdcOffsetFlagType to_ctp_offset_flag(XtOffset offset, const char* code) {
    switch (offset) {
    case XtOffset::Open:
        return THOST_FTDC_OF_Open;
    case XtOffset::CloseToday:
        return THOST_FTDC_OF_CloseToday;
    case XtOffset::CloseYesterday:
        return THOST_FTDC_OF_CloseYesterday;
    case XtOffset::Close:
    default:
        return close_offset_core::needs_close_today_split(code)
                   ? THOST_FTDC_OF_CloseYesterday
                   : THOST_FTDC_OF_Close;
    }
}

inline XtOffset to_xt_close_offset(TThostFtdcOffsetFlagType offset_flag) {
    if (offset_flag == THOST_FTDC_OF_CloseToday) return XtOffset::CloseToday;
    if (offset_flag == THOST_FTDC_OF_CloseYesterday) return XtOffset::CloseYesterday;
    return XtOffset::Close;
}

inline const char* offset_flag_label(TThostFtdcOffsetFlagType offset_flag) noexcept {
    switch (offset_flag) {
    case THOST_FTDC_OF_Open:
        return "open";
    case THOST_FTDC_OF_Close:
        return "close";
    case THOST_FTDC_OF_CloseToday:
        return "close_today";
    case THOST_FTDC_OF_CloseYesterday:
        return "close_yesterday";
    default:
        return "other";
    }
}

inline XtOrderStatus map_order_status(char status) {
    switch (status) {
    case THOST_FTDC_OST_AllTraded:
        return XtOrderStatus::Filled;
    case THOST_FTDC_OST_PartTradedQueueing:
        return XtOrderStatus::PartiallyFilled;
    case THOST_FTDC_OST_PartTradedNotQueueing:
    case THOST_FTDC_OST_NoTradeNotQueueing:

        return XtOrderStatus::Canceled;
    case THOST_FTDC_OST_NoTradeQueueing:
        return XtOrderStatus::Accepted;
    case THOST_FTDC_OST_Canceled:
        return XtOrderStatus::Canceled;
    default:
        return XtOrderStatus::Submitted;
    }
}

inline int status_rank(XtOrderStatus s) {
    switch (s) {
    case XtOrderStatus::Submitted: return 0;
    case XtOrderStatus::Accepted: return 1;
    case XtOrderStatus::PartiallyFilled: return 2;
    case XtOrderStatus::Filled: return 3;
    case XtOrderStatus::Canceled: return 3;
    case XtOrderStatus::Rejected: return 3;
    default: return 0;
    }
}

}
