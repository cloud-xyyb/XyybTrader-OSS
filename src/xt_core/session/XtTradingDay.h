#pragma once

#include <cstdint>
#include <ctime>

/// @file
/// @brief 按 venue 时间规则把 UTC 时间戳转换为交易日和本地事件时间。
///
/// `TimezonePolicy` 描述 venue 的自然日或夜盘归属规则。
/// `derive_trading_day()` 返回 trading_day（YYYYMMDD）；
/// `derive_action_time()` 返回 action_time（HHMMSSmmm）。
///
/// broker 已提供权威 TradingDay 时应直接透传 broker 字段。

namespace xt_core {

enum class TimezonePolicy {

    UtcNaturalDay,

    AsiaShanghaiNaturalDay,

    ChinaFuturesNightSession,

    UsEasternNaturalDay,
};

inline uint32_t derive_trading_day(int64_t epoch_ms, TimezonePolicy policy) noexcept {
    if (epoch_ms <= 0) return 0;
    const std::time_t sec = static_cast<std::time_t>(epoch_ms / 1000);
    std::tm tm_out{};

    switch (policy) {
        case TimezonePolicy::UtcNaturalDay:
            gmtime_r(&sec, &tm_out);
            break;

        case TimezonePolicy::AsiaShanghaiNaturalDay: {

            const std::time_t cst = sec + 8 * 3600;
            gmtime_r(&cst, &tm_out);
            break;
        }

        case TimezonePolicy::ChinaFuturesNightSession: {

            const std::time_t cst = sec + 8 * 3600;
            gmtime_r(&cst, &tm_out);

            if (tm_out.tm_hour >= 21) {

                std::time_t next = cst + 24 * 3600;
                gmtime_r(&next, &tm_out);
            }

            break;
        }

        case TimezonePolicy::UsEasternNaturalDay: {

            const std::time_t et = sec - 5 * 3600;
            gmtime_r(&et, &tm_out);
            break;
        }
    }

    return static_cast<uint32_t>(
        (tm_out.tm_year + 1900) * 10000 + (tm_out.tm_mon + 1) * 100 + tm_out.tm_mday);
}

inline uint32_t derive_action_time(int64_t epoch_ms, TimezonePolicy policy) noexcept {
    if (epoch_ms <= 0) return 0;
    const std::time_t sec = static_cast<std::time_t>(epoch_ms / 1000);
    const uint32_t ms = static_cast<uint32_t>(epoch_ms % 1000);
    std::tm tm_out{};

    switch (policy) {
        case TimezonePolicy::UtcNaturalDay:
            gmtime_r(&sec, &tm_out);
            break;
        case TimezonePolicy::AsiaShanghaiNaturalDay:
        case TimezonePolicy::ChinaFuturesNightSession: {
            const std::time_t cst = sec + 8 * 3600;
            gmtime_r(&cst, &tm_out);
            break;
        }
        case TimezonePolicy::UsEasternNaturalDay: {
            const std::time_t et = sec - 5 * 3600;
            gmtime_r(&et, &tm_out);
            break;
        }
    }
    return static_cast<uint32_t>(
        tm_out.tm_hour * 10000000 + tm_out.tm_min * 100000 +
        tm_out.tm_sec * 1000 + ms);
}

}
