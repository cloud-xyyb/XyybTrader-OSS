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

/// @brief 交易日时区 / session 模型策略。
enum class TimezonePolicy {
    /// UTC 自然日，无 session boundary。binance / 加密 perp 用此。
    UtcNaturalDay,
    /// Asia/Shanghai 自然日。crypto exchange 但希望日报对齐国内时区可用。
    AsiaShanghaiNaturalDay,
    /// 国内期货夜盘归次日。
    ///
    /// `[21:00, 24:00)` 和 `[0:00, 2:30)` 归同一夜盘交易日；仅在 venue
    /// 实现拿不到 broker trading_day 时用作 fallback。
    ChinaFuturesNightSession,
    /// US Eastern 自然日（盘后归当日）。美股用。
    UsEasternNaturalDay,
};

/// @brief 把 UTC epoch ms 派生为 trading_day = YYYYMMDD。
/// @param epoch_ms UTC milliseconds since 1970-01-01
/// @param policy   时区/session 策略；不同 venue 选不同
/// @return 0 if epoch_ms <= 0；否则 YYYYMMDD（如 20260425）
inline uint32_t derive_trading_day(int64_t epoch_ms, TimezonePolicy policy) noexcept {
    if (epoch_ms <= 0) return 0;
    const std::time_t sec = static_cast<std::time_t>(epoch_ms / 1000);
    std::tm tm_out{};

    switch (policy) {
        case TimezonePolicy::UtcNaturalDay:
            gmtime_r(&sec, &tm_out);
            break;

        case TimezonePolicy::AsiaShanghaiNaturalDay: {
            // CST = UTC + 8h，无 DST。
            const std::time_t cst = sec + 8 * 3600;
            gmtime_r(&cst, &tm_out);
            break;
        }

        case TimezonePolicy::ChinaFuturesNightSession: {
            // CST 时区 + 夜盘归次日。
            const std::time_t cst = sec + 8 * 3600;
            gmtime_r(&cst, &tm_out);
            // 夜盘 21:00-23:59 → +1 day。0:00 后的凌晨段（夜盘最晚约 02:30 收）
            // 跨午夜后 gmtime 已给次自然日，正是夜盘归属交易日，无需调整。
            if (tm_out.tm_hour >= 21) {
                // 21:00-23:59 归下一交易日。
                std::time_t next = cst + 24 * 3600;
                gmtime_r(&next, &tm_out);
            }
            // else 凌晨段：不调整（上界宽到日盘开盘前，02:30-09:00 无夜盘 tick，no-op）。
            break;
        }

        case TimezonePolicy::UsEasternNaturalDay: {
            // ET 简化为 UTC-5；需要 DST 精度时应接入 tz database。
            const std::time_t et = sec - 5 * 3600;
            gmtime_r(&et, &tm_out);
            break;
        }
    }

    return static_cast<uint32_t>(
        (tm_out.tm_year + 1900) * 10000 + (tm_out.tm_mon + 1) * 100 + tm_out.tm_mday);
}

/// @brief 把 UTC ms 派生为 action_time = HHMMSSmmm（uint32_t）。
/// @note  跟 trading_day 配对；时区跟 trading_day policy 一致。
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

}  // namespace xt_core
