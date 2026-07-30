#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

/// @file CtpMdSessionGate.h
/// @brief CTP MD "expected-tick window" gate — 纯逻辑，供 connector CLI 解析、
///        connector 观测 gauge、ParserCTP stale watchdog 决策共用。
///
/// 语义：窗口是「此刻应该持续有 tick」的分钟集合（expected-tick window），**不是**
/// 连接准备时段的超集。窗外不运行 stale watchdog（不评估、不重连、不置 unhealthy），
/// 消除休市 churn。收盘分钟本身不在窗内（半开区间），否则最后一笔 tick 后 ~30s
/// 仍会误触发一次重连。全部函数无 CTP SDK 依赖，header-only 可单测。

namespace ctp_md_session {

/// @brief 半开分钟窗 `[start_min, end_min)`，分钟为一天内偏移（0..1439）。
/// start_min < end_min 为普通日内窗；start_min > end_min 为跨午夜窗
/// （evening 段 `[start_min, 1440)` + early 段 `[0, end_min)`）。start_min == end_min
/// 非法（零宽），解析层拒绝。
struct TradingWindow {
    int start_min = 0;
    int end_min = 0;
};

/// @brief 交易窗 gate。gate_enabled=false 时 watchdog 恒活（等价现行行为，回滚开关）。
struct TradingWindowGate {
    bool gate_enabled = false;
    std::vector<TradingWindow> windows;
};

/// @brief `--trading-windows` 解析结果（all-or-nothing）。
struct ParseResult {
    bool ok = false;
    TradingWindowGate gate;
    std::string error;
};

inline TradingWindowGate built_in_ctp_gate() {
    TradingWindowGate g;
    g.gate_enabled = true;
    g.windows = {
        {9 * 60, 10 * 60 + 15},
        {10 * 60 + 30, 11 * 60 + 30},
        {13 * 60 + 30, 15 * 60},
        {21 * 60, 2 * 60 + 30},
    };
    return g;
}

namespace detail {

inline std::optional<int> parse_hhmm(std::string_view tok) {
    if (tok.size() != 4) return std::nullopt;
    for (char c : tok)
        if (!std::isdigit(static_cast<unsigned char>(c))) return std::nullopt;
    const int hh = (tok[0] - '0') * 10 + (tok[1] - '0');
    const int mm = (tok[2] - '0') * 10 + (tok[3] - '0');
    if (hh > 23 || mm > 59) return std::nullopt;
    return hh * 60 + mm;
}

}

inline ParseResult parse_trading_windows(std::string_view csv) {
    ParseResult r;
    TradingWindowGate g;
    g.gate_enabled = true;
    size_t i = 0;
    const size_t n = csv.size();

    bool any = false;
    while (i <= n) {
        size_t comma = csv.find(',', i);
        if (comma == std::string_view::npos) comma = n;
        std::string_view tok = csv.substr(i, comma - i);
        i = comma + 1;

        size_t b = tok.find_first_not_of(" \t");
        size_t e = tok.find_last_not_of(" \t");
        if (b == std::string_view::npos) {

            r.ok = false;
            r.gate.gate_enabled = false;
            r.error = "empty trading-window token";
            return r;
        }
        tok = tok.substr(b, e - b + 1);
        const size_t dash = tok.find('-');
        if (dash == std::string_view::npos) {
            r.ok = false;
            r.gate.gate_enabled = false;
            r.error = std::string("trading-window token missing '-': ") + std::string(tok);
            return r;
        }
        const auto s = detail::parse_hhmm(tok.substr(0, dash));
        const auto ee = detail::parse_hhmm(tok.substr(dash + 1));
        if (!s || !ee || *s == *ee) {
            r.ok = false;
            r.gate.gate_enabled = false;
            r.error = std::string("invalid trading-window token: ") + std::string(tok);
            return r;
        }
        g.windows.push_back({*s, *ee});
        any = true;
        if (comma == n) break;
    }
    if (!any) {
        r.ok = false;
        r.gate.gate_enabled = false;
        r.error = "no trading-window token";
        return r;
    }
    r.ok = true;
    r.gate = std::move(g);
    return r;
}

namespace detail {

/// @brief CST 固定 UTC+8：epoch ms → 天数 + 一天内分钟；不依赖 host TZ。
struct CstCivil {
    int64_t day;
    int minute;
    int weekday;
};

inline CstCivil to_cst_civil(int64_t wall_epoch_ms) {
    const int64_t cst_ms = wall_epoch_ms + 8LL * 3600 * 1000;

    int64_t day = cst_ms / 86400000LL;
    int64_t rem = cst_ms % 86400000LL;
    if (rem < 0) { rem += 86400000LL; --day; }
    CstCivil c;
    c.day = day;
    c.minute = static_cast<int>(rem / 60000LL);

    int wd = static_cast<int>(((day % 7) + 4) % 7);
    if (wd < 0) wd += 7;
    c.weekday = wd;
    return c;
}

inline bool is_trading_weekday(int weekday  ) {
    return weekday >= 1 && weekday <= 5;
}

}

inline bool in_expected_tick_window(const TradingWindowGate& gate, int64_t wall_epoch_ms) {
    if (!gate.gate_enabled) return false;
    const detail::CstCivil c = detail::to_cst_civil(wall_epoch_ms);
    const int prev_wd = (c.weekday + 6) % 7;
    for (const TradingWindow& w : gate.windows) {
        if (w.start_min == w.end_min) continue;
        if (w.start_min < w.end_min) {

            if (c.minute >= w.start_min && c.minute < w.end_min &&
                detail::is_trading_weekday(c.weekday))
                return true;
        } else {

            if (c.minute >= w.start_min && detail::is_trading_weekday(c.weekday))
                return true;
            if (c.minute < w.end_min && detail::is_trading_weekday(prev_wd))
                return true;
        }
    }
    return false;
}

inline bool watchdog_active(const TradingWindowGate& gate, int64_t wall_epoch_ms) {
    return gate.gate_enabled ? in_expected_tick_window(gate, wall_epoch_ms) : true;
}

inline uint32_t wrap_safe_age(uint32_t now_ms, uint32_t anchor_ms) {
    const int32_t signed_diff = static_cast<int32_t>(now_ms - anchor_ms);
    return signed_diff < 0 ? 0u : static_cast<uint32_t>(signed_diff);
}

inline std::optional<uint32_t> min_valid_age(uint32_t now_ms,
                                             uint32_t last_tick_ms,
                                             uint32_t synth_arm_ms) {
    std::optional<uint32_t> best;
    if (last_tick_ms != 0) best = wrap_safe_age(now_ms, last_tick_ms);
    if (synth_arm_ms != 0) {
        const uint32_t age = wrap_safe_age(now_ms, synth_arm_ms);
        best = best ? std::min(*best, age) : age;
    }
    return best;
}

enum class WatchdogAction {
    Healthy,
    Rebuild,
};

/// @brief stale watchdog 纯决策输入。
struct WatchdogInputs {
    bool gate_enabled = false;
    bool in_window = true;
    uint32_t stale_reconnect_ms = 0;
    bool connected = false;
    bool subscribed = false;
    uint32_t now_ms = 0;
    uint32_t last_tick_ms = 0;
    uint32_t synth_arm_ms = 0;
};

inline WatchdogAction decide_watchdog(const WatchdogInputs& in) {
    if (in.stale_reconnect_ms == 0) return WatchdogAction::Healthy;
    const bool armed = in.gate_enabled ? in.in_window : true;
    if (!armed) return WatchdogAction::Healthy;
    const auto age = min_valid_age(in.now_ms, in.last_tick_ms, in.synth_arm_ms);
    if (!age) return WatchdogAction::Healthy;
    if (*age < in.stale_reconnect_ms) return WatchdogAction::Healthy;

    if (!in.connected || !in.subscribed) return WatchdogAction::Healthy;
    return WatchdogAction::Rebuild;
}

inline WatchdogAction decide_watchdog_relocked(WatchdogInputs fast_inputs,
                                               uint32_t fresh_now_ms,
                                               uint32_t fresh_last_tick_ms) {
    fast_inputs.now_ms = fresh_now_ms;
    fast_inputs.last_tick_ms = fresh_last_tick_ms;
    return decide_watchdog(fast_inputs);
}

inline bool should_accept_tick(bool connected, bool subscribed) {
    return connected && subscribed;
}

}
