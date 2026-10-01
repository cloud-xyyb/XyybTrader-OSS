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
    int start_min = 0;  ///< 起始分钟（含）
    int end_min = 0;    ///< 结束分钟（不含）
};

/// @brief 交易窗 gate。gate_enabled=false 时 watchdog 恒活（等价现行行为，回滚开关）。
struct TradingWindowGate {
    bool gate_enabled = false;      ///< true=按 windows 门控 watchdog；false=不门控（现行行为）
    std::vector<TradingWindow> windows;  ///< gate_enabled 时生效的 expected-tick 窗集合
};

/// @brief `--trading-windows` 解析结果（all-or-nothing）。
struct ParseResult {
    bool ok = false;               ///< true=全部 token 合法；false=任一非法 → gate 已回落 disabled
    TradingWindowGate gate;        ///< ok 时 gate_enabled=true+windows；否则 gate_enabled=false 空窗
    std::string error;             ///< ok=false 时给出首个非法原因（供 loud warn）
};

/// @brief 内置 CTP 默认窗：`0900-1015,1030-1130,1330-1500,2100-0230`（半开，收盘分钟不在窗内）。
/// 覆盖日盘早/午/晚三段；午休 11:30-13:30、小节休 10:15-10:30、收盘 buffer 全在窗外。
inline TradingWindowGate built_in_ctp_gate() {
    TradingWindowGate g;
    g.gate_enabled = true;
    g.windows = {
        {9 * 60, 10 * 60 + 15},        // 0900-1015 → [540, 615)
        {10 * 60 + 30, 11 * 60 + 30},  // 1030-1130 → [630, 690)
        {13 * 60 + 30, 15 * 60},       // 1330-1500 → [810, 900)
        {21 * 60, 2 * 60 + 30},        // 2100-0230 → [1260, 150)（跨午夜）
    };
    return g;
}

namespace detail {

/// @brief "HHMM" → 分钟偏移；非法返回 nullopt。恰 4 位数字，HH∈[0,23]，MM∈[0,59]。
inline std::optional<int> parse_hhmm(std::string_view tok) {
    if (tok.size() != 4) return std::nullopt;
    for (char c : tok)
        if (!std::isdigit(static_cast<unsigned char>(c))) return std::nullopt;
    const int hh = (tok[0] - '0') * 10 + (tok[1] - '0');
    const int mm = (tok[2] - '0') * 10 + (tok[3] - '0');
    if (hh > 23 || mm > 59) return std::nullopt;
    return hh * 60 + mm;
}

}  // namespace detail

/// @brief 解析逗号分隔的 `HHMM-HHMM` 列表为 gate。any bad token → all-or-nothing
/// 回落 gate_enabled=false（绝不部分生效，绝不喂空 windows 给 in_session）。
/// 空 token / 非法 HHMM / start==end 均视为非法。
inline ParseResult parse_trading_windows(std::string_view csv) {
    ParseResult r;
    TradingWindowGate g;
    g.gate_enabled = true;
    size_t i = 0;
    const size_t n = csv.size();
    // 至少一个 token；空字符串本身视为非法（触发 fail-open disabled）。
    bool any = false;
    while (i <= n) {
        size_t comma = csv.find(',', i);
        if (comma == std::string_view::npos) comma = n;
        std::string_view tok = csv.substr(i, comma - i);
        i = comma + 1;
        // 去首尾空白
        size_t b = tok.find_first_not_of(" \t");
        size_t e = tok.find_last_not_of(" \t");
        if (b == std::string_view::npos) {
            // 空 token（含全空白）非法
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
    int64_t day;    ///< 自 1970-01-01（CST）以来的天数
    int minute;     ///< 一天内分钟偏移 [0,1439]
    int weekday;    ///< 0=Sun..6=Sat
};

inline CstCivil to_cst_civil(int64_t wall_epoch_ms) {
    const int64_t cst_ms = wall_epoch_ms + 8LL * 3600 * 1000;  // UTC+8 固定
    // floor division，负 epoch（1970 前）也正确。
    int64_t day = cst_ms / 86400000LL;
    int64_t rem = cst_ms % 86400000LL;
    if (rem < 0) { rem += 86400000LL; --day; }
    CstCivil c;
    c.day = day;
    c.minute = static_cast<int>(rem / 60000LL);
    // 1970-01-01 = Thursday；0=Sun → Thu=4。day 可能为负，取正模。
    int wd = static_cast<int>(((day % 7) + 4) % 7);
    if (wd < 0) wd += 7;
    c.weekday = wd;
    return c;
}

inline bool is_trading_weekday(int weekday /*0=Sun..6=Sat*/) {
    return weekday >= 1 && weekday <= 5;  // Mon..Fri
}

}  // namespace detail

/// @brief 当前 wall 时刻（CST）是否落在任一 expected-tick 窗内。
/// gate_enabled=false → 恒 false（调用方按「不门控」处理）。
/// weekend 按窗口起始日归属：跨午夜窗早段（min < end）用 CST 前一自然日 weekday 判定，
/// 故周五夜→周六 02:30 有效，周六/周日夜、周日凌晨无效。
inline bool in_expected_tick_window(const TradingWindowGate& gate, int64_t wall_epoch_ms) {
    if (!gate.gate_enabled) return false;
    const detail::CstCivil c = detail::to_cst_civil(wall_epoch_ms);
    const int prev_wd = (c.weekday + 6) % 7;
    for (const TradingWindow& w : gate.windows) {
        if (w.start_min == w.end_min) continue;  // 防御：零宽窗跳过
        if (w.start_min < w.end_min) {
            // 普通日内窗，起始日 = 当日
            if (c.minute >= w.start_min && c.minute < w.end_min &&
                detail::is_trading_weekday(c.weekday))
                return true;
        } else {
            // 跨午夜窗：evening 段起始日=当日；early 段起始日=前一日
            if (c.minute >= w.start_min && detail::is_trading_weekday(c.weekday))
                return true;
            if (c.minute < w.end_min && detail::is_trading_weekday(prev_wd))
                return true;
        }
    }
    return false;
}

/// @brief stale watchdog 是否应在此刻处于 armed（供 connector gauge
/// `xt_md_connector_stale_watchdog_active`）。gate_enabled=false → 恒 armed（现行语义、
/// 24/7 provider 亦然）；gate_enabled → 仅窗内 armed。
inline bool watchdog_active(const TradingWindowGate& gate, int64_t wall_epoch_ms) {
    return gate.gate_enabled ? in_expected_tick_window(gate, wall_epoch_ms) : true;
}

// ── stale watchdog 双锚决策（wrap-safe）─────────────────────────────

/// @brief 单锚 wrap-safe age：`now - anchor`，但对「anchor 落在 now 之后」的短暂未来锚
/// （steady 单调下 tick callback 在 caller 采样 now 之后落锚）按 age=0 处理，而非 raw
/// `uint32(now-anchor)` 得到的 ~49.7 天天文数（J1 防御 clamp）。判别 = signed 差
/// (int32_t)：`now-anchor < 0`（幅度界 2^31）→ 未来锚 → 0；否则真实 age。
/// 真 wrap（now 已 wrap、anchor 在 wrap 前、真实 age∈[0,2^31)）走 signed 正值分支仍算对。
/// 条件性边界（R4-2）：真实 age∈[2^31,2^32)（~24.8-49.7 天）会被误判未来锚 clamp 为 0——
/// connected&&subscribed 且 watchdog armed 的稳态下 1Hz 检查+rebuild re-arm 使其不可达；
/// 断线/未订阅长挂 >24.8 天后恢复订阅且一直无首 tick 的极端场景理论可达（届时压制到
/// uint32 wrap 为止），接受为已披露边界。
inline uint32_t wrap_safe_age(uint32_t now_ms, uint32_t anchor_ms) {
    const int32_t signed_diff = static_cast<int32_t>(now_ms - anchor_ms);
    return signed_diff < 0 ? 0u : static_cast<uint32_t>(signed_diff);
}

/// @brief 无符号 wrap-safe 年龄：对每个**有效**锚各算 `wrap_safe_age`，取最小 age。
/// anchor==0 视为无效（未 stamp）而排除；两锚皆无效返回 nullopt。
/// 绝不对 raw uint32 timestamp 取 max（跨 wrap 会选错锚）。
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
    Healthy,   ///< 不重连
    Rebuild,   ///< stale 命中 → self-reconnect
};

/// @brief stale watchdog 纯决策输入。
struct WatchdogInputs {
    bool gate_enabled = false;
    bool in_window = true;            ///< gate_enabled 时预算的窗内状态；gate_enabled=false 时忽略
    uint32_t stale_reconnect_ms = 0;  ///< 0=禁用（回滚）
    bool connected = false;
    bool subscribed = false;
    uint32_t now_ms = 0;
    uint32_t last_tick_ms = 0;        ///< tick-callback-only 锚（0=无效）
    uint32_t synth_arm_ms = 0;        ///< control-thread-only 合成锚（0=无效）
};

/// @brief stale watchdog 决策。Rebuild 仅当：启用 + （gate_enabled?窗内:恒）+ 已连+已订阅
/// + 存在有效锚 + min age >= threshold。其余一律 Healthy。
inline WatchdogAction decide_watchdog(const WatchdogInputs& in) {
    if (in.stale_reconnect_ms == 0) return WatchdogAction::Healthy;
    const bool armed = in.gate_enabled ? in.in_window : true;
    if (!armed) return WatchdogAction::Healthy;
    const auto age = min_valid_age(in.now_ms, in.last_tick_ms, in.synth_arm_ms);
    if (!age) return WatchdogAction::Healthy;             // 无有效锚 → 不重连
    if (*age < in.stale_reconnect_ms) return WatchdogAction::Healthy;
    // 断线恢复始终靠 CTP SDK 自动重连；本 gate 只消 connected+subscribed 但无 tick 的 churn。
    if (!in.connected || !in.subscribed) return WatchdogAction::Healthy;
    return WatchdogAction::Rebuild;
}

/// @brief slow-path 锁内重读决策：fast-path 无锁快照 `fast_inputs` 取锁后，必须用
/// 锁内**重采的 fresh_now** 与**重读的 fresh tick 锚**覆盖 `now_ms`/`last_tick_ms` 再
/// 决策，杜绝两类竞态（ParserCTP::check_health F1/J1）：
///   ① tick 恰落在 fast-path 探测与取锁之间 → 快照 tick 锚陈旧 → 误 rebuild（重读 fresh 锚）；
///   ② caller 的 now 采样早于取锁，可能落在 callback 未来锚之前 → `uint32(now-未来锚)` 天文数
///      → 把 fresh 锚误判 ~49.7 天 stale（重采 fresh_now：steady 单调 + 同锁互斥 ⇒ 凡已发布的
///      锚 ≤ fresh_now，无未来锚）。
/// `synthetic_arm_ms` 是 control-thread-only 锚（check_health 单线程写），fast-path
/// 探测与取锁之间不可能被并发移动，沿用 `fast_inputs` 值即可，无需重读。
inline WatchdogAction decide_watchdog_relocked(WatchdogInputs fast_inputs,
                                               uint32_t fresh_now_ms,
                                               uint32_t fresh_last_tick_ms) {
    fast_inputs.now_ms = fresh_now_ms;
    fast_inputs.last_tick_ms = fresh_last_tick_ms;
    return decide_watchdog(fast_inputs);
}

// ── tick publish/见证 gate 谓词 ─────────────────────────────────────────────

/// @brief tick 发布 + 见证锚 gate：仅当 front 已连 **且** 本 session 已完成订阅应答
/// （subscribed）时才接受 tick（ParserCTP::OnRtnDepthMarketData 入口，J2）。
/// 收紧自单纯 `connected` 判据：mu_ 无 FIFO——rebuild 解锁后新 session OnFrontConnected
/// 可先获锁置 `connected_=true`，此时若 gate 仅判 connected，被挡的旧 session tick callback
/// 醒来会过 gate、发布死 session tick 并用死 tick 武装见证锚（压制新 session 合法 rebuild）。
/// 加 subscribed 把窗口压到「新 session 整链 connect+resubscribe 完成之后」（rebuild 路径
/// 复位 subscribed_=false、resubscribe ack 才置 true）。残留（SDK 线程停摆跨完整重连+重订阅
/// 才可达）后果有界、自愈，见调用点注释。
inline bool should_accept_tick(bool connected, bool subscribed) {
    return connected && subscribed;
}

}  // namespace ctp_md_session
