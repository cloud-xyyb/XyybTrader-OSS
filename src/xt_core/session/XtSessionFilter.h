#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "xt_core/session/XtSessionWindow.h"
#include "xt_util/time/CtHHMM.h"
#include "xt_util/time/XtTime.h"

/// @file XtSessionFilter.h
/// @brief 交易时段（session）区间判断与收盘禁开仓/强平窗口判断。
///
/// 一个交易日分成若干可交易时段（section）——日盘、夜盘各是一段，中间还有午休。引擎要不停
/// 回答「当前时刻在不在可交易时段内」「是否已进入收盘前的禁开仓/强平窗口」，才知道能不能
/// 派 tick、能不能下开仓单。本文件把这些区间判断收成一处纯函数，半开与闭区间、跨午夜、
/// HHMM 与 HHMMSSmmm 两种编码各有对应 helper。
///
/// YAML 解析声明放在 `xt_backtest/infra/XtSessionFilterParser.h`，让 xt_core 不依赖 yaml-cpp。

namespace xt_core {

/// @brief 单个 session 区间；不内建时间编码转换。
/// @note `start/end` 可承载 HHMMSSmmm action_time 或 HHMM，调用方必须选择匹配的 helper。
struct SessionSection {
    uint32_t start = 0;
    uint32_t end = 0;
    bool cross_midnight = false;
};

/// @brief session filter 配置；enabled=true 但 sections 为空时视为未启用。
struct Config {
    bool enabled = false;
    std::vector<SessionSection> sections;
};

[[nodiscard]] inline bool in_section_halfopen(uint32_t value, const SessionSection& section) {
    if (!section.cross_midnight) {
        return value >= section.start && value < section.end;
    }
    return value >= section.start || value < section.end;
}

[[nodiscard]] inline bool in_session_halfopen(
        uint32_t value,
        const std::vector<SessionSection>& sections) {
    for (const auto& section : sections) {
        if (in_section_halfopen(value, section)) return true;
    }
    return false;
}

[[nodiscard]] inline SessionSection make_hhmm_section(int start_hhmm, int end_hhmm) {
    return SessionSection{
        static_cast<uint32_t>(start_hhmm),
        static_cast<uint32_t>(end_hhmm),
        end_hhmm < start_hhmm,
    };
}

[[nodiscard]] inline bool in_hhmm_section_halfopen(int hhmm, int start_hhmm, int end_hhmm) {
    if (!xt_hhmm::valid_hhmm(hhmm)
        || !xt_hhmm::valid_hhmm(start_hhmm)
        || !xt_hhmm::valid_hhmm(end_hhmm)) {
        return false;
    }
    return in_section_halfopen(static_cast<uint32_t>(hhmm),
                               make_hhmm_section(start_hhmm, end_hhmm));
}

[[nodiscard]] inline bool in_hhmm_sections_halfopen(
        int hhmm,
        const std::vector<std::pair<int, int>>& sections) {
    if (!xt_hhmm::valid_hhmm(hhmm)) return false;
    for (const auto& [start_hhmm, end_hhmm] : sections) {
        if (in_hhmm_section_halfopen(hhmm, start_hhmm, end_hhmm)) return true;
    }
    return false;
}

[[nodiscard]] inline bool hhmm_reaches_mark_in_sections(
        int hhmm,
        int mark_hhmm,
        const std::vector<std::pair<int, int>>& sections) {
    if (!xt_hhmm::valid_hhmm(hhmm) || !xt_hhmm::valid_hhmm(mark_hhmm)) return false;
    if (sections.empty()) return hhmm >= mark_hhmm;
    for (const auto& [start_hhmm, end_hhmm] : sections) {
        if (!in_hhmm_section_halfopen(mark_hhmm, start_hhmm, end_hhmm)) continue;
        if (!in_hhmm_section_halfopen(hhmm, start_hhmm, end_hhmm)) continue;
        if (end_hhmm >= start_hhmm) return hhmm >= mark_hhmm;
        const auto offset = [&](int t) {
            return (t >= start_hhmm) ? (t - start_hhmm) : (2400 - start_hhmm + t);
        };
        return offset(hhmm) >= offset(mark_hhmm);
    }
    return false;
}

[[nodiscard]] inline bool in_closeout_no_open_window_closed(
        int hhmm,
        const std::vector<XtSessionWindow>& windows) {
    for (const auto& w : windows) {
        if (xt_hhmm::hhmm_in_range(hhmm, w.no_open_hhmm, w.force_hhmm)) return true;
    }
    return false;
}

[[nodiscard]] inline bool in_section_closed(uint32_t action_time_ms, const SessionSection& section) {
    const uint32_t minute = xt_time::action_time_to_hhmm(action_time_ms);
    const uint32_t start_min = xt_time::action_time_to_hhmm(section.start);
    const uint32_t end_min = xt_time::action_time_to_hhmm(section.end);
    if (!section.cross_midnight) {
        return minute >= start_min && minute <= end_min;
    }
    return minute >= start_min || minute <= end_min;
}

[[nodiscard]] inline bool in_session_closed(
        uint32_t action_time_ms,
        const std::vector<SessionSection>& sections) {
    for (const auto& section : sections) {
        if (in_section_closed(action_time_ms, section)) return true;
    }
    return false;
}

[[nodiscard]] inline int section_index_closed(
        uint32_t action_time_ms,
        const std::vector<SessionSection>& sections) {
    for (int i = 0; i < static_cast<int>(sections.size()); ++i) {
        if (in_section_closed(action_time_ms, sections[static_cast<size_t>(i)])) {
            return i;
        }
    }
    return -1;
}

[[nodiscard]] inline bool is_last_of_section_closed(
        uint32_t action_time_ms,
        const std::vector<SessionSection>& sections) {
    const uint32_t minute = xt_time::action_time_to_hhmm(action_time_ms);
    for (const auto& section : sections) {
        const uint32_t end_min = xt_time::action_time_to_hhmm(section.end);
        if (minute == end_min) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] inline bool is_first_of_section_closed(
        uint32_t action_time_ms,
        const std::vector<SessionSection>& sections) {
    const uint32_t minute = xt_time::action_time_to_hhmm(action_time_ms);
    for (const auto& section : sections) {
        const uint32_t start_min = xt_time::action_time_to_hhmm(section.start);
        if (minute == start_min) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] inline bool is_enabled(const Config& cfg) {
    return cfg.enabled && !cfg.sections.empty();
}

inline void normalize(Config& cfg) {
    if (cfg.sections.empty()) cfg.enabled = false;
}

}
