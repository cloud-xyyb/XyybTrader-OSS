#pragma once

/// @file
/// @brief tick replay 准入判定的共享实现。
///
/// memory 加载和 stream 逐条读取共用本文件的 trading-day range、session/preopen
/// 判定与 skip-last-tick 逻辑，保证同一 datasource config 在两种 data mode 下输出
/// 同一 tick 序列。

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "xt_core/market/XtRawTick.h"
#include "xt_core/session/XtSessionFilter.h"
#include "xt_util/time/CtHHMM.h"

namespace xt_bt_admission {

enum class TickAdmit : uint8_t {
    Drop = 0,
    Normal,
    Preopen,
    NoSession,
};

inline uint32_t grouping_day(const XtRawTick& t) {
    return t.trading_date != 0 ? t.trading_date : t.action_date;
}

inline bool in_day_range(const XtRawTick& t, uint32_t start_day, uint32_t end_day) {
    if (start_day == 0 && end_day == 0) return true;
    const uint32_t day = grouping_day(t);
    if (start_day != 0 && day < start_day) return false;
    if (end_day != 0 && day > end_day) return false;
    return true;
}

inline TickAdmit classify_session(const XtRawTick& t, const xt_core::Config& session) {
    if (!xt_core::is_enabled(session)) return TickAdmit::NoSession;
    const uint32_t action_time = t.action_time;
    if (xt_core::in_session_closed(action_time, session.sections)) return TickAdmit::Normal;
    const uint32_t tick_min = action_time / 100000;
    for (const auto& sec : session.sections) {
        if (!sec.cross_midnight) continue;
        if (xt_hhmm::is_next_minute(tick_min, sec.start / 100000)) return TickAdmit::Preopen;
    }
    return TickAdmit::Drop;
}

inline std::string make_symbol_day_key(const XtRawTick& tick) {
    std::string key(tick.code);
    key.push_back('#');
    key += std::to_string(grouping_day(tick));
    return key;
}

inline void drop_last_tick_per_symbol_day(std::vector<XtRawTick>& ticks) {
    if (ticks.size() <= 1) return;

    std::unordered_set<std::string> last_keys;
    last_keys.reserve(ticks.size() / 8 + 1);
    std::vector<size_t> last_indices;
    last_indices.reserve(ticks.size() / 8 + 1);

    for (size_t i = ticks.size(); i-- > 0;) {
        const std::string key = make_symbol_day_key(ticks[i]);
        if (last_keys.insert(key).second) {
            last_indices.push_back(i);
        }
    }

    if (last_indices.empty()) return;

    std::vector<unsigned char> drop_mask(ticks.size(), 0);
    for (size_t idx : last_indices) drop_mask[idx] = 1;

    size_t dst = 0;
    for (size_t i = 0; i < ticks.size(); ++i) {
        if (!drop_mask[i]) {
            if (dst != i) ticks[dst] = ticks[i];
            ++dst;
        }
    }
    ticks.resize(dst);
}

}
