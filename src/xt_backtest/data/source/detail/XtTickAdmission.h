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

/// @brief 逐 tick 准入判定（trading-day range + session/preopen）的单一实现。
namespace xt_bt_admission {

/// @brief session 分类结果；range 判定独立（见 in_day_range）。
enum class TickAdmit : uint8_t {
    Drop = 0,   ///< session 外且非 preopen，不进回放。
    Normal,     ///< section 内，kind 置 Normal。
    Preopen,    ///< cross-midnight section 开盘前一分钟，kind 置 Preopen。
    NoSession,  ///< session 未启用：保留且不改写 kind。
};

/// @brief 分组交易日：入口层已归一化的 trading_date，老数据缺失回退 action_date。
inline uint32_t grouping_day(const XtRawTick& t) {
    return t.trading_date != 0 ? t.trading_date : t.action_date;
}

/// @brief 交易日窗判定（inclusive）；边界为 0 表示不设界。
inline bool in_day_range(const XtRawTick& t, uint32_t start_day, uint32_t end_day) {
    if (start_day == 0 && end_day == 0) return true;
    const uint32_t day = grouping_day(t);
    if (start_day != 0 && day < start_day) return false;
    if (end_day != 0 && day > end_day) return false;
    return true;
}

/// @brief session 时段分类。
///
/// 规则：
///   - session 未启用 → NoSession
///   - 任一 section 内 → Normal
///   - 不在 section 内但落在 cross-midnight section 开始前一分钟 → Preopen
///     （模拟集合竞价阶段真实盘口）
///   - 其余（如收盘后 idle 心跳） → Drop
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

/// @brief 就地删除每个 symbol#trading_day 的最后一条 tick，保持其余相对顺序。
///
/// 对齐 WT per-contract store_ticks 行为（skip_last_tick_per_day）。memory 加载
/// 对全量 vector 调用；stream 读端按交易日缓冲后逐日调用——key 含 day，二者
/// 语义等价。
inline void drop_last_tick_per_symbol_day(std::vector<XtRawTick>& ticks) {
    if (ticks.size() <= 1) return;

    // 从尾部扫描能在 O(N) 内定位每个 symbol/day 的最后一条 tick，保持原始相对顺序。
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

}  // namespace xt_bt_admission
