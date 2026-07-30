#pragma once

/// @file
/// @brief CTP 合约代码 → 交易所归属推导（无 SDK 依赖的纯映射）。
///
/// CTP `OnRtnDepthMarketData` 的 `ExchangeID` 在部分前置（如 SimNow）为空 →
/// recorder tick.fbs 分片键的 exchange gate 落空、fbs 流断档。中国期货的交易所
/// 由品种字母前缀唯一确定（大小写敏感：CZCE/CFFEX 品种大写、SHFE/DCE/INE/GFEX
/// 小写），本 helper 据此兜底填 `t.exchg`。fail-open：未知前缀返空串，由调用方
/// 保持原样（tick.fbs 仍 gate，行为不回归）。
///
/// 输出交易所字符串与 recorder `venue_kind_from_exchange` 识别集一致：
/// SHFE / DCE / CZCE / CFFEX / GFEX / INE。

#include <cctype>
#include <string>
#include <unordered_map>
#include <vector>

namespace ctp_util {

inline size_t ctp_product_prefix(const char* instrument, char* out, size_t cap) noexcept {
    if (!instrument || !out || cap == 0) {
        if (out && cap > 0) out[0] = '\0';
        return 0;
    }
    size_t n = 0;
    for (const char* p = instrument; *p != '\0' && n + 1 < cap; ++p) {
        const unsigned char c = static_cast<unsigned char>(*p);
        if (std::isalpha(c) == 0) break;
        out[n++] = *p;
    }
    out[n] = '\0';
    return n;
}

inline const char* ctp_exchange_for_prefix(const char* prefix) noexcept {
    if (!prefix || prefix[0] == '\0') return "";

    static const std::unordered_map<std::string, const char*> kMap = {

        {"IF", "CFFEX"}, {"IC", "CFFEX"}, {"IH", "CFFEX"}, {"IM", "CFFEX"},
        {"TF", "CFFEX"}, {"T", "CFFEX"}, {"TS", "CFFEX"}, {"TL", "CFFEX"},

        {"cu", "SHFE"}, {"al", "SHFE"}, {"zn", "SHFE"}, {"pb", "SHFE"},
        {"ni", "SHFE"}, {"sn", "SHFE"}, {"au", "SHFE"}, {"ag", "SHFE"},
        {"rb", "SHFE"}, {"wr", "SHFE"}, {"hc", "SHFE"}, {"ss", "SHFE"},
        {"bu", "SHFE"}, {"ru", "SHFE"}, {"sp", "SHFE"}, {"fu", "SHFE"},
        {"br", "SHFE"}, {"ao", "SHFE"}, {"ad", "SHFE"}, {"op", "SHFE"},

        {"sc", "INE"}, {"lu", "INE"}, {"nr", "INE"}, {"bc", "INE"}, {"ec", "INE"},

        {"a", "DCE"}, {"b", "DCE"}, {"c", "DCE"}, {"cs", "DCE"}, {"m", "DCE"},
        {"y", "DCE"}, {"p", "DCE"}, {"jd", "DCE"}, {"l", "DCE"}, {"v", "DCE"},
        {"pp", "DCE"}, {"j", "DCE"}, {"jm", "DCE"}, {"i", "DCE"}, {"eg", "DCE"},
        {"eb", "DCE"}, {"pg", "DCE"}, {"rr", "DCE"}, {"lh", "DCE"}, {"fb", "DCE"},
        {"bb", "DCE"}, {"lg", "DCE"}, {"bz", "DCE"},

        {"SR", "CZCE"}, {"CF", "CZCE"}, {"CY", "CZCE"}, {"TA", "CZCE"},
        {"MA", "CZCE"}, {"FG", "CZCE"}, {"RM", "CZCE"}, {"OI", "CZCE"},
        {"ZC", "CZCE"}, {"SF", "CZCE"}, {"SM", "CZCE"}, {"WH", "CZCE"},
        {"PM", "CZCE"}, {"RI", "CZCE"}, {"LR", "CZCE"}, {"JR", "CZCE"},
        {"RS", "CZCE"}, {"SA", "CZCE"}, {"PF", "CZCE"}, {"PK", "CZCE"},
        {"UR", "CZCE"}, {"AP", "CZCE"}, {"CJ", "CZCE"}, {"PX", "CZCE"},
        {"SH", "CZCE"}, {"PR", "CZCE"},

        {"si", "GFEX"}, {"lc", "GFEX"}, {"ps", "GFEX"},
    };
    const auto it = kMap.find(prefix);
    return it == kMap.end() ? "" : it->second;
}

inline const char* infer_ctp_exchange(const char* instrument) noexcept {
    char prefix[16];
    if (ctp_product_prefix(instrument, prefix, sizeof(prefix)) == 0) return "";
    return ctp_exchange_for_prefix(prefix);
}

/// @brief 未收录品种前缀的 warn-once 限频器：按前缀（而非合约全码）去重，每个不同
///        前缀只 warn 一次；超出 kMaxTrackedPrefixes 后退化为全局 once（防止无界
///        内存增长），仍多打一条覆盖第一个溢出前缀，此后彻底静默。
///
/// 单纯 per-process once（旧实现）会让第一个未收录前缀把后续所有不同未收录前缀
/// 的告警永久吞掉；本 gate 保证一个 session 内至多 kMaxTrackedPrefixes+1 个不同
/// 前缀各获得一次可见 warn。调用线程需与 SDK MD 回调线程一致（无 atomic/锁）。
class CtpExchangeWarnGate {
public:
    static constexpr size_t kMaxTrackedPrefixes = 8;

    bool should_warn(const char* prefix) {
        const std::string key = prefix ? prefix : "";
        for (const auto& seen : seen_prefixes_) {
            if (seen == key) return false;
        }
        if (seen_prefixes_.size() < kMaxTrackedPrefixes) {
            seen_prefixes_.push_back(key);
            return true;
        }
        if (global_fallback_warned_) return false;
        global_fallback_warned_ = true;
        return true;
    }

private:
    std::vector<std::string> seen_prefixes_;
    bool global_fallback_warned_ = false;
};

}
