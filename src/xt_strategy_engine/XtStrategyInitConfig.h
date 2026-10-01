#pragma once

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

/// @brief 策略初始化参数包。
///
/// yaml 策略节的参数在装配期展开成 key-value 存进 values；策略在 init()
/// 用 get_* 读取，key 缺失或解析失败时返回调用方传入的默认值。
/// 对象不做同步；跨线程共享时调用方保证只读发布或外部同步。
struct XtStrategyInitConfig {
    std::unordered_map<std::string, std::string> values;  ///< 展开后的原始 key-value 参数表。

    // ── 单值查询（scalar query） ────────────────────────────────────────

    bool has(const std::string& key) const {
        return values.find(key) != values.end();
    }

    std::string get_string(const std::string& key, const std::string& def = std::string()) const {
        const auto it = values.find(key);
        return it == values.end() ? def : it->second;
    }

    /// @note 全串严格解析（尾随非数字字符如 "10x" 返回 def），与
    ///       IStrategyContext::get_param_int 的严格性对齐。返回域差异：
    ///       本方法返回 int，超出 int32 范围的值按非法返回 def；
    ///       get_param_int 返回 int64 无此钳制——超大参数值两条路径会分叉，
    ///       参数取值应保持在 int32 域内。
    int get_int(const std::string& key, int def) const {
        const auto it = values.find(key);
        if (it == values.end()) return def;
        try {
            size_t parsed = 0;
            const long long v = std::stoll(it->second, &parsed);
            if (parsed != it->second.size()) return def;
            if (v < static_cast<long long>(std::numeric_limits<int>::min()) ||
                v > static_cast<long long>(std::numeric_limits<int>::max())) {
                return def;
            }
            return static_cast<int>(v);
        } catch (...) {
            return def;
        }
    }

    /// @note 全串严格解析，同 get_int。
    double get_double(const std::string& key, double def) const {
        const auto it = values.find(key);
        if (it == values.end()) return def;
        try {
            size_t parsed = 0;
            const double v = std::stod(it->second, &parsed);
            if (parsed != it->second.size()) return def;
            return v;
        } catch (...) {
            return def;
        }
    }

    bool get_bool(const std::string& key, bool def) const {
        const auto it = values.find(key);
        if (it == values.end()) return def;
        std::string v = it->second;
        std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        if (v == "1" || v == "true" || v == "yes" || v == "on") return true;
        if (v == "0" || v == "false" || v == "no" || v == "off") return false;
        return def;
    }

    // ── 列表/结构化查询（list & structured query） ────────────────────────

    /// @brief 按 `<prefix>.<index>` 提取有序 double 列表。
    ///
    /// index 必须是非负整数；缺失、非法或无法解析为 double 的条目会被跳过。
    /// 返回值按 index 升序排列，index 不要求连续。
    std::vector<double> get_double_list(const std::string& key_prefix) const {
        std::map<int, double> ordered;
        const std::string prefix = key_prefix + ".";
        for (const auto& kv : values) {
            if (kv.first.rfind(prefix, 0) != 0) continue;
            const std::string index_str = kv.first.substr(prefix.size());
            if (index_str.empty()) continue;
            char* end = nullptr;
            const long idx = std::strtol(index_str.c_str(), &end, 10);
            if (!end || *end != '\0' || idx < 0) continue;
            try {
                // 全串严格（同 scalar getter）：尾随垃圾按"无法解析"跳过该条目。
                size_t parsed = 0;
                const double v = std::stod(kv.second, &parsed);
                if (parsed != kv.second.size()) continue;
                ordered[static_cast<int>(idx)] = v;
            } catch (...) {
            }
        }
        std::vector<double> out;
        out.reserve(ordered.size());
        for (const auto& kv : ordered) out.push_back(kv.second);
        return out;
    }

    /// @brief 按 `<prefix>.<index>.start/end` 提取区间段列表。
    ///
    /// 仅返回同时具备正数 start/end 的 section；非法 index、未知字段和解析失败字段都会被丢弃。
    /// 返回值按 index 升序排列，调用方负责解释 start/end 的业务单位。
    std::vector<std::pair<int, int>> get_sections(const std::string& key_prefix) const {
        struct SectionDraft {
            int start = -1;
            int end = -1;
        };
        std::map<int, SectionDraft> drafts;
        const std::string prefix = key_prefix + ".";
        for (const auto& kv : values) {
            if (kv.first.rfind(prefix, 0) != 0) continue;
            const std::string suffix = kv.first.substr(prefix.size());
            const size_t dot = suffix.find('.');
            if (dot == std::string::npos) continue;
            const std::string idx_str = suffix.substr(0, dot);
            const std::string field = suffix.substr(dot + 1);
            if (field != "start" && field != "end") continue;
            char* end_ptr = nullptr;
            const long idx = std::strtol(idx_str.c_str(), &end_ptr, 10);
            if (!end_ptr || *end_ptr != '\0' || idx < 0) continue;
            int val = -1;
            try {
                size_t parsed = 0;
                val = std::stoi(kv.second, &parsed);
                if (parsed != kv.second.size()) continue;
            } catch (...) {
                continue;
            }
            SectionDraft& draft = drafts[static_cast<int>(idx)];
            if (field == "start") {
                draft.start = val;
            } else {
                draft.end = val;
            }
        }

        std::vector<std::pair<int, int>> out;
        out.reserve(drafts.size());
        for (const auto& kv : drafts) {
            if (kv.second.start > 0 && kv.second.end > 0) {
                out.emplace_back(kv.second.start, kv.second.end);
            }
        }
        return out;
    }
};
