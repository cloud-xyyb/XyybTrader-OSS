#include "ConfigSchemaValidator.h"

#include <cctype>
#include <cstddef>
#include <initializer_list>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

namespace {

using Segments = std::vector<std::string>;

// 路径模式字符串 → segment 列表（先按 '.' 切分，每段内再按 "[]" 切分）。
// "[]" 作为独立 segment 保留，供 pattern 匹配时识别 sequence 层。
// 示例："runtime.windows[].key" → ["runtime", "windows", "[]", "key"]
Segments parse_path_segments(const std::string& path) {
    Segments out;
    if (path.empty()) return out;

    std::size_t start = 0;
    while (true) {
        const std::size_t dot   = path.find('.', start);
        const std::string token = (dot == std::string::npos)
                                      ? path.substr(start)
                                      : path.substr(start, dot - start);

        // 在 dot-token 内切出所有 "[]" 标记，将其与两侧 key 片段分离
        std::size_t pos = 0;
        while (true) {
            const std::size_t br = token.find("[]", pos);
            if (br == std::string::npos) {
                if (pos < token.size()) out.push_back(token.substr(pos));
                break;
            }
            if (br > pos) out.push_back(token.substr(pos, br - pos));
            out.push_back("[]");
            pos = br + 2;
        }

        if (dot == std::string::npos) break;
        start = dot + 1;
    }
    return out;
}

// segment 级匹配：
//   "*"  → 任意 map key（不匹配 "[]"；"*" 不跨越 sequence 层）
//   "[]" → 仅匹配 "[]"（sequence 层标记）
//   其他 → 精确匹配
bool segment_match(const std::string& pattern_seg, const std::string& actual_seg) {
    if (pattern_seg == "*")  return actual_seg != "[]";
    if (pattern_seg == "[]") return actual_seg == "[]";
    return pattern_seg == actual_seg;
}

// DFS 遍历时叶子到达前只有 partial path 可用。
// 若 pattern 足够长且已走各段均与 pattern 吻合，则该 path 仍可能命中此 pattern。
bool pattern_can_reach(const Segments& pattern, const Segments& path) {
    if (pattern.size() < path.size()) return false;
    for (std::size_t i = 0; i < path.size(); ++i) {
        if (!segment_match(pattern[i], path[i])) return false;
    }
    return true;
}

// path 是否处于 prefix 子树内（prefix 是 path 的完整前缀）。
bool is_prefix_of(const Segments& prefix, const Segments& path) {
    if (prefix.size() > path.size()) return false;
    for (std::size_t i = 0; i < prefix.size(); ++i) {
        if (!segment_match(prefix[i], path[i])) return false;
    }
    return true;
}

// match_path 所指节点允许通过 schema 校验的两种路径：
//   open_prefix 命中     → 当前节点及其子树放行（用于策略自定义参数等动态键）；
//   allowed_pattern 覆盖 → 当前节点合规，子节点仍需各自命中规则或 open_prefix。
// 子树放行依赖递归时持续扩展 match_path，因此 is_prefix_of 必须按完整前缀判断。
bool is_path_allowed(const Segments&              match_path,
                      const std::vector<Segments>& allowed_patterns,
                      const std::vector<Segments>& open_prefixes) {
    for (const auto& pfx : open_prefixes) {
        if (is_prefix_of(pfx, match_path)) return true;
    }
    for (const auto& pattern : allowed_patterns) {
        if (pattern_can_reach(pattern, match_path)) return true;
    }
    return false;
}

// 为错误消息拼接来源前缀，格式 "{prefix}: {msg}"（冷路径，仅在发现 unknown key 时调用）。
std::string with_error_prefix(const std::string& msg, const char* prefix) {
    if (!prefix || prefix[0] == '\0') return msg;
    return std::string(prefix) + ": " + msg;
}

std::string to_lower_ascii(std::string s) {
    for (char& ch : s) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return s;
}

// yaml-cpp Node 赋值会共享底层 NodeData；路径游标必须从 Clone(root) 开始，
// 否则逐段下钻时可能改写调用方持有的 root。
YAML::Node find_path_node(const YAML::Node& root, const std::string& path) {
    YAML::Node cur = YAML::Clone(root);
    for (const auto& seg : parse_path_segments(path)) {
        if (seg == "[]") return YAML::Node();
        if (!cur || !cur.IsDefined() || cur.IsNull() || !cur.IsMap()) return YAML::Node();
        cur = cur[seg];
    }
    if (!cur || !cur.IsDefined() || cur.IsNull()) return YAML::Node();
    return cur;
}

std::string join_allowed_values(std::initializer_list<const char*> allowed_values) {
    std::string out;
    bool first = true;
    for (const char* value : allowed_values) {
        if (!first) out += ", ";
        out += value ? value : "";
        first = false;
    }
    return out;
}

bool is_allowed_enum_value(const std::string& value,
                           std::initializer_list<const char*> allowed_values,
                           bool case_sensitive) {
    const std::string actual = case_sensitive ? value : to_lower_ascii(value);
    for (const char* allowed : allowed_values) {
        const std::string candidate = case_sensitive
                                          ? std::string(allowed ? allowed : "")
                                          : to_lower_ascii(allowed ? allowed : "");
        if (actual == candidate) return true;
    }
    return false;
}

std::string yaml_type_name(const YAML::Node& node) {
    if (!node || !node.IsDefined()) return "undefined";
    if (node.IsNull()) return "null";
    if (node.IsScalar()) return "scalar";
    if (node.IsSequence()) return "sequence";
    if (node.IsMap()) return "map";
    return "unknown";
}

// DFS 校验 YAML 节点树中的所有 map key，unknown key 追加到 errors。
//
// match_path   递归共享的路径栈，持续反映所处节点位置，入口传空；
//              sequence 层使用 "[]"，map key 层使用实际 key。
// display_path 人类可读路径字符串，仅用于错误消息，不参与 pattern 匹配。
void validate_node(const YAML::Node&             node,
                   const std::vector<Segments>&  allowed_patterns,
                   const std::vector<Segments>&  open_prefixes,
                   Segments&                     match_path,
                   const std::string&            display_path,
                   std::vector<std::string>&     errors,
                   const char*                   error_prefix) {
    if (!node || node.IsNull()) return;

    if (node.IsSequence()) {
        // 所有 item 共享同一 "[]" segment，push/pop 置于循环外
        match_path.push_back("[]");
        for (std::size_t i = 0; i < node.size(); ++i) {
            const std::string child_display = display_path + "[" + std::to_string(i) + "]";
            validate_node(node[i], allowed_patterns, open_prefixes,
                          match_path, child_display, errors, error_prefix);
        }
        match_path.pop_back();
        return;
    }

    if (!node.IsMap()) return;

    for (auto it = node.begin(); it != node.end(); ++it) {
        std::string key;
        try {
            key = it->first.as<std::string>();
        } catch (...) {
            continue;
        }
        if (key.empty()) continue;

        const std::string child_display = display_path.empty() ? key : (display_path + "." + key);
        match_path.push_back(key);

        if (!is_path_allowed(match_path, allowed_patterns, open_prefixes)) {
            errors.push_back(with_error_prefix("unknown config key: " + child_display, error_prefix));
            match_path.pop_back();
            continue;
        }

        validate_node(it->second, allowed_patterns, open_prefixes,
                      match_path, child_display, errors, error_prefix);
        match_path.pop_back();
    }
}

}  // namespace

void validate_unknown_config_keys(const YAML::Node&          root,
                                  const XtConfigSchemaRules& rules,
                                  std::vector<std::string>&  errors,
                                  const char*                error_prefix) {
    if (!root || root.IsNull() || !root.IsMap()) return;

    std::vector<Segments> allowed_patterns;
    allowed_patterns.reserve(rules.allowed_paths.size());
    for (const auto& p : rules.allowed_paths) {
        allowed_patterns.push_back(parse_path_segments(p));
    }

    std::vector<Segments> open_prefixes;
    open_prefixes.reserve(rules.open_prefixes.size());
    for (const auto& p : rules.open_prefixes) {
        open_prefixes.push_back(parse_path_segments(p));
    }

    Segments match_path;
    validate_node(root, allowed_patterns, open_prefixes, match_path, "", errors, error_prefix);
}

void validate_enum_field(const YAML::Node& root,
                         const std::string& path,
                         std::initializer_list<const char*> allowed_values,
                         std::vector<std::string>& errors,
                         const char* error_prefix,
                         bool case_sensitive) {
    const YAML::Node node = find_path_node(root, path);
    if (!node || !node.IsDefined() || node.IsNull()) return;

    std::string actual;
    if (node.IsScalar()) {
        try {
            actual = node.as<std::string>();
        } catch (...) {
            actual = "<unparseable scalar>";
        }
    } else {
        actual = "<" + yaml_type_name(node) + ">";
    }

    if (node.IsScalar() && is_allowed_enum_value(actual, allowed_values, case_sensitive)) return;

    errors.push_back(with_error_prefix(
        path + " 必须为 [" + join_allowed_values(allowed_values) + "] 之一,实测 '" + actual + "'",
        error_prefix));
}
