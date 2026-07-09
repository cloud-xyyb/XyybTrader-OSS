#include "XtSessionFilterParser.h"

#include "xt_backtest/util/XtParseUtil.h"

#include <yaml-cpp/yaml.h>

#include <string>

namespace xt_core {

Config parse_config(const YAML::Node& node) {
    Config cfg;
    if (!node || node.IsNull()) return cfg;
    cfg.enabled = node["enabled"] ? node["enabled"].as<bool>() : true;
    if (!cfg.enabled) return cfg;

    const YAML::Node sections = node["sections"];
    if (!sections || !sections.IsSequence()) return cfg;
    for (const auto& section_node : sections) {
        if (!section_node.IsScalar()) continue;
        const std::string expr = section_node.as<std::string>();
        const auto sep = expr.find('-');
        if (sep == std::string::npos) continue;

        const std::string start_expr = expr.substr(0, sep);
        const std::string end_expr = expr.substr(sep + 1);
        uint32_t start = 0;
        uint32_t end = 0;
        if (!XtParseUtil::parse_time_with_ms(start_expr, start)) continue;
        if (!XtParseUtil::parse_time_with_ms(end_expr, end)) continue;

        SessionSection section{};
        section.start = start;
        section.end = end;
        section.cross_midnight = (end < start);
        cfg.sections.push_back(section);
    }
    normalize(cfg);
    return cfg;
}

}

