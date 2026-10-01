#include "xt_backtest/reporter/XtReportCfg.h"

#include <string>

#include <yaml-cpp/yaml.h>

namespace xt_report {

XtReportCfg parse_report_config(const YAML::Node& node) {
    XtReportCfg cfg;
    cfg.report_dir = node["report_dir"] ? node["report_dir"].as<std::string>() : cfg.report_dir;
    if (node["report_contract_dir"]) {
        cfg.report_dir = node["report_contract_dir"].as<std::string>();
    }
    return cfg;
}

} // namespace xt_report
