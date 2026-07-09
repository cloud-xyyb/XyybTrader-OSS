#pragma once

#include <string>
#include <vector>

#include "xt_util/path/PathUtil.h"

namespace YAML { class Node; }

namespace xt_report {
/// @brief 报告输出配置。
///
/// 本结构只保存回测报告输出目录。运行模式由 `XtContractDims::mode` 表示；
/// live 刷盘周期由 `XtLiveReporter::Config` 管理。
struct XtReportCfg {

    std::string report_dir   = "reports";

    void validate(std::vector<std::string>& errors) const {
        if (report_dir.empty()) {
            errors.push_back("report_dir 不能为空");
        }
    }
};

XtReportCfg parse_report_config(const YAML::Node& root_node);

inline std::string make_unique_report_dir(const std::string& report_dir) {
    return PathUtil::make_epoch_ms_suffixed_path(report_dir, "reports");
}

}
