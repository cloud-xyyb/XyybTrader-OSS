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
    /// 输出目录；为空时 validate() 报错。
    std::string report_dir   = "reports";

    void validate(std::vector<std::string>& errors) const {
        if (report_dir.empty()) {
            errors.push_back("report_dir 不能为空");
        }
    }
};

/// @brief 从 YAML 根节点解析报告配置。
XtReportCfg parse_report_config(const YAML::Node& root_node);

/// @brief 生成 `<report_dir>_<epoch_ms>` 路径；只计算路径，不创建目录。
inline std::string make_unique_report_dir(const std::string& report_dir) {
    return PathUtil::make_epoch_ms_suffixed_path(report_dir, "reports");
}

} // namespace xt_report
