#pragma once

/// @file
/// @brief 报表 Row 到 CSV 文本的转换 helper。

#include <string>
#include "xt_report/core/XtReportTypes.h"

namespace xt_report {

std::string csv_escape(const std::string& in);
std::string format_dims_prefix(const XtContractDims& dims);
std::string format_dims_prefix(const XtContractDims& dims,
                               const std::string& strategy_id_override,
                               const std::string& symbol_override);

std::string to_csv_line(const std::string& dims_prefix, const XtKpi1sRow& row);
std::string to_csv_line(const std::string& dims_prefix, const XtFillRow& row);
std::string to_csv_line(const std::string& dims_prefix, const XtPositionRow& row);
std::string to_csv_line(const std::string& dims_prefix, const XtAccountTruthRow& row);

inline constexpr const char* kDimsHeader = "mode,run_id,strategy_id,symbol,config_hash,code_version,session_date";

std::string csv_header_kpi_1s();
std::string csv_header_fill();
std::string csv_header_position_snapshot();
std::string csv_header_account_truth();

}
