#pragma once

/// @file
/// @brief 报表 Row 到 CSV 文本的转换 helper。

#include <string>
#include "xt_report/core/XtReportTypes.h"

namespace xt_report {

// ── 字段转义与维度前缀 ─────────────────────────────────────────
// 纯字符串转换，不持有状态；调用方负责把 prefix 与具体 row 拼成完整行。

std::string csv_escape(const std::string& in);
std::string format_dims_prefix(const XtContractDims& dims);
std::string format_dims_prefix(const XtContractDims& dims,
                               const std::string& strategy_id_override,
                               const std::string& symbol_override);

// ── Row → CSV line ─────────────────────────────────────────────
// `dims_prefix` 必须与 `kDimsHeader` 列顺序一致；返回值包含行尾换行。
// 每个 overload 的列顺序由对应 `csv_header_*()` 固定。

std::string to_csv_line(const std::string& dims_prefix, const XtKpi1sRow& row);
std::string to_csv_line(const std::string& dims_prefix, const XtFillRow& row);
std::string to_csv_line(const std::string& dims_prefix, const XtPositionRow& row);
std::string to_csv_line(const std::string& dims_prefix, const XtAccountTruthRow& row);

// ── 公共维度列 ─────────────────────────────────────────────────
// 所有 report CSV 的前置维度段；重排会影响 sink、exporter 与 analyzer。

inline constexpr const char* kDimsHeader = "mode,run_id,strategy_id,symbol,config_hash,code_version,session_date";

// ── CSV header 工厂 ────────────────────────────────────────────
// 返回完整 header（含公共维度列与行尾换行）；必须与 `to_csv_line()` 保持列数一致。

std::string csv_header_kpi_1s();
std::string csv_header_fill();
std::string csv_header_position_snapshot();
std::string csv_header_account_truth();

} // namespace xt_report
