#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "xt_core/market/XtRawTick.h"
#include "xt_core/market/XtTickBinCodec.h"

/// @warning [Legacy] bar -> SimTick 合成，仅服务 `XtBarModeDriver` 的 bar 数据输入路径。
///          HFT 回测优先使用 tick mode；新功能不应扩展 bar 合成语义。

// ── Bar 行解析（bar row parsing） ──────────────────────────────────────────────

/**
 * @brief 解析后的 K 线行数据（parsed bar CSV row）。
 *
 * CSV 行解析的中间产物，由 parse_bar_row_line() 填充，
 * 随后传入 generate_ticks_from_bar_row() 生成模拟 tick 序列。
 * 仅在 bar -> SimTick 生成流程内部使用。
 *
 * 注意：trading_date 存的是交易日（中国期货数据源惯例），夜盘 tick 的
 * 日历日期（action_date）与此不同，但 bar 模式不需要精确的 action_date。
 */
struct ParsedBarRow {
    uint32_t trading_date = 0; ///< 交易日（CSV date 列，非夜盘日历日期）。
    uint32_t action_time = 0;  ///< HHMMSSmmm；bar close time。
    double open = 0.0;         ///< bar open price。
    double high = 0.0;         ///< bar high price。
    double low = 0.0;          ///< bar low price。
    double close = 0.0;        ///< bar close price。
    double volume = 0.0;       ///< bar 成交量；不可解析时按 0 处理。
};

/// 从单行 CSV 文本解析 bar 数据（date,time,o,h,l,c,v 或 datetime,o,h,l,c,v）。
///
/// @return true 表示 `out` 已写入完整 OHLCV；false 表示列数、时间或价格字段不可解析。
bool parse_bar_row_line(const std::string& line, ParsedBarRow& out);

// ── SimTick 生成（simtick generation） ──────────────────────────────────────────────

/// 从单个 bar 生成一组模拟 tick。
///
/// 调用会先清空 `out_ticks`。`running_total_volume` 为 in/out 参数，跨 bar 累加 total_volume。
/// bar CSV 的 date 被视为 trading day，生成 tick 的 action_date 也沿用该值。
void generate_ticks_from_bar_row(const ParsedBarRow& row,
                                 const std::string& code,
                                 int interval_sec,
                                 double spread,
                                 double qty_base,
                                 double& running_total_volume,
                                 std::vector<XtRawTick>& out_ticks);

// ── SimTick 文件写入（simtick file write） ──────────────────────────────────────────────

/// 将 tick 列表写入 CSV 文件（逐行，8 列：date,time,code,price,bid1,ask1,bidq1,askq1）。
void write_ticks_csv(std::ofstream& out, const std::vector<XtRawTick>& ticks);

/// 将 tick 列表以 DualV2 二进制格式写入文件（via XtTickBinCodec）。
void write_ticks_bin(std::ofstream& out, XtTickBinFormat fmt, const std::vector<XtRawTick>& ticks);

// ── 辅助（helpers） ──────────────────────────────────────────────

/// 从 bar 文件路径中提取 YYMM 字符串（如 "2203"），失败返回空字符串。
std::string extract_yymm_from_bar_path(const std::string& path);
