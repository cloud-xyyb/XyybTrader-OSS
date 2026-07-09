#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "xt_core/market/XtRawTick.h"
#include "xt_core/market/XtTickBinCodec.h"

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
    uint32_t trading_date = 0;
    uint32_t action_time = 0;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    double volume = 0.0;
};

bool parse_bar_row_line(const std::string& line, ParsedBarRow& out);

void generate_ticks_from_bar_row(const ParsedBarRow& row,
                                 const std::string& code,
                                 int interval_sec,
                                 double spread,
                                 double qty_base,
                                 double& running_total_volume,
                                 std::vector<XtRawTick>& out_ticks);

void write_ticks_csv(std::ofstream& out, const std::vector<XtRawTick>& ticks);

void write_ticks_bin(std::ofstream& out, XtTickBinFormat fmt, const std::vector<XtRawTick>& ticks);

std::string extract_yymm_from_bar_path(const std::string& path);
