#pragma once

/// @file
/// @brief tick CSV 表头识别与逐行解析 helper。
///
/// 本文件把 CSV 字段解析成 `XtRawTick` 与统一 sort key

#include <string>
#include <unordered_set>

#include "xt_core/market/XtRawTick.h"
#include "xt_core/market/XtTickSortKey.h"

/// @brief tick CSV 列索引规格。
///
/// `idx_* == -1` 表示对应列缺失。调用方为每个 CSV 文件构建一份 spec，
/// 并复用于该文件的逐行解析。CSV 盘口最多识别 L1-L20；`XtRawTick` 更深档位
/// 由解析函数按本文件约定补齐。
struct XtTickCsvSpec {
    static constexpr int MAX_DEPTH = 20;

    XtTickCsvSpec() {
        for (int i = 0; i < MAX_DEPTH; ++i) {
            idx_bid_price_n[i] = -1;
            idx_bid_qty_n[i]   = -1;
            idx_ask_price_n[i] = -1;
            idx_ask_qty_n[i]   = -1;
        }
    }

    bool inited      = false;
    bool has_header  = false;
    bool simple      = false;

    int idx_instrument  = -1;
    int idx_trading_day = -1;
    int idx_update_time = -1;
    int idx_update_ms   = -1;
    int idx_last_price  = -1;
    int idx_volume      = -1;
    int idx_bid_price1  = -1;
    int idx_bid_qty1    = -1;
    int idx_ask_price1  = -1;
    int idx_ask_qty1    = -1;
    int idx_turnover    = -1;
    int idx_open_interest = -1;
    int idx_upper_limit   = -1;
    int idx_lower_limit   = -1;

    int idx_bid_price_n[MAX_DEPTH];
    int idx_bid_qty_n[MAX_DEPTH];
    int idx_ask_price_n[MAX_DEPTH];
    int idx_ask_qty_n[MAX_DEPTH];

    int idx_update_time_ms = -1;
    int idx_epoch_us       = -1;
    int idx_high           = -1;
    int idx_low            = -1;
    int detected_depth     = 1;
};

enum class XtTickVolumeMode {
    Cumulative,
    Incremental
};

enum class TickParseDrop {
    None = 0,
    BadRow,
    SymbolMiss
};

XtTickVolumeMode parse_tick_volume_mode(const std::string& mode);

bool build_tick_spec_from_header(const std::string& line, XtTickCsvSpec& spec);

bool parse_tick_csv_line(const std::string& line,
                         const XtTickCsvSpec& spec,
                         XtRawTick& out,
                         uint64_t& key,
                         const std::unordered_set<std::string>& symbol_filter,
                         double qty_base,
                         XtTickVolumeMode vol_mode,
                         TickParseDrop* out_drop = nullptr);
