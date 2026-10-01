#pragma once

/// @file
/// @brief tick CSV 表头识别与逐行解析 helper。
///
/// 本文件把 CSV 字段解析成 `XtRawTick` 与统一 sort key

#include <string>
#include <unordered_set>

#include "xt_core/market/XtRawTick.h"
#include "xt_core/market/XtTickSortKey.h"  // make_tick_sort_key 提供夜盘旋转排序键。

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

    bool inited      = false;  ///< 调用方状态标记；本模块不写
    bool has_header  = false;  ///< true 表示首行被识别为表头
    bool simple      = false;  ///< true 表示 code+price 无时间字段格式

    int idx_instrument  = -1;  ///< InstrumentID/code
    int idx_trading_day = -1;  ///< TradingDay，YYYYMMDD 或 YYYY-MM-DD
    int idx_update_time = -1;  ///< UpdateTime/UpdateTimeMs
    int idx_update_ms   = -1;  ///< UpdateMillisec，叠加到 UpdateTime
    int idx_last_price  = -1;  ///< LastPrice/price；标准格式必需
    int idx_volume      = -1;  ///< Volume；解释方式由 XtTickVolumeMode 决定
    int idx_bid_price1  = -1;  ///< BidPrice1；同步写入 idx_bid_price_n[0]
    int idx_bid_qty1    = -1;  ///< BidVolume1；同步写入 idx_bid_qty_n[0]
    int idx_ask_price1  = -1;  ///< AskPrice1；同步写入 idx_ask_price_n[0]
    int idx_ask_qty1    = -1;  ///< AskVolume1；同步写入 idx_ask_qty_n[0]
    int idx_turnover    = -1;  ///< Turnover
    int idx_open_interest = -1;  ///< OpenInterest
    int idx_upper_limit   = -1;  ///< UpperLimit/UpperLimitPrice（涨停价）
    int idx_lower_limit   = -1;  ///< LowerLimit/LowerLimitPrice（跌停价）

    int idx_bid_price_n[MAX_DEPTH];  ///< BidPrice1..N；构造时置 -1
    int idx_bid_qty_n[MAX_DEPTH];    ///< BidVolume1..N
    int idx_ask_price_n[MAX_DEPTH];  ///< AskPrice1..N
    int idx_ask_qty_n[MAX_DEPTH];    ///< AskVolume1..N

    int idx_update_time_ms = -1;  ///< 合并时间列，HHMMSS*1000+ms
    int idx_epoch_us       = -1;  ///< epoch microseconds 列索引
    int idx_high           = -1;  ///< HighPrice
    int idx_low            = -1;  ///< LowPrice
    int detected_depth     = 1;   ///< 表头检测到的盘口档位数，范围 1-MAX_DEPTH
};

/// @brief tick 成交量字段解释模式。
enum class XtTickVolumeMode {
    Cumulative,   ///< 当日累计成交量，写入 XtRawTick::total_volume
    Incremental   ///< 单 tick 增量成交量，写入 XtRawTick::volume
};

/// @brief 单行 tick 解析被丢弃的原因（consumed manifest 分类用）。
///
/// `parse_tick_csv_line` 返回 false 时按此分类：合约白名单不命中 = SymbolMiss，
/// 空行/必需字段缺失/数值解析失败 = BadRow。返回 true 时保持 None。
enum class TickParseDrop {
    None = 0,   ///< 解析成功（未丢弃）。
    BadRow,     ///< 坏行：空字段、必需字段缺失或数值解析失败。
    SymbolMiss  ///< 合约白名单不命中。
};

/// @brief 解析成交量模式字符串。
///
/// `"incremental"` 和 `"inc"` 返回 Incremental；其他值返回 Cumulative。
XtTickVolumeMode parse_tick_volume_mode(const std::string& mode);

/// @brief 从 CSV 表头构建 tick 列索引规格。
///
/// `line` 不含字母列名时返回 false，表示调用方应按无表头数据行处理。`spec`
/// 应传入默认构造值；成功时填充 `has_header/simple/idx_*` 和 `detected_depth`。
/// 函数使用 thread_local split buffer，同线程内不得递归调用。
bool build_tick_spec_from_header(const std::string& line, XtTickCsvSpec& spec);

/// @brief 解析一行 tick CSV 到 `XtRawTick`。
///
/// simple 格式读取 `code,price`；标准格式按 `spec.idx_*` 读取时间、价格、成交量
/// 和 L1-L20 盘口。`symbol_filter` 为空时不过滤。函数使用 zero-copy 字段视图，
/// `line` 必须在调用期间保持有效；同线程内不得递归调用。
///
/// @return true 表示 `out` 和 `key` 已更新；false 表示空行、合约过滤或必需字段缺失。
/// @param out_drop 非空时：返回 false 写入丢弃原因（BadRow/SymbolMiss），
///        返回 true 写入 None。默认 nullptr（既有调用方不受影响）。
bool parse_tick_csv_line(const std::string& line,
                         const XtTickCsvSpec& spec,
                         XtRawTick& out,
                         uint64_t& key,
                         const std::unordered_set<std::string>& symbol_filter,
                         double qty_base,
                         XtTickVolumeMode vol_mode,
                         TickParseDrop* out_drop = nullptr);
