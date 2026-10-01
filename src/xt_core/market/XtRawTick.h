#pragma once
#include <cstdint>
#include <type_traits>

#pragma pack(push, 8)

/**
 * @brief 原始行情 tick（double precision）。
 *
 * 网关接收层和回测数据入口使用的行情快照，字段与 `XtTick` 对齐。
 * 进入策略/撮合热路径前统一转换为 fixed-point `XtTick`。
 */
struct XtRawTick {
    static constexpr int DEPTH = 20;  ///< 盘口深度档位数（order book depth levels）
    char exchg[10] = {};              ///< 交易所代码；固定长度 C 字符数组，写入时必须保留 null terminator。
    char code[32] = {};               ///< 合约代码；固定长度 C 字符数组，禁止无边界 strcpy。

    double price = 0.0;               ///< 最新价（last price）
    double open = 0.0;                ///< 开盘价（open price）
    double high = 0.0;                ///< 最高价（high price）
    double low = 0.0;                 ///< 最低价（low price）
    double settle = 0.0;              ///< 结算价（settlement price）

    double upper_limit = 0.0;         ///< 涨停价（upper limit price）
    double lower_limit = 0.0;         ///< 跌停价（lower limit price）

    double total_volume = 0.0;        ///< 累计成交量（cumulative volume）
    double volume = 0.0;              ///< 本 tick 增量成交量（incremental volume）
    double total_turnover = 0.0;      ///< 累计成交额（cumulative turnover）
    double turnover = 0.0;            ///< 本 tick 增量成交额（incremental turnover）
    double open_interest = 0.0;       ///< 持仓量（open interest）
    double diff_interest = 0.0;       ///< 持仓量变化（change in open interest）

    uint32_t trading_date = 0;        ///< 交易日（trading date，YYYYMMDD）
    uint32_t action_date = 0;         ///< 发生日期（action date，YYYYMMDD）
    uint32_t action_time = 0;         ///< 发生时间 HHMMSSmmm 编码（HHMMSS × 1000 + ms，例如 93000500 = 09:30:00.500）
    uint32_t kind = 0;                ///< `TickKind` 枚举值（见 XtTick.h，0 = Normal）

    double pre_close = 0.0;           ///< 昨收价（previous close price）
    double pre_settle = 0.0;          ///< 昨结算价（previous settlement price）
    double pre_interest = 0.0;        ///< 昨持仓量（previous open interest）

    double bid_prices[DEPTH] = {};    ///< 买盘价（bid prices，[0]=best bid）
    double ask_prices[DEPTH] = {};    ///< 卖盘价（ask prices，[0]=best ask）
    double bid_qty[DEPTH] = {};       ///< 买盘量（bid quantities）
    double ask_qty[DEPTH] = {};       ///< 卖盘量（ask quantities）
};

#pragma pack(pop)

static_assert(std::is_trivially_copyable_v<XtRawTick>,
              "XtRawTick must remain trivially_copyable for raw market data transport");
