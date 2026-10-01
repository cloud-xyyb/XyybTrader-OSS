#pragma once
#include <cstdint>
#include <type_traits>

#pragma pack(push, 8)

/// @brief tick 在数据层的 session 分类。
///
/// 由 market data source 或 gateway 写入，引擎按该字段区分正常交易与集合竞价。
/// 数值 0 对应 Normal，未显式填充的缓存记录按正常交易处理。
enum class TickKind : uint8_t {
    Normal  = 0,   ///< 在 session 内的正常交易 tick
    Preopen = 1,   ///< 集合竞价阶段 tick（session 前一分钟，有真实盘口）
};

/**
 * @brief 热路径行情 tick（fixed-point）。
 *
 * 承载最新价、OHLC、成交量、涨跌停价及 L1-L20 盘口深度。价格、数量与成交额
 * 使用 fixed-point 整数，供策略层和撮合引擎直接比较；原始 double 行情由
 * `XtRawTick` 表示并通过转换层进入本结构。
 */
struct XtTick {
    static constexpr int DEPTH = 20;  ///< 盘口深度档位数（order book depth levels）

    char exchg[10] = {};              ///< 交易所代码；固定长度 C 字符数组，写入时必须保留 null terminator。
    char code[32] = {};               ///< 合约代码；固定长度 C 字符数组，禁止无边界 strcpy。

    int64_t price_fp = 0;             ///< 最新价（last price，fixed-point）
    int64_t open_fp = 0;              ///< 开盘价（open price，fixed-point）
    int64_t high_fp = 0;              ///< 最高价（high price，fixed-point）
    int64_t low_fp = 0;               ///< 最低价（low price，fixed-point）
    int64_t settle_fp = 0;            ///< 结算价（settlement price，fixed-point）

    int64_t upper_limit_fp = 0;       ///< 涨停价（upper limit，fixed-point）
    int64_t lower_limit_fp = 0;       ///< 跌停价（lower limit，fixed-point）

    int64_t total_volume_fp = 0;      ///< 累计成交量（cumulative volume，fixed-point）
    int64_t volume_fp = 0;            ///< 本 tick 增量成交量（incremental volume，fixed-point）
    int64_t total_turnover_fp = 0;    ///< 累计成交额（cumulative turnover，fixed-point）
    int64_t turnover_fp = 0;          ///< 本 tick 增量成交额（incremental turnover，fixed-point）
    int64_t open_interest_fp = 0;     ///< 持仓量（open interest，fixed-point）
    int64_t diff_interest_fp = 0;     ///< 持仓量变化（change in open interest，fixed-point）

    uint32_t trading_date = 0;        ///< 交易日（trading date，YYYYMMDD）
    uint32_t action_date = 0;         ///< 发生日期（action date，YYYYMMDD）
    uint32_t action_time = 0;         ///< 发生时间 HHMMSSmmm 编码（HHMMSS × 1000 + ms，例如 93000500 = 09:30:00.500）
    uint32_t kind = 0;                ///< `TickKind` 枚举值，uint32_t 保持缓存 layout 兼容（0 = Normal）

    int64_t pre_close_fp = 0;         ///< 昨收价（previous close，fixed-point）
    int64_t pre_settle_fp = 0;        ///< 昨结算价（previous settlement，fixed-point）
    int64_t pre_interest_fp = 0;      ///< 昨持仓量（previous open interest，fixed-point）

    int64_t bid_price_fp[DEPTH] = {}; ///< 买盘价（bid prices，[0]=best bid，fixed-point）
    int64_t ask_price_fp[DEPTH] = {}; ///< 卖盘价（ask prices，[0]=best ask，fixed-point）
    int64_t bid_qty_fp[DEPTH] = {};   ///< 买盘量（bid quantities，fixed-point）
    int64_t ask_qty_fp[DEPTH] = {};   ///< 卖盘量（ask quantities，fixed-point）
};

#pragma pack(pop)

// POD invariant: hot-path tick type passed by value.
static_assert(std::is_trivially_copyable_v<XtTick>,
              "XtTick must remain trivially_copyable for hot-path pass-by-value transport");
