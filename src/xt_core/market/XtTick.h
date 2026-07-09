#pragma once
#include <cstdint>
#include <type_traits>

#pragma pack(push, 8)

enum class TickKind : uint8_t {
    Normal  = 0,
    Preopen = 1,
};

/**
 * @brief 热路径行情 tick（fixed-point）。
 *
 * 承载最新价、OHLC、成交量、涨跌停价及 L1-L10 盘口深度。价格、数量与成交额
 * 使用 fixed-point 整数，供策略层和撮合引擎直接比较；原始 double 行情由
 * `XtRawTick` 表示并通过转换层进入本结构。
 */
struct XtTick {
    static constexpr int DEPTH = 10;

    char exchg[10] = {};
    char code[32] = {};

    int64_t price_fp = 0;
    int64_t open_fp = 0;
    int64_t high_fp = 0;
    int64_t low_fp = 0;
    int64_t settle_fp = 0;

    int64_t upper_limit_fp = 0;
    int64_t lower_limit_fp = 0;

    int64_t total_volume_fp = 0;
    int64_t volume_fp = 0;
    int64_t total_turnover_fp = 0;
    int64_t turnover_fp = 0;
    int64_t open_interest_fp = 0;
    int64_t diff_interest_fp = 0;

    uint32_t trading_date = 0;
    uint32_t action_date = 0;
    uint32_t action_time = 0;
    uint32_t kind = 0;

    int64_t pre_close_fp = 0;
    int64_t pre_settle_fp = 0;
    int64_t pre_interest_fp = 0;

    int64_t bid_price_fp[DEPTH] = {};
    int64_t ask_price_fp[DEPTH] = {};
    int64_t bid_qty_fp[DEPTH] = {};
    int64_t ask_qty_fp[DEPTH] = {};
};

#pragma pack(pop)

static_assert(std::is_trivially_copyable_v<XtTick>,
              "XtTick must remain trivially_copyable for hot-path pass-by-value transport");
