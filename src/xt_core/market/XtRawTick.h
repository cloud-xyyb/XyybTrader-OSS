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
    static constexpr int DEPTH = 20;
    char exchg[10] = {};
    char code[32] = {};

    double price = 0.0;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double settle = 0.0;

    double upper_limit = 0.0;
    double lower_limit = 0.0;

    double total_volume = 0.0;
    double volume = 0.0;
    double total_turnover = 0.0;
    double turnover = 0.0;
    double open_interest = 0.0;
    double diff_interest = 0.0;

    uint32_t trading_date = 0;
    uint32_t action_date = 0;
    uint32_t action_time = 0;
    uint32_t kind = 0;

    double pre_close = 0.0;
    double pre_settle = 0.0;
    double pre_interest = 0.0;

    double bid_prices[DEPTH] = {};
    double ask_prices[DEPTH] = {};
    double bid_qty[DEPTH] = {};
    double ask_qty[DEPTH] = {};
};

#pragma pack(pop)

static_assert(std::is_trivially_copyable_v<XtRawTick>,
              "XtRawTick must remain trivially_copyable for raw market data transport");
