#pragma once
#include <cstdint>
#include <type_traits>

/// @file
/// @brief 交易所盘口 Level-2 数据结构：逐档委托队列、逐笔委托、逐笔成交。
///
/// Level-2（L2）行情在每档价量之外，还给出每笔委托和每笔成交的明细。这三个结构承载这些
/// 明细，字段对齐交易所常见定义并保持稳定 layout，供 L2 行情回放、排队量（queue-ahead）
/// 模型和订单流不平衡（OFI）计算共用。

/**
 * @brief 订单队列深度（order queue snapshot）。
 *
 * 表示某价格档位的委托队列分布，记录该档位前 N 笔委托量（volumes[]）
 * 及委托总笔数（order_items）。用于回测中估算排队量（queue_ahead 模型输入）。
 */
struct XtOrderQueue {
    char exchg[10] = {};
    char code[32] = {};

    uint32_t trading_date = 0;
    uint32_t action_date = 0;
    uint32_t action_time = 0;

    uint8_t side = 0;
    uint8_t reserved8[3] = {};

    double price = 0.0;
    uint32_t order_items = 0;
    uint32_t qsize = 0;
    double volumes[50] = {};
};

/**
 * @brief 单笔委托明细（order detail / individual order record）。
 *
 * 记录交易所推送的单笔委托信息，含委托时间、价格、量、买卖方向
 * 及交易所内部序号（index），用于 L2 订单流回放与高频撮合仿真。
 */
struct XtOrderDetail {
    char exchg[10] = {};
    char code[32] = {};

    uint32_t trading_date = 0;
    uint32_t action_date = 0;
    uint32_t action_time = 0;

    uint64_t index = 0;
    double price = 0.0;
    double volume = 0.0;
    uint8_t side = 0;
    uint8_t order_type = 0;
    uint8_t reserved8[6] = {};
};

/**
 * @brief 成交明细（transaction / trade tick）。
 *
 * 记录交易所推送的单笔成交记录，含成交时间、价格、量、主被动方向
 * 及交易所成交序号（index），用于 L2 订单流回放与 OFI 计算。
 */
struct XtTransaction {
    char exchg[10] = {};
    char code[32] = {};

    uint32_t trading_date = 0;
    uint32_t action_date = 0;
    uint32_t action_time = 0;

    uint64_t index = 0;
    double price = 0.0;
    double volume = 0.0;
    uint8_t side = 0;
    uint8_t flags = 0;
    uint8_t reserved8[6] = {};
};

static_assert(std::is_trivially_copyable_v<XtOrderQueue>,
              "XtOrderQueue must remain trivially_copyable for L2 data transport");
static_assert(std::is_trivially_copyable_v<XtOrderDetail>,
              "XtOrderDetail must remain trivially_copyable for L2 data transport");
static_assert(std::is_trivially_copyable_v<XtTransaction>,
              "XtTransaction must remain trivially_copyable for L2 data transport");
