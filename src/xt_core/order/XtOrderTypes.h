#pragma once
#ifndef XT_CORE_ORDER_XT_ORDER_TYPES_H
#define XT_CORE_ORDER_XT_ORDER_TYPES_H
#include <cstdint>
#include <type_traits>

#include "XtOffset.h"
#include "XtFixedPoint.h"

/**
 * @file
 * @brief 订单执行事件的基础类型与 fixed-point helper。
 *
 * 本文件定义提交后执行链路共用的枚举和 POD：报单有效期、订单状态、
 * 执行增量更新和账户快照。
 * `order_exec_to_fp()` / `order_exec_from_fp()` 统一 `_fp` 字段与 double 边界的转换口径；
 * `order_status_*()` 提供状态字符串、终态判断和更新排序。
 *
 * 这些类型不携带 broker/system order id 等 venue-specific 字段。
 */

enum class XtTimeInForce {
    GTC, // good till cancel（挂单直到成交或主动撤单）
    IOC, // immediate or cancel（立即成交剩余撤单，即 FAK）
    FOK  // fill or kill（全部成交否则全撤）
};

// ---------------------------------------------------------------------------
// 固定点转换（fixed-point conversion）
// ---------------------------------------------------------------------------

/** 固定点 scale 因子：1 double unit = 1_000_000 fixed-point units。 */
constexpr int64_t kOrderExecFpScale = kPriceFpScale;
// 锚定到 SSOT (XtFixedPoint.h kPriceFpScale)，避免常量漂移。
static_assert(kOrderExecFpScale == kPriceFpScale,
              "kOrderExecFpScale must equal kPriceFpScale (SSOT)");

/**
 * double → fixed-point（截断，非四舍五入）。
 *
 * 实现委托到 ::to_fp_trunc(v)，与价格域 fixed-point 截断语义一致。
 * @pre 调用方保证输入是有限数。
 */
inline int64_t order_exec_to_fp(double v) {
    return ::to_fp_trunc(v);
}

/** fixed-point → double。 */
inline double order_exec_from_fp(int64_t v) {
    return static_cast<double>(v) / static_cast<double>(kOrderExecFpScale);
}

// ---------------------------------------------------------------------------
// 保留订单号哨兵（reserved order-id sentinel）
// ---------------------------------------------------------------------------

/// @brief 保留的「永不会被分配」内部订单号哨兵。
///
/// PerVenueLocalIdGenerator 产出的 local_id 远低于此值（legacy 域约 2^51 量级；
/// per-instance tag 域 < 2^61，见该类「唯一性域」注释），此哨兵落在 uint64 顶端保留区。用于负路径探针（如 conformance 压测
/// 故意撤一个不存在的单），让网关侧能把「故意探针」与「真幽灵单丢失
/// cancel target」区分开（避免探针噪声淹没真信号）。
constexpr uint64_t kReservedProbeCancelOrderId = 0xFFFFFFFFFFFFFF00ULL;


// ---------------------------------------------------------------------------
// XtOrderStatus — 订单生命周期状态枚举
// ---------------------------------------------------------------------------

/**
 * @brief 订单状态枚举（order lifecycle status）。
 *
 * 状态迁移约定（state transition）：
 *   Submitted → Accepted → PartiallyFilled → Filled   （正常成交路径）
 *   Submitted / Accepted → Canceled                   （主动撤单或 TTL 过期）
 *   Submitted / Accepted → Rejected                   （报单被拒）
 *   (进程恢复反向灌) → Recovered                       （exch status 未知，非终态）
 *
 * 终态（final states）：Filled、Canceled、Rejected。
 * 终态更新完成该订单生命周期。见 order_status_is_final()。
 */
enum class XtOrderStatus {
    Submitted,        ///< 已报单，等待交易所确认
    Accepted,         ///< 交易所已接受，排队中
    PartiallyFilled,  ///< 部分成交，剩余量继续等待
    Filled,           ///< 完全成交（终态）
    Canceled,         ///< 已撤单（终态）
    Rejected,         ///< 被拒绝（终态）
    Recovered         ///< 进程恢复反向灌出的 alive 单，exch status/时间戳未知（非终态）
};

/** @brief 订单状态转字符串（调试/日志用）。 */
inline const char* order_status_to_str(XtOrderStatus s) {
    switch (s) {
    case XtOrderStatus::Submitted:       return "Submitted";
    case XtOrderStatus::Accepted:        return "Accepted";
    case XtOrderStatus::PartiallyFilled: return "PartiallyFilled";
    case XtOrderStatus::Filled:          return "Filled";
    case XtOrderStatus::Canceled:        return "Canceled";
    case XtOrderStatus::Rejected:        return "Rejected";
    case XtOrderStatus::Recovered:       return "Recovered";
    default:                             return "Unknown";
    }
}

/** @brief 判断是否为终态（no further updates expected after final state）。 */
inline bool order_status_is_final(XtOrderStatus s) {
    return s == XtOrderStatus::Filled ||
           s == XtOrderStatus::Canceled ||
           s == XtOrderStatus::Rejected;
}

/** @brief 用于 dedup / 升级判定的状态等级（rank）。
 *
 *  非终态升级链 Submitted(0) → Accepted(1) → PartiallyFilled(2) → Filled(4)
 *  单调递增。终态 Filled/Canceled/Rejected 用 4/5/6 三个不同值，防御交易所
 *  推矛盾终态（如 UDS Filled 后 ACK Rejected）时 dedup 吞掉冲突。
 *  未知枚举值按最低 rank 处理，调用方不应把它当作升级。
 */
inline int order_status_rank(XtOrderStatus s) noexcept {
    switch (s) {
    case XtOrderStatus::Submitted:        return 0;
    case XtOrderStatus::Accepted:         return 1;  // wire "NEW"
    case XtOrderStatus::PartiallyFilled:  return 2;
    case XtOrderStatus::Filled:           return 4;  // 终态，强制 emit
    case XtOrderStatus::Canceled:         return 5;  // 终态
    case XtOrderStatus::Rejected:         return 6;  // 终态
    case XtOrderStatus::Recovered:        return 1;  // venue alive，等同 Accepted 级（不经 dedup，仅 switch 完备）
    }
    return 0;
}

// ---------------------------------------------------------------------------
// XtOrderExecUpdate — 热路径订单执行增量更新
// ---------------------------------------------------------------------------

/**
 * @brief 热路径订单执行增量更新。
 *
 * 来源为 exchange gateway 或 sim match engine。结构保持 trivially_copyable：
 * symbol 与 strategy 均使用整型 intern key，成交量字段区分累计成交
 * `filled_fp`、本次增量 `last_fill_fp` 和剩余量 `leaves_fp`。
 */
struct XtOrderExecUpdate {
    /** 内部订单号，与 XtFillEvent::order_id 及 XtTradeRecord::order_id 一致。 */
    uint64_t id = 0;

    /** 合约 intern key（整型，不含 string），通过 symbol table 映射回合约码。 */
    uint64_t symbol_id = 0;

    /** true = 买入，false = 卖出。 */
    bool is_buy = true;

    /** 开平语义，详见 XtOffset。 */
    XtOffset close_offset = XtOffset::Open;

    /** 订单状态。 */
    XtOrderStatus status = XtOrderStatus::Submitted;

    /** 报单价（fixed-point）。 */
    int64_t price_fp = 0;

    /** 报单总量（fixed-point）。 */
    int64_t qty_fp = 0;

    /** 累计已成交量（fixed-point，cumulative）。 */
    int64_t filled_fp = 0;

    /** 剩余未成交量（fixed-point）。 */
    int64_t leaves_fp = 0;

    /** 本次增量成交量（fixed-point，本次 update 新增的成交量）。 */
    int64_t last_fill_fp = 0;

    /**
     * 交易所时间戳（Unix epoch us）。
     * 实盘：来自 venue 回报时间；回测：合成事件时间。
     * 若来源不可靠，值为 0。
     */
    uint64_t exch_ts_us = 0;

    /** 成交回报到达本地的时间戳（us），即回调进入时刻。 */
    uint64_t local_cb_us = 0;

    /** 成交回报到达本地的时间戳（ns），高精度版本，用于 HFT 延迟度量。 */
    uint64_t local_cb_ns = 0;

    /** 成交事件进入策略的时间戳（us），用于度量 td→strategy 时延。 */
    uint64_t local_strategy_us = 0;

    /// 策略 intern key；0 表示未绑定策略或单策略默认路由。
    uint32_t sid_id = 0;

    // ── 访问器（accessor：fp → double） ──────────────────────────────────

    bool is_close() const { return close_offset != XtOffset::Open; }
    double price() const { return order_exec_from_fp(price_fp); }
    double qty() const { return order_exec_from_fp(qty_fp); }
    double filled() const { return order_exec_from_fp(filled_fp); }
    double remaining() const { return order_exec_from_fp(leaves_fp); }
    double last_fill() const { return order_exec_from_fp(last_fill_fp); }
};

// ---------------------------------------------------------------------------
// XtAccountUpdate — 热路径账户快照
// ---------------------------------------------------------------------------

/**
 * @brief 成交后账户快照（hot-path account snapshot）。
 *
 * @details
 * 经独立 on_account 回调推送（与 on_fill 独立），表示成交后的账户状态。
 * 所有值为快照（非增量）。
 */
struct XtAccountUpdate {
    /** 可用资金（fixed-point，扣除保证金后的可用现金）。 */
    int64_t cash_fp = 0;

    /** 总权益（fixed-point，cash + 持仓市值 + 浮动盈亏）。 */
    int64_t equity_fp = 0;

    /** 浮动盈亏（fixed-point，未实现 PnL）。 */
    int64_t unrealized_fp = 0;

    double cash() const { return order_exec_from_fp(cash_fp); }
    double equity() const { return order_exec_from_fp(equity_fp); }
    double unrealized() const { return order_exec_from_fp(unrealized_fp); }
};

// ---------------------------------------------------------------------------
// POD invariant: hot-path update/event types must stay trivially_copyable.
// ---------------------------------------------------------------------------
static_assert(std::is_trivially_copyable_v<XtOrderExecUpdate>,
              "XtOrderExecUpdate must be trivially_copyable for hot-path POD invariant; "
              "uses uint32 sid_id (not std::string strategy_id) to satisfy this.");

#endif // XT_CORE_ORDER_XT_ORDER_TYPES_H
