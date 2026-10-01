#pragma once
#include <cstdint>
#include <string>
#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrderTypes.h"

/// @brief 表示 HFT 回测撮合中的单笔模拟订单。
///
/// `XtOrder` 是策略提交的订单意图；`XtSimOrder` 是该意图进入回测撮合后的可变状态。
/// 它保存成交进度、排队位置、ACK/TTL、撤单延迟和回报生成所需的模拟字段。
///
/// `XtOrderManager` 创建并更新本结构，`XtOrderBook` 只按 id 引用挂单。
/// 对象只在回测撮合线程内读写，不提供并发同步。
/// 价格、数量和成交量使用 `kOrderExecFpScale` fixed-point；`queue_ahead` 使用
/// double，以匹配队列模型和 L2 盘口数量。
struct XtSimOrder {
    // ── Order terms ─────────────────────────────────────────────────

    uint64_t id = 0;          ///< XtOrderManager 分配的本地订单号；0 仅表示尚未分配
    std::string code;
    bool is_buy = true;
    int64_t price_fp = 0;     ///< 委托价 fixed-point（恒 > 0；市价单已在上游 context 解析为激进限价 IOC）
    int64_t qty_fp   = 0;     ///< 委托总量 fixed-point

    // ── Matching state ──────────────────────────────────────────────

    int64_t filled_fp   = 0;      ///< 累积成交量 fixed-point
    double  queue_ahead = 0.0;    ///< 同价位前置排队量估计；单位与盘口数量一致
    XtOrderStatus status = XtOrderStatus::Submitted; ///< 生命周期状态；终态订单会归档到 done 容器。
    XtTimeInForce tif = XtTimeInForce::GTC;           ///< FOK/IOC/GTC 语义由 manager 和 order book 共同执行。

    // ── Timing ──────────────────────────────────────────────
    //
    // 时长/到期一律挂 event-axis（自然日 epoch ns，`xt_time::to_ns` 域），
    // 午夜/夜盘不回绕；tick 计数域（create_tick/ack_due_tick）独立。

    uint64_t create_tick = 0;       ///< 下单时钟（tick index）
    int64_t  create_event_ns = 0;   ///< 下单时刻 event ns；时间维度 TTL 基准
    uint32_t order_date = 0;        ///< 报单日期 YYYYMMDD；透传到成交记录
    uint32_t order_time = 0;        ///< 报单时刻 HHMMSSmmm；透传到成交记录
    uint64_t ack_due_tick = 0;      ///< tick 维度 ACK 到期（0=unused）
    int64_t  ack_due_event_ns = 0;  ///< 时间维度 ACK 到期，event ns（0=unused）
    int64_t  fill_eligible_event_ns = 0; ///< 可入簿撮合时刻，event ns（0=立即可撮合）
    /// 撤单请求到达撮合的到期时刻，event ns（0=无在途撤单请求）。
    /// cancel entry latency 开启时由 manager 写入；到期前订单继续参与撮合。
    int64_t  cancel_due_event_ns = 0;
    /// submit 时刻与 entry 相邻配对采样的 response latency（ns）；-1 = 无 latency model。
    /// fill 可见性用本存值而非 fill 时刻再采——paired CDF 单槽在多在途单交错时会错配 percentile。
    int64_t response_latency_ns = -1;

    // ── Order metadata ──────────────────────────────────────────────

    /// 成本归类标记；成交记录从该字段继承 maker/taker 语义。
    /// true 表示 passive/maker，false 表示 aggressive/taker。
    bool is_passive = true;

    /// 撤单后重新入队标记；队列模型可据此惩罚丢失的排队优先权。
    bool is_resubmit = false;
    std::string user_tag;                             ///< 透传到成交/回报，用于上游策略侧关联。
    XtOffset close_offset = XtOffset::Open;           ///< 平今/平昨等 close intent；成本归类由下游解释。

    /// @return 未成交数量 fixed-point；终态订单返回值只用于诊断，不再驱动撮合。
    int64_t remaining_fp() const noexcept { return qty_fp - filled_fp; }

    /// @return 未成交数量的 double 视图，单位与 order_exec fixed-point 转换一致。
    double remaining() const noexcept { return order_exec_from_fp(remaining_fp()); }
};
