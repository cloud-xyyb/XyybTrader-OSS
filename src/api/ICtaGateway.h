#pragma once

#include "api/IExecutionGateway.h"
#include "api/XtOrderTerminalRecord.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"  // XtOrderExecUpdate

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace xt_runtime {

// ── ICtaGateway — CTA 执行网关扩展接口 ────────────────────────────────────

/// @brief CTA 执行网关扩展接口（CTA execution gateway extension）
///
/// 继承自 IExecutionGateway（submit + cancel），新增回测撮合驱动、日结与
/// 回报 drain / 终态通知方法。新增方法均有默认空操作实现，实盘 Gateway
/// 可直接继承按需重写。
class ICtaGateway : virtual public IExecutionGateway {
public:
    /// @brief 更新市场价格，驱动回测撮合（CTA tick/bar 驱动入口）。
    ///
    /// 回测实现：调用 MatchEngine.process_pending，执行完整回报链。
    /// 实盘实现：空操作（填单由交易所异步回报驱动）。
    ///
    /// @param code      合约代码
    /// @param price_fp  最新价定点数
    /// @param date      行情日期（action_date）
    /// @param time_ms   行情时间（action_time_ms）
    /// @return 本次撮合成交笔数（实盘恒返回 0）
    ///
    /// Safety invariant (View Causality):
    /// - update_market_price 触发的撮合产生的回报，必须晚于本次调用之后
    ///   的下一个 advance_clock 才对 view 可见
    /// - 实盘对应物：回报通过独立队列异步到达，策略在下一个 tick 入口才能看到
    /// - 回测对应物：延迟队列（XtCtaMatchEngine 内部）+ advance_clock 推进
    virtual int update_market_price(const char* /*code*/, int64_t /*price_fp*/,
                                    uint32_t /*date*/, uint32_t /*time_ms*/) {
        return 0;
    }

    /// @brief 推进网关内部时钟，并释放到期的延迟回报事件。
    /// @param date    自然日 YYYYMMDD（event-axis，与 update_market_price 的
    ///                action_date 同轴——入队/释放两侧必须同一时间轴）
    /// @param time_ms HHMMSSmmm
    /// @note 默认 no-op；不需要 clock-driven 延迟回报的实现可不重写。
    virtual void advance_clock(uint32_t /*date*/, uint32_t /*time_ms*/) {}

    /// @brief 日结处理（trading day end flush）。
    ///
    /// 回测实现：市价单用尾价成交，限价单发 Canceled，清空 pending 队列。
    /// 实盘实现：空操作（日结由交易所通知处理）。
    ///
    /// @param date     日结日期（action_date）
    /// @param time_ms  日结时间（action_time_ms）
    /// @param allow_market_fill  传入时间戳是否为 in-session 合法时刻；false 时市价单
    ///        不成交、改发 Canceled（回测语义，实盘实现忽略）。
    /// @return 日结成交笔数（实盘恒返回 0）
    virtual int flush_session(uint32_t /*date*/, uint32_t /*time_ms*/,
                              bool /*allow_market_fill*/ = true) {
        return 0;
    }

    /// @brief 当前活跃挂单数（active pending order count）。
    ///
    /// 回测实现：返回 pending 队列长度；实盘实现可接入 OMS 查询。
    /// 默认返回 0（对不需要此计数的实现透明）。
    [[nodiscard]] virtual size_t active_orders() const { return 0; }

    // ── 对称化：回报 drain + 订单终态通知（与 IHftGateway 对称） ────────────

    /// @brief 订单终态通知（order terminal notify）— STP 按 id 释放该单边界贡献。
    ///
    /// 实盘 XtLiveOrderGateway（dual-impl IHftGateway + ICtaGateway，单 override 覆盖
    /// 两 vtable 槽）维护 STP per-order active set，订单终态时由 engine 按 id erase。
    /// 回测（CTA 侧）当前无 STP 接入，保持默认空操作。
    /// @note 只在订单真终态时调用（对活单误调会错删 active entry）。
    virtual void notify_order_terminal(uint64_t /*order_id*/) {}

    /// @brief reverse-sync fillup 实际接受/复活一单 → STP re-arm（R4：跟随 book 裁决）。
    ///
    /// 与 IHftGateway 对称：engine 对 book fillup 返回的 accepted 清单逐单调用；实盘
    /// XtLiveOrderGateway（dual-impl，单 override 覆盖两 vtable 槽）转发到 STP
    /// rearm_active_order（upsert + 清 tombstone）。回测/未启用 STP 默认空操作。
    /// @note 只碰 STP（不回写 book）；warm reverse-sync 回调改 no-op 后本方法是 re-arm 唯一来源。
    virtual void rearm_active_order(const std::string& /*code*/, uint64_t /*id*/,
                                    bool /*is_buy*/, int64_t /*price_fp*/) {}

    // STP reverse-sync cold clear/seed 走 XtLiveOrderGateway 具体方法 stp_rearm_reverse_sync
    //（cold clear+seed / warm no-op 分叉），CTA channels 回调点持具体 gateway 指针直调。
    // warm re-arm 改由 rearm_active_order（book fillup 接受点驱动）。回测无 reverse-sync 路径。

    /// @brief 订单终态记录（terminal status record）— 用于 order_cancel_ratio 统计。
    ///
    /// 与 IHftGateway 对称：live gateway 把 Canceled 终态计入累计计数器，
    /// HftRunnerControl 据此计算 order_cancel_ratio 输出到 KPI CSV。
    /// 回测实现：默认空操作（回测无 live telemetry 上报链路）。
    virtual void record_order_terminal(XtOrderStatus /*status*/) {}

    /// @brief 订单终态清理 sidecar — 清除 per-order 内部 map 条目防 leak。
    ///
    /// engine.on_order_update 在 order 终态（Filled/Canceled/Rejected）时调用，
    /// gateway 实现清除 per-order 内部 metadata（如 order_strategy_ sidecar）。
    /// 默认空操作（gateway 无 per-order 状态时 no-op）。
    virtual void erase_order_tracking(uint64_t /*order_id*/) {}

    /// @brief Drain 已到期的 fill 到输出 vec（pull 模式）。
    ///
    /// 回测实现：弹出 visible_at_ns <= now_ns 的 pending fill 到 out；
    /// 调用方（engine）遍历 out 调 engine.on_trade 完成 domain 处理。
    /// 实盘实现：空操作（fill 由交易所回报直接驱动，无 pending 概念）。
    ///
    /// @return 本次 drain 的 fill 数量（== out 新增元素数）
    virtual size_t drain_fills(uint32_t /*date*/, uint32_t /*time_ms*/,
                               std::vector<XtTradeRecord>& /*out*/) { return 0; }

    /// @brief Drain 已产生的 order_update 到 out（pull 模式，与 fill drain 对称）。
    ///
    /// 回测实现：弹出 match_engine 在 update_market_price 内生成的订单状态流；
    /// 调用方（engine）遍历 out 调 engine.on_order_update 完成 domain 处理。
    /// 实盘实现：空操作（order_update 由 CTP 回报直接驱动）。
    virtual size_t drain_order_updates(std::vector<XtOrderExecUpdate>& /*out*/) { return 0; }

    /// @brief venue 是否已将该 local_id 视为终态（lost-terminal reconcile 用，对称 IHftGateway）。
    /// @note 默认 false（回测/无此概念的 venue inert，CTA BT gateway 走默认 → reconcile no-op）；
    ///       XtLiveOrderGateway（dual-impl IHftGateway + ICtaGateway）经其 is_order_terminal
    ///       override 同签名覆盖本 vtable 槽，转发到 venue td_->is_order_terminal。
    [[nodiscard]] virtual bool is_order_terminal(uint64_t /*local_id*/) const { return false; }

    /// @brief venue 终态 payload 按值出参（is_order_terminal 的 payload 版，对称 IHftGateway）。
    /// @note 默认 false；false 不携带任何状态信息，调用方退回布尔探测。
    ///       XtLiveOrderGateway 的单一 lookup_terminal override 同签名覆盖两接口的
    ///       本 vtable 槽，转发到 venue td_->lookup_terminal。
    [[nodiscard]] virtual bool lookup_terminal(uint64_t /*local_id*/,
                                               XtOrderTerminalRecord& /*out*/) const {
        return false;
    }
};

} // namespace xt_runtime
