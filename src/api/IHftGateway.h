#pragma once

#include "api/IExecutionGateway.h"
#include "api/XtOrderTerminalRecord.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"     // XtOrderExecUpdate

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace xt_runtime {

/// @brief HFT 执行网关扩展接口（HFT execution gateway extension）。
///
/// 继承自 IExecutionGateway（submit + cancel），新增回测撮合驱动方法。
/// 实盘 Gateway 可直接继承并接受默认空操作实现。
/// 回报链（fill/order 回调）由 Gateway 实现内部通过 exec sink 完成，不在此接口暴露。
class IHftGateway : virtual public IExecutionGateway {
public:
    // ── 热路径（每 tick 按序调用）────────────────────────────────────────

    /// @brief 推进撮合引擎时钟（advance match clock）。
    ///
    /// 回测实现：驱动 fill_latency 延迟队列到期判断。
    /// 实盘实现：空操作（时序由交易所回报驱动）。
    ///
    /// @param action_date    自然日 YYYYMMDD（event-axis，`to_ns` 契约域；0 = 源未携带，
    ///                       实现方 fallback trading_date）
    /// @param action_time_ms HHMMSSmmm
    /// @param trading_date   交易日 YYYYMMDD（仅供 report/session 戳，禁入时长运算）
    /// @note 应在策略 dispatch 之前调用，使提交时间戳与当前 tick 对齐。
    virtual void advance_clock(uint32_t /*action_date*/, uint32_t /*action_time_ms*/,
                               uint32_t /*trading_date*/) {}

    /// @brief 驱动撮合 + 完整回报链（trigger matching + full report chain）。
    ///
    /// 回测实现：XtHftMatchEngine.on_tick → cost → apply_fills；调用方经 engine 派发 exec sink。
    /// 实盘实现：空操作（成交由异步交易所回报驱动）。
    virtual void process_tick(const XtTick& /*tick*/) {}

    /// @brief Pull 已可见 fill 到输出 vec（pull contract）。
    ///
    /// 实现把当前已可见 fill 追加到 `out`，不清空 `out`；调用方负责消费新增
    /// 元素。默认 no-op（无 pending fill 的实现返回 0）。
    ///
    /// @param date    自然日 YYYYMMDD（event 轴截止，与 fill 可见性同轴）
    /// @param time_ms HHMMSSmmm
    /// @return 本次 drain 的 fill 数量（== out 新增元素数）
    virtual size_t drain_fills(uint32_t /*date*/, uint32_t /*time_ms*/,
                                std::vector<XtTradeRecord>& /*out*/) { return 0; }

    /// @brief 丢弃所有 pending fill (不通知 listener,不送 engine)。
    ///
    /// 用于 EOF 严格因果收尾——visible_at_ns > EOF
    /// clock 的 fill 等于"策略真实场景下也收不到"(进程已退出),不应进 P&L。
    /// 调用者应先 drain_fills(eof_date, eof_time_ms, out) 拉走窗口内的 fill,
    /// 再调本接口清掉剩余 pending。
    /// 实盘实现：空操作。
    /// @return 丢弃的 fill 数量(供 metric / 日志用)
    virtual size_t drop_pending_fills() { return 0; }

    /// @brief Pull 已可见 order_update 到 out（pull contract）。
    ///
    /// 实现把当前已可见 order_update 追加到 `out`，不清空 `out`；调用方负责
    /// 消费新增元素。默认 no-op（无 pending order_update 的实现返回 0）。
    virtual size_t drain_order_updates(std::vector<XtOrderExecUpdate>& /*out*/) { return 0; }

    /// @brief 标定下一次 process_tick 的 fill 时间戳（= 策略 dispatch tick 时间）。
    ///
    /// next_tick / N-tick delay 模式下，撮合用 match_tick 盘口，但 trade_time
    /// 应归属到策略信号 tick 时间（即 dispatch tick）。
    /// 默认空操作，非回测实现可忽略。
    virtual void set_dispatch_clock(uint32_t /*date*/, uint32_t /*time_ms*/) {}

    // ── 会话生命周期 ──────────────────────────────────────────────────────

    /// @brief 交易日开始事件（account day-begin + risk year-init）。
    ///
    /// 回测实现：账户换日重置 + 风控年度初始化。
    /// 实盘实现：默认空操作（可重载）。
    virtual void on_session_begin(uint32_t /*trading_date*/) {}

    /// @brief 订单终态通知（order terminal notify）— STP 按 id 释放该单边界贡献。
    ///
    /// 实盘 XtLiveOrderGateway 维护 STP（Self-Trade Prevention）per-order active set，
    /// 订单终态时由 engine 调用按 id erase 并重算该品种边界。id-keyed 消除旧
    /// code-scan 路径（终态 erase slot 后 code 反查失败 → 漏释放 bound）。
    /// 回测 XtHftBacktestOrderGateway 在 self_match_check 开启时同样 override；
    /// 未开启时等价空操作。
    /// @note 只在订单真终态时调用（对活单误调会错删 active entry）。
    virtual void notify_order_terminal(uint64_t /*order_id*/) {}

    /// @brief reverse-sync fillup 实际接受/复活一单 → STP re-arm（R4：跟随 book 裁决）。
    ///
    /// engine 对 book fillup 返回的 accepted 清单（AcceptedRearmOrder）逐单调用；实盘
    /// XtLiveOrderGateway 转发到 STP rearm_active_order（upsert + 清 tombstone，book
    /// 真值覆盖 STP 私有 tombstone）。回测/未启用 STP 的实现默认空操作。
    /// @note 只碰 STP（不回写 book）；warm reverse-sync 回调改 no-op 后，本方法是 STP
    ///       re-arm 的唯一来源。
    virtual void rearm_active_order(const std::string& /*code*/, uint64_t /*id*/,
                                    bool /*is_buy*/, int64_t /*price_fp*/) {}

    // STP reverse-sync cold clear/seed 走 XtLiveOrderGateway 具体方法 stp_rearm_reverse_sync
    //（cold clear+seed / warm no-op 分叉），三 runner 回调点持具体 gateway 指针直调。
    // warm re-arm 改由 rearm_active_order（book fillup 接受点驱动）。回测无 reverse-sync 路径。

    /// @brief 订单终态记录（terminal status record）— 用于 order_cancel_ratio 统计。
    ///
    /// 实盘 XtLiveOrderGateway 把 status == Canceled 的订单计入累计计数器,
    /// HftRunnerControl 据此计算 order_cancel_ratio 输出到 KPI CSV。
    /// 回测实现：默认空操作（回测无 live telemetry 上报链路）。
    virtual void record_order_terminal(XtOrderStatus /*status*/) {}

    /// @brief 订单终态清理 strategy_id sidecar (live gateway 有 order_strategy_
    ///   map → logical_order_id → strategy_id 反查)。
    ///   engine.on_order_update 在 Filled/Canceled/Rejected 时调用 → 防 long-running
    ///   live RSS 增长 + 缩 race window (map 元素少 rehash 概率低)。
    ///   回测实现:默认空操作 (backtest gateway 无 sidecar)。
    /// @note ICtaGateway 已有同名同语义虚函数;本接口加上让 HFT engine 也能调。
    virtual void erase_order_tracking(uint64_t /*order_id*/) {}

    // ── 状态查询 ─────────────────────────────────────────────────────────

    /// @brief 活跃挂单数（用于 IReplayRuntime::active_orders）。
    [[nodiscard]] virtual size_t live_orders() const = 0;

    /// @brief venue 是否已将该 local_id 视为终态（lost-terminal reconcile 用）。
    /// @note 默认 false（回测/无此概念的 venue inert）；XtLiveOrderGateway 转发到 venue td_。
    [[nodiscard]] virtual bool is_order_terminal(uint64_t /*local_id*/) const { return false; }

    /// @brief venue 终态 payload 按值出参（is_order_terminal 的 payload 版，对齐
    ///        ITradeGateway::lookup_terminal 契约）。
    /// @note 默认 false（回测/无此概念的 venue inert）；false 不携带任何状态信息，
    ///       调用方退回 is_order_terminal 布尔探测。XtLiveOrderGateway 转发 venue td_。
    [[nodiscard]] virtual bool lookup_terminal(uint64_t /*local_id*/,
                                               XtOrderTerminalRecord& /*out*/) const {
        return false;
    }
};

} // namespace xt_runtime
