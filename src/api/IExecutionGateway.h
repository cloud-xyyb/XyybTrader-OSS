#pragma once

#include "XtRejectCode.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtOrderRequest.h"
#include "api/ExecFlags.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

namespace xt_runtime {

/**
 * @brief 下单结果 DTO（submit result）。
 *
 * IExecutionGateway::submit() 的同步返回值，携带本地分配的
 * order_id（0 表示本地预检未通过）与拒单原因（reject_reason）。
 * 用于策略层在下单后立即判断是否受理，无需等待异步回调。
 */
struct SubmitResult {
    uint64_t order_id = 0; // 0 = 本地未受理
    XtRejectCode reject_reason = XtRejectCode::NoError;
    [[nodiscard]] bool accepted() const { return order_id != 0; }
};

/**
 * @brief 统一执行网关接口（execution gateway contract）。
 *
 * @details
 * - 核心命令面仅保留：`submit` + `cancel(id)`。
 * - 条件撤单（如按 code/side/qty）由上层上下文/OMS 组装后下沉为 `cancel(id)`。
 */
class IExecutionGateway {
public:
    virtual ~IExecutionGateway() = default;

    /// @param exec_flags  正交 bitflag（ReduceOnly/PostOnly/Hidden）。CTP 期货
    ///   通常忽略；binance perp 映射到 reduceOnly/GTX。调用方必须显式传参
    ///   （`exec_none()` 或 `with_flag(..., ExecFlag::...)`），无默认值，须显式传参。
    /// @param strategy_id  策略归属标识，gateway 内部 sidecar
    ///   记 `order_id → strategy_id`，apply_fills_ 回写 `trade.strategy_id`
    ///   供 shared-domain BT 场景 listener 按策略分发（runner-wide path 可传空）。
    [[nodiscard]] virtual SubmitResult submit(const char* code,
                                bool is_buy,
                                int64_t price_fp,
                                int64_t qty_fp,
                                XtTimeInForce tif,
                                std::string_view user_tag,
                                XtOffset offset,
                                ExecFlags exec_flags,
                                std::string_view strategy_id = "") = 0;

    virtual bool cancel(uint64_t id) = 0;

    /// @brief XtOrderRequest 入口 (in-memory metadata wrapper).
    ///
    /// @details
    /// Default impl forwards to the scalar submit overload. Live gateways
    /// override this version to consume request metadata, sidecar routing,
    /// and audit trace fields directly.
    ///
    /// @note exec_flags 必须显式传参（与 scalar overload 对齐）—
    ///   `exec_none()` 或 `with_flag(..., ExecFlag::...)`。默认值会让 binance perp
    ///   ReduceOnly/PostOnly 路径 silent 丢 flag，故不提供默认。
    [[nodiscard]] virtual SubmitResult submit(const xt_core::XtOrderRequest& req,
                                ExecFlags exec_flags) {
        // Default forward for test/backtest gateways that only need scalar fields.
        return submit(req.order.code,
                      req.order.is_buy,
                      req.order.price_fp,
                      req.order.qty_fp,
                      req.order.tif,
                      req.order.tag,
                      req.order.close_offset,
                      exec_flags,
                      req.strategy_id);
    }

    /// @brief 反查 order_id → strategy_id (multi-strategy filter)。
    ///
    /// @details
    /// 用于 ctx.get_open_orders 按 strategy_id filter 自己的 orders
    /// (避免 strategy A 看到 strategy B 的 open orders,违反 multi-strategy 隔离)。
    ///
    /// Default impl 返空字符串 — backtest / mock gateway 不做 sid 路由。
    /// XtLiveOrderGateway override 查 order_strategy_ sidecar。
    ///
    /// @return 已 register 的 strategy_id (string),unknown order_id 返空。
    [[nodiscard]] virtual std::string lookup_strategy_id(uint64_t /*order_id*/) const {
        return std::string{};
    }

    /// @brief 把 get_open_orders 的物理切片行合并为策略持有的逻辑订单行。
    ///
    /// @details
    /// 多切片单（平今昨拆分 / max_order_qty chunk）在 order book 中是 N 个
    /// 物理切片行，而策略 submit 拿到的是逻辑 clord_id；本方法把同一逻辑单的
    /// 切片行就地合并为单行（id=clord，聚合 qty/filled/leaves/status，意图
    /// 字段还原），与 on_order 回调的翻译口径一致。未注册的行（单切片/直发单）
    /// 原样保留，相对顺序不变。
    ///
    /// Default impl 恒等返回 — backtest / mock gateway 无切片翻译层。
    /// XtLiveOrderGateway override 委托 LogicalOrderTracker。
    ///
    /// @return 合并后的行数（<= n）。
    [[nodiscard]] virtual size_t translate_open_orders(XtOrderExecUpdate* /*rows*/,
                                                       size_t n) const {
        return n;
    }

    /// @brief 优雅停机：取消所有 in-flight 挂单并等待终态(graceful stop with cancel-all)。
    ///
    /// @param timeout  最长等待时间(等待 broker 回报 cancel/terminal 完成)。
    ///                 默认 5s;超时后即使仍有挂单残留也返回,不阻塞 Runner shutdown。
    ///
    /// @details
    /// 调用契约（call ordering）：
    ///   - **必须**在 channels.stop() / td_->stop() 之前调用,否则 cancel_order
    ///     无法下发(td_ 已停)。
    ///   - 调用后 submit() 应即时拒单(reject_reason=TradingNotReady 或类似),
    ///     避免 stop() 期间又有新单注入。
    ///   - 幂等：多次调用安全;首次调用之后 stopping_ flag 持续 true。
    ///
    /// backtest gateway / mock gateway 可显式实现 no-op；实盘 gateway 必须提供
    /// 真实的 cancel-all 语义。
    virtual void stop(std::chrono::milliseconds timeout = std::chrono::seconds(5)) noexcept = 0;

    /// @brief 携带已采样 intent_trace_id 的 submit 入口（append-only vtable slot）。
    /// @note 默认实现忽略 trace id 并委托 scalar submit；实盘 gateway override
    ///       后将 trace id 作为显式参数继续传到 TD，避免未采样路径读取 TLS。
    [[nodiscard]] virtual SubmitResult submit_with_intent_trace(const char* code,
                                bool is_buy,
                                int64_t price_fp,
                                int64_t qty_fp,
                                XtTimeInForce tif,
                                std::string_view user_tag,
                                XtOffset offset,
                                ExecFlags exec_flags,
                                std::string_view strategy_id,
                                uint64_t intent_trace_id) {
        (void)intent_trace_id;
        return submit(code,
                      is_buy,
                      price_fp,
                      qty_fp,
                      tif,
                      user_tag,
                      offset,
                      exec_flags,
                      strategy_id);
    }
};

} // namespace xt_runtime
