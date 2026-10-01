#pragma once

/// @file
/// @brief Binance TD 下单发送层的统一接口。
///
/// TraderBinanceUGateway 不直接关心“这笔单是走 REST 还是走 WebSocket 发出去”
/// 它只把报单/撤单包装成这里定义的结构，然后交给 ITradeTransport。

#include <functional>
#include <memory>
#include <string>

#include "api/ExecFlags.h"
#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrderTypes.h"  // XtTimeInForce

namespace binance_u {

/// @brief TD transport 的统一报单输入，REST/WS 实现共用。
/// @note `local_id` 与 `client_order_id` 由 TraderBinanceUGateway 在策略线程入口分配；
///       transport 只负责发送与回报归因，不生成业务标识。
struct TransportSubmitReq {
    uint64_t      local_id      = 0;   ///< 本地委托编号，用于 ack 回填到订单状态机。
    std::string   client_order_id;     ///< Binance `newClientOrderId`，当前格式为 `ct_<base36>_<base36>`。
    std::string   symbol;              ///< Binance U 本位合约代码，如 `BTCUSDT`。
    bool          is_buy        = false; ///< true=BUY，false=SELL。
    int64_t       price_fp      = 0;   ///< 价格定点值（×1e6），发送前按 tick size 截断。
    int64_t       qty_fp        = 0;   ///< 数量定点值（×1e6），发送前按 lot size 截断。
    XtTimeInForce tif           = XtTimeInForce::GTC; ///< LIMIT 单 time-in-force。
	    XtOffset      close_offset  = XtOffset::Open;     ///< 非 Open 时映射为 reduceOnly。
	    ExecFlags     exec_flags    = 0;   ///< PostOnly/ReduceOnly 等执行约束。
	    uint64_t      t0_us         = 0;   ///< 策略线程 submit 入口的 steady µs，作为 latency 起点。
	    uint64_t      trace_id      = 0;   ///< INTENT_TRACE 归因 ID；transport worker 不依赖 TLS。
	};

/// @brief TD transport 的统一撤单输入，REST/WS 实现共用。
struct TransportCancelReq {
    uint64_t    local_id = 0;          ///< 本地委托编号，用于 ack 回填到订单状态机。
    std::string client_order_id;       ///< 原报单的 Binance `newClientOrderId`。
    std::string symbol;                ///< Binance U 本位合约代码。
    uint64_t    t0_us    = 0;          ///< 策略线程 cancel 入口的 steady µs，作为 latency 起点。
};

/// @brief Transport 层异步 ack（成功 / 失败统一表达）。
/// @note 交易所业务拒绝使用 kRejected；网络、队列、超时等发送层失败使用
///       kTransportError_*。调用方可用 `is_transport_error()` 区分错误来源，
///       用 `needs_reconcile()` 判断是否必须按 client_order_id 补查交易所状态。
/// @note ack 不持有时间戳；同一次派发的 TransportLatencyTrace 承载时序切片。
struct TransportAck {
    enum class Kind {
        kAccepted,                ///< 交易所接受（NEW / PARTIALLY_FILLED / FILLED 等）
        kRejected,                ///< 交易所业务拒绝（err_code 透传）
        kCanceled,                ///< cancel 成功
        kCancelNoop,              ///< cancel 时交易所返回"unknown order"（-2011）；不是订单终态——cancel 可能跑赢订单注册，wrapper 保持订单存活由重试/reconcile 收敛
        // ── transport 失败（非交易所业务拒绝） ───────────────────────────
        kTransportError_Stale,                ///< enq→drain 间隔超 stale_drop_ms
        kTransportError_TimeoutBeforeSend,    ///< 队列等待超时，未 send
        kTransportError_TimeoutAfterSend,     ///< write 完成但 ack 超时；也是 REST/WS 回包 execution-unknown（5xx / -1007 等）的统一载体——委托可能已挂出/成交，保持在途交 reconcile
        kTransportError_Disconnect,           ///< 连接断开，inflight 清空
        kTransportError_QueueOverflow,        ///< send queue 满（未连接时 burst）
        kTransportError_Internal,             ///< codec parse 失败 / unexpected
    };

    /// @brief 本 ack 归属的原始请求类型；transport 在每个派发点 stamp。
    /// @note kRejected / kTransportError_* 对 submit 与 cancel 的订单语义完全不同：
    ///       submit 失败 = 订单未挂出（可安全终态化）；cancel 失败 = 订单可能仍
    ///       live 在交易所（绝不能据此终态化订单，否则本地销账后委托在 venue 挂着
    ///       吃 margin / max-open-order 限额）。wrapper 必须按 req_kind 分支。
    enum class ReqKind { kSubmit, kCancel };

    /// @brief ack 到达时刻的来源（latency 端点可采性判据）。
    /// @note 由 REST/WS **构造点**盖章，禁止从 Kind 或 t1_us 推断（kTimeoutAfterSend
    ///       双义复用是根因）：收到真实 HTTP/WS 响应（含 5xx / -1007 execution-unknown，
    ///       `http_status != 0`）→ kVenueResponse；DNS/connect/read-timeout/pre-send/
    ///       stale/queue/internal 等本地失败（`http==0` 路径）→ kLocalFailure。
    ///       gateway 据此决定 raw ack 采样与 order_update eligibility。
    enum class TimingOrigin : uint8_t { kVenueResponse, kLocalFailure };

    Kind         kind          = Kind::kRejected;
    ReqKind      req_kind      = ReqKind::kSubmit;
    TimingOrigin timing_origin = TimingOrigin::kLocalFailure;  ///< 默认本地失败（构造点显式盖章 venue 响应）
    uint64_t     local_id    = 0;

    // ── 成功路径用 ────────────────────────────────────────────────────────
    std::string  sys_id;              ///< exchange orderId（kAccepted/kCanceled 必填）
    int64_t      filled_fp   = 0;     ///< 交易所 ack 已含 partial fill 时填
    std::string  raw_status;          ///< 交易所原文 status（如 "NEW" / "PARTIALLY_FILLED"，调试用）

    // ── 失败路径用 ────────────────────────────────────────────────────────
    std::string  err_msg;             ///< 交易所 error.msg 或 transport 描述
    int          err_code    = 0;     ///< 交易所业务错误码（透传，wrapper 翻译为 XtRejectCode）

    /// @brief 是否 transport 层失败（vs 业务拒绝）。
    bool is_transport_error() const noexcept {
        return kind == Kind::kTransportError_Stale
            || kind == Kind::kTransportError_TimeoutBeforeSend
            || kind == Kind::kTransportError_TimeoutAfterSend
            || kind == Kind::kTransportError_Disconnect
            || kind == Kind::kTransportError_QueueOverflow
            || kind == Kind::kTransportError_Internal;
    }

    /// @brief 是否必须走 reconcile by client_order_id（可能交易所已成交）。
    /// @note TimeoutAfterSend / Disconnect 两种 kind 即使 transport 失败，
    ///       交易所端也可能已挂单/成交。
    bool needs_reconcile() const noexcept {
        return kind == Kind::kTransportError_TimeoutAfterSend
            || kind == Kind::kTransportError_Disconnect;
    }
};

/// @brief 单笔 submit/cancel 的 transport 延迟切片。
/// @note t0/t0a/t0b/t0c/t1 与 enq_us 覆盖策略入口、排队、序列化签名、
///       client write 完成、ack 收到等关键时点。字段为 0 表示对应时点未发生。
struct TransportLatencyTrace {
    uint64_t    local_id = 0;
    const char* transport = "rest";   ///< "rest" / "ws"，c-string 避免短生命周期 std::string

    // 所有字段为 steady_clock µs；REST 路径复用同一组字段表达 worker 阶段。
    uint64_t t0_us  = 0;   ///< 策略线程入口，来自 TransportSubmitReq/TransportCancelReq。
    uint64_t enq_us = 0;   ///< transport 入队时间，stale/timeout-before-send 的检测基准。
    uint64_t t0a_us = 0;   ///< WS: do_drain 进入；REST: worker 取到请求。
    uint64_t t0b_us = 0;   ///< WS: build json + sign 完成；REST: REST 参数构造完成。
    uint64_t t0c_us = 0;   ///< WS: async_write completion；REST: HTTP 调用返回后记录。
    uint64_t t1_us  = 0;   ///< transport ack 派发时刻。
};


/// @brief Binance U TD 发送通道抽象，屏蔽 REST 与 WebSocket 下单差异。
class ITradeTransport {
public:
    using AckHandler = std::function<void(const TransportAck&,
                                          const TransportLatencyTrace&)>;

    virtual ~ITradeTransport() = default;

    /// @brief 启动 transport。返回 true 表示本地资源已建立。
    /// @note WS 实现为非阻塞建连；start() 返回时可能尚未连上，先到的请求进入发送队列。
    virtual bool start() = 0;

    /// @brief 停止 transport，并把仍在队列或 inflight 中的请求按断连失败派发。
    /// @note 返回后内部线程已退出，ack_handler 不再被触发。
    virtual void stop() = 0;

    /// @brief 非阻塞 submit；最终结果通过 AckHandler 异步返回。
    /// @note req.t0_us 必须由调用方在策略线程入口填好。接口接收 rvalue ref，
    ///       要求调用方显式移交字符串字段所有权，避免热路径复制。
    virtual void submit(TransportSubmitReq&& req) = 0;

    /// @brief 非阻塞 cancel；最终结果通过 AckHandler 异步返回。
    virtual void cancel(TransportCancelReq&& req) = 0;

    /// @brief 注入 transport ack 回调；调用方必须在 start() 前完成绑定。
    virtual void bind_ack_handler(AckHandler handler) = 0;

    /// @brief 健康指标（用于 wrapper check_health 和 metrics 暴露）。
    /// @note 三个 atomic 独立读，接受 microsecond-level torn read（field 间略
    ///       不一致，但单字段都是原子读）。
    struct Health {
        bool     connected      = false;   ///< WS: ws_open；REST: 恒 true
        uint32_t inflight_count = 0;       ///< 当前在途请求数
        uint64_t last_ack_us    = 0;       ///< 最近一次 ack 的 steady µs（0 = 从未）
    };

    /// @brief 返回 transport 健康指标快照；不阻塞发送路径。
    virtual Health health() const = 0;
};

}  // namespace binance_u
