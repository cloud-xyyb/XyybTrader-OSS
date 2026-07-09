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
#include "xt_core/order/XtOrderTypes.h"

namespace binance_u {

/// @brief TD transport 的统一报单输入，REST/WS 实现共用。
/// @note `local_id` 与 `client_order_id` 由 TraderBinanceUGateway 在策略线程入口分配；
///       transport 只负责发送与回报归因，不生成业务标识。
struct TransportSubmitReq {
    uint64_t      local_id      = 0;
    std::string   client_order_id;
    std::string   symbol;
    bool          is_buy        = false;
    int64_t       price_fp      = 0;
    int64_t       qty_fp        = 0;
    XtTimeInForce tif           = XtTimeInForce::GTC;
	    XtOffset      close_offset  = XtOffset::Open;
	    ExecFlags     exec_flags    = 0;
	    uint64_t      t0_us         = 0;
	    uint64_t      trace_id      = 0;
	};

/// @brief TD transport 的统一撤单输入，REST/WS 实现共用。
struct TransportCancelReq {
    uint64_t    local_id = 0;
    std::string client_order_id;
    std::string symbol;
    uint64_t    t0_us    = 0;
};

/// @brief Transport 层异步 ack（成功 / 失败统一表达）。
/// @note 交易所业务拒绝使用 kRejected；网络、队列、超时等发送层失败使用
///       kTransportError_*。调用方可用 `is_transport_error()` 区分错误来源，
///       用 `needs_reconcile()` 判断是否必须按 client_order_id 补查交易所状态。
/// @note ack 不持有时间戳；同一次派发的 TransportLatencyTrace 承载时序切片。
struct TransportAck {
    enum class Kind {
        kAccepted,
        kRejected,
        kCanceled,
        kCancelNoop,

        kTransportError_Stale,
        kTransportError_TimeoutBeforeSend,
        kTransportError_TimeoutAfterSend,
        kTransportError_Disconnect,
        kTransportError_QueueOverflow,
        kTransportError_Internal,
    };

    enum class ReqKind { kSubmit, kCancel };

    Kind         kind        = Kind::kRejected;
    ReqKind      req_kind    = ReqKind::kSubmit;
    uint64_t     local_id    = 0;

    std::string  sys_id;
    int64_t      filled_fp   = 0;
    std::string  raw_status;

    std::string  err_msg;
    int          err_code    = 0;

    bool is_transport_error() const noexcept {
        return kind == Kind::kTransportError_Stale
            || kind == Kind::kTransportError_TimeoutBeforeSend
            || kind == Kind::kTransportError_TimeoutAfterSend
            || kind == Kind::kTransportError_Disconnect
            || kind == Kind::kTransportError_QueueOverflow
            || kind == Kind::kTransportError_Internal;
    }

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
    const char* transport = "rest";

    uint64_t t0_us  = 0;
    uint64_t enq_us = 0;
    uint64_t t0a_us = 0;
    uint64_t t0b_us = 0;
    uint64_t t0c_us = 0;
    uint64_t t1_us  = 0;
};

/// @brief Binance U TD 发送通道抽象，屏蔽 REST 与 WebSocket 下单差异。
class ITradeTransport {
public:
    using AckHandler = std::function<void(const TransportAck&,
                                          const TransportLatencyTrace&)>;

    virtual ~ITradeTransport() = default;

    virtual bool start() = 0;

    virtual void stop() = 0;

    virtual void submit(TransportSubmitReq&& req) = 0;

    virtual void cancel(TransportCancelReq&& req) = 0;

    virtual void bind_ack_handler(AckHandler handler) = 0;

    /// @brief 健康指标（用于 wrapper check_health 和 metrics 暴露）。
    /// @note 三个 atomic 独立读，接受 microsecond-level torn read（field 间略
    ///       不一致，但单字段都是原子读）。
    struct Health {
        bool     connected      = false;
        uint32_t inflight_count = 0;
        uint64_t last_ack_us    = 0;
    };

    virtual Health health() const = 0;
};

}
