#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "api/XtPositionSnapshot.h"
#include "api/AliveInflightOrder.h"
#include "api/ExecFlags.h"
#include "api/BrokerVenueTruth.h"

/**
 * @brief 实盘 TD 网关抽象接口（provider-neutral）。
 *
 * @details
 * `init/start/stop` 定义 TD lifecycle；`submit_order/cancel_order` 定义报撤单
 * 命令面；`Callbacks` 定义 order/trade/account/position/ready/front/latency
 * 与 reverse-sync batch 事件的交付契约。
 *
 * @warning callback 调用线程由 gateway 实现决定。需要 thread affinity 的 consumer
 * 必须 bridge 到自身 owner thread 后再处理。
 *
 * @note 各方法在 CTP / Binance / XTP 三个实现间存在刻意保留的语义差异（返回值、
 * 恢复窗口、终态查询能力），以本文件各方法注释为契约；完整矩阵与下游消费链见
 * `docs/architecture/trade_gateway_contract.md`。
 */
class ITradeGateway {
public:
    enum class TdState : uint8_t {
        Unknown = 0,
        Live = 1,
        Recovering = 2,
    };

    virtual ~ITradeGateway() = default;

    struct Config {
        std::string front;
        std::string user;

        std::string password;
        bool        reconnect_enabled             = true;
        int         reconnect_interval_ms         = 2000;
        int         account_refresh_ms            = 0;
        int         position_refresh_ms           = 0;
        double      account_snapshot_max_abs      = 1e12;

        uint32_t    stale_reconnect_ms            = 0;

        bool        recovery_enabled              = true;

        std::string expected_investor_id;
        std::string expected_api_key_sha8;
    };

    struct Capabilities {
        bool supports_close_today = true;
        bool supports_l2_orderflow = false;
        bool supports_ioc_fok = true;
    };
    struct QueryControlStats {
        uint64_t query_pos_throttled = 0;
        uint64_t query_pos_inflight_skip = 0;
        uint64_t query_pos_dirty_merged = 0;
    };

    using OrderUpdateHandler = std::function<void(const XtOrderExecUpdate&)>;
    using TradeHandler = std::function<void(const XtTradeRecord&)>;
    using ReadyHandler = std::function<void()>;
    using FrontHandler = std::function<void(bool connected, int reason)>;
    struct DailySettlementDelta {
        double realized_delta = 0.0;
        double fee = 0.0;
        uint32_t trading_day = 0;
    };
    struct CashTransferDelta {
        double amount = 0.0;
        uint32_t trading_day = 0;
        uint64_t serial = 0;
    };
    using DailySettlementHandler = std::function<void(const DailySettlementDelta&)>;
    using CashTransferHandler = std::function<void(const CashTransferDelta&)>;

    using PositionSnapshot = ::XtPositionSnapshot;
    struct AccountSnapshot {
        double balance = 0.0;
        double available = 0.0;
        double margin = 0.0;
        double frozen_margin = 0.0;
        double frozen_cash = 0.0;
        double commission = 0.0;
        double close_profit = 0.0;
        double position_profit = 0.0;
        double pre_balance = 0.0;
        double deposit = 0.0;
        double withdraw = 0.0;
    };
    using AccountHandler = std::function<void(const AccountSnapshot&)>;
    using PositionHandler = std::function<void(const std::vector<PositionSnapshot>&)>;

    using BrokerVenueTruthHandler = std::function<void(const BrokerVenueTruth&)>;

    using TransportAckHandler = std::function<void(uint64_t local_id, uint64_t ack_us)>;

    using UdsArrivalHandler = std::function<void(uint64_t local_id, uint64_t uds_ts_us)>;

    using TransportAckUnavailableHandler = std::function<void(uint64_t local_id, uint64_t mark_us)>;

    using ReverseSyncBatchHandler =
        std::function<void(const std::vector<AliveInflightOrder>& orders, bool is_cold_start)>;
    /// @par 线程契约（Thread contract）
    /// 回调的调用线程因 venue 实现而异，consumer 按「任意线程（含调用者自身线程
    /// 同步重入）」对待：
    ///   - CTP：SDK SPI 回调线程同步进入 + recovery / query-timer 后台线程
    ///     + submit/cancel 调用线程（成功路径同步发 Submitted 等回调）；
    ///   - Binance：UDS ws 线程、transport 线程、reconcile 冷路径 worker 多线程并发
    ///     + submit 调用线程（fail-closed 合成 Rejected 同步触发）；
    ///   - XTP：SDK 工作线程。
    /// handler 只做轻量入队或 O(1) 工作，不得阻塞等待网关自身，也不得假设自己
    /// 不会在 submit 调用栈内被重入（跨 submit 持非重入锁是死锁面）。
    /// 绑定必须在 start() 前完成；运行中 rebind 不安全（实现层多为无锁读取）。
    struct Callbacks {
        OrderUpdateHandler on_order_update;
        TradeHandler on_trade;
        AccountHandler on_account;
        PositionHandler on_position;
        BrokerVenueTruthHandler on_broker_account_truth;
        DailySettlementHandler on_daily_settlement;
        CashTransferHandler on_cash_transfer;
        ReadyHandler on_ready;
        FrontHandler on_front;
        TransportAckHandler on_transport_ack;
        UdsArrivalHandler on_uds_arrival;
        TransportAckUnavailableHandler on_transport_ack_unavailable;
        ReverseSyncBatchHandler on_reverse_sync_batch;
    };

    [[nodiscard]] virtual bool init(const Config& cfg) = 0;

    [[nodiscard]] virtual bool start() = 0;

    virtual void stop() = 0;

    [[nodiscard]] virtual uint64_t submit_order(const char* code,
                                  bool is_buy,
                                  int64_t price_fp,
                                  int64_t qty_fp,
                                  XtTimeInForce tif,
                                  const char* user_tag,
                                  XtOffset close_offset,
                                  ExecFlags exec_flags) = 0;

    virtual bool cancel_order(uint64_t local_id) = 0;

    [[nodiscard]] virtual bool is_recovering() const = 0;

    [[nodiscard]] virtual TdState state() const {
        return is_recovering() ? TdState::Recovering : TdState::Live;
    }

    [[nodiscard]] virtual uint32_t trading_day() const { return 0; }

    [[nodiscard]] virtual Capabilities capabilities() const = 0;
    [[nodiscard]] virtual QueryControlStats query_control_stats() const = 0;

    virtual void bind_callbacks(Callbacks callbacks) = 0;

    [[nodiscard]] virtual bool check_health(uint32_t now_ms) = 0;

    [[nodiscard]] virtual bool is_order_terminal(uint64_t  ) const { return false; }

    [[nodiscard]] virtual uint64_t submit_order_with_intent_trace(const char* code,
                                  bool is_buy,
                                  int64_t price_fp,
                                  int64_t qty_fp,
                                  XtTimeInForce tif,
                                  const char* user_tag,
                                  XtOffset close_offset,
                                  ExecFlags exec_flags,
                                  uint64_t intent_trace_id) {
        (void)intent_trace_id;
        return submit_order(code, is_buy, price_fp, qty_fp, tif, user_tag, close_offset, exec_flags);
    }

    [[nodiscard]] virtual bool query_wallet_balance_sync(double&  ) { return false; }
};
