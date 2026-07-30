#pragma once

#include <memory>
#include <optional>
#include <vector>

#ifdef XT_BUILD_TESTS
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#endif

#include "api/ITradeGateway.h"
#include "api/IReconcilableGateway.h"
#include "BinanceTdCfg.h"
#include "xt_live/gateway/common/TdReadinessGate.h"

namespace binance_u {

#ifdef XT_BUILD_TESTS
struct TransportAck;

class BinanceRestClient;
class BinanceOrderLedger;
#endif

/// @brief Binance U 本位永续的交易网关，实现 ITradeGateway / IReconcilableGateway
///
/// TraderBinanceUGateway 是 Binance 的下单出口：把 submit/cancel 发给交易所，
/// 把回报、成交和账户/持仓变化传回上层；本地状态跟交易所对不上时用 REST 对账拉回来。
///
/// submit/cancel 返回只代表请求已受理；ACK、成交、终态经 callbacks 异步返回。
/// @warning callbacks 来自内部多个线程；handler 必须非阻塞，把事件快速入队。
class TraderBinanceUGateway : public ITradeGateway, public IReconcilableGateway {
public:

    TraderBinanceUGateway();

    ~TraderBinanceUGateway() override;

    TraderBinanceUGateway(const TraderBinanceUGateway&)            = delete;
    TraderBinanceUGateway& operator=(const TraderBinanceUGateway&) = delete;

    void configure_binance(const BinanceTdCfg& cfg);

    bool init(const Config& cfg) override;

    bool start() override;

    void stop() override;

    bool check_health(uint32_t now_ms) override;

    bool query_wallet_balance_sync(double& out_balance) override;

    bool reconcile_supported() const override { return true; }

    std::optional<std::vector<XtPositionSnapshot>>
        fetch_positions_for_reconcile() override;

    std::optional<std::size_t>
        detect_and_resolve_ghost_orders_for_reconcile(uint32_t min_age_ms) override;

    void request_resnap_for_reconcile() override;

    Capabilities capabilities() const override {
        return Capabilities{
            .supports_close_today  = false,
            .supports_l2_orderflow = false,
            .supports_ioc_fok      = true,
        };
    }

    QueryControlStats query_control_stats() const override { return QueryControlStats{}; }

    [[nodiscard]] bool is_order_terminal(uint64_t local_id) const override;

    [[nodiscard]] bool lookup_terminal(uint64_t local_id,
                                       XtOrderTerminalRecord& out) const override;

    [[nodiscard]] bool collect_open_delivery_ids(std::vector<uint64_t>& out) const override;

    uint64_t submit_order(const char* code,
                          bool is_buy,
                          int64_t price_fp,
                          int64_t qty_fp,
                          XtTimeInForce tif,
                          const char* user_tag,
                          XtOffset close_offset,
                          ExecFlags exec_flags) override;

    uint64_t submit_order_with_intent_trace(const char* code,
                          bool is_buy,
                          int64_t price_fp,
                          int64_t qty_fp,
                          XtTimeInForce tif,
                          const char* user_tag,
                          XtOffset close_offset,
                          ExecFlags exec_flags,
                          uint64_t intent_trace_id) override;

    bool cancel_order(uint64_t local_id) override;

    bool is_recovering() const override;

    void bind_callbacks(Callbacks callbacks) override;

    xt_live::gateway::TdReadinessReport td_readiness_report() const;

#ifdef XT_BUILD_TESTS

    void* _test_impl_void() noexcept;
#endif

private:
    class Impl;
    std::unique_ptr<Impl> impl_;

#ifdef XT_BUILD_TESTS

    friend void  trader_binance_test_inject_transport_ack(void*, const TransportAck&);
    friend void  trader_binance_test_inject_uds_order(void*, const XtOrderExecUpdate&);
    friend void  trader_binance_test_emit_reconcile_ghost(void*, const XtOrderExecUpdate&);
    friend bool  trader_binance_test_emit_blank_sys_id_reject(void*, uint64_t);

    friend const BinanceOrderLedger& trader_binance_test_ledger(void*);
	    friend std::size_t trader_binance_test_cold_worker_task_count(void*);
	    friend uint64_t trader_binance_test_trace_id_for_local(void*, uint64_t);
	    friend void  trader_binance_test_rebuild_from_open_orders_body(void*, const std::string&, bool);

    friend long long trader_binance_test_detect_ghosts(void*, uint32_t);
    friend uint64_t trader_binance_test_user_trades_pages_capped(void*);
    friend void  trader_binance_test_set_rest(void*, std::unique_ptr<BinanceRestClient>);
    friend void  trader_binance_test_reconcile_resync(void*);
    friend void  trader_binance_test_note_tracked_symbol(void*, const std::string&);
    friend void  trader_binance_test_forward_uds_trade(void*, const XtTradeRecord&);
    friend void  trader_binance_test_bootstrap_snapshot(void*);
    friend void  trader_binance_test_request_resnap(void*);
    friend void* trader_binance_test_resync_gate_open(void*);
    friend void  trader_binance_test_resync_gate_close(void*, void*);

    friend int   trader_binance_test_keepalive_pick(void*, long long);
    friend void  trader_binance_test_reset_td_readiness(void*);
    friend void  trader_binance_test_boot_cold_worker_with_uds(
        void*, const std::string&, std::function<void(const XtTradeRecord&, XtLatencyEligibility)>);
    friend void  trader_binance_test_install_uds_prod_wired(void*, const std::string&);
#endif
};

}
