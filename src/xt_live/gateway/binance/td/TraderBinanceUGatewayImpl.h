#pragma once

#include "TraderBinanceUGateway.h"
#include "td/BinanceOrderLedger.h"
#include "td/BinanceReconciler.h"
#include "td/BinanceTdParse.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "rest/BinanceRestClient.h"
#include "BinanceTdCfg.h"
#include "transport/BinanceUserDataStream.h"
#include "transport/ITradeTransport.h"
#include "api/StrategySid.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_live/gateway/binance/td/EffectiveSignalDedup.h"
#include "xt_core/order/PerVenueLocalIdGenerator.h"

namespace binance_u {

/// @brief Binance U 本位 TD 网关的 PImpl 实现。
///
/// 外层 TraderBinanceUGateway 只是接口壳，真正的运行状态都在 Impl 里：
/// - 把下单/撤单交给 transport 发出去，接收 UDS 回报，跑 REST 维护任务
/// - 每笔订单是谁的、走到什么状态了，统一记在 ledger_ 里
/// - 本地跟交易所对不上时，由 reconciler_ 负责拉回一致
///
/// 多个线程会同时进出这个类，具体哪把锁保护什么见各成员分组注释。
class TraderBinanceUGateway::Impl final {
public:
    Impl()
        : reconciler_(rest, uds, ledger_, cb, readiness_gate_, tracked_symbols_,
                      BinanceReconciler::ImplHooks{

                          [this](const XtOrderExecUpdate& upd,
                                 EffectiveSignalDedup::Source src) {
                              emit_through_dedup(upd, src);
                          },

                          [this] { return queue_reconcile_resync_if_absent_(); },

                          [this](int64_t wait_ms) {
                              return wait_shutdown_interruptible_(wait_ms);
                          },

                          [this](const std::vector<uint64_t>& ids) {
                              std::lock_guard<std::mutex> lk(signal_state_mu_);
                              signal_dedup_.erase_for_local_ids(ids);
                          },

                          [this] { emit_ready_if_td_readiness_satisfied(); }}),
          local_id_gen_(xt_core::local_id::PerVenueLocalIdGenerator::default_state_path(
              "binance_u")) {}

    /// @brief cold worker 的维护任务描述符。
    struct ColdWorkerTask {
        enum Kind {
            kTimeSync,
            kBootstrapSnapshot,
            kRefreshSnapshot,
            kReconcileResync,
            kPlainCloseFallback,
        };
        Kind kind = kTimeSync;

        std::string fb_symbol;
        bool        fb_is_buy   = false;
        int64_t     fb_price_fp = 0;
        int64_t     fb_qty_fp   = 0;
    };

    void configure(const BinanceTdCfg& cfg) {
        td_cfg = cfg;
        reset_td_readiness();
    }

    bool init(const ITradeGateway::Config& cfg);

    bool start();

    void stop();

    bool query_wallet_balance_sync(double& out_balance);

    xt_live::gateway::TdReadinessReport td_readiness_report() const {
        return readiness_gate_.report();
    }

    uint64_t parse_local_id_from_client_order_id(std::string_view s);

    bool check_health(uint32_t now_ms);

    void bind_callbacks(ITradeGateway::Callbacks cb_in) { cb = std::move(cb_in); }

    uint64_t submit_order_inq(const char* code, bool is_buy,
                              int64_t price_fp, int64_t qty_fp,
                              XtTimeInForce tif, const char* user_tag,
                              XtOffset close_offset, ExecFlags exec_flags,
                              uint64_t intent_trace_id);

    bool cancel_order_inq(uint64_t local_id);

private:

    bool create_trade_transport();

    void check_trade_transport_health(uint32_t now_ms);

    void cold_worker_loop();

    void reset_td_readiness();

    void emit_ready_if_td_readiness_satisfied();

    std::optional<ColdWorkerTask::Kind> keepalive_pick_(int64_t now_ms);

    bool wait_shutdown_interruptible_(int64_t wait_ms) {
        std::unique_lock<std::mutex> lk(shutdown_mu_);
        return shutdown_cv_.wait_for(lk, std::chrono::milliseconds(wait_ms),
            [this] { return !running.load(std::memory_order_acquire); });
    }

public:

    std::deque<ColdWorkerTask> cold_worker_tasks;

private:

    void emit_through_dedup(const XtOrderExecUpdate& upd,
                            EffectiveSignalDedup::Source src);

    void on_uds_order_update(const XtOrderExecUpdate& upd);

    void on_transport_ack(const TransportAck& ack,
                          const TransportLatencyTrace& trace);

    void handle_rejected_ack(const TransportAck& ack, const XtOrderExecUpdate& upd);

public:

    bool queue_reconcile_resync_if_absent_();

    void request_resnap();

    void forward_uds_trade(const XtTradeRecord& tr) { reconciler_.forward_uds_trade(tr); }

    void forward_uds_position(const std::vector<XtPositionSnapshot>& positions) {
        reconciler_.forward_uds_position(positions);
    }

    bool is_recovering() const { return reconciler_.is_recovering(); }

#ifdef XT_BUILD_TESTS
public:
    void test_inject_transport_ack(const TransportAck& ack,
                                   const TransportLatencyTrace& trace) {
        on_transport_ack(ack, trace);
    }

    void test_inject_uds_order(const XtOrderExecUpdate& upd) {
        on_uds_order_update(upd);
    }

    void test_emit_reconcile_ghost(const XtOrderExecUpdate& upd) {

        XtOrderExecUpdate u = upd;
        if (ledger_.on_reconcile_terminal(
                u.id, BinanceOrderLedger::ReconcileTerminal::kVenueTerminal, u).action ==
            BinanceOrderLedger::TerminalDecision::Action::kEmit) {
            reconciler_.emit_reconcile_terminal_update(u);
        }
    }

    bool test_emit_blank_sys_id_reject(uint64_t local_id) {
        return reconciler_.synthesize_blank_sys_id_reject(local_id);
    }

	    std::size_t test_cold_worker_task_count() {
	        std::lock_guard<std::mutex> lk(cold_worker_mu);
	        return cold_worker_tasks.size();
	    }

	    uint64_t test_trace_id_for_local(uint64_t local_id) {
	        return ledger_.trace_id_for(local_id);
	    }

	    void test_forward_uds_trade(const XtTradeRecord& tr) { forward_uds_trade(tr); }

    void test_bootstrap_snapshot() { reconciler_.bootstrap_snapshot(); }

    void test_request_resnap() { request_resnap(); }

    void test_rebuild_from_open_orders_body(const std::string& body, bool is_cold_start) {
        (void)reconciler_.rebuild_from_open_orders_body(body, is_cold_start);
    }

    void* test_resync_gate_open() {
        return new BinanceReconciler::ResyncFillGate(reconciler_);
    }
    void  test_resync_gate_close(void* gate) {
        delete static_cast<BinanceReconciler::ResyncFillGate*>(gate);
    }
    std::optional<ColdWorkerTask::Kind> test_keepalive_pick(int64_t now_ms) {
        return keepalive_pick_(now_ms);
    }

    void test_reset_td_readiness() { reset_td_readiness(); }

    void test_install_uds_prod_wired(const std::string& tracked_symbol) {
        BinanceUserDataStream::Config ucfg;
        uds = std::make_unique<BinanceUserDataStream>(ucfg, *rest);
        XtTradeRecord seed;
        seed.trade_id = "1";
        seed.code = tracked_symbol;
        (void)uds->inject_gap_fill_trade(seed);
        uds->bind_callbacks(
            [this](const XtOrderExecUpdate& upd) { this->on_uds_order_update(upd); },
            [this](const XtTradeRecord& tr) { forward_uds_trade(tr); },
            {}, {},
            [this](std::string_view s) { return parse_local_id_from_client_order_id(s); });
    }

    void test_boot_cold_worker_with_uds(
        const std::string& tracked_symbol,
        std::function<void(const XtTradeRecord&)> on_gap_fill_trade) {
        reset_td_readiness();
        BinanceUserDataStream::Config ucfg;
        uds = std::make_unique<BinanceUserDataStream>(ucfg, *rest);
        XtTradeRecord seed;
        seed.trade_id = "1";
        seed.code = tracked_symbol;
        (void)uds->inject_gap_fill_trade(seed);
        uds->bind_callbacks({}, std::move(on_gap_fill_trade), {}, {}, {});
        running.store(true, std::memory_order_release);
        cold_worker_thread = std::thread([this] { cold_worker_loop(); });
    }

#endif

private:

    EffectiveSignalDedup signal_dedup_;
    mutable std::mutex signal_state_mu_;

public:

    BinanceTdCfg td_cfg;
    ITradeGateway::Callbacks cb;
    xt_live::gateway::TdReadinessGate readiness_gate_{"binance_u"};
    std::atomic<bool> ready_emitted_{false};

    std::unique_ptr<BinanceRestClient> rest;
    std::unique_ptr<ITradeTransport> td_transport;
    std::unique_ptr<BinanceUserDataStream> uds;

    std::mutex uds_mu_;
    std::thread cold_worker_thread;
    std::thread keepalive_thread;
    std::atomic<bool> running{false};

    int      last_reject_err_code_logged_ = 0;
    uint64_t last_reject_log_us_          = 0;
    uint64_t reject_suppressed_count_     = 0;
    uint64_t last_reject_resync_us_       = 0;
    uint64_t last_unattributed_resync_us_ = 0;
    uint64_t last_reject_fallback_us_     = 0;
    uint64_t last_cancel_fail_log_us_     = 0;
    uint64_t cancel_fail_suppressed_count_ = 0;
    static constexpr uint64_t kLatTraceLogSampleN = 100;
    uint64_t lat_trace_log_seq_ = 0;
    uint64_t lat_t0_log_seq_ = 0;

    std::mutex cold_worker_mu;
    std::condition_variable cold_worker_cv;

    std::mutex shutdown_mu_;
    std::condition_variable shutdown_cv_;

    BinanceOrderLedger ledger_;

    TrackedSymbolSet tracked_symbols_;

    BinanceReconciler reconciler_;

    xt_core::local_id::PerVenueLocalIdGenerator local_id_gen_;
    std::atomic<uint64_t> local_id_persist_fail_seen_{0};
    std::atomic<uint64_t> nonce_ctr{0};
    std::atomic<uint32_t> last_time_sync_ms{0};
    std::atomic<uint32_t> transport_down_since_ms{0};
    std::atomic<uint32_t> transport_down_warn_ms{0};

};

}
