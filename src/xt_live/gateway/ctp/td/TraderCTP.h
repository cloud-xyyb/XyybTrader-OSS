#pragma once
#include <functional>
#include <string>
#include <atomic>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <array>
#include <vector>
#include <utility>

#include "ThostFtdcTraderApi.h"

#include "api/ITradeGateway.h"
#include "ctp/common/CtpFrontDisconnectLogGate.h"
#include "api/AliveInflightOrder.h"
#include "xt_core/order/XtOrderTypes.h"
#include "ctp/recovery/CTPStateTracker.h"
#include "ctp/recovery/CTPRecovery.h"
#include "ctp/td/CTPTdCfg.h"
#include "ctp/td/CtpOrderRefStore.h"
#include "xt_live/gateway/common/TdReadinessGate.h"
#include "xt_live/gateway/common/DeferredTimerQueue.h"
#include "common/XtLiveRecoveryError.h"

/// @brief CTP 交易通道的核心实现：登录、恢复、下单撤单、回报归一化。
///
/// TraderCTP 实现 CTP Trader SPI，是本进程与 CTP 柜台之间的全部交易交互所在：
/// - 连接登录后先做结算确认和恢复查询（账户/持仓/在途单），凑齐才放行交易
/// - 把内部下单/撤单请求翻译成 CTP 报文发出，把 CTP 回报翻译回统一 DTO
/// - 订单号分配 crash-safe 持久化，重启不复用旧单号
///
/// @warning CTP SDK 回调线程同步进入 TraderCTP；绑定的 callbacks 必须只做
/// 轻量入队，运行中不应重新绑定。
class TraderCTP final : public CThostFtdcTraderSpi {
public:

    using Config = ITradeGateway::Config;

    TraderCTP();

    ~TraderCTP();

    void configure_ctp(const CTPTdCfg& ctp_cfg);

    bool init(const Config& cfg);

    bool start();

    void stop();

    bool check_health(uint32_t now_ms);

    using Callbacks             = ITradeGateway::Callbacks;
    using DailySettlementDelta  = ITradeGateway::DailySettlementDelta;
    using CashTransferDelta     = ITradeGateway::CashTransferDelta;
    using AccountSnapshot       = ITradeGateway::AccountSnapshot;
    using PositionSnapshot      = ITradeGateway::PositionSnapshot;

    enum class QueryKind {
        Position,
        AccountState,
        XtOrder,
        Trade
    };

    struct QueryControlStats {
        uint64_t query_pos_throttled     = 0;
        uint64_t query_pos_inflight_skip = 0;
        uint64_t query_pos_dirty_merged  = 0;
    };

    void bind_callbacks(Callbacks callbacks) { callbacks_ = std::move(callbacks); }

    void rebuild_alive_inflight_and_emit(bool is_cold_start);

    uint64_t submit_order(const char* code,
                          bool is_buy,
                          int64_t price_fp,
                          int64_t qty_fp,
                          XtTimeInForce tif,
                          const char* user_tag,
                          XtOffset close_offset);

    bool cancel_order(uint64_t local_id);

    bool is_local_terminal(uint64_t local_id) const;

    bool lookup_terminal(uint64_t local_id, XtOrderTerminalRecord& out) const;

    void set_recovery_enabled(bool enabled) { recovery_.set_recovery_enabled(enabled); }

    bool is_recovering() const { return recovery_.is_recovering(); }

    uint32_t trading_day() const { return current_trading_day_.load(std::memory_order_acquire); }

    bool cancel_target_snapshot(uint64_t local_id, CtpStateTracker::CancelTarget& out) const {
        return state_.get_cancel_target(local_id, out);
    }

    xt_live::gateway::TdReadinessReport td_readiness_report() const {
        return readiness_gate_.report();
    }

    QueryControlStats query_control_stats() const;

    bool is_recovery_wedged() const { return recovery_wedged_.load(std::memory_order_acquire); }

    uint64_t recovery_rearm_scheduled_total() const {
        return recovery_rearm_scheduled_total_.load(std::memory_order_acquire);
    }

    uint64_t refresh_query_timeout_total() const {
        return refresh_query_timeout_total_.load(std::memory_order_acquire);
    }

    bool is_refresh_query_wedged() const {
        if (refresh_query_wedge_rounds_ <= 0) return false;
        for (const auto& q : qry_) {
            if (q.refresh_timeout_count.load(std::memory_order_acquire) >= refresh_query_wedge_rounds_) {
                return true;
            }
        }
        return false;
    }

    uint64_t stale_query_row_dropped_total() const {
        return stale_query_row_dropped_.load(std::memory_order_acquire);
    }

    uint64_t stale_query_row_unfenceable_total() const {
        return stale_query_row_unfenceable_.load(std::memory_order_acquire);
    }

    uint64_t query_session_gen() const {
        return query_session_gen_.load(std::memory_order_acquire);
    }

    void set_connected_for_test(bool v) { connected_.store(v, std::memory_order_release); }

    void set_active_request_id_for_test(QueryKind kind, int request_id) {
        qry_[qry_idx(kind)].active_request_id.store(request_id, std::memory_order_release);
    }

    bool identity_mismatch_latched() const {
        return identity_mismatch_latched_.load(std::memory_order_acquire);
    }

    void set_expected_investor_id_for_test(std::string v) { expected_investor_id_ = std::move(v); }

    void set_process_start_epoch_us_for_test(int64_t us) { process_start_epoch_us_ = us; }

    void set_recovery_anchor_epoch_us_for_test(int64_t us) {
        recovery_position_anchor_us_.store(us, std::memory_order_release);
    }

    const xt_live::gateway::XtExecDeliveryLedger& exec_ledger() const {
        return state_.exec_ledger();
    }

    uint64_t watermark_breach_total() const { return state_.watermark_breach_total(); }

    uint64_t synth_cum_filled_total() const { return state_.synth_cum_filled_total(); }

    uint64_t cum_filled_conflict_total() const { return state_.cum_filled_conflict_total(); }

    void OnFrontConnected() override;

    void OnFrontDisconnected(int nReason) override;

    void OnRspUserLogin(CThostFtdcRspUserLoginField* pRspUserLogin,
                        CThostFtdcRspInfoField* pRspInfo,
                        int nRequestID,
                        bool bIsLast) override;

    void OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField* pConfirm,
                                    CThostFtdcRspInfoField* pRspInfo,
                                    int nRequestID,
                                    bool bIsLast) override;

    void OnRspError(CThostFtdcRspInfoField* pRspInfo,
                    int nRequestID,
                    bool bIsLast) override;

    void OnRspQryOrder(CThostFtdcOrderField* pOrder,
                       CThostFtdcRspInfoField* pRspInfo,
                       int nRequestID,
                       bool bIsLast) override;

    void OnRspQryTrade(CThostFtdcTradeField* pTrade,
                       CThostFtdcRspInfoField* pRspInfo,
                       int nRequestID,
                       bool bIsLast) override;

    void OnRspQryInvestorPosition(CThostFtdcInvestorPositionField* pPosition,
                                  CThostFtdcRspInfoField* pRspInfo,
                                  int nRequestID,
                                  bool bIsLast) override;

    void OnRspQryTradingAccount(CThostFtdcTradingAccountField* pAccount,
                                CThostFtdcRspInfoField* pRspInfo,
                                int nRequestID,
                                bool bIsLast) override;

    void OnRspOrderInsert(CThostFtdcInputOrderField* pInputOrder,
                          CThostFtdcRspInfoField* pRspInfo,
                          int nRequestID,
                          bool bIsLast) override;

    void OnRspOrderAction(CThostFtdcInputOrderActionField* pInputOrderAction,
                          CThostFtdcRspInfoField* pRspInfo,
                          int nRequestID,
                          bool bIsLast) override;

    void OnErrRtnOrderInsert(CThostFtdcInputOrderField* pInputOrder,
                             CThostFtdcRspInfoField* pRspInfo) override;

    void OnErrRtnOrderAction(CThostFtdcOrderActionField* pOrderAction,
                             CThostFtdcRspInfoField* pRspInfo) override;

    void OnRtnOrder(CThostFtdcOrderField* pOrder) override;

    void OnRtnTrade(CThostFtdcTradeField* pTrade) override;

    void OnRtnFromBankToFutureByFuture(CThostFtdcRspTransferField* pRspTransfer) override;

    void OnRtnFromFutureToBankByFuture(CThostFtdcRspTransferField* pRspTransfer) override;

private:

    enum class SessionPhase {
        Stopped,
        FrontConnected,
        LoginOk,
        SettlementPending,
        Recovering,
        Ready,
        FrontDisconnected
    };

    static constexpr int kQueryRequestIdInvalidated = -1;
    struct QueryCtrl {
        std::atomic<bool>     inflight{false};
        std::atomic<int>      active_request_id{0};
        std::atomic<bool>     dirty{false};
        std::atomic<bool>     retry_pending{false};
        std::atomic<int64_t>  next_allowed_ms{0};
        std::atomic<int>      backoff_level{0};
        std::atomic<uint64_t> timeout_token{0};
        std::atomic<int>      timeout_count{0};
        std::atomic<uint64_t> refresh_timeout_token{0};
        std::atomic<int>      refresh_timeout_count{0};
        std::atomic<uint64_t> throttled_count{0};
        std::atomic<uint64_t> inflight_skip_count{0};
        std::atomic<uint64_t> dirty_merge_count{0};

        QueryCtrl() = default;
        QueryCtrl(const QueryCtrl&) = delete;
        QueryCtrl& operator=(const QueryCtrl&) = delete;
    };

    /// @brief QueryKind 的日志和诊断标签。
    ///
    /// 仅承载稳定的可读名与 CTP SDK Req* 函数名，供查询发送、retry 与
    /// recovery timeout 路径复用；不承载 readiness、timeout 或 retry policy。
    struct QueryMeta {
        const char* name     = "unknown";
        const char* req_name = "ReqUnknown";
    };

    int next_req_id() { return req_id_.fetch_add(1, std::memory_order_relaxed) + 1; }

    bool login();

    void handle_login_failed(CThostFtdcRspInfoField* pRspInfo);

    void schedule_login_retry();

    bool verify_login_identity_(const CThostFtdcRspUserLoginField* login);

    void set_session_phase(SessionPhase phase);

    bool is_session_alive() const noexcept;

    void reset_session_state();

    void reset_td_readiness();

    void emit_ready_if_td_readiness_satisfied();

    void enter_recovery_pipeline(bool settlement_confirmed);

    void maybe_schedule_recovery_rearm_(QueryKind kind);

    void reset_query_ctrl_for_rearm_(QueryKind kind);

    void set_recovery_wedged_(bool wedged);

    void maybe_log_stale_position_frame_(int request_id);

    bool query_row_owned_(QueryKind kind, int request_id);

    void maybe_log_stale_query_row_(QueryKind kind, int request_id);

    bool confirm_settlement();

    bool request_settlement_confirm_with_timeout();

    void schedule_settlement_confirm_retry(XtLiveRecoveryError reason);

    void arm_settlement_timeout();

    static constexpr size_t qry_idx(QueryKind kind) noexcept { return static_cast<size_t>(kind); }

    QueryMeta query_meta(QueryKind kind);

    bool can_run_refresh_query() const;

    void query_once(QueryKind kind);

    void handle_query_throttled(QueryKind kind, QueryCtrl& q, const QueryMeta& meta, int64_t now_ms);

    int request_query(QueryKind kind);

    void on_query_response_done(QueryKind kind, bool success);

    void schedule_query_retry(QueryKind kind);

    bool process_query_retry(QueryKind kind);

    void run_query_retry_worker();

    void arm_recovery_query_timeout(QueryKind kind);

    void arm_refresh_query_timeout(QueryKind kind);

    void clear_recovery_query_timeout(QueryKind kind);

    void clear_recovery_query_timeout_states();

    void clear_query_retry_states();

    void reset_query_control_states();

    void refresh_once_after_ready();

    void start_refresh_loop();

    static CtpRecovery::HostPorts make_recovery_host_ports(TraderCTP* self);

    std::vector<PositionSnapshot> convert_position_snapshot(
        const std::vector<CtpRecovery::PositionSnapshot>& in) const;

    void on_recovery_ready();

    void emit_recovery_finish_sequence_(bool is_cold_start);

    void stage_recovery_positions_(std::vector<PositionSnapshot> out);

    void emit_staged_recovery_positions_(bool is_cold_start);

    void reopen_recovery_window_gate_and_drain_();

    void flush_recovery_pending_terminals_();

    void on_rtn_trade_(CThostFtdcTradeField* pTrade, bool from_query);

    void maybe_emit_cum_filled_completion_(uint64_t local_id, uint64_t local_cb_us);

    bool classify_query_fill_absorbed_(const CThostFtdcTradeField* pTrade) const;

    static int64_t wall_now_epoch_us_();

    bool is_error(CThostFtdcRspInfoField* pRspInfo);

    bool should_log_foreign_cancel_error_once(const char* order_ref, int error_id);

    void maybe_emit_daily_settlement_event(const CThostFtdcTradingAccountField* pAccount);

    void emit_cash_transfer_event(const CThostFtdcRspTransferField* pRspTransfer, bool bank_to_future);

    void schedule_after_ms(int ms, std::function<void()> action);

    CThostFtdcTraderApi* api_ = nullptr;
    mutable std::shared_mutex api_mu_;
    std::string front_;
    std::string broker_;
    std::string user_;
    std::string pass_;
    std::string expected_investor_id_;
    std::string flow_dir_;
    std::atomic<int>  front_id_{0};
    std::atomic<int>  session_id_{0};
    std::atomic<int>  req_id_{0};
    CtpOrderRefStore order_ref_store_;
    bool reconnect_enabled_         = true;
    int  reconnect_interval_ms_     = 2000;
    uint32_t                       stale_reconnect_ms_ = 0;
    std::atomic<uint32_t>          last_front_active_ms_{0};
    CtpFrontDisconnectLogGate      disc_log_gate_;
    bool confirm_settlement_        = true;
    bool settlement_confirmed_      = false;
    int  settlement_confirm_timeout_ms_ = 8000;
    int  settlement_confirm_retry_ms_   = 2000;
    int  login_retry_ms_                = 5000;
    int  recovery_query_timeout_ms_     = 8000;
    int  recovery_query_max_timeouts_   = 6;
    int  recovery_rearm_backoff_ms_     = 5000;
    int  recovery_rearm_alert_rounds_   = 10;
    int  refresh_query_timeout_ms_      = 30000;
    int  refresh_query_wedge_rounds_    = 6;

    std::mutex recovery_rearm_mu_;
    int  recovery_rearm_round_    = 0;
    bool recovery_rearm_pending_  = false;
    std::atomic<uint64_t> recovery_rearm_token_{0};
    std::atomic<uint64_t> recovery_rearm_scheduled_total_{0};
    std::atomic<bool>     recovery_wedged_{false};

    alignas(64) std::atomic<bool>         stopping_{false};
    std::atomic<SessionPhase>             session_phase_{SessionPhase::Stopped};
    std::atomic<bool>                     connected_{false};

    std::atomic<bool>                     identity_mismatch_latched_{false};
    std::atomic<bool>                     refresh_once_running_{false};
    std::atomic<bool>                     refresh_running_{false};

    alignas(64) std::atomic<bool>         settlement_confirm_pending_{false};
    std::atomic<bool>                     settlement_confirm_retry_pending_{false};
    std::atomic<bool>                     login_retry_pending_{false};

    std::atomic<int>                      consecutive_login_failures_{0};
    std::atomic<int>                      last_login_error_id_{0};
    std::atomic<uint64_t>                 settlement_confirm_token_{0};
    std::atomic<bool>                     settlement_event_pending_{false};
    std::atomic<uint32_t>                 current_trading_day_{0};
    std::atomic<uint32_t>                 last_settlement_event_day_{0};

    std::array<QueryCtrl, 4> qry_{};
    std::atomic<bool> qry_retry_worker_running_{false};

    std::atomic<int64_t> next_global_query_allowed_ms_{0};
    std::atomic<uint64_t> refresh_query_timeout_total_{0};

    bool query_generation_fence_ = true;
    std::atomic<uint64_t> query_session_gen_{0};
    std::atomic<uint64_t> stale_query_row_dropped_{0};
    std::atomic<uint64_t> stale_query_row_unfenceable_{0};
    std::atomic<int64_t>  last_stale_query_row_log_ms_{0};

    int account_refresh_ms_  = 0;
    int position_refresh_ms_ = 0;
    double account_snapshot_max_abs_ = 1e12;
    mutable std::mutex             foreign_cancel_warn_mu_;
    std::unordered_set<std::string> foreign_cancel_warned_keys_;
    std::deque<std::string>         foreign_cancel_warn_fifo_;

    xt_live::gateway::DeferredTimerQueue timer_queue_;
    std::mutex                           wait_mu_;
    std::condition_variable              wait_cv_;

    std::thread qry_retry_thread_;
    std::thread refresh_once_thread_;
    std::thread refresh_thread_;

    CtpStateTracker state_;
    CtpRecovery     recovery_;
    xt_live::gateway::TdReadinessGate readiness_gate_{"ctp"};

    Callbacks callbacks_;

    mutable std::mutex                recovery_alive_mu_;
    std::vector<AliveInflightOrder>   recovery_alive_orders_;
    /// recovery 查询行的撤单路由字段（AliveInflightOrder 不携带 OrderSysID/
    /// FrontID/SessionID），rebuild 注册后回填 tracker —— 保证跨重启回灌单
    /// 可撤。
    struct RecoveredCancelRouting {
        std::string exchange_id;
        std::string order_sys_id;
        int front_id = 0;
        int session_id = 0;
    };
    std::unordered_map<uint64_t, RecoveredCancelRouting> recovery_alive_routing_;

    uint64_t                          recovery_skipped_foreign_ = 0;

    mutable std::mutex recovery_term_mu_;
    /// 缓冲终态 + 其构造时 latency eligibility（per-entry 透传，flush 时携带）：
    /// OnRtnOrder 真实 push buffered 保留原 cb_us = kEligible / cum-filled completion
    /// 派生 = kIneligible（latency-terminal 批 L-1.2 CTP）。
    struct RecoveryPendingTerminal {
        XtOrderExecUpdate    upd;
        XtLatencyEligibility elig = XtLatencyEligibility::kIneligible;
    };
    std::unordered_map<uint64_t, RecoveryPendingTerminal> recovery_pending_terminals_;

    mutable std::mutex recovery_window_mu_;
    std::vector<XtTradeRecord> recovery_window_trades_;
    bool     recovery_window_gate_closed_ = false;
    uint64_t recovery_window_overflow_    = 0;

    std::atomic<int64_t> last_stale_position_log_ms_{0};
    static constexpr size_t kRecoveryWindowTradeCap = 4096;

    static constexpr int64_t kFillSealMarginUs = 2'000'000;

    std::atomic<int64_t> recovery_position_anchor_us_{0};

    int64_t process_start_epoch_us_ = 0;

    mutable std::mutex recovery_staged_pos_mu_;
    std::vector<PositionSnapshot> recovery_staged_positions_;
    bool recovery_staged_pos_valid_ = false;

    bool exec_ledger_query_fill_delivery_ = true;

    bool                              is_first_recovery_ = true;
};
