// TraderCTP 主实现分片：recovery host wiring、生命周期、self-heal rebuild 与核心 SPI 查询回包。
// login/settlement 的请求调度在 TraderCTPSession.cpp；submit/cancel 与订单成交回报在
// TraderCTPOrderOps.cpp；query 发送和 retry 控制在 TraderCTPQuery.cpp。
// 本文件维护 session phase、TD readiness gate、account/position snapshot 归一化和
// check_health() 主动重建路径；所有 api_ Release/rebuild 必须受 api_mu_ unique 保护。

#include "xt_identity/OrderAttributionStore.h"
#include "ctp/td/TraderCTP.h"
#include "ctp/td/CtpLoginBackoff.h"

#include "ctp/common/CTPUtil.h"
#include "xt_util/path/PathUtil.h"
#include "recording/XtLiveRecorder.h"
#include "XtLogger.h"
#include "api/XtLiveRejectReason.h"
#include "common/XtLiveRecoveryError.h"
#include "xt_util/time/XtSteadyClock.h"
#include "XtFixedPoint.h"
#include "xt_util/runtime/ThreadUtil.h"
#include "xt_util/string/XtStringUtil.h"
#include "xt_util/time/XtTimeParse.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <sys/types.h>
#include <thread>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>

namespace {

constexpr double kDefaultAccountSnapshotMaxAbs = 1e12;

static const char* rc(XtLiveRejectReason code)  { return live_reject_reason_str(code); }
static const char* rc(XtLiveRecoveryError code) { return live_recovery_error_str(code); }

inline bool account_snapshot_amount_ok(double v, double max_abs) {
    return std::isfinite(v) && std::fabs(v) <= max_abs;
}

}

CtpRecovery::HostPorts TraderCTP::make_recovery_host_ports(TraderCTP* self) {
    CtpRecovery::HostPorts ports;
    ports.publish_positions = [self](const std::vector<CtpRecovery::PositionSnapshot>& in) {
        auto out = self->convert_position_snapshot(in);

        self->readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::Position,
                                      "ctp position query completed");
        if (self->recovery_.is_recovering()) {

            self->stage_recovery_positions_(std::move(out));
            return;
        }

        if (self->callbacks_.on_position) self->callbacks_.on_position(out);
    };

    ports.start_position_query = [self]() { self->query_once(QueryKind::Position); };
    ports.start_account_query  = [self]() { self->query_once(QueryKind::AccountState); };
    ports.start_order_query    = [self]() { self->query_once(QueryKind::XtOrder); };
    ports.start_trade_query    = [self]() { self->query_once(QueryKind::Trade); };
    ports.publish_ready        = [self]() { self->on_recovery_ready(); };
    return ports;
}

TraderCTP::TraderCTP() : recovery_(make_recovery_host_ports(this)) {

    process_start_epoch_us_ = wall_now_epoch_us_();
}

int64_t TraderCTP::wall_now_epoch_us_() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

TraderCTP::~TraderCTP() {
    stop();
}

std::vector<TraderCTP::PositionSnapshot> TraderCTP::convert_position_snapshot(
    const std::vector<CtpRecovery::PositionSnapshot>& in) const {
    std::vector<PositionSnapshot> out;
    out.reserve(in.size());

    const auto now_ms = static_cast<uint64_t>(XtSteadyClock::steady_now_ms());
    for (const auto& p : in) {
        PositionSnapshot s;
        s.code             = p.code;
        s.long_qty         = p.long_qty;
        s.long_today       = p.long_today;
        s.long_avg         = p.long_avg;
        s.short_qty        = p.short_qty;
        s.short_today      = p.short_today;
        s.short_avg        = p.short_avg;

        s.long_frozen      = p.long_frozen;
        s.short_frozen     = p.short_frozen;
        s.broker_frozen_last_update_ms = now_ms;
        out.push_back(s);
    }
    return out;
}

void TraderCTP::on_recovery_ready() {
    set_session_phase(SessionPhase::Ready);
    refresh_once_after_ready();
    start_refresh_loop();

    const bool cold = is_first_recovery_;
    is_first_recovery_ = false;
    emit_recovery_finish_sequence_(cold);
    emit_ready_if_td_readiness_satisfied();
}

void TraderCTP::configure_ctp(const CTPTdCfg& ctp_cfg) {
    broker_   = ctp_cfg.broker;
    flow_dir_ = ctp_cfg.flow_dir;
    confirm_settlement_ = ctp_cfg.confirm_settlement;
    settlement_confirm_timeout_ms_ = ctp_cfg.settlement_confirm_timeout_ms;
    if (settlement_confirm_timeout_ms_ < 0) settlement_confirm_timeout_ms_ = 0;
    recovery_query_timeout_ms_ = ctp_cfg.recovery_query_timeout_ms;
    if (recovery_query_timeout_ms_ < 0) recovery_query_timeout_ms_ = 0;
    recovery_query_max_timeouts_ = ctp_cfg.recovery_query_max_timeouts;
    if (recovery_query_max_timeouts_ <= 0) recovery_query_max_timeouts_ = 1;

    recovery_rearm_backoff_ms_ = ctp_cfg.recovery_rearm_backoff_ms;
    recovery_rearm_alert_rounds_ = ctp_cfg.recovery_rearm_alert_rounds;
    if (recovery_rearm_alert_rounds_ < 0) recovery_rearm_alert_rounds_ = 0;

    refresh_query_timeout_ms_ = ctp_cfg.refresh_query_timeout_ms;
    if (refresh_query_timeout_ms_ < 0) refresh_query_timeout_ms_ = 0;
    refresh_query_wedge_rounds_ = ctp_cfg.refresh_query_wedge_rounds;
    if (refresh_query_wedge_rounds_ < 0) refresh_query_wedge_rounds_ = 0;

    query_generation_fence_ = ctp_cfg.query_generation_fence;

    exec_ledger_query_fill_delivery_ = ctp_cfg.exec_ledger_query_fill_delivery;
}

bool TraderCTP::init(const Config& cfg) {

    front_ = cfg.front;
    user_  = cfg.user;
    pass_  = cfg.password;
    expected_investor_id_ = cfg.expected_investor_id;
    reconnect_enabled_         = cfg.reconnect_enabled;
    reconnect_interval_ms_     = cfg.reconnect_interval_ms;

    if (cfg.stale_reconnect_ms > 0 && cfg.stale_reconnect_ms < 5000) {
        XtLogger::warn_m("TD", "stale_reconnect_ms={} < 5000 下限, clamp 到 5000",
                         cfg.stale_reconnect_ms);
        stale_reconnect_ms_ = 5000;
    } else {
        stale_reconnect_ms_ = cfg.stale_reconnect_ms;
    }
    last_front_active_ms_.store(0, std::memory_order_release);
    settlement_confirm_retry_ms_ = std::max(1000, reconnect_interval_ms_);
    account_refresh_ms_  = cfg.account_refresh_ms;
    position_refresh_ms_ = cfg.position_refresh_ms;

    if (position_refresh_ms_ <= 0) {
        XtLogger::error_m("TD",
                          "init rejected: position_refresh_ms must be > 0 (got {}); "
                          "periodic position reconcile is the recovery mechanism for any "
                          "missed/dropped fill (network/SDK/restart)",
                          position_refresh_ms_);
        return false;
    }
    account_snapshot_max_abs_ =
        (std::isfinite(cfg.account_snapshot_max_abs) && cfg.account_snapshot_max_abs > 0.0)
            ? cfg.account_snapshot_max_abs
            : kDefaultAccountSnapshotMaxAbs;

    if (front_.empty() || broker_.empty() || user_.empty()) {
        XtLogger::error_m("TD", "missing td_front/broker/user in config");
        return false;
    }
    std::string flow_dir_err;
    if (!PathUtil::ensure_writable_directory(flow_dir_, &flow_dir_err, ".xt_flow_probe")) {
        XtLogger::error_m("TD", "flow_dir not usable: {} ({})", flow_dir_, flow_dir_err);
        return false;
    }
    if (!order_ref_store_.init(flow_dir_)) return false;

    api_ = CThostFtdcTraderApi::CreateFtdcTraderApi(flow_dir_.c_str());
    if (!api_) {
        XtLogger::error_m("TD", "CreateFtdcTraderApi failed");
        return false;
    }
    api_->RegisterSpi(this);

    recovery_.set_recovery_enabled(cfg.recovery_enabled);
    return true;
}

bool TraderCTP::start() {
    if (!api_) return false;

    api_->SubscribePrivateTopic(THOST_TERT_QUICK);
    api_->SubscribePublicTopic(THOST_TERT_QUICK);
    api_->RegisterFront(const_cast<char*>(front_.c_str()));
    api_->Init();
    return true;
}

void TraderCTP::stop() {
    stopping_.store(true, std::memory_order_release);
    connected_.store(false, std::memory_order_release);
    qry_retry_worker_running_.store(false, std::memory_order_release);
    refresh_once_running_.store(false, std::memory_order_release);
    refresh_running_.store(false, std::memory_order_release);

    wait_cv_.notify_all();

    timer_queue_.stop();

    clear_query_retry_states();
    reset_query_control_states();
    settlement_confirm_pending_.store(false, std::memory_order_release);
    settlement_confirm_retry_pending_.store(false, std::memory_order_release);
    login_retry_pending_.store(false, std::memory_order_release);

    settlement_confirm_token_.fetch_add(1, std::memory_order_acq_rel);
    settlement_event_pending_.store(false, std::memory_order_release);
    current_trading_day_.store(0, std::memory_order_release);
    clear_recovery_query_timeout_states();

    ThreadUtil::join_or_detach(qry_retry_thread_);
    ThreadUtil::join_or_detach(refresh_once_thread_);
    ThreadUtil::join_or_detach(refresh_thread_);

    set_session_phase(SessionPhase::Stopped);

    std::unique_lock<std::shared_mutex> api_lk(api_mu_);
    if (api_) {
        api_->Release();
        api_ = nullptr;
    }
}

bool TraderCTP::check_health(uint32_t now_ms) {

    if (stale_reconnect_ms_ == 0) return true;
    if (stopping_.load(std::memory_order_acquire)) return true;
    if (connected_.load(std::memory_order_acquire)) {

        last_front_active_ms_.store(now_ms, std::memory_order_relaxed);
        return true;
    }
    const uint32_t last = last_front_active_ms_.load(std::memory_order_acquire);

    if (last == 0) return true;
    const uint32_t elapsed = now_ms - last;

    if (elapsed < stale_reconnect_ms_) return true;

    XtLogger::warn_m("TD", "stale disconnect ({}ms > {}ms), forcing self-rebuild",
                     elapsed, stale_reconnect_ms_);
    if (auto* rec = XtLiveRecorder::global()) {
        char reason[48];
        std::snprintf(reason, sizeof(reason), "elapsed_ms=%u", elapsed);
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "stale_rebuild", reason);
    }

    if (callbacks_.on_front) callbacks_.on_front(false, -99);

    std::unique_lock<std::shared_mutex> api_lk(api_mu_);

    if (api_) {
        api_->Release();
        api_ = nullptr;
    }

    reset_session_state();
    set_session_phase(SessionPhase::Stopped);
    connected_.store(false, std::memory_order_release);

    std::string flow_dir_err;
    if (!PathUtil::ensure_writable_directory(flow_dir_, &flow_dir_err, ".xt_flow_probe")) {
        XtLogger::error_m("TD",
                          "stale rebuild: flow_dir not usable: {} ({}), next tick retry",
                          flow_dir_,
                          flow_dir_err);
        return false;
    }

    api_ = CThostFtdcTraderApi::CreateFtdcTraderApi(flow_dir_.c_str());
    if (!api_) {
        XtLogger::error_m("TD", "stale rebuild: CreateFtdcTraderApi failed");

        return false;
    }
    api_->RegisterSpi(this);
    api_->SubscribePrivateTopic(THOST_TERT_QUICK);
    api_->SubscribePublicTopic(THOST_TERT_QUICK);
    api_->RegisterFront(const_cast<char*>(front_.c_str()));
    api_->Init();

    last_front_active_ms_.store(now_ms, std::memory_order_release);
    return false;
}

bool TraderCTP::is_session_alive() const noexcept {
    return !stopping_.load(std::memory_order_acquire) &&
            connected_.load(std::memory_order_acquire) &&
            api_ != nullptr;
}

void TraderCTP::reset_session_state() {
    front_id_.store(0, std::memory_order_release);
    session_id_.store(0, std::memory_order_release);
    recovery_.reset();
    settlement_confirmed_ = false;
    settlement_confirm_pending_.store(false, std::memory_order_release);
    settlement_confirm_retry_pending_.store(false, std::memory_order_release);
    settlement_confirm_token_.fetch_add(1, std::memory_order_acq_rel);
    settlement_event_pending_.store(false, std::memory_order_release);
    clear_recovery_query_timeout_states();
    reset_query_control_states();
    reset_td_readiness();

    query_session_gen_.fetch_add(1, std::memory_order_acq_rel);

    {
        std::lock_guard<std::mutex> lk(recovery_rearm_mu_);
        recovery_rearm_round_ = 0;
        recovery_rearm_pending_ = false;
    }
    recovery_rearm_token_.fetch_add(1, std::memory_order_acq_rel);
    set_recovery_wedged_(false);

    {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        recovery_window_trades_.clear();
        recovery_window_gate_closed_ = false;
        recovery_window_overflow_ = 0;
    }

    {
        std::lock_guard<std::mutex> lk(recovery_staged_pos_mu_);
        recovery_staged_positions_.clear();
        recovery_staged_pos_valid_ = false;
    }
    recovery_position_anchor_us_.store(0, std::memory_order_release);
}

void TraderCTP::reset_td_readiness() {
    readiness_gate_.reset("ctp");
    if (!confirm_settlement_) {
        readiness_gate_.set_not_required(xt_live::gateway::TdReadinessStage::Settlement,
                                         "ctp settlement confirm disabled by config");
    }
}

void TraderCTP::emit_ready_if_td_readiness_satisfied() {
    const auto report = readiness_gate_.report();
    if (!report.ready) {
        XtLogger::error_m("TD",
                          "td readiness blocked venue={} stage={} reason={}",
                          report.venue, report.blocking_stage, report.blocking_reason);
        return;
    }

    {
        std::lock_guard<std::mutex> lk(recovery_rearm_mu_);
        recovery_rearm_round_ = 0;
    }
    set_recovery_wedged_(false);
    if (callbacks_.on_ready) callbacks_.on_ready();
}

void TraderCTP::schedule_after_ms(int ms, std::function<void()> action) {

    timer_queue_.schedule(ms, std::move(action));
}

bool TraderCTP::is_error(CThostFtdcRspInfoField* pRspInfo) {
    if (pRspInfo && pRspInfo->ErrorID != 0) {
        XtLogger::error_m("TD", "error {} {}",
                          pRspInfo->ErrorID,
                          XtStringUtil::gbk_to_utf8_or_original(pRspInfo->ErrorMsg));
        return true;
    }
    return false;
}

void TraderCTP::OnFrontConnected() {

    const uint32_t down_callbacks = disc_log_gate_.on_reconnect();
    connected_.store(true, std::memory_order_release);
    current_trading_day_.store(0, std::memory_order_release);

    last_front_active_ms_.store(
        static_cast<uint32_t>(XtSteadyClock::steady_now_ms()),
        std::memory_order_release);
    reset_session_state();
    set_session_phase(SessionPhase::FrontConnected);
    XtLogger::info_m("TD", "front connected");
    if (down_callbacks > 1) {
        XtLogger::info_m("TD", "front 重连成功 (断开期间 {} 次 disconnect 回调)", down_callbacks);
    }

    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "front_connected", "");
    }
    if (callbacks_.on_front) callbacks_.on_front(true, 0);
    login();
}

void TraderCTP::OnFrontDisconnected(int nReason) {

    switch (disc_log_gate_.on_disconnect(static_cast<uint32_t>(XtSteadyClock::steady_now_ms()))) {
    case CtpFrontDisconnectLogGate::Action::LogError:
        XtLogger::error_m("TD", "event=td_front_lost reason_code={} reason={}",
                          rc(XtLiveRejectReason::TdFrontDisconnected), nReason);
        break;
    case CtpFrontDisconnectLogGate::Action::LogWarn:
        XtLogger::warn_m("TD", "event=td_front_lost reason_code={} reason={} (仍未连上, 重复已抑制)",
                         rc(XtLiveRejectReason::TdFrontDisconnected), nReason);
        break;
    case CtpFrontDisconnectLogGate::Action::Suppress:
        break;
    }
    connected_.store(false, std::memory_order_release);
    reset_session_state();
    clear_query_retry_states();
    set_session_phase(SessionPhase::FrontDisconnected);

    if (auto* rec = XtLiveRecorder::global()) {
        char reason[32];
        std::snprintf(reason, sizeof(reason), "reason=%d", nReason);
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "disconnected", reason);
    }

#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().inc_gateway_disconnect_total(
        "ctp", "td", "front_disconnect");
#endif
    if (callbacks_.on_front) callbacks_.on_front(false, nReason);
}

void TraderCTP::handle_login_failed(CThostFtdcRspInfoField* pRspInfo) {
    set_session_phase(SessionPhase::FrontConnected);

    const int err_id = pRspInfo ? pRspInfo->ErrorID : 0;
    last_login_error_id_.store(err_id, std::memory_order_release);
    const int fails = consecutive_login_failures_.fetch_add(1, std::memory_order_acq_rel) + 1;
    XtLogger::error_m("TD", "event=td_login_failed reason_code={} error_id={} consecutive_failures={}",
                      rc(XtLiveRejectReason::TdLoginFailed), err_id, fails);

    if (ctp_login::is_credential_class_login_error(err_id)) {

        XtLogger::error_m("TD",
            "CTP error {} credential-class login failure — operator action required "
            "(reset/update password or auth info; SimNow: https://www.simnow.com.cn forgot-password). "
            "Backing off to 1-hour intervals to avoid (re-)tripping broker lockout (error 75).",
            err_id);
    }

    if (auto* rec = XtLiveRecorder::global()) {

        const std::string err_msg =
            pRspInfo ? XtStringUtil::gbk_to_utf8_or_original(pRspInfo->ErrorMsg) : "";
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "login_failed", err_msg.c_str());
    }

    schedule_login_retry();
}

bool TraderCTP::verify_login_identity_(const CThostFtdcRspUserLoginField* login) {
    if (expected_investor_id_.empty()) return true;

    const std::string got = login
        ? std::string(login->UserID, ::strnlen(login->UserID, sizeof(login->UserID)))
        : std::string();
    if (got == expected_investor_id_) return true;
    identity_mismatch_latched_.store(true, std::memory_order_release);

    XtLogger::error_m("TD",
                      "event=td_identity_mismatch expected_investor_id={} got={} — "
                      "凭证与部署身份错配 (multibook isolation)，冻结 TD readiness，"
                      "不进入结算确认/恢复流程",
                      expected_investor_id_, got);
    readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Settlement,
                         "ctp identity mismatch: expected_investor_id=" +
                             expected_investor_id_ + " got=" + got);
    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "identity_mismatch",
                             expected_investor_id_.c_str());
    }
    return false;
}

void TraderCTP::OnRspUserLogin(CThostFtdcRspUserLoginField* pRspUserLogin,
                               CThostFtdcRspInfoField* pRspInfo,
                               int,
                               bool) {
    if (is_error(pRspInfo)) {
        handle_login_failed(pRspInfo);
        return;
    }

    consecutive_login_failures_.store(0, std::memory_order_release);
    last_login_error_id_.store(0, std::memory_order_release);
    set_session_phase(SessionPhase::LoginOk);

    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "logged_in", "");
    }
    if (pRspUserLogin) {
        front_id_.store(pRspUserLogin->FrontID, std::memory_order_release);
        session_id_.store(pRspUserLogin->SessionID, std::memory_order_release);
        const uint32_t trading_day =
            XtTimeParse::parse_fixed_date_yyyymmdd(pRspUserLogin->TradingDay);
        current_trading_day_.store(trading_day, std::memory_order_release);
        XtLogger::info_m("TD", "login ok, trading day {}", pRspUserLogin->TradingDay);
    } else {
        front_id_.store(0, std::memory_order_release);
        session_id_.store(0, std::memory_order_release);
        current_trading_day_.store(0, std::memory_order_release);
    }

    if (!verify_login_identity_(pRspUserLogin)) return;

    if (confirm_settlement_) {
        set_session_phase(SessionPhase::SettlementPending);
        if (!request_settlement_confirm_with_timeout()) {
            XtLogger::warn_m("TD",
                             "event=settlement_confirm_failed reason_code={} hold recovery until confirm ok",
                             rc(XtLiveRecoveryError::SettlementConfirmReqFailed));
            schedule_settlement_confirm_retry(XtLiveRecoveryError::SettlementConfirmReqFailed);
        }
    } else {

        readiness_gate_.set_not_required(xt_live::gateway::TdReadinessStage::Settlement,
                                         "ctp settlement confirm disabled by config");
        settlement_event_pending_.store(true, std::memory_order_release);
        enter_recovery_pipeline(true);
    }
}

void TraderCTP::OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField*,
                                           CThostFtdcRspInfoField* pRspInfo,
                                           int,
                                           bool bIsLast) {
    if (!bIsLast) return;
    if (session_phase_.load(std::memory_order_acquire) != SessionPhase::SettlementPending) {
        XtLogger::warn_m("TD", "ignore stale settlement confirm rsp");
        return;
    }
    settlement_confirm_pending_.store(false, std::memory_order_release);
    settlement_confirm_retry_pending_.store(false, std::memory_order_release);
    settlement_confirm_token_.fetch_add(1, std::memory_order_acq_rel);
    if (is_error(pRspInfo)) {
        settlement_confirmed_ = false;
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Settlement,
                             "ctp settlement confirm response failed");
        XtLogger::error_m("TD", "event=settlement_confirm_rsp_failed reason_code={}",
                          rc(XtLiveRecoveryError::SettlementConfirmRspFailed));
        schedule_settlement_confirm_retry(XtLiveRecoveryError::SettlementConfirmRspFailed);
        return;
    }
    settlement_confirmed_ = true;
    readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::Settlement,
                            "ctp settlement confirmed");
    settlement_event_pending_.store(true, std::memory_order_release);
    XtLogger::info_m("TD", "settlement confirm ok");
    enter_recovery_pipeline(true);
}

void TraderCTP::OnRspError(CThostFtdcRspInfoField* pRspInfo,
                           int nRequestID,
                           bool bIsLast) {
    const int err_id = pRspInfo ? pRspInfo->ErrorID : 0;
    const std::string err_msg =
        pRspInfo ? XtStringUtil::gbk_to_utf8_or_original(pRspInfo->ErrorMsg) : "";
    if (err_id != 0) {
        XtLogger::error_m("TD",
                          "event=td_rsp_error req_id={} is_last={} error_id={} error_msg={}",
                          nRequestID, bIsLast ? 1 : 0, err_id, err_msg);
    } else {
        XtLogger::warn_m("TD", "event=td_rsp_error req_id={} is_last={} empty_error",
                         nRequestID, bIsLast ? 1 : 0);
    }

    if (session_phase_.load(std::memory_order_acquire) != SessionPhase::SettlementPending) return;

    bool expected = true;
    if (!settlement_confirm_pending_.compare_exchange_strong(expected, false,
                                                             std::memory_order_acq_rel)) {
        return;
    }
    settlement_confirmed_ = false;
    readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Settlement,
                         "ctp settlement confirm error");
    settlement_confirm_retry_pending_.store(false, std::memory_order_release);
    settlement_confirm_token_.fetch_add(1, std::memory_order_acq_rel);
    XtLogger::error_m("TD",
                      "event=settlement_confirm_rsp_error req_id={} error_id={} reason_code={}",
                      nRequestID, err_id, rc(XtLiveRecoveryError::SettlementConfirmRspFailed));
    schedule_settlement_confirm_retry(XtLiveRecoveryError::SettlementConfirmRspFailed);
}

void TraderCTP::set_session_phase(SessionPhase phase) {
    session_phase_.store(phase, std::memory_order_release);
}

TraderCTP::QueryControlStats TraderCTP::query_control_stats() const {
    const auto& pos = qry_[qry_idx(QueryKind::Position)];
    QueryControlStats out;
    out.query_pos_throttled     = pos.throttled_count.load(std::memory_order_relaxed);
    out.query_pos_inflight_skip = pos.inflight_skip_count.load(std::memory_order_relaxed);
    out.query_pos_dirty_merged  = pos.dirty_merge_count.load(std::memory_order_relaxed);
    return out;
}

void TraderCTP::OnRspQryOrder(CThostFtdcOrderField* pOrder,
                              CThostFtdcRspInfoField* pRspInfo,
                              int nRequestID,
                              bool bIsLast) {

    if (!query_row_owned_(QueryKind::XtOrder, nRequestID)) return;
    if (is_error(pRspInfo)) {
        XtLogger::error_m("TD", "query order failed");
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::OpenOrders,
                             "ctp order query failed");
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "ctp order query failed before reverse-sync");
        if (bIsLast) {
            on_query_response_done(QueryKind::XtOrder, false);
            maybe_schedule_recovery_rearm_(QueryKind::XtOrder);
            clear_recovery_query_timeout(QueryKind::XtOrder);
        }
        return;
    }

    if (pOrder && recovery_.is_recovering()) {
        const XtOrderStatus st = ctp_util::map_order_status(pOrder->OrderStatus);
        const bool is_alive = !order_status_is_final(st);
        if (is_alive) {
            const uint64_t lid = ctp_util::parse_local_id(pOrder->OrderRef);
            if (lid == 0) {

                ++recovery_skipped_foreign_;
            } else {
                AliveInflightOrder a;
                a.local_id      = lid;
                a.code          = pOrder->InstrumentID;
                a.is_buy        = (pOrder->Direction == THOST_FTDC_D_Buy);
                a.total_qty_fp  = order_exec_to_fp(pOrder->VolumeTotalOriginal);
                a.filled_qty_fp = order_exec_to_fp(pOrder->VolumeTraded);
                a.price_fp      = order_exec_to_fp(pOrder->LimitPrice);
                a.strategy_sid  = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(lid);
                const char offset = pOrder->CombOffsetFlag[0];
                if (offset == THOST_FTDC_OF_CloseToday) {
                    a.close_offset = XtOffset::CloseToday;
                } else if (offset == THOST_FTDC_OF_CloseYesterday) {
                    a.close_offset = XtOffset::CloseYesterday;
                } else if (offset == THOST_FTDC_OF_Open) {
                    a.close_offset = XtOffset::Open;
                } else {
                    a.close_offset = XtOffset::Close;
                }

                RecoveredCancelRouting routing;
                routing.exchange_id  = pOrder->ExchangeID;
                routing.order_sys_id = pOrder->OrderSysID;
                routing.front_id     = pOrder->FrontID;
                routing.session_id   = pOrder->SessionID;
                std::lock_guard<std::mutex> lk(recovery_alive_mu_);
                recovery_alive_orders_.push_back(std::move(a));
                recovery_alive_routing_[lid] = std::move(routing);
            }
        }
    }
    if (pOrder) OnRtnOrder(pOrder);
    if (bIsLast) {
        clear_recovery_query_timeout(QueryKind::XtOrder);
        on_query_response_done(QueryKind::XtOrder, true);
        readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::OpenOrders,
                                "ctp order query completed");
        XtLogger::info_m("TD", "query order done");
        recovery_.on_order_done(true);
    }
}

void TraderCTP::OnRspQryTrade(CThostFtdcTradeField* pTrade,
                              CThostFtdcRspInfoField* pRspInfo,
                              int nRequestID,
                              bool bIsLast) {

    if (!query_row_owned_(QueryKind::Trade, nRequestID)) return;
    if (is_error(pRspInfo)) {
        XtLogger::error_m("TD", "query trade failed");
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "ctp trade query failed before recovery ready");
        if (bIsLast) {
            on_query_response_done(QueryKind::Trade, false);
            maybe_schedule_recovery_rearm_(QueryKind::Trade);
            clear_recovery_query_timeout(QueryKind::Trade);
        }
        return;
    }
    if (pTrade) on_rtn_trade_(pTrade,  true);
    if (bIsLast) {
        clear_recovery_query_timeout(QueryKind::Trade);
        on_query_response_done(QueryKind::Trade, true);
        XtLogger::info_m("TD", "query trade done");
        recovery_.on_trade_done(true);
    }
}

void TraderCTP::OnRspQryInvestorPosition(CThostFtdcInvestorPositionField* pPosition,
                                         CThostFtdcRspInfoField* pRspInfo,
                                         int nRequestID,
                                         bool bIsLast) {

    if (!query_row_owned_(QueryKind::Position, nRequestID)) return;
    if (is_error(pRspInfo)) {

        if (!recovery_.position_query_owns(nRequestID)) {
            recovery_.note_stale_position_frame();
            maybe_log_stale_position_frame_(nRequestID);
            return;
        }
        XtLogger::error_m("TD", "query position failed");
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Position,
                             "ctp position query failed");
        if (bIsLast) {
            on_query_response_done(QueryKind::Position, false);

            maybe_schedule_recovery_rearm_(QueryKind::Position);
            clear_recovery_query_timeout(QueryKind::Position);
        }
        return;
    }

    const bool owned = recovery_.on_position(pPosition, bIsLast, nRequestID);
    if (!owned) {
        maybe_log_stale_position_frame_(nRequestID);
        return;
    }
    if (bIsLast) {
        clear_recovery_query_timeout(QueryKind::Position);
        on_query_response_done(QueryKind::Position, true);

        if (recovery_.is_recovering())
            XtLogger::info_m("TD", "query position done");
        else
            XtLogger::debug_m("TD", "query position done");
    }
}

bool TraderCTP::query_row_owned_(QueryKind kind, int request_id) {
    if (!query_generation_fence_) return true;
    const int active =
        qry_[qry_idx(kind)].active_request_id.load(std::memory_order_acquire);

    if (request_id <= 0 || active == 0) {
        stale_query_row_unfenceable_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    if (request_id == active) return true;

    stale_query_row_dropped_.fetch_add(1, std::memory_order_relaxed);
    maybe_log_stale_query_row_(kind, request_id);
    return false;
}

void TraderCTP::maybe_log_stale_query_row_(QueryKind kind, int request_id) {

    const int64_t now_ms = XtSteadyClock::steady_now_ms();
    int64_t last = last_stale_query_row_log_ms_.load(std::memory_order_relaxed);
    if (now_ms - last < 5000) return;
    if (!last_stale_query_row_log_ms_.compare_exchange_strong(last, now_ms,
                                                              std::memory_order_relaxed))
        return;
    XtLogger::warn_m("TD",
        "event=stale_query_row_dropped stage={} req_id={} dropped_total={} "
        "(过期轮查询回包，generation fence 拦截)",
        query_meta(kind).name, request_id,
        stale_query_row_dropped_.load(std::memory_order_relaxed));
}

void TraderCTP::maybe_log_stale_position_frame_(int request_id) {

    const int64_t now_ms = XtSteadyClock::steady_now_ms();
    int64_t last = last_stale_position_log_ms_.load(std::memory_order_relaxed);
    if (now_ms - last < 5000) return;
    if (!last_stale_position_log_ms_.compare_exchange_strong(last, now_ms,
                                                             std::memory_order_relaxed))
        return;
    XtLogger::warn_m("TD",
        "event=stale_position_frame_dropped req_id={} total={} (过期轮持仓回包，归属栅栏拦截)",
        request_id, recovery_.stale_position_frame_dropped());
}

void TraderCTP::maybe_emit_daily_settlement_event(const CThostFtdcTradingAccountField* pAccount) {
    if (!pAccount || !callbacks_.on_daily_settlement) return;
    if (!settlement_event_pending_.exchange(false, std::memory_order_acq_rel)) return;

    uint32_t trading_day = XtTimeParse::parse_fixed_date_yyyymmdd(pAccount->TradingDay);
    if (trading_day == 0)
        trading_day = current_trading_day_.load(std::memory_order_relaxed);
    if (trading_day != 0 &&
        last_settlement_event_day_.load(std::memory_order_relaxed) == trading_day) {
        return;
    }

    const double transfer_net = pAccount->Deposit - pAccount->Withdraw;
    double settlement_net = pAccount->Balance - pAccount->PreBalance - transfer_net;
    if (!std::isfinite(settlement_net)) settlement_net = 0.0;

    double settlement_fee = pAccount->Commission;

    if (!std::isfinite(settlement_fee) || settlement_fee < 0.0) settlement_fee = 0.0;
    double realized_delta = settlement_net + settlement_fee;
    if (!std::isfinite(realized_delta)) {
        realized_delta = settlement_net;
        settlement_fee = 0.0;
    }

    DailySettlementDelta ev;
    ev.realized_delta = realized_delta;
    ev.fee            = settlement_fee;
    ev.trading_day    = trading_day;
    callbacks_.on_daily_settlement(ev);

    if (trading_day != 0)
        last_settlement_event_day_.store(trading_day, std::memory_order_release);
    XtLogger::info_m("TD", "emit daily settlement trading_day={} realized_delta={} fee={}",
                     trading_day, ev.realized_delta, ev.fee);
}

void TraderCTP::OnRspQryTradingAccount(CThostFtdcTradingAccountField* pAccount,
                                       CThostFtdcRspInfoField* pRspInfo,
                                       int nRequestID,
                                       bool bIsLast) {

    if (!query_row_owned_(QueryKind::AccountState, nRequestID)) return;
    if (is_error(pRspInfo)) {
        XtLogger::error_m("TD", "query account failed");
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                             "ctp account query failed");
        if (bIsLast) {
            on_query_response_done(QueryKind::AccountState, false);
            maybe_schedule_recovery_rearm_(QueryKind::AccountState);
            clear_recovery_query_timeout(QueryKind::AccountState);
        }
        return;
    }
    bool account_truth_ok = false;
    if (pAccount) {
        AccountSnapshot snap;
        snap.balance          = pAccount->Balance;
        snap.available        = pAccount->Available;
        snap.margin           = pAccount->CurrMargin;
        snap.frozen_margin    = pAccount->FrozenMargin;
        snap.frozen_cash      = pAccount->FrozenCash;
        snap.commission       = pAccount->Commission;
        snap.close_profit     = pAccount->CloseProfit;
        snap.position_profit  = pAccount->PositionProfit;
        snap.pre_balance      = pAccount->PreBalance;
        snap.deposit          = pAccount->Deposit;
        snap.withdraw         = pAccount->Withdraw;
        const double max_abs = account_snapshot_max_abs_;
        if (!account_snapshot_amount_ok(snap.balance, max_abs)) {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                                 "ctp account snapshot invalid");
            XtLogger::error_m(
                "TD",
                "drop invalid account snapshot: field=balance value={} threshold={}",
                snap.balance, max_abs);
        } else {
            int sanitized_fields = 0;
            auto sanitize = [&](double& v) {
                if (account_snapshot_amount_ok(v, max_abs)) return;
                v = 0.0;
                ++sanitized_fields;
            };
            sanitize(snap.available);
            sanitize(snap.margin);
            sanitize(snap.frozen_margin);
            sanitize(snap.frozen_cash);
            sanitize(snap.commission);
            sanitize(snap.close_profit);
            sanitize(snap.position_profit);
            sanitize(snap.pre_balance);
            sanitize(snap.deposit);
            sanitize(snap.withdraw);
            if (sanitized_fields > 0) {
                XtLogger::warn_m(
                    "TD",
                    "sanitize account snapshot fields={} threshold={}",
                    sanitized_fields, max_abs);
            }

            maybe_emit_daily_settlement_event(pAccount);
            if (callbacks_.on_account) callbacks_.on_account(snap);

            if (callbacks_.on_broker_account_truth) {
                BrokerVenueTruth bt;
                bt.available_balance_fp = xt_fp::to_fp(snap.available);
                bt.total_margin_balance_fp = xt_fp::to_fp(snap.balance);
                callbacks_.on_broker_account_truth(bt);
            }
            account_truth_ok = true;
        }
    }
    if (bIsLast) {
        clear_recovery_query_timeout(QueryKind::AccountState);
        if (!account_truth_ok) {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                                 "ctp account query completed without valid account snapshot");
            on_query_response_done(QueryKind::AccountState, false);

            maybe_schedule_recovery_rearm_(QueryKind::AccountState);
            return;
        }
        on_query_response_done(QueryKind::AccountState, true);
        readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::Account,
                                "ctp account query completed");

        if (recovery_.is_recovering())
            XtLogger::info_m("TD", "query account done");
        else
            XtLogger::debug_m("TD", "query account done");
        recovery_.on_account_done(true);
    }
}
