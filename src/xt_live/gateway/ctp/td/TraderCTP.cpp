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

// 账户快照异常值兜底阈值；可由 runtime.td.account_snapshot_max_abs 覆盖。
constexpr double kDefaultAccountSnapshotMaxAbs = 1e12;

// ── 通用辅助 ──────────────────────────────────

// 日志 reason 映射只在本分片使用，保留在 anonymous namespace 避免暴露到 header。
static const char* rc(XtLiveRejectReason code)  { return live_reject_reason_str(code); }
static const char* rc(XtLiveRecoveryError code) { return live_recovery_error_str(code); }

// account snapshot 以 broker 原始 double 入场；先做 finite/range gate，再进入 reducer。
inline bool account_snapshot_amount_ok(double v, double max_abs) {
    return std::isfinite(v) && std::fabs(v) <= max_abs;
}

} // namespace

// ── 生命周期 ──────────────────────────────────

CtpRecovery::HostPorts TraderCTP::make_recovery_host_ports(TraderCTP* self) {
    CtpRecovery::HostPorts ports;
    ports.publish_positions = [self](const std::vector<CtpRecovery::PositionSnapshot>& in) {
        auto out = self->convert_position_snapshot(in);
        // readiness Position stage 满足点不动（检查点 vi）：查询完成即 satisfy，
        // 与快照发射点解耦。
        self->readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::Position,
                                      "ctp position query completed");
        if (self->recovery_.is_recovering()) {
            // 发射分区（§4.4）：recovery 期快照暂存，recovery finish 按
            // pre-fills → snapshot → reverse-sync → post-fills 序发射。
            self->stage_recovery_positions_(std::move(out));
            return;
        }
        // Ready 稳态周期 refresh：保持即时发射（周期持仓对账路径）。
        if (self->callbacks_.on_position) self->callbacks_.on_position(out);
    };
    // 查询端口只负责发起请求；阶段顺序由 CtpRecovery 状态机推进。
    ports.start_position_query = [self]() { self->query_once(QueryKind::Position); };
    ports.start_account_query  = [self]() { self->query_once(QueryKind::AccountState); };
    ports.start_order_query    = [self]() { self->query_once(QueryKind::XtOrder); };
    ports.start_trade_query    = [self]() { self->query_once(QueryKind::Trade); };
    ports.publish_ready        = [self]() { self->on_recovery_ready(); };
    return ports;
}

TraderCTP::TraderCTP() : recovery_(make_recovery_host_ports(this)) {
    // pre-restart 封印锚：venue TradeTime（墙钟域）早于此刻的 query 重放行属
    // 前进程成交，absorbed 记账不重交付。
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

// ── recovery 宿主端口 handler（ctor wiring 实现）──────────────

std::vector<TraderCTP::PositionSnapshot> TraderCTP::convert_position_snapshot(
    const std::vector<CtpRecovery::PositionSnapshot>& in) const {
    std::vector<PositionSnapshot> out;
    out.reserve(in.size());
    // broker_frozen 使用 steady clock 时间戳，供 risk path 直接做 staleness delta。
    // 与 XtSteadyClock::steady_now_ms() 同源；CTP 持仓刷新正常为亚秒级，30s 只兜底 stale。
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
        // 透传 broker-side frozen 量。
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
    // 必须先于 TD ready 事件执行：发射分区收尾序把 pre fills / 快照 /
    // reverse-sync batch / post fills / pending terminals 按序交付到 engine/book。
    // is_first_recovery_ 只负责区分 cold-start 与 warm reconnect。
    const bool cold = is_first_recovery_;
    is_first_recovery_ = false;
    emit_recovery_finish_sequence_(cold);
    emit_ready_if_td_readiness_satisfied();
}

// ── 初始化与启动 ──────────────────────────────

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
    // <=0 关闭 re-arm（退回 fail-closed-forever，回滚开关）；正值作 backoff 基准。
    recovery_rearm_backoff_ms_ = ctp_cfg.recovery_rearm_backoff_ms;
    recovery_rearm_alert_rounds_ = ctp_cfg.recovery_rearm_alert_rounds;
    if (recovery_rearm_alert_rounds_ < 0) recovery_rearm_alert_rounds_ = 0;
    // Ready 稳态 refresh 查询超时自愈；<=0 关闭（回滚开关）。
    refresh_query_timeout_ms_ = ctp_cfg.refresh_query_timeout_ms;
    if (refresh_query_timeout_ms_ < 0) refresh_query_timeout_ms_ = 0;
    refresh_query_wedge_rounds_ = ctp_cfg.refresh_query_wedge_rounds;
    if (refresh_query_wedge_rounds_ < 0) refresh_query_wedge_rounds_ = 0;
    // false=关闭 query generation fence（回滚开关，回退全收）。
    query_generation_fence_ = ctp_cfg.query_generation_fence;
    // false=回退 query 重放行一律 absorbed 记账不下发的旧行为（回滚开关）。
    exec_ledger_query_fill_delivery_ = ctp_cfg.exec_ledger_query_fill_delivery;
}

bool TraderCTP::init(const Config& cfg) {
    // CTP 专属字段由 configure_ctp() 预先注入；init() 仅读 provider-neutral common 字段。
    front_ = cfg.front;
    user_  = cfg.user;
    pass_  = cfg.password;
    expected_investor_id_ = cfg.expected_investor_id;
    reconnect_enabled_         = cfg.reconnect_enabled;
    reconnect_interval_ms_     = cfg.reconnect_interval_ms;
    // stuck-disconnect self-heal 阈值，0 表示禁用。
    // 正常 YAML 路径由 validator 拒绝 <5000ms；这里保留 direct init/test 的
    // defensive clamp，避免过小阈值高频 Release+Init 放大 SDK 线程竞争和 broker 限流。
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
    // CTP SDK 回调遵循 "不抛" 契约 (不逐回调 guard, 对齐 wt/vnpy/openctp); 因此周期性
    // 持仓查询是任何漏单 (网络丢包 / SDK 边界 / 进程重启) 的兜底对账机制。
    // position_refresh_ms<=0 = 无周期对账, 漏单会静默漂移到下次重连。实盘不可接受 → 拒绝启动。
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
    // recovery_enabled 是 init-time cfg 配置，由 init 一次性应用（非运行期切换）。
    recovery_.set_recovery_enabled(cfg.recovery_enabled);
    return true;
}

bool TraderCTP::start() {
    if (!api_) return false;
    // QUICK 模式只接收登录后的新回报；RESTART 会回放整日流，恢复期有已有查询覆盖
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

    // 立即唤醒所有 cv-wait 的周期循环（reconnect/refresh/retry），避免秒级 join 阻塞。
    wait_cv_.notify_all();
    // join timer worker：返回后不再有任何延时回调触发或处于执行中（detached `[this]` UAF 闭合）。
    timer_queue_.stop();

    clear_query_retry_states();
    reset_query_control_states();
    settlement_confirm_pending_.store(false, std::memory_order_release);
    settlement_confirm_retry_pending_.store(false, std::memory_order_release);
    login_retry_pending_.store(false, std::memory_order_release);
    // token 递增令任何遗留 timeout 回调自动 skip（timer_queue_.stop() 后理论上已无）。
    settlement_confirm_token_.fetch_add(1, std::memory_order_acq_rel);
    settlement_event_pending_.store(false, std::memory_order_release);
    current_trading_day_.store(0, std::memory_order_release);
    clear_recovery_query_timeout_states();

    ThreadUtil::join_or_detach(qry_retry_thread_);
    ThreadUtil::join_or_detach(refresh_once_thread_);
    ThreadUtil::join_or_detach(refresh_thread_);

    set_session_phase(SessionPhase::Stopped);
    // Release 在 api_mu_ unique 下：与 submit/cancel/query 的 shared 读、check_health
    // 的 rebuild 互斥，杜绝 api_ 被 Release 后仍被 ReqXxx 解引用。Release 会 join CTP
    // SDK 网络线程，返回后不再有 SPI 回调；callbacks_ 目标由 runner 在 gateway stop 之后
    // 才析构（orderly shutdown 不变量），此处不重置 callbacks_ 以免与 SDK 线程读竞争。
    std::unique_lock<std::shared_mutex> api_lk(api_mu_);
    if (api_) {
        api_->Release();
        api_ = nullptr;
    }
}

bool TraderCTP::check_health(uint32_t now_ms) {
    // 断线重建未配置时不启用 watchdog；stop() 已接管 API 释放时禁止并发重建。
    if (stale_reconnect_ms_ == 0) return true;
    if (stopping_.load(std::memory_order_acquire)) return true;
    if (connected_.load(std::memory_order_acquire)) {
        // 连接态刷新 watchdog 锚点；OnFrontConnected 只负责初始置位。
        last_front_active_ms_.store(now_ms, std::memory_order_relaxed);
        return true;
    }
    const uint32_t last = last_front_active_ms_.load(std::memory_order_acquire);
    // 尚未连上过 front，初始连接仍由 start()/CTP SDK 自己推进。
    if (last == 0) return true;
    const uint32_t elapsed = now_ms - last;
    // 门限内继续等待 CTP SDK reconnect，避免重复销毁/创建 API。
    if (elapsed < stale_reconnect_ms_) return true;

    // Front 断线超过门限：由控制线程释放并重建 TD API。
    XtLogger::warn_m("TD", "stale disconnect ({}ms > {}ms), forcing self-rebuild",
                     elapsed, stale_reconnect_ms_);
    if (auto* rec = XtLiveRecorder::global()) {
        char reason[48];
        std::snprintf(reason, sizeof(reason), "elapsed_ms=%u", elapsed);
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "stale_rebuild", reason);
    }

    // SDK 可能未触发 OnFrontDisconnected，先同步关闭上层 TD ready；-99 为保留 reason。
    if (callbacks_.on_front) callbacks_.on_front(false, -99);

    // api_ Release + 重建期间持 unique 锁，与 submit/cancel/query 的 shared 读互斥。
    // 锁持有至函数返回，覆盖下方 CreateFtdcTraderApi / RegisterSpi / RegisterFront / Init 全程。
    std::unique_lock<std::shared_mutex> api_lk(api_mu_);
    // Release 会 join CTP SDK 内部网络线程，返回后原 SPI 不再回调。
    if (api_) {
        api_->Release();
        api_ = nullptr;
    }
    // 清空 session，让 rebuild 后的首次 OnFrontConnected 走完整登录/恢复流程。
    reset_session_state();
    set_session_phase(SessionPhase::Stopped);
    connected_.store(false, std::memory_order_release);

    // flow_dir 可能被外部清理，rebuild 前重新确保存在。
    std::string flow_dir_err;
    if (!PathUtil::ensure_writable_directory(flow_dir_, &flow_dir_err, ".xt_flow_probe")) {
        XtLogger::error_m("TD",
                          "stale rebuild: flow_dir not usable: {} ({}), next tick retry",
                          flow_dir_,
                          flow_dir_err);
        return false;
    }

    // 新 TD API 沿 start() 同一路径订阅 topic、注册前置并启动连接。
    api_ = CThostFtdcTraderApi::CreateFtdcTraderApi(flow_dir_.c_str());
    if (!api_) {
        XtLogger::error_m("TD", "stale rebuild: CreateFtdcTraderApi failed");
        // 保留超时锚点，下次 check_health 继续按已超时状态重试。
        return false;
    }
    api_->RegisterSpi(this);
    api_->SubscribePrivateTopic(THOST_TERT_QUICK);
    api_->SubscribePublicTopic(THOST_TERT_QUICK);
    api_->RegisterFront(const_cast<char*>(front_.c_str()));
    api_->Init();

    // Init() 只启动 SDK 连接流程；重置 watchdog 锚点，给本次 rebuild 一个完整等待窗口。
    last_front_active_ms_.store(now_ms, std::memory_order_release);
    return false;  // 本次检查已触发 rebuild
}

// ── 辅助函数 ──────────────────────────────────

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
    // 查询会话代际推进：旧 session 查询行不得记入新代（provenance 消费）。
    query_session_gen_.fetch_add(1, std::memory_order_acq_rel);
    // re-arm 世代复位：++token 让上一 session 调度的旧回调 no-op；新 session 全新预算。
    {
        std::lock_guard<std::mutex> lk(recovery_rearm_mu_);
        recovery_rearm_round_ = 0;
        recovery_rearm_pending_ = false;
    }
    recovery_rearm_token_.fetch_add(1, std::memory_order_acq_rel);
    set_recovery_wedged_(false);
    // 跨 session 窗口成交缓冲不得泄入新 session；gate 复位为开
    // （下一轮 enter_recovery_pipeline 再关）。
    {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        recovery_window_trades_.clear();
        recovery_window_gate_closed_ = false;
        recovery_window_overflow_ = 0;
    }
    // 暂存快照与发射分区锚点同属本 session recovery 态，一并清。
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
    // recovery 真正 ready：清 wedged + re-arm 轮数（成功才清，ReverseSync-fail 不到此分支）。
    {
        std::lock_guard<std::mutex> lk(recovery_rearm_mu_);
        recovery_rearm_round_ = 0;
    }
    set_recovery_wedged_(false);
    if (callbacks_.on_ready) callbacks_.on_ready();
}

void TraderCTP::schedule_after_ms(int ms, std::function<void()> action) {
    // owned worker 串行执行；stop() join 该 worker，杜绝 detached `[this]` 析构后访问。
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

// ── CTP SPI：会话与登录 ──────────────────────────

void TraderCTP::OnFrontConnected() {
    // CTP SDK 每次重连都分配新 FrontID，必须重置恢复状态
    const uint32_t down_callbacks = disc_log_gate_.on_reconnect();
    connected_.store(true, std::memory_order_release);
    current_trading_day_.store(0, std::memory_order_release);
    // front 活跃时间戳, 供 check_health 判 stuck-disconnect.
    last_front_active_ms_.store(
        static_cast<uint32_t>(XtSteadyClock::steady_now_ms()),
        std::memory_order_release);
    reset_session_state();
    set_session_phase(SessionPhase::FrontConnected);
    XtLogger::info_m("TD", "front connected");
    if (down_callbacks > 1) {
        XtLogger::info_m("TD", "front 重连成功 (断开期间 {} 次 disconnect 回调)", down_callbacks);
    }
    // 记录 front_connected，供 live trace / incident 对齐。
    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "front_connected", "");
    }
    if (callbacks_.on_front) callbacks_.on_front(true, 0);
    login();
}

void TraderCTP::OnFrontDisconnected(int nReason) {
    // 限频 OnFrontDisconnected 日志:CTP SDK 在 front 不可达时每 ~2-3s 自动重连一次,
    // 逐条打 error 会在长断开(收盘/网络抖动)刷出上万条、淹没真错误。首条 error,
    // 之后整段断开期内每 60s 一条 warn,重连复位。metric/recorder 仍计全量(在下方)。
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
    // 记录 front disconnect 原因，供 live trace / incident 对齐。
    if (auto* rec = XtLiveRecorder::global()) {
        char reason[32];
        std::snprintf(reason, sizeof(reason), "reason=%d", nReason);
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "disconnected", reason);
    }
    // 更新 gateway disconnect counter。
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
        // 凭证/运营干预类（码集见 CtpLoginBackoff.h，按 error.xml 官方定义）：重试无意义，
        // 快退避会把密码错磨成 75/143 锁定。
        XtLogger::error_m("TD",
            "CTP error {} credential-class login failure — operator action required "
            "(reset/update password or auth info; SimNow: https://www.simnow.com.cn forgot-password). "
            "Backing off to 1-hour intervals to avoid (re-)tripping broker lockout (error 75).",
            err_id);
    }

    if (auto* rec = XtLiveRecorder::global()) {
        // recorder JSONL 必须写 UTF-8；CTP ErrorMsg 原始字节为 GBK。
        const std::string err_msg =
            pRspInfo ? XtStringUtil::gbk_to_utf8_or_original(pRspInfo->ErrorMsg) : "";
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "td", "login_failed", err_msg.c_str());
    }

    // login 失败不代表 front 会断开；这里主动进入 login retry。
    schedule_login_retry();
}

bool TraderCTP::verify_login_identity_(const CThostFtdcRspUserLoginField* login) {
    if (expected_investor_id_.empty()) return true;  // 未配置 = 不校验（dev / 向后兼容）
    // CTP 字段按约定 null-terminated；strnlen 有界防御非规范回包。
    const std::string got = login
        ? std::string(login->UserID, ::strnlen(login->UserID, sizeof(login->UserID)))
        : std::string();
    if (got == expected_investor_id_) return true;
    identity_mismatch_latched_.store(true, std::memory_order_release);
    // investor id 非密可打印。闩锁为进程级终态：本 session 不推进，后续重连
    // 重登录会再次命中本校验，td_ready 永不置位（runner 拒绝交易）。
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
    // 登录成功后清空失败计数，下一次失败重新计算退避。
    consecutive_login_failures_.store(0, std::memory_order_release);
    last_login_error_id_.store(0, std::memory_order_release);
    set_session_phase(SessionPhase::LoginOk);
    // 记录 logged_in，供 live trace / incident 对齐。
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
    // identity 自证：登录回包 UserID 是账号身份的权威源（multibook 凭证错配防护）。
    // mismatch → 闩锁 + readiness fail，不进入结算确认/恢复流程 → td_ready 永不置位。
    if (!verify_login_identity_(pRspUserLogin)) return;
    // 结算确认是恢复查询的前置门控：broker 端未确认时查持仓/资金可能拿到 T-1 数据
    if (confirm_settlement_) {
        set_session_phase(SessionPhase::SettlementPending);
        if (!request_settlement_confirm_with_timeout()) {
            XtLogger::warn_m("TD",
                             "event=settlement_confirm_failed reason_code={} hold recovery until confirm ok",
                             rc(XtLiveRecoveryError::SettlementConfirmReqFailed));
            schedule_settlement_confirm_retry(XtLiveRecoveryError::SettlementConfirmReqFailed);
        }
    } else {
        // 即使跳过确认，也需要在首次资金查询时发一次日结事件给 AccountReducer
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

    // CTP SDK 有时不走 OnRspSettlementInfoConfirm 而走通用 OnRspError 返回结算失败，
    // 此处拦截以避免仅靠 timeout 兜底导致的额外等待
    if (session_phase_.load(std::memory_order_acquire) != SessionPhase::SettlementPending) return;
    // CAS 保证即使 OnRspError 和 timeout 并发触发，也只有一个路径能消费 pending
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

// ── 会话状态与恢复入口 ──────────────────────────

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

// ── CTP SPI：查询/回报 ──────────────────────────

void TraderCTP::OnRspQryOrder(CThostFtdcOrderField* pOrder,
                              CThostFtdcRspInfoField* pRspInfo,
                              int nRequestID,
                              bool bIsLast) {
    // generation fence：过期轮回包整帧丢弃——row 不进 alive batch / OnRtnOrder 委托，
    // bIsLast 不释放当前轮 inflight、不推进 recovery FSM。
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
    // recovery 查询期间只累积 alive(non-terminal) 委托；ready 发布口中由
    // rebuild_alive_inflight_and_emit 统一处理 reverse-sync。is_recovering()
    // gate 防止周期刷新 OnRspQryOrder 混入 recovery batch。
    if (pOrder && recovery_.is_recovering()) {
        const XtOrderStatus st = ctp_util::map_order_status(pOrder->OrderStatus);
        const bool is_alive = !order_status_is_final(st);
        if (is_alive) {
            const uint64_t lid = ctp_util::parse_local_id(pOrder->OrderRef);
            if (lid == 0) {
                // foreign / 非本进程 OrderRef (panel 手工下单或老进程残留)。
                // 累加 skipped counter 后 rebuild 时 emit 一次 (per-row metric 也可
                // 但 rebuild 一次性 emit 减 cardinality 抖动)。
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
                // book 通过 id_to_slot_idx_ 命中区分本进程 submit vs case B 回灌单。
                // 路由字段（OrderSysID/FrontID/SessionID）不在 AliveInflightOrder
                // 内，另存一份供 rebuild 注册后回填 tracker，保跨重启回灌单可撤。
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
    // generation fence：过期轮回包整帧丢弃——row 不进交付投影 fill 记账，
    // bIsLast 不释放当前轮 inflight、不推进 recovery FSM。
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
    if (pTrade) on_rtn_trade_(pTrade, /*from_query=*/true);
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
    // generation fence 外层拦截；下方 CtpRecovery 归属栅栏为 accumulator 内层保护
    // （fence 关闭构型与 recovery_ 直调路径仍受其覆盖）。
    if (!query_row_owned_(QueryKind::Position, nRequestID)) return;
    if (is_error(pRspInfo)) {
        // 过期轮次错误帧（超时重发后旧轮迟到）→ 整帧丢弃：不 fail readiness /
        // 不动当前在途 inflight / 不 rearm，交由当前轮自身收尾。request_id 归属判定与
        // query_once 的 inflight CAS 串行化（当前轮 inflight 未清前不会有新 send 改 owner），
        // 故此处 owns 检查无 TOCTOU。
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
            // 查询专用回包 error 走有界 re-arm（is_recovering gate 内判，refresh 不触发）。
            maybe_schedule_recovery_rearm_(QueryKind::Position);
            clear_recovery_query_timeout(QueryKind::Position);
        }
        return;
    }
    // 归属栅栏在 on_position 内与 merge 同锁原子判定：过期轮 row/bIsLast → 丢弃返回 false，
    // 不 emit / 不动当前在途；owned==true 才走 bIsLast 收尾（防旧轮 bIsLast 提前收口 + 清在途）。
    const bool owned = recovery_.on_position(pPosition, bIsLast, nRequestID);
    if (!owned) {
        maybe_log_stale_position_frame_(nRequestID);
        return;
    }
    if (bIsLast) {
        clear_recovery_query_timeout(QueryKind::Position);
        on_query_response_done(QueryKind::Position, true);
        // 周期 refresh 降噪：recovery/bootstrap 期保 INFO，稳态周期刷新 DEBUG。
        if (recovery_.is_recovering())
            XtLogger::info_m("TD", "query position done");
        else
            XtLogger::debug_m("TD", "query position done");
    }
}

bool TraderCTP::query_row_owned_(QueryKind kind, int request_id) {
    if (!query_generation_fence_) return true;  // 开关关闭：回退全收
    const int active =
        qry_[qry_idx(kind)].active_request_id.load(std::memory_order_acquire);
    // fail-open：双方 id 均有效才比对。request_id<=0 = 柜台/front 不回显 nRequestID；
    // active==0 = 本侧未绑定（未经 request_query 的直调路径）。两者都无法判归属，
    // 一律接受 + 计数——fence 不得成为可用性单点。
    if (request_id <= 0 || active == 0) {
        stale_query_row_unfenceable_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    if (request_id == active) return true;
    // 不匹配（含 active==kQueryRequestIdInvalidated：rearm 已放弃旧轮、等新发送覆盖，
    // 属本侧主动失效而非无信息态）= 旧代行 → 整帧丢弃。
    stale_query_row_dropped_.fetch_add(1, std::memory_order_relaxed);
    maybe_log_stale_query_row_(kind, request_id);
    return false;
}

void TraderCTP::maybe_log_stale_query_row_(QueryKind kind, int request_id) {
    // fence 丢弃告警限频：单次 stall 的旧轮可连丢数十行 → 至多每 5s 一条。
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
    // 混轮拦截告警限频：过期轮回报本应罕见（仅稳态 refresh / recovery 超时重发的窄
    // 竞态窗口触发），单次 stall 可连丢数十行 → 至多每 5s 打一条，防病态循环刷屏。
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

    // Balance - PreBalance - transfer_net 是扣除手续费后的日结净变化；
    // AccountReducer 会单独扣 fee，因此 realized_delta 传入未扣费口径。
    const double transfer_net = pAccount->Deposit - pAccount->Withdraw;
    double settlement_net = pAccount->Balance - pAccount->PreBalance - transfer_net;
    if (!std::isfinite(settlement_net)) settlement_net = 0.0;

    double settlement_fee = pAccount->Commission;
            // 部分 broker 返回负 Commission 表示返佣；结算 delta 统一将该字段 clamp 为 0。
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
    // generation fence：过期轮回包整帧丢弃——row 不 emit account/broker truth/日结事件，
    // bIsLast 不释放当前轮 inflight、不推进 recovery FSM。
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
            // broker venue-truth overlay：CTP Available/Balance 由 read-side truth
            // source 消费（on_account 消费端 account_store 只留 balance）。venue-neutral
            // callback，runner 层 bridge 转发 update_broker_overlay。snap 已 sanitize；
            // double→fp。Balance 是 broker 动态权益（含 PositionProfit），作 venue raw
            // equity 供 xt_venue_equity_raw ↔ xt_account_truth_equity 交叉校验。
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
            // invalid snapshot 可能瞬时；有界 re-arm 重发（持久坏数据则 page-then-continue，不永久烧预算）。
            maybe_schedule_recovery_rearm_(QueryKind::AccountState);
            return;
        }
        on_query_response_done(QueryKind::AccountState, true);
        readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::Account,
                                "ctp account query completed");
        // 周期 refresh 降噪：recovery/bootstrap 期保 INFO，稳态周期刷新 DEBUG。
        if (recovery_.is_recovering())
            XtLogger::info_m("TD", "query account done");
        else
            XtLogger::debug_m("TD", "query account done");
        recovery_.on_account_done(true);
    }
}
