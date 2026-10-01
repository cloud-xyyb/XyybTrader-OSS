// TraderCTP session 分片：TD login、login retry、settlement confirm gate 与 settlement timeout。
// 本文件只负责 Ready 前的登录/结算确认阶段；recovery query 和 reverse-sync 在 TraderCTPRecovery.cpp。
// login/settlement 请求都在 api_mu_ shared 下发送，避免 stop()/check_health() Release/rebuild 并发释放 api_。
// retry timer 通过 schedule_after_ms() 进入 owned timer queue；回调再次检查 session phase 后才发送请求。

#include "ctp/td/TraderCTP.h"

#include "XtLogger.h"
#include "common/XtLiveRecoveryError.h"
#include "ctp/td/CtpLoginBackoff.h"
#include "xt_util/string/XtCStringUtil.h"

#include <algorithm>
#include <cstdint>
#include <cstring>

namespace {

// 文件内日志辅助：保持 session/recovery reason 输出一致。
const char* rc(XtLiveRecoveryError code) { return live_recovery_error_str(code); }

// login retry 退避分级见 CtpLoginBackoff.h（凭证类固定 1h / NOT_INITED cap 30s /
// 其它瞬态 cap 5min）。consecutive_login_failures_ 登录失败递增、成功清零。
}  // namespace

// ── Login request 与 retry ──────────────────────────────────────────────

bool TraderCTP::login() {
    // SDK 回调线程与 login retry timer 都可能进入；发送前持 shared 锁，
    // 与 stop()/check_health 的 Release/rebuild 互斥。
    std::shared_lock<std::shared_mutex> api_lk(api_mu_);
    if (!api_) return false;
    CThostFtdcReqUserLoginField req;
    std::memset(&req, 0, sizeof(req));  // CTP 要求所有未用字段为零。
    XtCStringUtil::copy(req.BrokerID, broker_.c_str());
    XtCStringUtil::copy(req.UserID, user_.c_str());
    XtCStringUtil::copy(req.Password, pass_.c_str());
    int ret = api_->ReqUserLogin(&req, next_req_id());
    if (ret != 0) {
        XtLogger::error_m("TD", "ReqUserLogin failed: {}", ret);
        return false;
    }
    return true;
}

void TraderCTP::schedule_login_retry() {
    if (!reconnect_enabled_) return;
    if (!is_session_alive()) return;
    // 只在 front 已连上但 login 未成功时重试；断线由 CTP SDK 自动重连 + check_health 重建兜底。
    if (session_phase_.load(std::memory_order_acquire) != SessionPhase::FrontConnected) return;
    if (login_retry_pending_.exchange(true, std::memory_order_acq_rel)) return;

    // retry_ms 由最近 ErrorID 与连续失败次数决定；无错误时使用配置下限。
    const int err_id = last_login_error_id_.load(std::memory_order_acquire);
    const int fails  = consecutive_login_failures_.load(std::memory_order_acquire);
    const int retry_ms = ctp_login::compute_login_backoff_ms(login_retry_ms_, err_id, fails);
    schedule_after_ms(retry_ms, [this, retry_ms]() {
        login_retry_pending_.store(false, std::memory_order_release);
        if (!is_session_alive()) return;
        if (!connected_.load(std::memory_order_acquire)) return;
        if (session_phase_.load(std::memory_order_acquire) != SessionPhase::FrontConnected) return;
        XtLogger::warn_m("TD", "event=td_login_retry retry_ms={}", retry_ms);
        if (!login()) {
            // ReqUserLogin 同步失败极少见；继续保留 retry 链。
            schedule_login_retry();
        }
    });
}

// ── 结算确认 gate ──────────────────────────────────────────────

bool TraderCTP::confirm_settlement() {
    // settlement retry timer 与 SDK 回调路径都可能进入；发送前持 shared 锁，
    // 与 stop()/check_health 的 Release/rebuild 互斥。
    std::shared_lock<std::shared_mutex> api_lk(api_mu_);
    if (!api_) return false;
    CThostFtdcSettlementInfoConfirmField req;
    std::memset(&req, 0, sizeof(req));
    XtCStringUtil::copy(req.BrokerID, broker_.c_str());
    XtCStringUtil::copy(req.InvestorID, user_.c_str());
    int ret = api_->ReqSettlementInfoConfirm(&req, next_req_id());
    if (ret != 0) {
        XtLogger::error_m("TD", "ReqSettlementInfoConfirm failed: {}", ret);
        return false;
    }
    settlement_confirm_pending_.store(true, std::memory_order_release);
    XtLogger::info_m("TD", "settlement confirm requested");
    return true;
}

bool TraderCTP::request_settlement_confirm_with_timeout() {
    if (!confirm_settlement()) return false;
    arm_settlement_timeout();
    return true;
}

void TraderCTP::schedule_settlement_confirm_retry(XtLiveRecoveryError reason) {
    if (!confirm_settlement_) return;
    if (!is_session_alive()) return;
    if (session_phase_.load(std::memory_order_acquire) != SessionPhase::SettlementPending) return;
    if (settlement_confirm_retry_pending_.exchange(true, std::memory_order_acq_rel)) return;

    const uint64_t token    = settlement_confirm_token_.load(std::memory_order_acquire);
    const int      retry_ms = std::max(1000, settlement_confirm_retry_ms_);
    schedule_after_ms(retry_ms, [this, token, retry_ms, reason]() {
        settlement_confirm_retry_pending_.store(false, std::memory_order_release);
        if (!is_session_alive()) return;
        if (session_phase_.load(std::memory_order_acquire) != SessionPhase::SettlementPending) return;
        if (settlement_confirm_token_.load(std::memory_order_acquire) != token) return;
        XtLogger::warn_m("TD", "event=settlement_confirm_retry reason_code={} retry_ms={}",
                         rc(reason), retry_ms);
        if (!request_settlement_confirm_with_timeout()) {
            schedule_settlement_confirm_retry(reason);
        }
    });
}

void TraderCTP::arm_settlement_timeout() {
    if (settlement_confirm_timeout_ms_ <= 0) return;
    // token 递增使上一轮 timeout 定时器失效，保证同时只有一个活跃 timeout。
    const uint64_t token      = settlement_confirm_token_.fetch_add(1, std::memory_order_acq_rel) + 1;
    const int      timeout_ms = settlement_confirm_timeout_ms_;
    schedule_after_ms(timeout_ms, [this, token, timeout_ms]() {
        if (!is_session_alive()) return;
        if (session_phase_.load(std::memory_order_acquire) != SessionPhase::SettlementPending) return;
        if (settlement_confirm_token_.load(std::memory_order_acquire) != token) return;
        bool expected = true;
        if (!settlement_confirm_pending_.compare_exchange_strong(expected, false,
                                                                 std::memory_order_acq_rel)) {
            return;
        }
        settlement_confirmed_ = false;
        XtLogger::warn_m("TD",
                         "event=settlement_confirm_timeout reason_code={} timeout_ms={} retry confirm",
                         rc(XtLiveRecoveryError::SettlementConfirmRspFailed), timeout_ms);
        schedule_settlement_confirm_retry(XtLiveRecoveryError::SettlementConfirmRspFailed);
    });
}
