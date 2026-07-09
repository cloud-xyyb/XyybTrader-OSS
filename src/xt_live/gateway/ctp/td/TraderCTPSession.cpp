// TraderCTP session 分片：TD login、login retry、settlement confirm gate 与 settlement timeout。
// 本文件只负责 Ready 前的登录/结算确认阶段；recovery query 和 reverse-sync 在 TraderCTPRecovery.cpp。
// login/settlement 请求都在 api_mu_ shared 下发送，避免 stop()/check_health() Release/rebuild 并发释放 api_。
// retry timer 通过 schedule_after_ms() 进入 owned timer queue；回调再次检查 session phase 后才发送请求。

#include "ctp/td/TraderCTP.h"

#include "XtLogger.h"
#include "common/XtLiveRecoveryError.h"
#include "xt_util/string/XtCStringUtil.h"

#include <algorithm>
#include <cstdint>
#include <cstring>

namespace {

const char* rc(XtLiveRecoveryError code) { return live_recovery_error_str(code); }

constexpr int kCtpErrLoginForbidden = 75;
constexpr int kCtpErrNotInited      = 7;

int compute_login_backoff_ms_(int login_retry_base_ms, int err_id, int fails) {
    constexpr int kCap5min = 5 * 60 * 1000;
    constexpr int kCap30s  = 30 * 1000;
    constexpr int kCap1h   = 60 * 60 * 1000;

    if (err_id == kCtpErrLoginForbidden) {

        return kCap1h;
    }

    const int cap = (err_id == kCtpErrNotInited) ? kCap30s : kCap5min;

    int retry = std::max(1000, login_retry_base_ms);
    if (fails > 1) {

        const int shift = std::min(fails - 1, 12);
        const int64_t scaled = static_cast<int64_t>(retry) << shift;
        retry = (scaled > cap) ? cap : static_cast<int>(scaled);
    }
    return retry;
}

}

bool TraderCTP::login() {

    std::shared_lock<std::shared_mutex> api_lk(api_mu_);
    if (!api_) return false;
    CThostFtdcReqUserLoginField req;
    std::memset(&req, 0, sizeof(req));
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

    if (session_phase_.load(std::memory_order_acquire) != SessionPhase::FrontConnected) return;
    if (login_retry_pending_.exchange(true, std::memory_order_acq_rel)) return;

    const int err_id = last_login_error_id_.load(std::memory_order_acquire);
    const int fails  = consecutive_login_failures_.load(std::memory_order_acquire);
    const int retry_ms = compute_login_backoff_ms_(login_retry_ms_, err_id, fails);
    schedule_after_ms(retry_ms, [this, retry_ms]() {
        login_retry_pending_.store(false, std::memory_order_release);
        if (!is_session_alive()) return;
        if (!connected_.load(std::memory_order_acquire)) return;
        if (session_phase_.load(std::memory_order_acquire) != SessionPhase::FrontConnected) return;
        XtLogger::warn_m("TD", "event=td_login_retry retry_ms={}", retry_ms);
        if (!login()) {

            schedule_login_retry();
        }
    });
}

bool TraderCTP::confirm_settlement() {

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
