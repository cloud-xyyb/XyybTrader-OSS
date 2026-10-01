#include "live/XtLiveRejectSink.h"

#include <chrono>

#include "XtLogger.h"
#include "xt_risk/engine/XtRiskEngine.h"
#include "live/LiveRejectReason.h"

namespace {

// reject 慢路径专用时钟；evaluate 放行热路径不触达本文件。
uint64_t steady_now_us() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

// 抑制状态 process-global：sink 是 submit() 的 per-call 栈对象，承载不了跨调用窗口。
XtLiveRejectLogThrottle& reject_log_throttle() {
    static XtLiveRejectLogThrottle throttle;
    return throttle;
}

}  // namespace

bool XtLiveRejectLogThrottle::should_log(std::string_view strategy_id,
                                         int reason_code,
                                         uint64_t now_us,
                                         uint64_t& out_suppressed) {
    std::lock_guard<std::mutex> lk(mu_);
    Window& w = windows_[{std::string(strategy_id), reason_code}];
    if (w.last_log_us != 0 && now_us - w.last_log_us < kWindowUs) {
        ++w.suppressed;
        return false;
    }
    out_suppressed = w.suppressed;
    w.last_log_us = now_us;
    w.suppressed = 0;
    return true;
}

XtLiveRejectSink::XtLiveRejectSink(ILiveRejectOutput& output) : output_(output) {}

void XtLiveRejectSink::on_margin_reject(const xt_core::XtOrder& req,
                                      const XtRiskResult& decision,
                                      int64_t now_ms,
                                      std::string_view strategy_id) {
    uint64_t suppressed = 0;
    if (reject_log_throttle().should_log(strategy_id,
                                         static_cast<int>(decision.reason_code),
                                         steady_now_us(), suppressed)) {
        XtLogger::warn_m("RISK",
                         "reject_margin sym={} side={} price_fp={} qty_fp={} avail_cash_fp={} req_margin_fp={} reason={} stage={} sid={} suppressed_since_last={}",
                         req.code,
                         req.is_buy ? "buy" : "sell",
                         req.price_fp, req.qty_fp,
                         decision.available_cash_fp, decision.required_margin_fp,
                         decision.reason_text.empty() ? risk_reason_text(decision.reason_code)
                                                      : decision.reason_text.c_str(),
                         risk_stage_text(decision.stage),
                         strategy_id,
                         suppressed);
    }
    note_first_(XtLiveRejectReason::InsufficientMargin);
    output_.publish_margin_reject(req, now_ms, strategy_id);
}

void XtLiveRejectSink::on_reject(const xt_core::XtOrder& req,
                                 const XtRiskResult& decision,
                                 std::string_view strategy_id) {
    uint64_t suppressed = 0;
    if (reject_log_throttle().should_log(strategy_id,
                                         static_cast<int>(decision.reason_code),
                                         steady_now_us(), suppressed)) {
        XtLogger::warn_m("RISK",
                         "reject sym={} side={} qty_fp={} reason={} stage={} sid={} suppressed_since_last={}",
                         req.code,
                         req.is_buy ? "buy" : "sell",
                         req.qty_fp,
                         decision.reason_text.empty() ? risk_reason_text(decision.reason_code)
                                                      : decision.reason_text.c_str(),
                         risk_stage_text(decision.stage),
                         strategy_id,
                         suppressed);
    }
    const XtLiveRejectReason mapped = live_reject_reason::from_risk(decision.reason_code);
    note_first_(mapped);
    output_.publish_reject_reason(mapped);
}
