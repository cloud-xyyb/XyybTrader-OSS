#include "ctp/md/ParserCTP.h"

#include "ctp/common/CTPUtil.h"
#include "xt_util/path/PathUtil.h"
#include "recording/XtLiveRecorder.h"
#include "XtLogger.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_live/telemetry/recording/XtMdTickRecorderUtil.h"
#include "xt_util/string/XtCStringUtil.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_util/time/XtTimeParse.h"
#include "xt_util/string/XtStringUtil.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

#include <cstdio>
#include <cstring>

namespace {

void fill_level1_to_5(const CThostFtdcDepthMarketDataField& md, XtRawTick& t) {
    const double bid_px[5] = {md.BidPrice1, md.BidPrice2, md.BidPrice3, md.BidPrice4, md.BidPrice5};
    const double ask_px[5] = {md.AskPrice1, md.AskPrice2, md.AskPrice3, md.AskPrice4, md.AskPrice5};
    const int bid_qty[5] = {md.BidVolume1, md.BidVolume2, md.BidVolume3, md.BidVolume4, md.BidVolume5};
    const int ask_qty[5] = {md.AskVolume1, md.AskVolume2, md.AskVolume3, md.AskVolume4, md.AskVolume5};
    for (int i = 0; i < 5; ++i) {
        t.bid_prices[i] = ctp_util::normalize_price(bid_px[i]);
        t.ask_prices[i] = ctp_util::normalize_price(ask_px[i]);
        t.bid_qty[i] = bid_qty[i];
        t.ask_qty[i] = ask_qty[i];
    }
}

}

ParserCTP::ParserCTP() = default;

ParserCTP::~ParserCTP() {
    stop();
}

void ParserCTP::configure_ctp(const CTPMdCfg& ctp_cfg) {
    std::lock_guard<std::mutex> lock(mu_);
    broker_ = ctp_cfg.broker;
    flow_dir_ = ctp_cfg.flow_dir;
}

bool ParserCTP::init(const IMdGateway::Config& cfg) {
    std::lock_guard<std::mutex> guard(mu_);
    return init_locked(cfg);
}

bool ParserCTP::start() {
    std::lock_guard<std::mutex> guard(mu_);
    return start_locked();
}

void ParserCTP::stop() {
    std::lock_guard<std::mutex> guard(mu_);
    if (api_) {
        api_->Release();
        api_ = nullptr;
    }
}

bool ParserCTP::check_health(uint32_t now_ms) {

    if (stale_reconnect_ms_ == 0) return true;

    const uint32_t last = last_tick_steady_ms_.load(std::memory_order_relaxed);
    if (last == 0) return true;
    const uint32_t elapsed = now_ms - last;
    if (elapsed < stale_reconnect_ms_) return true;

    std::lock_guard<std::mutex> guard(mu_);
    if (!connected_ || !subscribed_) return true;

    XtLogger::warn_m("MD", "stale tick detected ({}ms > {}ms), forcing self-reconnect",
                     elapsed, stale_reconnect_ms_);

    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "md", "stale_reconnect", "");
    }

    if (api_) {
        api_->Release();
        api_ = nullptr;
    }
    connected_ = false;
    logged_in_ = false;
    subscribed_ = false;
    ready_emitted_ = false;

    last_tick_steady_ms_.store(now_ms, std::memory_order_relaxed);
    if (!init_locked(saved_cfg_)) {
        XtLogger::error_m("MD", "stale self-reconnect: re-init failed");
        return false;
    }
    start_locked();
    return false;
}

void ParserCTP::bind_callbacks(Callbacks callbacks) {
    on_tick_ = std::move(callbacks.on_tick);
    on_ready_ = std::move(callbacks.on_ready);
    on_front_ = std::move(callbacks.on_front);
}

void ParserCTP::OnFrontConnected() {
    std::lock_guard<std::mutex> guard(mu_);
    const uint32_t down_callbacks = disc_log_gate_.on_reconnect();

    connected_ = true;
    logged_in_ = false;
    subscribed_ = false;
    ready_emitted_ = false;
    XtLogger::info_m("MD", "front connected");
    if (down_callbacks > 1) {
        XtLogger::info_m("MD", "front 重连成功 (断开期间 {} 次 disconnect 回调)", down_callbacks);
    }
    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "md", "front_connected", "");
    }
    if (on_front_) on_front_(true, 0);
    login();
}

void ParserCTP::OnFrontDisconnected(int nReason) {
    std::lock_guard<std::mutex> guard(mu_);

    connected_ = false;
    logged_in_ = false;
    subscribed_ = false;
    ready_emitted_ = false;

    switch (disc_log_gate_.on_disconnect(static_cast<uint32_t>(XtSteadyClock::steady_now_ms()))) {
    case CtpFrontDisconnectLogGate::Action::LogError:
        XtLogger::error_m("MD", "front disconnected: {}", nReason);
        break;
    case CtpFrontDisconnectLogGate::Action::LogWarn:
        XtLogger::warn_m("MD", "front disconnected: {} (仍未连上, 重复已抑制)", nReason);
        break;
    case CtpFrontDisconnectLogGate::Action::Suppress:
        break;
    }

    if (auto* rec = XtLiveRecorder::global()) {
        char reason[32];
        std::snprintf(reason, sizeof(reason), "reason=%d", nReason);
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "md", "disconnected", reason);
    }
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().inc_gateway_disconnect_total(
        "ctp", "md", "front_disconnect");
#endif
    if (on_front_) on_front_(false, nReason);
}

void ParserCTP::OnRspUserLogin(CThostFtdcRspUserLoginField* pRspUserLogin,
                               CThostFtdcRspInfoField* pRspInfo,
                               int,
                               bool) {
    std::lock_guard<std::mutex> guard(mu_);
    if (is_error(pRspInfo)) {
        XtLogger::error_m("MD", "login failed");
        return;
    }
    if (pRspUserLogin) {
        XtLogger::info_m("MD", "login ok, trading day {}", pRspUserLogin->TradingDay);
    }

    logged_in_ = true;

    if (subs_.empty()) {
        XtLogger::warn_m("MD", "no ctp.subs in config");
        subscribed_ = true;
        emit_ready();
    } else {
        subscribe();
    }
}

void ParserCTP::OnRspSubMarketData(CThostFtdcSpecificInstrumentField* pSpecificInstrument,
                                   CThostFtdcRspInfoField* pRspInfo,
                                   int,
                                   bool bIsLast) {
    std::lock_guard<std::mutex> guard(mu_);
    if (is_error(pRspInfo)) {
        XtLogger::error_m("MD", "subscribe failed");
        return;
    }
    if (pSpecificInstrument) {
        XtLogger::info_m("MD", "subscribed {}", pSpecificInstrument->InstrumentID);
    }

    if (bIsLast) {
        subscribed_ = true;
        emit_ready();
    }
}

void ParserCTP::OnRtnDepthMarketData(CThostFtdcDepthMarketDataField* p) {
    if (!p) {
        XtLogger::warn_m("MD", "event=md_depth_rtn_null");
        return;
    }

    {
        std::lock_guard<std::mutex> guard(mu_);
        if (!connected_) return;
    }
    XtRawTick t;
    XtCStringUtil::copy(t.code, p->InstrumentID);
    XtCStringUtil::copy(t.exchg, p->ExchangeID);

    const double last = ctp_util::normalize_price(p->LastPrice);
    t.price = last;
    t.open = ctp_util::normalize_price(p->OpenPrice);
    t.high = ctp_util::normalize_price(p->HighestPrice);
    t.low = ctp_util::normalize_price(p->LowestPrice);
    t.settle = ctp_util::normalize_price(p->SettlementPrice);
    t.upper_limit = ctp_util::normalize_price(p->UpperLimitPrice);
    t.lower_limit = ctp_util::normalize_price(p->LowerLimitPrice);
    t.total_volume = p->Volume;
    t.total_turnover = p->Turnover;
    t.open_interest = p->OpenInterest;
    fill_level1_to_5(*p, t);

    t.trading_date = XtTimeParse::parse_fixed_date_yyyymmdd(p->TradingDay);
    t.action_date = XtTimeParse::parse_fixed_date_yyyymmdd(p->ActionDay);
    t.action_time = XtTimeParse::parse_fixed_time_hms_ms(p->UpdateTime, p->UpdateMillisec);

    if (auto* rec = XtLiveRecorder::global()) {
        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();
        const int64_t last_fp = xt_live_recorder_util::to_recorder_fp(last);
        constexpr int kRecDepth = XtLiveRecorder::kMdDepth;
        int64_t bid_px_fp[kRecDepth] = {}, ask_px_fp[kRecDepth] = {},
                bid_q[kRecDepth] = {}, ask_q[kRecDepth] = {};
        xt_live_recorder_util::fill_md_depth_fp(t.bid_prices, t.ask_prices,
                                                t.bid_qty, t.ask_qty, 5,
                                                bid_px_fp, ask_px_fp, bid_q, ask_q);
        rec->emit_md_tick(ts_local_ns,
                          t.trading_date, t.action_date, t.action_time,
                          t.code, t.exchg,
                          last_fp, static_cast<int64_t>(t.total_volume),
                          bid_px_fp, ask_px_fp, bid_q, ask_q);
    }

    last_tick_steady_ms_.store(
        static_cast<uint32_t>(XtSteadyClock::steady_now_ms()),
        std::memory_order_relaxed);

    if (on_tick_) on_tick_(t);
}

bool ParserCTP::init_locked(const IMdGateway::Config& cfg) {
    saved_cfg_ = cfg;
    stale_reconnect_ms_ = cfg.stale_reconnect_ms;

    front_ = cfg.front;
    user_ = cfg.user;
    pass_ = cfg.password;
    subs_ = cfg.subs;

    if (front_.empty() || broker_.empty() || user_.empty()) {
        XtLogger::error_m("MD", "missing md_front/broker/user in config");
        return false;
    }
    std::string flow_dir_err;
    if (!PathUtil::ensure_writable_directory(flow_dir_, &flow_dir_err, ".xt_flow_probe")) {
        XtLogger::error_m("MD", "flow_dir not usable: {} ({})", flow_dir_, flow_dir_err);
        return false;
    }

    api_ = CThostFtdcMdApi::CreateFtdcMdApi(flow_dir_.c_str(), false, false);
    if (!api_) {
        XtLogger::error_m("MD", "CreateFtdcMdApi failed");
        return false;
    }
    api_->RegisterSpi(this);
    return true;
}

bool ParserCTP::start_locked() {
    if (!api_) return false;

    api_->RegisterFront(const_cast<char*>(front_.c_str()));
    api_->Init();
    return true;
}

bool ParserCTP::login() {
    CThostFtdcReqUserLoginField req;
    std::memset(&req, 0, sizeof(req));
    XtCStringUtil::copy(req.BrokerID, broker_.c_str());
    XtCStringUtil::copy(req.UserID, user_.c_str());
    XtCStringUtil::copy(req.Password, pass_.c_str());
    int ret = api_->ReqUserLogin(&req, ++req_id_);
    if (ret != 0) {
        XtLogger::error_m("MD", "ReqUserLogin failed: {}", ret);
        return false;
    }
    return true;
}

bool ParserCTP::subscribe() {
    if (subs_.empty()) {
        XtLogger::warn_m("MD", "no ctp.subs in config");
        return false;
    }

    std::vector<char*> ids;
    ids.reserve(subs_.size());
    for (auto& s : subs_) ids.push_back(const_cast<char*>(s.c_str()));
    int ret = api_->SubscribeMarketData(ids.data(), static_cast<int>(ids.size()));
    if (ret != 0) {
        XtLogger::error_m("MD", "SubscribeMarketData failed: {}", ret);
        return false;
    }
    return true;
}

bool ParserCTP::is_error(CThostFtdcRspInfoField* pRspInfo) {
    if (pRspInfo && pRspInfo->ErrorID != 0) {
        XtLogger::error_m("MD", "error {} {}", pRspInfo->ErrorID,
                          XtStringUtil::gbk_to_utf8_or_original(pRspInfo->ErrorMsg));
        return true;
    }
    return false;
}

void ParserCTP::emit_ready() {
    if (ready_emitted_) return;
    ready_emitted_ = true;

    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "md", "ready", "");
    }
    if (on_ready_) on_ready_();
}
