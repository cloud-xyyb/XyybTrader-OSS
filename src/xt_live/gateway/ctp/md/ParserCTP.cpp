#include "ctp/md/ParserCTP.h"

#include "ctp/common/CTPUtil.h"
#include "ctp/common/CtpExchangeInfer.h"
#include "xt_util/path/PathUtil.h"
#include "recording/XtLiveRecorder.h"
#include "XtLogger.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_live/telemetry/recording/XtMdTickRecorderUtil.h"
#include "xt_util/string/XtCStringUtil.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_util/time/XtTimeParse.h"
#include "xt_util/string/XtStringUtil.h"

// 指标依赖由 XT_HAVE_METRICS 控制；xt_live_telemetry 通过 PUBLIC compile
// definition 透传，binary feature gate 关闭时该路径为 noop。
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

#include <chrono>
#include <cstdio>
#include <cstring>

namespace {

// CTP 五档行情逐字段展开到数组，normalize_price 统一清洗无效哨兵。
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

} // namespace

// ── 生命周期 ──────────────────────────────────────────────

ParserCTP::ParserCTP() = default;

ParserCTP::~ParserCTP() {
    stop();
}

void ParserCTP::configure_ctp(const CTPMdCfg& ctp_cfg) {
    std::lock_guard<std::mutex> lock(mu_);
    broker_ = ctp_cfg.broker;
    flow_dir_ = ctp_cfg.flow_dir;
    // window_gate_ 只被 check_health（控制线程）读写；configure_ctp 在 init 前调用，
    // 此时无并发 check_health，故直接赋值安全。gate_enabled=false（默认）时行为不变。
    window_gate_ = ctp_cfg.window_gate;
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

// ── 健康检查 ──────────────────────────────────────────────

bool ParserCTP::check_health(uint32_t now_ms) {
    // ── expected-tick 窗门控（off→in 边沿 arm synthetic 锚）───────────────────
    // gate_enabled=false（含 direct runner / gate disabled 回滚）→ in_window 恒 true，
    // watchdog 恒活，行为等价现行。gate_enabled → 仅窗内运行 watchdog。
    // wall-clock 用固定 UTC+8（见 CtpMdSessionGate），不依赖 host TZ。
    const int64_t wall_epoch_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    const bool in_window =
        window_gate_.gate_enabled
            ? ctp_md_session::in_expected_tick_window(window_gate_, wall_epoch_ms)
            : true;
    // 窗口进入沿：arm 合成锚，给重连/开盘首 tick 一个 stale_reconnect_ms 宽限，避免入窗即误判。
    if (!prev_in_window_ && in_window) {
        synthetic_arm_steady_ms_ = (now_ms == 0) ? 1u : now_ms;  // 0 保留为「未 arm」哨兵
    }
    prev_in_window_ = in_window;

    if (stale_reconnect_ms_ == 0) return true;
    // 窗外：watchdog inactive，不评估、不重连、不置 unhealthy。
    if (window_gate_.gate_enabled && !in_window) return true;

    // 双锚 wrap-safe staleness：对每个有效锚各算 age 取最小与阈值比。
    ctp_md_session::WatchdogInputs wi;
    wi.gate_enabled = window_gate_.gate_enabled;
    wi.in_window = in_window;
    wi.stale_reconnect_ms = stale_reconnect_ms_;
    wi.now_ms = now_ms;
    wi.last_tick_ms = last_tick_steady_ms_.load(std::memory_order_relaxed);
    wi.synth_arm_ms = synthetic_arm_steady_ms_;
    // fast path：先无锁判 age/anchor，不满足直接 healthy，避免每次加锁。
    {
        // connected/subscribed 用锁内真值，但先做一次无锁的 age gate 短路。
        ctp_md_session::WatchdogInputs probe = wi;
        probe.connected = true;   // 探测仅看 age；真正的 connected/subscribed 复核在锁内
        probe.subscribed = true;
        if (ctp_md_session::decide_watchdog(probe) == ctp_md_session::WatchdogAction::Healthy)
            return true;
    }

    // 进入 slow path：加锁后再次确认状态，防止与 SPI 回调竞争。
    std::lock_guard<std::mutex> guard(mu_);
    wi.connected = connected_;
    wi.subscribed = subscribed_;
    // F1/J1：取锁后重采 fresh_now + 重读 fresh tick 锚再决策。
    //   ① fresh tick 锚：fast-path 在 :108 无锁读 last_tick_steady_ms_ 进 wi，取锁前若有
    //      callback 落新锚，wi 里的快照即陈旧 → 用它决策会在活跃行情期误 rebuild；OnRtnDepth
    //      MarketData 在同 mu_ 内落锚，取锁后重读得 fresh 值。
    //   ② fresh_now：caller 的 now_ms 采样早于取锁，可能落在 callback 未来锚之前——若沿用它
    //      决策，uint32(now-未来锚) ≈ UINT32_MAX 会把 fresh 锚误判 ~49.7 天 stale → 误 rebuild
    //      （synthetic 锚此时必 invalid/stale，兜不住）。重采 fresh_now：steady 单调 + 同锁互斥
    //      ⇒ 凡已发布的锚 ≤ fresh_now，无未来锚。fresh_now 同时喂决策与下方 age 日志（同源）。
    // synthetic_arm_steady_ms_ 仅本线程写、无并发移动，沿用快照即可（min_valid_age 另带未来锚
    // clamp 作 defense-in-depth）。
    const uint32_t fresh_now_ms = static_cast<uint32_t>(XtSteadyClock::steady_now_ms());
    const uint32_t fresh_tick_ms = last_tick_steady_ms_.load(std::memory_order_relaxed);
    if (ctp_md_session::decide_watchdog_relocked(wi, fresh_now_ms, fresh_tick_ms)
        != ctp_md_session::WatchdogAction::Rebuild)
        return true;
    wi.now_ms = fresh_now_ms;
    wi.last_tick_ms = fresh_tick_ms;

    // stale tick 检测命中：执行 self-reconnect（release -> re-init -> start）。
    const auto age = ctp_md_session::min_valid_age(fresh_now_ms, wi.last_tick_ms, wi.synth_arm_ms);
    XtLogger::warn_m("MD", "stale tick detected ({}ms > {}ms), forcing self-reconnect",
                     age ? *age : 0u, stale_reconnect_ms_);
    // 记录本地健康检查触发的重连，便于和 SDK 主动断连区分。
    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "md", "stale_reconnect", "");
    }
    // 持锁执行 release -> init -> start，确保 SPI 回调不会在中间状态操作 api_。
    if (api_) {
        api_->Release();
        api_ = nullptr;
    }
    connected_ = false;
    logged_in_ = false;
    subscribed_ = false;
    ready_emitted_ = false;
    // 重连节流：arm 合成锚（非 tick 锚）为 now，保留现行一个 stale_reconnect_ms 冷却——
    // 否则重连完成 re-subscribed 但首 tick 未到时，下一次 1Hz 检查会立即二次 rebuild。
    // gate disabled 回滚路径同样经此保持冷却等价现行。
    synthetic_arm_steady_ms_ = (now_ms == 0) ? 1u : now_ms;
    if (!init_locked(saved_cfg_)) {
        XtLogger::error_m("MD", "stale self-reconnect: re-init failed");
        return false;
    }
    start_locked();
    return false;
}

// ── 回调绑定 ──────────────────────────────────────────────

void ParserCTP::bind_callbacks(Callbacks callbacks) {
    on_tick_ = std::move(callbacks.on_tick);
    on_ready_ = std::move(callbacks.on_ready);
    on_front_ = std::move(callbacks.on_front);
}

// ── CThostFtdcMdSpi 回调 ──────────────────────────────────────────────

void ParserCTP::OnFrontConnected() {
    std::lock_guard<std::mutex> guard(mu_);
    const uint32_t down_callbacks = disc_log_gate_.on_reconnect();
    // 新 front connection 只表示 TCP/front 层已连上，不代表本次 session 已登录/订阅。
    // ParserCTP 实例会跨断线重连复用；这里清掉上一 session 的状态，
    // 后续必须重新 login -> subscribe -> emit_ready，避免旧 ready/subscribed 状态泄漏。
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
    // front 断开后当前 CTP session 整体失效：登录态、订阅态、ready 事件都不能复用。
    // 先清内部状态，再通知上层 on_front(false)，让 XtHftChannels 将 md_ready 拉低。
    connected_ = false;
    logged_in_ = false;
    subscribed_ = false;
    ready_emitted_ = false;
    // 限频:CTP SDK 在 front 不可达时每 ~2-3s 重连一次,逐条 error 会刷爆日志。
    // 首条 error,之后整段断开期内每 60s 一条 warn,重连复位。recorder/metric 仍全量。
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
    // recorder 保留原始 nReason 便于 incident 复盘；metric label 仍保持低基数。
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
    // login ok 只代表认证完成；行情通道是否可用仍取决于后续 subscribe 应答。
    logged_in_ = true;
    // 允许空订阅列表，用于连通性冒烟测试。
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
    // 多合约订阅会产生多次响应；只有 bIsLast 表示本批订阅应答已收齐。
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
    // publish/见证 gate 与 tick 到达见证在同一 mu_ 临界区内原子完成（F1/R2-1/J2）。
    // 短锁 gate：tick 来自已订阅行情，防止 check_health 正在 release/re-init api_ 时继续
    // 向上游推送旧 session 数据。见证锚 last_tick_steady_ms_ 必须与 gate 同锁落锚——若把见证
    // 留到函数尾部（gate 解锁后）落锚，则「已过 gate、尚未落锚」的在飞 callback 会被
    // check_health slow-path 持锁读到陈旧锚而误判 rebuild（R2-1）。
    //
    // gate 收紧为 connected_ && subscribed_（J2 / should_accept_tick）：mu_ 无 FIFO——rebuild
    // 解锁后新 session OnFrontConnected 可先获锁置 connected_=true，此时若 gate 仅判 connected_，
    // 被挡的旧 session tick callback 醒来会过 gate、发布死 session tick 并用死 tick 武装见证锚
    // （压制新 session 合法 rebuild ≤ threshold）。加 subscribed_ 把窗口压到「新 session 整链
    // connect+resubscribe 完成之后」：rebuild 路径复位 subscribed_=false（check_health），
    // resubscribe ack（OnRspSubMarketData bIsLast / 空订阅 OnRspUserLogin）才置 true；订阅未
    // 完成时旧 parked callback 必见 subscribed_=false 被拒。
    //
    // 残留（有界、非全称闭合，R4 裁决接受）：若 OS 把 SDK 回调线程在锁已释放后饿死跨越新
    // session 完整 connect+resubscribe 网络往返，被挡的旧 callback 才可能在 subscribed_=true
    // 后醒来过 gate——这是调度饥饿假设，非锁序洞。后果上界可证：CTP MD SPI 为单回调线程，
    // in-flight callback 至多 1 个 ⇒ stale tick 至多 1 笔 + 见证锚污染 ≤ stale_reconnect_ms
    // 延迟，自愈；且严格优于旧 gate（仅判 connected_，connect 一完成即可穿）。完全闭合
    // （per-session SPI generation shim）留 follow-up 票，不在本 fold。锚仍是 atomic
    // （check_health fast-path 无锁读不变）；见证取入口时刻（tick 到达）而非处理完成时刻，
    // 语义更准。
    {
        std::lock_guard<std::mutex> guard(mu_);
        if (!ctp_md_session::should_accept_tick(connected_, subscribed_)) return;
        last_tick_steady_ms_.store(
            static_cast<uint32_t>(XtSteadyClock::steady_now_ms()),
            std::memory_order_relaxed);
    }
    XtRawTick t;
    XtCStringUtil::copy(t.code, p->InstrumentID);
    XtCStringUtil::copy(t.exchg, p->ExchangeID);
    // SimNow 等前置 ExchangeID 为空 → recorder tick.fbs 分片键的 exchange gate 落空、
    // fbs 流断档。按品种字母前缀兜底推导交易所（JSONL + tick.fbs + shm 下游三处同时受益）。
    // fail-open：推导不到保持空 + 按前缀 warn-once（不阻塞 tick，tick.fbs 仍 gate，行为不回归）。
    if (t.exchg[0] == '\0') {
        const char* inferred = ctp_util::infer_ctp_exchange(p->InstrumentID);
        if (inferred[0] != '\0') {
            XtCStringUtil::copy(t.exchg, inferred);
        } else {
            char prefix[16];
            ctp_util::ctp_product_prefix(p->InstrumentID, prefix, sizeof(prefix));
            if (exchg_enrich_warn_gate_.should_warn(prefix)) {
                XtLogger::warn_m("MD",
                                 "exchange_id 为空且品种前缀未收录, code={} → tick.fbs 分片跳过(fail-open)",
                                 p->InstrumentID);
            }
        }
    }

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

    // CTP 协议源头硬上限 5 档（BidPrice1..5），但 recorder schema 是 kMdDepth=20。
    // 索引 5..19 写 0：JSONL/fbs 输出与 binance 路径同 width，下游分析工具不必区分
    // venue。replay 路径若需 ladder 信号，CTP 数据 best 5 是真实，外侧零是缺口标记。
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
                          bid_px_fp, ask_px_fp, bid_q, ask_q,
                          /*depth_levels=*/5);
    }

    // 直连 venue provider：无 SHM connector capture，capture_ns 传 0（未 stamp）。
    // 注：tick 到达见证锚（last_tick_steady_ms_）已在函数入口 connected_ gate 同锁内落锚
    // （见上 F1/R2-1），此处不再重复 store。
    if (on_tick_) on_tick_(t, /*capture_ns=*/0);
}

// ── 生命周期内部步骤 ──────────────────────────────────────

bool ParserCTP::init_locked(const IMdGateway::Config& cfg) {
    saved_cfg_ = cfg;
    stale_reconnect_ms_ = cfg.stale_reconnect_ms;

    // broker_ / flow_dir_ 由 configure_ctp() 注入，是当前唯一配置路径。
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
    // CTP SDK 要求 char*（非 const），const_cast 是已知惯例。
    api_->RegisterFront(const_cast<char*>(front_.c_str()));
    api_->Init();
    return true;
}

// ── CTP 请求辅助 ──────────────────────────────────────────

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
    // CTP SDK 要求 char**（非 const），const_cast 是已知惯例。
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

// ── 回调辅助 ──────────────────────────────────────────────

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
    // ready 是 per-session 事件；重连会清 ready_emitted_，允许新 session 再次通知上层。
    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_conn_state(XtSteadyClock::steady_now_ns(), "md", "ready", "");
    }
    if (on_ready_) on_ready_();
}
