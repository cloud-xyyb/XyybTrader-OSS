#include "MdBinanceUGateway.h"

#include "BinanceAggGap.h"
#include "BinanceAggTradesFetcher.h"
#include "BinanceDepthSync.h"
#include "BinanceMdMerge.h"
#include "BinanceOrderBook.h"
#include "BinanceSnapshotFetcher.h"
#include "PerSymbolFreshnessWitness.h"
#include "transport/BinanceWsUrl.h"
#include "xt_live/gateway/common/IoContextThread.h"
#include "xt_live/gateway/common/XtWsConnection.h"
#include "xt_live/gateway/common/XtMdFreshness.h"

#ifdef XT_HAVE_METRICS
#include "xt_live/gateway/common/XtClockSkewEstimator.h"
#include "xt_metrics/XtMetricRegistry.h"
#endif

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>

#include <nlohmann/json.hpp>

#include "xt_core/market/XtRawTick.h"
#include "xt_logger/XtLogger.h"
#include "xt_live/telemetry/recording/XtMdTickRecorderUtil.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"
#include "xt_util/string/XtCStringUtil.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_core/session/XtTradingDay.h"

namespace binance_u {

namespace net       = boost::asio;
namespace ssl       = net::ssl;

namespace {

std::string to_lower(std::string_view s) {
    std::string out(s);
    for (auto& c : out) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return out;
}

std::string to_upper(std::string_view s) {
    std::string out(s);
    for (auto& c : out) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return out;
}

uint64_t steady_now_us() noexcept {
    return XtSteadyClock::steady_now_us();
}

struct BackfillGate;

/// @brief detached aggTrade-backfill 线程的自包含控制块（UAF-safe detach）。
///
/// 线程 lambda **只按值捕获本块的 shared_ptr**（禁捕 this / &SymbolState）：stop 超时
/// 放弃等待后 Impl 可先亡，线程仅持本块即无悬垂。每次 launch 建全新控制块（fresh
/// cancel flag），旧线程持旧块——per-symbol cancel flag 不被 relaunch 复位，无 lost-cancel race。
struct BackfillCtx {

    std::string symbol;
    std::string host;
    int64_t lo = 0;
    int64_t hi = 0;
    uint64_t gen = 0;

    std::atomic<bool> should_exit{false};
    std::shared_ptr<BackfillGate> gate;
};

/// @brief 全部 backfill 线程共享的 io 存活门 + 在途注册表（每 Impl 一份）。
///
/// TOCTOU 约束：detached 线程的「io 存活检查 + net::post」与 stop()
/// 的「翻 io_alive」在**同一把 mu** 下执行——检查通过即保证 post 期间 ioc 存活。
/// 注册表让 stop 与 launch 同锁互斥：要么新 launch 对 stop 可见（被 cancel + 计入
/// bounded wait），要么门已关直接不起线程——收口 stop-vs-launch race。
struct BackfillGate {
    std::mutex mu;
    bool io_alive = true;
    boost::asio::io_context* ioc = nullptr;
    void* owner = nullptr;
    std::vector<std::weak_ptr<BackfillCtx>> ctxs;
    std::atomic<int> inflight{0};
};

constexpr int64_t kBackfillStopWaitMs = 2000;

/// @brief 单个 symbol 的盘口与 recovery 状态（一连接订多 symbol 时各持一份）。
///
/// 本结构字段由 ws_thread（io_context 线程）串行访问；跨线程部分已组件化：
/// freshness witness（内部 atomic，ws_thread 写 / check_health 控制线程读）与
/// backfill 控制块（shared_ptr，detached 线程只持块不持本结构）。snapshot 拉取由
/// 常驻 snapshot worker 线程做，但 book/diff_buffer 的变更一律 net::post 回
/// ws_thread（单写者，depth_ 由 ws_thread 串行驱动）。
/// 容器用 `vector<SymbolState>` + `reserve` 锁地址，使 recovery/worker 线程可安全
/// 持有 `SymbolState*`（map rehash 会让 value 地址悬空）。
/// move 语义：states_ reserve 后只 index 访问、运行期从不重排，vector::reserve 仅在
/// 编译期要求可 move；成员均可 move（witness 显式实现），default 即可。复制禁止。
struct SymbolState {

    explicit SymbolState(std::string sym) : symbol(std::move(sym)) {}

    SymbolState(SymbolState&&) noexcept = default;
    SymbolState& operator=(SymbolState&&) = delete;
    SymbolState(const SymbolState&) = delete;
    SymbolState& operator=(const SymbolState&) = delete;

    std::string symbol;

    BinanceDepthSync depth_;

    double last_price = 0.0;
    double total_volume = 0.0;

    FreshL1 fresh_l1_;

    bool mark_seed_pending_ = false;

    int64_t last_agg_id_ = 0;
    int64_t last_agg_gap_log_ms_ = 0;
    bool reconnect_pending_ = false;
    bool agg_backfill_in_flight_ = false;
    uint64_t agg_backfill_gen_ = 0;

    std::shared_ptr<BackfillCtx> agg_backfill_ctx_;

    PerSymbolFreshnessWitness freshness_;
};

}

// ===========================================================================
// MdBinanceUGateway::Impl — 所有状态与逻辑
// ===========================================================================
class MdBinanceUGateway::Impl {
public:
    Impl() {
        backfill_gate_ = std::make_shared<BackfillGate>();
        backfill_gate_->ioc   = &io_ctx_.ioc();
        backfill_gate_->owner = this;
    }

    ~Impl() { stop(); }

    void configure(const BinanceMdCfg& cfg) { binance_cfg = cfg; }

    bool init(const IMdGateway::Config& cfg) {

        if (!states_.empty()) {
            XtLogger::error_m("BinanceMD", "init 重复调用（已 init），拒绝");
            return false;
        }
        common_cfg = cfg;
        if (common_cfg.subs.empty()) {
            XtLogger::error_m("BinanceMD", "subs 为空，无法订阅行情");
            return false;
        }
        if (binance_cfg.base_ws_url.empty()) {
            XtLogger::error_m("BinanceMD", "binance.base_ws_url 为空");
            return false;
        }

        if (binance_cfg.base_rest_url.empty()) {
            XtLogger::error_m("BinanceMD",
                "binance.base_rest_url 为空 — depth snapshot 无法拉取；"
                "demo 行情环境填 https://fapi.binance.com");
            return false;
        }
        auto ws_parsed = parse_binance_ws_url(binance_cfg.base_ws_url, "443");
        auto rest_parsed = parse_binance_ws_url(binance_cfg.base_rest_url, "443");
        snapshot_host = rest_parsed.host;

        states_.reserve(common_cfg.subs.size());
        symbol_index_.clear();

        std::string streams;
        for (size_t i = 0; i < common_cfg.subs.size(); ++i) {
            const std::string& sym = common_cfg.subs[i];
            states_.emplace_back(sym);
            symbol_index_[sym] = i;
            const std::string s = to_lower(sym);
            if (!streams.empty()) streams += "/";
            streams += s + "@depth/" + s + "@aggTrade/" + s + "@bookTicker";
        }
        ws_target = "/stream?streams=" + streams;
        ws_host   = ws_parsed.host;
        ws_port   = ws_parsed.port;

        const size_t stream_count = common_cfg.subs.size() * 3;
        if (stream_count > 1024) {
            XtLogger::error_m("BinanceMD",
                "combined stream 数 {} 超 binance 单连接上限 1024（{} symbol × 3 channel）；"
                "需 connection sharding，首版不支持",
                stream_count, common_cfg.subs.size());
            return false;
        }

        if (ws_target.size() > 6000) {
            XtLogger::warn_m("BinanceMD",
                "combined stream URL 偏长（{} 字符，{} stream）；接近 binance URL 长度上限，"
                "继续启动但请留意连接稳定性",
                ws_target.size(), stream_count);
        }

        std::string subs_csv;
        for (const auto& s : common_cfg.subs) {
            if (!subs_csv.empty()) subs_csv += ",";
            subs_csv += s;
        }
        XtLogger::info_m("BinanceMD",
            "price 量化精度：kPriceScale={}（最细档 {}）；订阅 {} symbol [{}]，{} stream。"
            "细 tick alt（tickSize<{}）会塌档，首版仅接 tick≥0.01 主流对",
            BinanceOrderBook::kPriceScale,
            1.0 / static_cast<double>(BinanceOrderBook::kPriceScale),
            common_cfg.subs.size(), subs_csv, stream_count,
            1.0 / static_cast<double>(BinanceOrderBook::kPriceScale));

        XtLogger::info_m("BinanceMD", "init ok: ws=wss://{}:{}{}, snapshot_host={}",
                         ws_host, ws_port, ws_target, snapshot_host);
        return true;
    }

    bool start() {
        if (running.exchange(true)) return true;

        snapshot_should_exit_.store(false, std::memory_order_release);
        snapshot_worker_thread_ = std::thread([this] { snapshot_worker_loop_(); });

        io_ctx_.start([this] { start_connection(); });
        return true;
    }

    void stop() {
        const bool was_running = running.exchange(false);
        if (was_running) {

            snapshot_should_exit_.store(true, std::memory_order_release);
            snapshot_queue_cv_.notify_all();
        }

        shutdown_backfills_();
        if (!was_running) return;

        io_ctx_.stop([this] { if (ws_connection) ws_connection->stop(); },
                      false);

        XtLogger::warn_m("BinanceMD",
            "ws_thread: ioc.run() exited; running={}", running.load());
        XtLogger::info_m("BinanceMD", "ws_thread: clean exit");
        snapshot_queue_cv_.notify_all();
        if (snapshot_worker_thread_.joinable()) snapshot_worker_thread_.join();
    }

    void shutdown_backfills_() {
        std::vector<std::shared_ptr<BackfillCtx>> live;
        {
            std::lock_guard<std::mutex> lk(backfill_gate_->mu);
            backfill_gate_->io_alive = false;
            for (auto& w : backfill_gate_->ctxs) {
                if (auto c = w.lock()) live.push_back(std::move(c));
            }
            backfill_gate_->ctxs.clear();
        }
        for (auto& c : live) c->should_exit.store(true, std::memory_order_release);
        const int64_t deadline_ms =
            static_cast<int64_t>(XtSteadyClock::steady_now_ms()) + kBackfillStopWaitMs;
        while (backfill_gate_->inflight.load(std::memory_order_acquire) > 0
               && static_cast<int64_t>(XtSteadyClock::steady_now_ms()) < deadline_ms) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        const int remaining = backfill_gate_->inflight.load(std::memory_order_acquire);
        if (remaining > 0) {
            XtLogger::error_m("BinanceMD",
                "stop: {} 个 aggTrade backfill 线程 {}ms 内未退出，放弃等待"
                "（detached，仅持控制块，无悬垂）",
                remaining, kBackfillStopWaitMs);
        }
    }

    void bind_callbacks(IMdGateway::Callbacks callbacks) { cb = std::move(callbacks); }

    bool check_health(uint32_t now_ms) {

        const uint64_t now64 = static_cast<uint64_t>(now_ms);
        const auto sweep = sweep_md_freshness(
            states_.size(),
            [this](size_t i) -> const PerSymbolFreshnessWitness& {
                return states_[i].freshness_;
            },
            now64, stale_last_close_ms_, common_cfg.stale_reconnect_ms,
             15000);

        if (sweep.any_should_reconnect) {

            const uint64_t anchor =
                std::max(sweep.trigger_last_tick, sweep.trigger_last_ready);
            const uint64_t delta  = now64 - anchor;
            stale_last_close_ms_ = static_cast<uint32_t>(now64);
            XtLogger::warn_m("BinanceMD",
                             "stale symbol={} anchor {}ms > {}ms (last_tick={}ms last_ready={}ms)，触发 ws reconnect",
                             states_[sweep.trigger_index].symbol,
                             delta, common_cfg.stale_reconnect_ms,
                             sweep.trigger_last_tick, sweep.trigger_last_ready);
            net::post(io_ctx_.ioc(), [this] {
                if (ws_connection) ws_connection->request_reconnect();
            });
            return false;
        }

        return !sweep.any_throttled;
    }

    void start_connection() {
        xt_live::gateway::XtWsConnection::Config cfg;
        cfg.host = ws_host;
        cfg.port = ws_port;
        cfg.target = ws_target;
        cfg.log_tag = "BinanceMD";
        cfg.stale_threshold_ms = common_cfg.stale_reconnect_ms;
        ws_connection = std::make_unique<xt_live::gateway::XtWsConnection>(
            io_ctx_.ioc(), io_ctx_.ssl_ctx(), std::move(cfg));
        ws_connection->bind_callbacks(
            [this](std::string_view sv) { handle_message(sv); },
            [this] {
                XtLogger::info_m("BinanceMD", "ws connected: {}{}", ws_host, ws_target);

                for (auto& st : states_) enqueue_snapshot_(st);
            },
            [this] { on_connection_disconnect(); });
        ws_connection->start();
    }

    void on_connection_disconnect() {

        for (auto& st : states_) {

            st.depth_.reset_for_reconnect();

            st.fresh_l1_ = FreshL1{};
            st.mark_seed_pending_ = false;

            ++st.agg_backfill_gen_;
            if (st.agg_backfill_ctx_) {
                st.agg_backfill_ctx_->should_exit.store(true, std::memory_order_release);
                st.agg_backfill_ctx_.reset();
            }
            st.agg_backfill_in_flight_ = false;
            st.reconnect_pending_ = true;
        }

#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_gateway_disconnect_total(
            "binance_u", "md", "ws_close");
        xt_live::gateway::XtClockSkewEstimator::shared("binance_u", "depth")
            .on_ws_reconnect("depth");
        xt_live::gateway::XtClockSkewEstimator::shared("binance_u", "aggTrade")
            .on_ws_reconnect("aggTrade");
        xt_live::gateway::XtClockSkewEstimator::shared("binance_u", "bookTicker")
            .on_ws_reconnect("bookTicker");
#endif
    }

    SymbolState& state_for(const std::string& symbol) {
        const auto it = symbol_index_.find(symbol);

        assert(it != symbol_index_.end() && "state_for: unknown symbol");
        return states_[it->second];
    }

    SymbolState* state_for_ptr(const std::string& symbol) {
        const auto it = symbol_index_.find(symbol);
        if (it == symbol_index_.end()) return nullptr;
        return &states_[it->second];
    }

    void handle_message(std::string_view payload) {
        const uint64_t recv_mono_us = steady_now_us();
        auto j = nlohmann::json::parse(payload, nullptr,  false);
        if (j.is_discarded()) return;

        if (!j.contains("stream") || !j.contains("data")) return;
        const auto& stream = j["stream"];
        const auto& data = j["data"];
        if (!stream.is_string()) return;

        SymbolState* stp = resolve_symbol_state(data);
        if (stp == nullptr) return;
        SymbolState& st = *stp;

        const std::string& stream_name = stream.get_ref<const std::string&>();

        if (stream_name.size() > 6 && stream_name.compare(stream_name.size() - 6, 6, "@depth") == 0) {
            ++cnt_depth_;
            observe_md_latency("depth", data, recv_mono_us);
            handle_depth(st, data);
        } else if (stream_name.find("@aggTrade") != std::string::npos) {
            ++cnt_aggtrade_;
            observe_md_latency("aggTrade", data, recv_mono_us);
            handle_agg_trade(st, data);
        } else if (stream_name.find("@bookTicker") != std::string::npos) {
            ++cnt_bookticker_;
            observe_md_latency("bookTicker", data, recv_mono_us);
            handle_book_ticker(st, data);
        }
        maybe_log_rates();
    }

    SymbolState* resolve_symbol_state(const nlohmann::json& data) {
        const auto it = data.find("s");
        if (it == data.end() || !it->is_string()) {
            note_drop_unknown_symbol_("<missing s>");
            return nullptr;
        }
        std::string symbol = to_upper(it->get_ref<const std::string&>());
        SymbolState* stp = state_for_ptr(symbol);
        if (stp == nullptr) note_drop_unknown_symbol_(symbol);
        return stp;
    }

    void note_drop_unknown_symbol_(const std::string& symbol) {
        const auto now_ms = XtSteadyClock::steady_now_ms();
        if (now_ms - last_unknown_symbol_log_ms_ < 5000) return;
        last_unknown_symbol_log_ms_ = now_ms;
        XtLogger::warn_m("BinanceMD",
            "drop md msg: symbol '{}' 未订阅或缺 `s` 字段（不 fallback）", symbol);
    }

    void observe_md_latency(const char* channel,
                            const nlohmann::json& data,
                            uint64_t recv_mono_us) {
#ifdef XT_HAVE_METRICS
        uint64_t event_ms = 0;
        uint64_t transaction_ms = 0;
        try {
            event_ms = data.value("E", 0ULL);
            transaction_ms = data.value("T", 0ULL);
        } catch (...) {
            return;
        }
        if (event_ms == 0) return;

        auto& reg = ct::metrics::XtMetricRegistry::instance();
        const uint64_t event_us = event_ms * 1000ULL;
        auto& skew = xt_live::gateway::XtClockSkewEstimator::shared(
            "binance_u", channel);
        skew.record_sample(recv_mono_us, event_us);
        skew.publish_state(channel);

        const double offset_us = skew.offset_us();
        if (!std::isfinite(offset_us)) return;
        const double corrected_latency =
            static_cast<double>(recv_mono_us) + offset_us
            - static_cast<double>(event_us);
        if (corrected_latency < 0.0) {
            reg.inc_md_negative_latency(channel, 1);
            reg.observe_md_event_to_recv_us(channel, 0);
        } else {
            reg.observe_md_event_to_recv_us(
                channel, static_cast<uint64_t>(corrected_latency));
        }

        if (transaction_ms > 0 && event_ms >= transaction_ms) {
            reg.observe_md_internal_queue_us(
                channel, (event_ms - transaction_ms) * 1000ULL);
        }
#else
        (void)channel;
        (void)data;
        (void)recv_mono_us;
#endif
    }

    void maybe_log_rates() {
        const auto now_ms = XtSteadyClock::steady_now_ms();
        if (rate_log_last_ms_ == 0) { rate_log_last_ms_ = now_ms; return; }
        if (now_ms - rate_log_last_ms_ < 5000) return;
        const double secs = (now_ms - rate_log_last_ms_) / 1000.0;
        XtLogger::info_m("BinanceMD",
            "rate(5s): bookTicker={:.1f}/s aggTrade={:.1f}/s depth={:.1f}/s tick_push={:.1f}/s",
            cnt_bookticker_ / secs, cnt_aggtrade_ / secs, cnt_depth_ / secs, cnt_tick_push_ / secs);
        cnt_bookticker_ = cnt_aggtrade_ = cnt_depth_ = cnt_tick_push_ = 0;
        rate_log_last_ms_ = now_ms;
    }

    static bool parse_level_array(const nlohmann::json& arr,
                                  std::vector<std::pair<double, double>>& out) {
        out.clear();
        if (!arr.is_array()) return false;
        out.reserve(arr.size());
        for (const auto& lv : arr) {
            if (!lv.is_array() || lv.size() < 2) return false;
            try {
                const double price = std::stod(lv[0].get<std::string>());
                const double qty   = std::stod(lv[1].get<std::string>());

                if (!std::isfinite(price) || !std::isfinite(qty)) return false;
                out.emplace_back(price, qty);
            } catch (...) { return false; }
        }
        return true;
    }

    void handle_depth(SymbolState& st, const nlohmann::json& data) {
        BinanceDepthSync::Diff d;
        try {
            d.U  = data.value("U", 0ULL);
            d.u  = data.value("u", 0ULL);
            d.pu = data.value("pu", 0ULL);
        } catch (...) { return; }
        if (data.contains("b") && !parse_level_array(data["b"], d.bids)) return;
        if (data.contains("a") && !parse_level_array(data["a"], d.asks)) return;

        const uint64_t U = d.U, u = d.u, pu = d.pu;
        BinanceDepthSync::ResnapshotCause cause{};
        uint64_t pre_reset_update_id = 0;
        const auto res = st.depth_.on_diff(std::move(d), &cause, &pre_reset_update_id);
        if (res == BinanceDepthSync::DiffResult::NeedResnapshot) {
            if (cause == BinanceDepthSync::ResnapshotCause::BufferOverflow) {
                XtLogger::warn_m("BinanceMD",
                    "depth diff_buffer 超上限({}) symbol={}（snapshot 卡住？），reset + re-snapshot",
                    BinanceDepthSync::kMaxDiffBuffer, st.symbol);
            } else {

                XtLogger::warn_m("BinanceMD",
                                 "depth gap detected symbol={} (U={}, u={}, pu={}, last_update_id={})，re-snapshot",
                                 st.symbol, U, u, pu, pre_reset_update_id);
            }
            enqueue_snapshot_(st);
        }

    }

    void handle_agg_trade(SymbolState& st, const nlohmann::json& data) {
        uint64_t ts_exch_ms = 0;
        try {
            const double p = std::stod(data["p"].get<std::string>());
            const double q = std::stod(data["q"].get<std::string>());

            if (!std::isfinite(p) || !std::isfinite(q)) return;

            const int64_t agg_id = static_cast<int64_t>(data.value("a", 0LL));
            const int64_t missed = agg_gap_missed(st.last_agg_id_, agg_id);
            if (missed > 0) {
                note_agg_gap_(st, missed, agg_id);

                if (st.reconnect_pending_) launch_agg_backfill_(st, st.last_agg_id_ + 1, agg_id);
            }
            st.reconnect_pending_ = false;
            st.last_price = p;
            st.total_volume += q;
            if (agg_id > st.last_agg_id_) st.last_agg_id_ = agg_id;
            ts_exch_ms = data.value("E", 0ULL);
        } catch (...) { return; }
        if (st.depth_.is_live()) push_tick(st, "aggTrade", ts_exch_ms);
    }

    void note_agg_gap_(SymbolState& st, int64_t missed, int64_t agg_id) {
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance().inc_md_aggtrade_gap("aggTrade");
#endif
        const auto now_ms = XtSteadyClock::steady_now_ms();
        if (now_ms - st.last_agg_gap_log_ms_ >= 5000) {
            st.last_agg_gap_log_ms_ = now_ms;
            XtLogger::warn_m("BinanceMD",
                "aggTrade gap: last_agg_id={} -> {} (missed ~{})，疑重连漏成交",
                st.last_agg_id_, agg_id, missed);
        }
    }

    void launch_agg_backfill_(SymbolState& st, int64_t lo, int64_t hi) {
        if (lo >= hi || common_cfg.subs.empty()) return;
        if (st.agg_backfill_in_flight_) return;
        auto ctx = std::make_shared<BackfillCtx>();
        ctx->symbol = st.symbol;
        ctx->host   = snapshot_host;
        ctx->lo     = lo;
        ctx->hi     = hi;
        ctx->gen    = st.agg_backfill_gen_;
        ctx->gate   = backfill_gate_;
        {

            std::lock_guard<std::mutex> lk(backfill_gate_->mu);
            if (!backfill_gate_->io_alive) return;
            auto& ctxs = backfill_gate_->ctxs;
            ctxs.erase(std::remove_if(ctxs.begin(), ctxs.end(),
                                      [](const std::weak_ptr<BackfillCtx>& w) {
                                          return w.expired();
                                      }),
                       ctxs.end());
            ctxs.push_back(ctx);
            backfill_gate_->inflight.fetch_add(1, std::memory_order_acq_rel);
        }
        st.agg_backfill_in_flight_ = true;
        st.agg_backfill_ctx_ = ctx;
        XtLogger::info_m("BinanceMD",
                         "aggTrade backfill launch: fromId=[{}, {}) symbol={}", lo, hi, ctx->symbol);
        std::thread([ctx] {
            // stop 的 bounded wait 依赖 in-flight 归零；RAII 保证任何退出路径都递减。
            struct InflightGuard {
                std::shared_ptr<BackfillGate> gate;
                ~InflightGuard() { gate->inflight.fetch_sub(1, std::memory_order_acq_rel); }
            } inflight_guard{ctx->gate};

            BinanceAggTradesFetcher fetcher({ctx->host, "443", 10});
            double total_qty = 0.0;
            int total_n = 0, pages = 0;
            int64_t from_id = ctx->lo;
            bool failed = false, truncated = false;
            constexpr int kMaxPages = 50;
            while (from_id < ctx->hi
                   && !ctx->should_exit.load(std::memory_order_acquire)) {
                if (pages >= kMaxPages) { truncated = true; break; }
                std::vector<AggTrade> page;
                std::string err;
                if (!fetcher.fetch_sync(ctx->symbol, from_id, 1000,
                                        &ctx->should_exit, &page, &err)) {
                    XtLogger::warn_m("BinanceMD",
                        "aggTrade backfill fetch failed fromId={}: {}", from_id, err);
                    failed = true;
                    break;
                }
                if (page.empty()) break;
                const auto res = process_agg_backfill_page(page, ctx->lo, ctx->hi);
                total_qty += res.accounted_qty;
                total_n += res.accounted_n;
                if (res.max_a < from_id) break;
                from_id = res.max_a + 1;
                ++pages;
                if (res.reached_hi) break;
            }
            if (ctx->should_exit.load(std::memory_order_acquire)) return;

            std::lock_guard<std::mutex> lk(ctx->gate->mu);
            if (!ctx->gate->io_alive) return;
            net::post(*ctx->gate->ioc, [ctx, total_qty, total_n, failed, truncated] {

                auto* impl = static_cast<MdBinanceUGateway::Impl*>(ctx->gate->owner);
                SymbolState* stp = impl->state_for_ptr(ctx->symbol);
                if (stp == nullptr) return;
                if (ctx->gen != stp->agg_backfill_gen_) return;
                impl->apply_agg_backfill_(*stp, total_qty, total_n, failed, truncated);
            });
        }).detach();
    }

    void apply_agg_backfill_(SymbolState& st, double total_qty, int total_n, bool failed, bool truncated) {
        st.agg_backfill_in_flight_ = false;
        st.total_volume += total_qty;
#ifdef XT_HAVE_METRICS
        auto& reg = ct::metrics::XtMetricRegistry::instance();
        if (total_n > 0) reg.inc_md_aggtrade_backfill("replayed", static_cast<uint64_t>(total_n));
        if (failed)      reg.inc_md_aggtrade_backfill("failed", 1);
        if (truncated)   reg.inc_md_aggtrade_backfill("truncated", 1);
#endif
        XtLogger::info_m("BinanceMD",
            "aggTrade backfill done: replayed={} qty={} failed={} truncated={}",
            total_n, total_qty, failed, truncated);
    }

    void handle_book_ticker(SymbolState& st, const nlohmann::json& data) {

        parse_fresh_l1(st, data);
        if (!st.depth_.is_live()) return;

        const bool push_quote =
            binance_cfg.tick_source == BinanceMdCfg::TickSource::TradeAndQuote
            || st.mark_seed_pending_;
        if (push_quote) {
            st.mark_seed_pending_ = false;
            const uint64_t ts_exch_ms = data.value("E", 0ULL);
            push_tick(st, "bookTicker", ts_exch_ms);
        }
    }

    void parse_fresh_l1(SymbolState& st, const nlohmann::json& data) {
        try {
            FreshL1 l1;
            l1.bid_px  = std::stod(data.at("b").get<std::string>());
            l1.bid_qty = std::stod(data.at("B").get<std::string>());
            l1.ask_px  = std::stod(data.at("a").get<std::string>());
            l1.ask_qty = std::stod(data.at("A").get<std::string>());

            if (!std::isfinite(l1.bid_px) || !std::isfinite(l1.bid_qty) ||
                !std::isfinite(l1.ask_px) || !std::isfinite(l1.ask_qty)) {
                return;
            }
            l1.valid   = true;
            st.fresh_l1_ = l1;
        } catch (...) {

        }
    }

    void push_tick(SymbolState& st, const char*  , uint64_t ts_exch_ms) {
        ++cnt_tick_push_;
        XtRawTick t;

        XtCStringUtil::copy(t.code, st.symbol.c_str());
        XtCStringUtil::copy(t.exchg, "BINANCE");
        t.price = st.last_price;
        t.total_volume = st.total_volume;

        t.action_date = xt_core::derive_trading_day(
            static_cast<int64_t>(ts_exch_ms), xt_core::TimezonePolicy::UtcNaturalDay);
        t.action_time = xt_core::derive_action_time(
            static_cast<int64_t>(ts_exch_ms), xt_core::TimezonePolicy::UtcNaturalDay);

        constexpr int kRecDepth = XtLiveRecorder::kMdDepth;
        static_assert(XtRawTick::DEPTH <= kRecDepth,
                      "engine DEPTH must not exceed recorder kMdDepth — recorder is the wider sink");
        double bps[kRecDepth], bqs[kRecDepth], aps[kRecDepth], aqs[kRecDepth];
        st.depth_.book().top_n(kRecDepth, bps, bqs, aps, aqs);

        overlay_fresh_l1(st.fresh_l1_, kRecDepth, bps, bqs, aps, aqs);
        for (int i = 0; i < XtRawTick::DEPTH; ++i) {
            t.bid_prices[i] = bps[i];
            t.bid_qty[i]    = bqs[i];
            t.ask_prices[i] = aps[i];
            t.ask_qty[i]    = aqs[i];
        }

        if (t.price <= 0.0 && bps[0] > 0.0 && aps[0] > 0.0) {
            t.price = (bps[0] + aps[0]) / 2.0;
        }

        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();

        if (auto* rec = XtLiveRecorder::global()) {

            const int64_t last_fp = xt_live_recorder_util::to_recorder_fp(t.price);
            int64_t bid_px_fp[kRecDepth], ask_px_fp[kRecDepth], bid_q[kRecDepth], ask_q[kRecDepth];
            xt_live_recorder_util::fill_md_depth_fp(bps, aps, bqs, aqs, kRecDepth,
                                                    bid_px_fp, ask_px_fp, bid_q, ask_q);

            rec->emit_md_tick(ts_local_ns,
                              t.action_date, t.action_date, t.action_time,
                              t.code, t.exchg,
                              last_fp, xt_live_recorder_util::to_recorder_fp(st.total_volume),
                              bid_px_fp, ask_px_fp, bid_q, ask_q);
        }

        st.freshness_.note_tick(static_cast<uint32_t>(ts_local_ns / 1'000'000));
        if (cb.on_tick) cb.on_tick(t);
    }

    void enqueue_snapshot_(SymbolState& st) {
        if (!st.depth_.begin_await()) return;
        const size_t idx = symbol_index_.at(st.symbol);
        {
            std::lock_guard<std::mutex> lk(snapshot_queue_mu_);
            snapshot_queue_.push_back(idx);
        }
        snapshot_queue_cv_.notify_one();
        XtLogger::info_m("BinanceMD", "enqueue snapshot: symbol={}", st.symbol);
    }

    void snapshot_worker_loop_() {
        while (true) {
            size_t idx = 0;
            {
                std::unique_lock<std::mutex> lk(snapshot_queue_mu_);
                snapshot_queue_cv_.wait(lk, [this] {
                    return !snapshot_queue_.empty()
                           || snapshot_should_exit_.load(std::memory_order_acquire);
                });
                if (snapshot_should_exit_.load(std::memory_order_acquire)) return;
                idx = snapshot_queue_.front();
                snapshot_queue_.pop_front();
            }
            fetch_one_snapshot_(idx);
        }
    }

    void fetch_one_snapshot_(size_t idx) {
        SymbolState& st = states_[idx];
        const std::string symbol = st.symbol;
        const std::string host = snapshot_host;
        XtLogger::info_m("BinanceMD", "snapshot worker: fetching symbol={} host={}", symbol, host);
        BinanceSnapshotFetcher::Config fc;
        fc.host = host;
        fc.port = "443";
        fc.timeout_sec = 10;
        BinanceSnapshotFetcher fetcher(fc);
        std::string err;

        auto snap = fetcher.fetch_sync(symbol,  1000, &err, &snapshot_should_exit_);
        XtLogger::info_m("BinanceMD", "snapshot fetch done: symbol={} ok={} err='{}'",
                         symbol, (snap ? "yes" : "no"), err);

        if (snapshot_should_exit_.load(std::memory_order_acquire)) {
            XtLogger::info_m("BinanceMD", "snapshot worker exiting early (shutdown signaled)");
            return;
        }
        if (!snap) {

            XtLogger::warn_m("BinanceMD", "snapshot fetch failed symbol={}: {}，1s 后重入队重试",
                             symbol, err);
            {
                std::unique_lock<std::mutex> lk(snapshot_queue_mu_);
                snapshot_queue_cv_.wait_for(lk, std::chrono::seconds(1), [this] {
                    return snapshot_should_exit_.load(std::memory_order_acquire);
                });
                if (snapshot_should_exit_.load(std::memory_order_acquire)) return;
                snapshot_queue_.push_back(idx);
            }
            snapshot_queue_cv_.notify_one();
            return;
        }

        net::post(io_ctx_.ioc(), [this, &st, snap = std::move(snap)]() mutable {
            on_snapshot_ready(st, std::move(snap));
        });
    }

    void on_snapshot_ready(SymbolState& st, std::optional<DepthSnapshot> snap) {
        if (!snap) {

            enqueue_snapshot_(st);
            return;
        }
        BinanceDepthSync::DrainStats stats;
        const auto res = st.depth_.on_snapshot(*snap, &stats);
        XtLogger::info_m("BinanceMD", "snapshot applied: symbol={} lastUpdateId={}, bids={}, asks={}",
                         st.symbol, snap->last_update_id, stats.bid_levels, stats.ask_levels);
        if (res == BinanceDepthSync::SnapshotResult::NeedResnapshot) {

            enqueue_snapshot_(st);
            XtLogger::warn_m("BinanceMD",
                             "snapshot drain mid-gap symbol={}: stale={}, applied={}, retry",
                             st.symbol, stats.stale_dropped, stats.applied);
            return;
        }

        st.freshness_.note_ready(static_cast<uint32_t>(XtSteadyClock::steady_now_ms()));

        st.mark_seed_pending_ = true;

        if (!on_ready_fired_) {
            on_ready_fired_ = true;
            if (cb.on_ready) cb.on_ready();
        }
        XtLogger::info_m("BinanceMD", "book ready: symbol={} stale_dropped={}, applied={}, gapped={}",
                         st.symbol, stats.stale_dropped, stats.applied, stats.gapped);
    }

    BinanceMdCfg binance_cfg;
    IMdGateway::Config common_cfg;
    IMdGateway::Callbacks cb;

    xt_live::gateway::IoContextThread io_ctx_;
    std::atomic<bool> running{false};

    std::atomic<bool> snapshot_should_exit_{false};

    std::thread snapshot_worker_thread_;
    std::deque<size_t> snapshot_queue_;
    std::mutex snapshot_queue_mu_;
    std::condition_variable snapshot_queue_cv_;

    std::unique_ptr<xt_live::gateway::XtWsConnection> ws_connection;

    std::string ws_host;
    std::string ws_port;
    std::string ws_target;
    std::string snapshot_host;

    std::vector<SymbolState> states_;
    std::unordered_map<std::string, size_t> symbol_index_;

    std::shared_ptr<BackfillGate> backfill_gate_;

    bool on_ready_fired_ = false;

    uint64_t cnt_bookticker_ = 0;
    uint64_t cnt_aggtrade_   = 0;
    uint64_t cnt_depth_      = 0;
    uint64_t cnt_tick_push_  = 0;
    int64_t  rate_log_last_ms_ = 0;
    int64_t  last_unknown_symbol_log_ms_ = 0;

    uint32_t stale_last_close_ms_ = 0;

#ifdef XT_BUILD_TESTS
public:

    void test_make_book_ready(const std::string& symbol) {
        SymbolState* st = state_for_ptr(to_upper(symbol));
        if (st == nullptr) return;
        DepthSnapshot snap;
        snap.last_update_id = 1;
        snap.bids = {{100.0, 1.0}};
        snap.asks = {{101.0, 1.0}};
        on_snapshot_ready(*st, std::move(snap));
    }

    void test_disconnect() { on_connection_disconnect(); }

    int test_backfill_inflight() const {
        return backfill_gate_->inflight.load(std::memory_order_acquire);
    }
#endif
};

MdBinanceUGateway::MdBinanceUGateway()
    : impl_(std::make_unique<Impl>()) {}

MdBinanceUGateway::~MdBinanceUGateway() = default;

void MdBinanceUGateway::configure_binance(const BinanceMdCfg& cfg) { impl_->configure(cfg); }

bool MdBinanceUGateway::init(const Config& cfg) { return impl_->init(cfg); }
bool MdBinanceUGateway::start() { return impl_->start(); }
void MdBinanceUGateway::stop() { impl_->stop(); }

void MdBinanceUGateway::bind_callbacks(Callbacks callbacks) {
    impl_->bind_callbacks(std::move(callbacks));
}

bool MdBinanceUGateway::check_health(uint32_t now_ms) { return impl_->check_health(now_ms); }

#ifdef XT_BUILD_TESTS

void* MdBinanceUGateway::_test_impl_void() noexcept { return static_cast<void*>(impl_.get()); }

void md_binance_test_inject_message(void* impl_void, const char* payload, unsigned long len) {
    auto* impl = static_cast<MdBinanceUGateway::Impl*>(impl_void);
    impl->handle_message(std::string_view(payload, static_cast<size_t>(len)));
}

void md_binance_test_make_book_ready(void* impl_void, const char* symbol) {
    auto* impl = static_cast<MdBinanceUGateway::Impl*>(impl_void);
    impl->test_make_book_ready(symbol);
}

void md_binance_test_disconnect(void* impl_void) {
    auto* impl = static_cast<MdBinanceUGateway::Impl*>(impl_void);
    impl->test_disconnect();
}

int md_binance_test_backfill_inflight(void* impl_void) {
    auto* impl = static_cast<MdBinanceUGateway::Impl*>(impl_void);
    return impl->test_backfill_inflight();
}
#endif

}
