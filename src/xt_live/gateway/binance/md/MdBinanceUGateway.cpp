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

// gateway / MD latency metrics；XT_HAVE_METRICS 由 CMake 在 xt_metrics 存在时开启。
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
    // ── 线程只读的值拷贝（launch 时定格）──────────────────────────
    std::string symbol;
    std::string host;
    int64_t lo = 0;
    int64_t hi = 0;
    uint64_t gen = 0;  ///< launch 时的 agg_backfill_gen_；post 回 io 后比对弃旧。
    /// per-launch cancel flag：disconnect / stop 置位；线程 load + curl progress abort。
    std::atomic<bool> should_exit{false};
    std::shared_ptr<BackfillGate> gate;  ///< io 存活门 + in-flight 计数（Impl 级共享）。
};

/// @brief 全部 backfill 线程共享的 io 存活门 + 在途注册表（每 Impl 一份）。
///
/// TOCTOU 约束：detached 线程的「io 存活检查 + net::post」与 stop()
/// 的「翻 io_alive」在**同一把 mu** 下执行——检查通过即保证 post 期间 ioc 存活。
/// 注册表让 stop 与 launch 同锁互斥：要么新 launch 对 stop 可见（被 cancel + 计入
/// bounded wait），要么门已关直接不起线程——收口 stop-vs-launch race。
struct BackfillGate {
    std::mutex mu;
    bool io_alive = true;  ///< stop 在拆 ioc 前持 mu 翻 false；一次性不复位（生命周期 init→start→stop 一次性）。
    boost::asio::io_context* ioc = nullptr;  ///< 只在 mu 下且 io_alive==true 时取用（post 目标）。
    void* owner = nullptr;  ///< Impl*（私有嵌套类型，此处用 void*）；只在 io 线程内 cast + deref。
    std::vector<std::weak_ptr<BackfillCtx>> ctxs;  ///< 在途 launch 注册表（mu 保护）。
    std::atomic<int> inflight{0};  ///< 在跑线程数；stop bounded wait 至归零。
};

/// stop() 对 detached backfill 线程的有界等待上限。cancel 经 curl progress abort
/// 通常 ~ms~1s 级返回；超时即 loud log 放弃（线程只持控制块，无悬垂）。
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
    /// @param sym 大写归一后的 symbol（如 "BTCUSDT"）；XtRawTick.code 直接取此。
    explicit SymbolState(std::string sym) : symbol(std::move(sym)) {}

    SymbolState(SymbolState&&) noexcept = default;
    SymbolState& operator=(SymbolState&&) = delete;
    SymbolState(const SymbolState&) = delete;
    SymbolState& operator=(const SymbolState&) = delete;

    std::string symbol;  ///< 本 SymbolState 对应的 venue symbol（push_tick 写入 t.code）。

    /// depth 同步显式状态机（book + diff_buffer + awaiting 收编，ws_thread 串行驱动）。
    BinanceDepthSync depth_;

    // ── tick cache 与 L1 鲜值（ws_thread 私有）─────────────────────
    double last_price = 0.0;
    double total_volume = 0.0;
    /// bookTicker 自带最优盘口鲜值；push_tick 用它覆盖 tick 的 L1。
    FreshL1 fresh_l1_;
    /// trade-only 模式：book-ready 后用首笔 bookTicker 推一帧 mark seed（一次性）。
    bool mark_seed_pending_ = false;

    // ── aggTrade gap 检测与回补 ───────────────────────────────────
    /// aggTrade aggregate-id 高水位（live gap 检测基线）；**不**随断线重置（重连后据此检出 gap）。
    int64_t last_agg_id_ = 0;
    int64_t last_agg_gap_log_ms_ = 0;  ///< gap warn 日志 5s rate-limit。
    bool reconnect_pending_ = false;        ///< 断线后置位；重连后首笔 aggTrade 触发 backfill 后清。
    bool agg_backfill_in_flight_ = false;   ///< 单 in-flight 守卫（ws_thread）。
    uint64_t agg_backfill_gen_ = 0;         ///< 跨重连作废旧 backfill 的 post（ws_thread）。
    /// 最近一次 launch 的控制块（ws_thread 私有）：disconnect 经它 cancel 本 symbol
    /// 在途回补；线程自身另持一份 shared_ptr，本指针 reset 不影响线程。
    std::shared_ptr<BackfillCtx> agg_backfill_ctx_;

    /// per-symbol freshness witness：check_health 逐 symbol 判 stale。
    PerSymbolFreshnessWitness freshness_;
};

}  // anonymous namespace

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

    // ── 配置与生命周期 ─────────────────────────────────────────────────────
    void configure(const BinanceMdCfg& cfg) { binance_cfg = cfg; }

    bool init(const IMdGateway::Config& cfg) {
        // 生命周期契约是 init→start→stop 一次性。再次 init 会重 reserve+emplace states_：
        // 若已 start()，worker/backfill 线程持有的 SymbolState*（reserve 锁定的地址）会因
        // reallocation 悬空。已 init 直接拒绝，防 API 误用。
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
        // Public MD WS 与 depth snapshot 必须来自同一数据面，否则 update id 不连续。
        // base_rest_url 是 snapshot 唯一来源，不能为空。
        if (binance_cfg.base_rest_url.empty()) {
            XtLogger::error_m("BinanceMD",
                "binance.base_rest_url 为空 — depth snapshot 无法拉取；"
                "demo 行情环境填 https://fapi.binance.com");
            return false;
        }
        auto ws_parsed = parse_binance_ws_url(binance_cfg.base_ws_url, "443");
        auto rest_parsed = parse_binance_ws_url(binance_cfg.base_rest_url, "443");
        snapshot_host = rest_parsed.host;

        // per-symbol 状态预建：reserve 锁地址（后续 recovery/backfill 线程可安全持
        // SymbolState*），逐 sub 构造 + 登记 index。subs 的原始大小写写入 t.code。
        states_.reserve(common_cfg.subs.size());
        symbol_index_.clear();
        // 订阅 target：每 symbol 的 depth / aggTrade / bookTicker combined stream。
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

        // ── stream 数 / URL 长度 sanity ────────────────────────────────────
        // 每 symbol 订 3 channel（depth/aggTrade/bookTicker）。binance 单连接上限 1024
        // stream；超限需 connection sharding（多 WS 连接分摊），首版不支持。fail-fast
        // 拒绝启动好过静默截断成只订前若干 symbol（漏行情 = money-path 隐患）。
        const size_t stream_count = common_cfg.subs.size() * 3;
        if (stream_count > 1024) {
            XtLogger::error_m("BinanceMD",
                "combined stream 数 {} 超 binance 单连接上限 1024（{} symbol × 3 channel）；"
                "需 connection sharding，首版不支持",
                stream_count, common_cfg.subs.size());
            return false;
        }
        // combined-stream URL 过长只警告不拒绝：binance 实测 ~8KB 量级，取保守阈值
        // 提示（symbol 多到此长度通常先撞 1024 stream 上限）。
        if (ws_target.size() > 6000) {
            XtLogger::warn_m("BinanceMD",
                "combined stream URL 偏长（{} 字符，{} stream）；接近 binance URL 长度上限，"
                "继续启动但请留意连接稳定性",
                ws_target.size(), stream_count);
        }

        // ── kPriceScale 量化精度 note ─────────────────────────────────────
        // 盘口 price_key 用 llround(price × kPriceScale)；kPriceScale 决定最细可分辨档位
        // = 1/kPriceScale。本网关无 commodity catalog（拿不到 per-symbol tickSize），无法
        // 在此做真 tickSize × kPriceScale 校验。细 tick alt（tickSize < 1/kPriceScale）会被
        // 量化塌档（多个相邻价位映射到同一 key）；首版只接 tick≥0.01 的主流对
        // （BTCUSDT/ETHUSDT 等），真要接细 tick alt 需在 config/catalog 层校验
        // tickSize × kPriceScale。这里只记录值与订阅清单供运维核对。
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
        if (running.exchange(true)) return true;  // 幂等
        // 常驻 snapshot worker：drain pending 队列，逐 symbol 阻塞 REST 拉 snapshot，
        // 结果 post 回 io 线程 apply。io 线程绝不 join/阻塞在 REST 上。
        snapshot_should_exit_.store(false, std::memory_order_release);
        snapshot_worker_thread_ = std::thread([this] { snapshot_worker_loop_(); });
        // WS io 线程 lifecycle 由 IoContextThread 持有；首个连接任务在 ioc.run() 前 post
        // （非阻塞建连）。work_guard 保活，ioc.run() 不因队空提前退出。
        io_ctx_.start([this] { start_connection(); });
        return true;
    }

    void stop() {
        const bool was_running = running.exchange(false);
        if (was_running) {
            // 先通知 snapshot worker 退出，避免 stop 等待正在 TLS handshake/read 的同步
            // REST。snapshot_should_exit_ 也是 worker 拉取时传给 fetch_sync 的 cancel flag；
            // 同时唤醒 worker 退出 wait。
            snapshot_should_exit_.store(true, std::memory_order_release);
            snapshot_queue_cv_.notify_all();
        }
        // detached backfill 收口：cancel 全部在途 + 关 io 门 + bounded wait，
        // **必须在停 ioc 之前**——门关后 detached 线程绝不可能再 post 向将亡的 ioc。
        // 未 start 也要走（测试/析构路径可能已 launch 过 backfill）。
        shutdown_backfills_();
        if (!was_running) return;
        // ws_connection->stop() post 到 io 线程后不等其跑完（wait_for_teardown=false，
        // 保留原「post + ioc.stop() 竞争，端态一致」行为）；stop() 内部完成 ioc.stop() + join。
        io_ctx_.stop([this] { if (ws_connection) ws_connection->stop(); },
                     /*wait_for_teardown=*/false);
        // io 线程已 join（ioc.run() 已退出）→ 复刻原 ws_thread 退出日志（现于控制线程记）。
        XtLogger::warn_m("BinanceMD",
            "ws_thread: ioc.run() exited; running={}", running.load());
        XtLogger::info_m("BinanceMD", "ws_thread: clean exit");
        snapshot_queue_cv_.notify_all();
        if (snapshot_worker_thread_.joinable()) snapshot_worker_thread_.join();
    }

    /// @brief detach 协议的 stop 侧：cancel 全部在途 backfill + 关 io 门 + bounded wait。
    ///
    /// 顺序（TOCTOU 约束）：持 gate 锁翻 io_alive=false（与线程侧「存活
    /// 检查 + post」同一临界区互斥）→ 锁外置全部 should_exit → bounded wait in-flight
    /// 归零。超时 loud log 放弃等待——线程只持控制块（shared_ptr），Impl 亡后亦无悬垂。
    void shutdown_backfills_() {
        std::vector<std::shared_ptr<BackfillCtx>> live;
        {
            std::lock_guard<std::mutex> lk(backfill_gate_->mu);
            backfill_gate_->io_alive = false;  // 此后任何 detached 线程都无法再 post
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
        // 判定核在 sweep_md_freshness（逐 symbol witness 判 + 聚合，rationale 见
        // PerSymbolFreshnessWitness.h）；这里只做副作用：日志 + throttle 戳 + reconnect post。
        //
        // ⚠️ 低量 symbol 的 false-positive：合法低频不 tick 的 symbol 会被判 stale → 误触发
        // 连接级 reconnect（连累活跃 symbol 一起 re-snapshot）。首版灰度只接活跃主流对
        // （BTCUSDT/ETHUSDT，高频，stale_reconnect_ms 内必有 tick），故此 trade-off 可接受；
        // 未来若接低频 symbol 需 per-symbol stale 阈值或心跳兜底。
        const uint64_t now64 = static_cast<uint64_t>(now_ms);
        const auto sweep = sweep_md_freshness(
            states_.size(),
            [this](size_t i) -> const PerSymbolFreshnessWitness& {
                return states_[i].freshness_;
            },
            now64, stale_last_close_ms_, common_cfg.stale_reconnect_ms,
            /*throttle_ms=*/15000);

        if (sweep.any_should_reconnect) {
            // 任一 symbol stale 且 throttle 已过 → 连接级 reconnect 一次（不对每个 stale
            // symbol 重复调）。anchor/delta 只为日志重算；判定已由 sweep 完成。
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
        // 无 symbol 触发 reconnect：有 stale 但被 throttle 吞 → false；否则全健康 → true。
        return !sweep.any_throttled;
    }

    // ── 内部：session + message handling ──────────────────────────────────
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
                // bootstrap fan-out：对每个 symbol 入 snapshot 队列，worker 串行 drain。
                // 入队非阻塞；worker 逐个拉 + post 回 io 线程 apply。
                for (auto& st : states_) enqueue_snapshot_(st);
            },
            [this] { on_connection_disconnect(); });
        ws_connection->start();
    }

    void on_connection_disconnect() {
        // WS 整断 → 所有 symbol 的 book/snapshot 都已过期；逐个 reset（per-symbol，
        // 不互相连累，但断的是连接所以全 reset）。重连成功回调会对每个 symbol 重新入队
        // snapshot（on-connect bootstrap fan-out）。
        // on_connection_disconnect 与 handle_message 同在单 io 线程串行执行，遍历期间无并发撞。
        for (auto& st : states_) {
            // book/buffer/awaiting 全重置（awaiting 清零让重连后 on-connect 的
            // enqueue_snapshot_ 能重新入队；旧 worker post 会被 reset 后的 drain 自然纠正）。
            st.depth_.reset_for_reconnect();
            // 鲜 L1 也随断线失效：重连后首笔 bookTicker 到达前 push_tick 退回 depth 顶档。
            st.fresh_l1_ = FreshL1{};
            st.mark_seed_pending_ = false;  // 重连后 book 未 ready；下次 on_snapshot_ready 再置位。
            // gap 回补：作废在途 backfill 的 post（gen++）+ cancel 其控制块（线程自持一份
            // shared_ptr，reset 本指针不影响其退出路径）+ 允许重连后重新 launch
            // （in_flight_=false）；标记重连后首笔 aggTrade 触发回补。
            // last_agg_id_ 故意不重置 → 据此检出断线窗口的 gap（per-symbol 高水位跨断保留）。
            ++st.agg_backfill_gen_;
            if (st.agg_backfill_ctx_) {
                st.agg_backfill_ctx_->should_exit.store(true, std::memory_order_release);
                st.agg_backfill_ctx_.reset();
            }
            st.agg_backfill_in_flight_ = false;
            st.reconnect_pending_ = true;
        }
        // 不清各 symbol 的 tick witness：重连尚未 book-ready 前，watchdog 仍需旧 tick
        // anchor（清零会落回 cold-start grace → zombie-WS 静默，见 XtMdFreshness 设计）。
        // 各 symbol 的 ready witness 在其 snapshot drain 完成后刷新。
        // metric / clock-skew 是 per-channel（连接属性），整次断连记一次。
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

    /// @brief 取（已大写归一的）symbol 对应的 SymbolState；调用方保证 symbol 已注册。
    /// @note 仅 bootstrap/disconnect 等内部已知 symbol（subs.front()）走此 assert 变体；
    ///       WS demux 路由走 state_for_ptr（未知 symbol 返 nullptr 而非 assert）。
    SymbolState& state_for(const std::string& symbol) {
        const auto it = symbol_index_.find(symbol);
        // states_ 在 init() 已为每个 sub 建好；已知内部 symbol 必命中。
        assert(it != symbol_index_.end() && "state_for: unknown symbol");
        return states_[it->second];
    }

    /// @brief 同 state_for，但 symbol 未注册返回 nullptr（不 assert）。WS demux 路由用。
    SymbolState* state_for_ptr(const std::string& symbol) {
        const auto it = symbol_index_.find(symbol);
        if (it == symbol_index_.end()) return nullptr;
        return &states_[it->second];
    }

    void handle_message(std::string_view payload) {
        const uint64_t recv_mono_us = steady_now_us();
        auto j = nlohmann::json::parse(payload, nullptr, /*allow_exceptions=*/false);
        if (j.is_discarded()) return;  // 解析失败静默忽略
        // combined stream 消息格式：{"stream":"xxx","data":{...}}
        if (!j.contains("stream") || !j.contains("data")) return;
        const auto& stream = j["stream"];
        const auto& data = j["data"];
        if (!stream.is_string()) return;

        // demux：channel 由 stream suffix 判，symbol 由 payload `data.s` 判（多 symbol
        // 路由唯一真值源）。三 channel（depth/aggTrade/bookTicker）payload 都带 `s`=symbol；
        // 与 bookTicker `a`=ask price / aggTrade `a`=agg id 同名异义无关（此处只读 `s`）。
        // `s` 缺失或 symbol 未订阅 → DROP，绝不 fallback subs.front()（fallback = 数据错配）。
        SymbolState* stp = resolve_symbol_state(data);
        if (stp == nullptr) return;
        SymbolState& st = *stp;

        const std::string& stream_name = stream.get_ref<const std::string&>();
        // combined stream 通过 stream suffix 路由到 depth/aggTrade/bookTicker。
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

    /// @brief 从 combined-stream payload 的 `s` 字段提取并归一 symbol，路由到 SymbolState。
    /// @return 命中的 SymbolState*；`s` 缺失/非字符串/未订阅 symbol → nullptr（调用方 DROP）。
    /// @note symbol 大写归一后对 symbol_index_ 查找；rate-limited warn 记未知 symbol（防刷屏）。
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

    /// @brief 未知/缺失 symbol drop 的 5s rate-limited warn（防错配 symbol 刷屏）。
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

    /// @brief 每 5s 打一次 per-stream 频率统计，用于观察真实行情节奏。
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
                // P15：stod("nan"/"inf") 不抛，try-catch 接不住；非有限 = 整条 diff
                // 按 malformed 拒收（与 parse 失败同语义，depth gap 自愈链兜底）。
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

        // 转移决策全在 BinanceDepthSync（buffer 上限 / gap re-buffer / stale drop 语义
        // 见其 header）；这里只做日志 + 把 NeedResnapshot 转成 enqueue。
        const uint64_t U = d.U, u = d.u, pu = d.pu;  // 日志用（d 即将被 move）
        BinanceDepthSync::ResnapshotCause cause{};
        uint64_t pre_reset_update_id = 0;
        const auto res = st.depth_.on_diff(std::move(d), &cause, &pre_reset_update_id);
        if (res == BinanceDepthSync::DiffResult::NeedResnapshot) {
            if (cause == BinanceDepthSync::ResnapshotCause::BufferOverflow) {
                XtLogger::warn_m("BinanceMD",
                    "depth diff_buffer 超上限({}) symbol={}（snapshot 卡住？），reset + re-snapshot",
                    BinanceDepthSync::kMaxDiffBuffer, st.symbol);
            } else {
                // sequence gap 说明本地 book 不再可信；只 reset 该 symbol，重排 snapshot。
                XtLogger::warn_m("BinanceMD",
                                 "depth gap detected symbol={} (U={}, u={}, pu={}, last_update_id={})，re-snapshot",
                                 st.symbol, U, u, pu, pre_reset_update_id);
            }
            enqueue_snapshot_(st);
        }
        // depth 只维护 full book；tick push 由 aggTrade/bookTicker 驱动，避免重复推送。
    }

    void handle_agg_trade(SymbolState& st, const nlohmann::json& data) {
        uint64_t ts_exch_ms = 0;
        try {
            // aggregate trade id（fromId 回补锚点）。gap 检测与 book 解耦，先于记账，
            // 且先于坏值丢弃与 p/q 解析（解析 throw 同属坏值行）——坏值行的 agg_id
            // 是真实序号，不推进高水位会让下一笔正常成交虚计 gap（reconnect 窗口
            // 还会触发虚假 backfill）。
            const int64_t agg_id = static_cast<int64_t>(data.value("a", 0LL));
            const int64_t missed = agg_gap_missed(st.last_agg_id_, agg_id);
            if (missed > 0) {
                note_agg_gap_(st, missed, agg_id);                 // metric + rate-limited log
                // reconnect-anchored：仅在重连后首笔 aggTrade 检出 gap 时回补。
                // gap 区间 [last_agg_id_+1, agg_id) 严格低于本笔(live resume)，与 live 不相交。
                if (st.reconnect_pending_) launch_agg_backfill_(st, st.last_agg_id_ + 1, agg_id);
            }
            st.reconnect_pending_ = false;  // 重连后首笔 aggTrade 一律清（无 gap 也清）→ 不误触发
            if (agg_id > st.last_agg_id_) st.last_agg_id_ = agg_id;  // 高水位（live gap 检测基线）
            const double p = std::stod(data["p"].get<std::string>());
            const double q = std::stod(data["q"].get<std::string>());
            // 非有限价/量 = 坏消息，丢弃记账（不污染 last_price/total_volume）；
            // gap/高水位已按真实序号处理。venue 从不发 NaN，逐次 warn 不限频。
            if (!std::isfinite(p) || !std::isfinite(q)) {
                ++cnt_aggtrade_dropped_;
                XtLogger::warn_m("BinanceMD",
                    "aggTrade non-finite p/q dropped sym={} agg_id={} (total={})",
                    st.symbol, agg_id, cnt_aggtrade_dropped_);
                return;
            }
            st.last_price = p;
            st.total_volume += q;
            ts_exch_ms = data.value("E", 0ULL);
        } catch (...) { return; }
        if (st.depth_.is_live()) push_tick(st, "aggTrade", ts_exch_ms);
    }

    /// @brief gap 可观测：metric + rate-limited(5s) warn log。
    /// @note 不依赖 agg-id 严格 +1。last_agg_id_ 不在 on_connection_disconnect
    ///       重置 → 重连后首笔 aggTrade 能检出断线窗口的 gap。
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

    /// @brief reconnect-anchored aggTrades 回补：detached 线程拉 [lo, hi) 缺失聚合成交，
    ///        post 回 ws_thread 仅累加 total_volume（不 emit tick）。
    ///
    /// UAF-safe detach：线程全部所需状态入 shared_ptr<BackfillCtx> 控制块，
    /// lambda **只按值捕获该 shared_ptr**（禁捕 this / &st——stop 超时放弃等待后
    /// Impl 可先亡）；net::post 前在 gate 锁内查 io_alive（TOCTOU 同临界区）。
    /// @note 单 in-flight；fetcher 纯返回不碰 gateway 状态；gen token 作废跨重连的旧 post。
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
            // stop-vs-launch race 收口：注册与「门是否已关」判定同锁——要么本 launch 对
            // stop 可见（被 cancel + 计入 bounded wait），要么门已关直接不起线程。
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
            constexpr int kMaxPages = 50;  // 50×1000 = 50k 成交上限，防大 gap 失控翻页
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
                if (res.max_a < from_id) break;  // 防御：无前进 → 停（避免死循环）
                from_id = res.max_a + 1;
                ++pages;
                if (res.reached_hi) break;
            }
            if (ctx->should_exit.load(std::memory_order_acquire)) return;  // 放弃，不 post
            // TOCTOU 约束：io 存活检查与 net::post 必须在 gate 同一把锁内
            // 原子完成；stop 在拆 ioc 前持同锁翻 io_alive——检查通过即保证 post 目标存活。
            std::lock_guard<std::mutex> lk(ctx->gate->mu);
            if (!ctx->gate->io_alive) return;
            net::post(*ctx->gate->ioc, [ctx, total_qty, total_n, failed, truncated] {
                // 只在 io 线程执行：owner(Impl) 必活——stop 先关 gate（阻断新 post）、
                // 再 join io 线程、之后才可能析构 Impl。
                auto* impl = static_cast<MdBinanceUGateway::Impl*>(ctx->gate->owner);
                SymbolState* stp = impl->state_for_ptr(ctx->symbol);
                if (stp == nullptr) return;
                if (ctx->gen != stp->agg_backfill_gen_) return;  // 跨重连旧 backfill，丢弃
                impl->apply_agg_backfill_(*stp, total_qty, total_n, failed, truncated);
            });
        }).detach();
    }

    /// @brief 回补结果落地（ws_thread）：累加 total_volume（不 emit tick）+ metric。
    void apply_agg_backfill_(SymbolState& st, double total_qty, int total_n, bool failed, bool truncated) {
        st.agg_backfill_in_flight_ = false;
        st.total_volume += total_qty;  // 量记入累计；下一笔真 tick 携带（阶跃）
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
        // bookTicker 自带最优盘口（b/B/a/A，逐 BBO 变化实时推）→ **始终**写入 fresh_l1_
        // 作为 tick 的 L1 鲜值源（即便本流不推 tick，aggTrade-triggered tick 也用它）。
        // 注：bookTicker 的 `a`=ask price，与 aggTrade 的 `a`=aggregate id 同名异义。
        parse_fresh_l1(st, data);
        if (!st.depth_.is_live()) return;
        // tick_source 控制 bookTicker 是否额外推 quote tick；aggTrade 永远推（见
        // handle_agg_trade）。trade-only 模式下 book-ready 后推一帧 quote 作 mark seed，
        // 保证首笔成交前 mark/unrealized 有盘口值。
        const bool push_quote =
            binance_cfg.tick_source == BinanceMdCfg::TickSource::TradeAndQuote
            || st.mark_seed_pending_;
        if (push_quote) {
            st.mark_seed_pending_ = false;
            const uint64_t ts_exch_ms = data.value("E", 0ULL);
            push_tick(st, "bookTicker", ts_exch_ms);
        }
    }

    /// @brief 解析 bookTicker 自带 best bid/ask 价与量写入 fresh_l1_。
    /// @note 价格字段是字符串（"25.35190000"）；缺/坏字段保留旧 fresh_l1_（不致命，
    ///       push_tick overlay 会因 invalid 退回 depth 顶档）。
    void parse_fresh_l1(SymbolState& st, const nlohmann::json& data) {
        try {
            FreshL1 l1;
            l1.bid_px  = std::stod(data.at("b").get<std::string>());
            l1.bid_qty = std::stod(data.at("B").get<std::string>());
            l1.ask_px  = std::stod(data.at("a").get<std::string>());
            l1.ask_qty = std::stod(data.at("A").get<std::string>());
            // P15：overlay 的 <=0 判定接不住 NaN（NaN<=0 为 false），非有限保留旧
            // fresh_l1_（与字段缺失/格式异常同语义）。
            if (!std::isfinite(l1.bid_px) || !std::isfinite(l1.bid_qty) ||
                !std::isfinite(l1.ask_px) || !std::isfinite(l1.ask_qty)) {
                return;
            }
            l1.valid   = true;
            st.fresh_l1_ = l1;
        } catch (...) {
            // 字段缺失/格式异常：保留上一笔 fresh_l1_，不污染。
        }
    }

    /// @brief 构建并发布一笔 XtRawTick（唯一 emit 点）。
    ///
    /// 契约（下游须知）：MD tick 流是**事件驱动**的——aggTrade（每笔成交）与
    /// bookTicker（每次 BBO 变化，tick_source=trade+quote 时）各自触发一笔 tick。
    /// 同一笔吃单成交可能同时触发 aggTrade + bookTicker → 一个市场事件两笔 tick；
    /// **相邻 tick 字段可能完全相同，下游必须幂等**（成交印记 vs 盘口刷新是互补信息，
    /// 不去重）。**tick 计数不是稳定时钟**（速率随成交/报价活跃度剧烈波动，且双触发）
    /// → 按-tick-计数的窗口逻辑应改用时间/成交数为基准。trade-only 模式下 tick=成交。
    void push_tick(SymbolState& st, const char* /*source*/, uint64_t ts_exch_ms) {
        ++cnt_tick_push_;
        XtRawTick t;
        // 每笔 tick 的 code 取产生它的 symbol（多 symbol 路由真值源；reader 按
        // tick.code 对 declared_ 过滤，写错会静默错配）。recorder emit 复用 t.code 自动跟对。
        XtCStringUtil::copy(t.code, st.symbol.c_str());
        XtCStringUtil::copy(t.exchg, "BINANCE");
        t.price = st.last_price;
        t.total_volume = st.total_volume;
        // action_date/action_time 从 Binance event_time 按 UTC 自然日派生。
        t.action_date = xt_core::derive_trading_day(
            static_cast<int64_t>(ts_exch_ms), xt_core::TimezonePolicy::UtcNaturalDay);
        t.action_time = xt_core::derive_action_time(
            static_cast<int64_t>(ts_exch_ms), xt_core::TimezonePolicy::UtcNaturalDay);
        // 按 recorder / XtRawTick 共同的 20 档宽度取盘口。
        constexpr int kRecDepth = XtLiveRecorder::kMdDepth;
        static_assert(XtRawTick::DEPTH <= kRecDepth,
                      "engine DEPTH must not exceed recorder kMdDepth — recorder is the wider sink");
        double bps[kRecDepth], bqs[kRecDepth], aps[kRecDepth], aqs[kRecDepth];
        st.depth_.book().top_n(kRecDepth, bps, bqs, aps, aqs);
        // 用 bookTicker 鲜 L1 覆盖 L1 + 裁掉破坏单调的更深档（原地）。
        // 单一构建点：下面 cb 与 recorder 都读这份覆盖后的数组，二者 L1
        // 永不分叉。fresh_l1_ invalid 时 no-op → 退回 depth 顶档。
        overlay_fresh_l1(st.fresh_l1_, kRecDepth, bps, bqs, aps, aqs);
        for (int i = 0; i < XtRawTick::DEPTH; ++i) {
            t.bid_prices[i] = bps[i];
            t.bid_qty[i]    = bqs[i];
            t.ask_prices[i] = aps[i];
            t.ask_qty[i]    = aqs[i];
        }

        // 首笔 aggTrade 前 last_price 可能为 0；此时用盘口 mid 作为 mark price fallback。
        // 一旦 aggTrade 到达，last_price 会锁定真实成交价，本分支自然停止生效。
        if (t.price <= 0.0 && bps[0] > 0.0 && aps[0] > 0.0) {
            t.price = (bps[0] + aps[0]) / 2.0;
        }

        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();

        // [Tap] MD_TICK：实盘事件录制（kMdDepth 档深度 + Binance event_time 派生字段）。
        if (auto* rec = XtLiveRecorder::global()) {
            // 读取 t.price 而不是 last_price，使 recorder 与 live mark fallback 保持一致。
            const int64_t last_fp = xt_live_recorder_util::to_recorder_fp(t.price);
            int64_t bid_px_fp[kRecDepth], ask_px_fp[kRecDepth], bid_q[kRecDepth], ask_q[kRecDepth];
            xt_live_recorder_util::fill_md_depth_fp(bps, aps, bqs, aqs, kRecDepth,
                                                    bid_px_fp, ask_px_fp, bid_q, ask_q);
            // trading_date 与 action_date 一致；Binance perp 24h 无夜盘切换语义。
            rec->emit_md_tick(ts_local_ns,
                              t.action_date, t.action_date, t.action_time,
                              t.code, t.exchg,
                              last_fp, xt_live_recorder_util::to_recorder_fp(st.total_volume),
                              bid_px_fp, ask_px_fp, bid_q, ask_q,
                              /*depth_levels=*/kRecDepth);
        }

        // check_health 用本 symbol 的 steady ms 作真实 tick witness（per-symbol）。
        st.freshness_.note_tick(static_cast<uint32_t>(ts_local_ns / 1'000'000));
        // 直连 venue provider：无 SHM connector capture，capture_ns 传 0（未 stamp）。
        if (cb.on_tick) cb.on_tick(t, /*capture_ns=*/0);
    }

    // ── snapshot worker + pending 队列 ─────────────────────────────────────────
    //
    // 三方分工（并发模型）：
    //   - io 线程（ws_thread）：enqueue_snapshot_ 非阻塞入队；on_snapshot_ready apply
    //     book/drain（book 单写者）。绝不 join/阻塞在 REST 上。
    //   - snapshot worker 线程：drain 队列，逐 symbol 阻塞 REST 拉 snapshot；book 变更
    //     一律 net::post 回 io 线程，worker 自己绝不碰 book。失败 backoff + 重入队
    //     （只动 worker 自己的 queue，不卡其它 symbol，保留无限重试语义）。

    /// @brief io 线程入队某 symbol 的 snapshot 请求（非阻塞）。
    /// @note 幂等：begin_await 已 awaiting 时返 false 直接跳过（守重复入队 + 与「worker
    ///       忙不忙」解耦：B 的 re-snapshot 不被 A 的拉取吞）。仅 ws_thread 调用。
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

    /// @brief 常驻 snapshot worker 主循环：wait 队列非空/should_exit → pop → 阻塞拉 →
    ///        成功 post on_snapshot_ready（book 回 io 线程）/失败 backoff + 重入队。
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

    /// @brief worker 阻塞拉一个 symbol 的 REST snapshot；结果按三态处理。
    /// @note 在 worker 线程跑；只读 states_[idx].symbol（init 后不变，地址 reserve 稳定）。
    ///       book 变更不在此做，net::post 回 io 线程。
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
        // stop() 置 snapshot_should_exit_ 后，fetch_sync 通过 curl progress callback 退出。
        auto snap = fetcher.fetch_sync(symbol, /*limit=*/1000, &err, &snapshot_should_exit_);
        XtLogger::info_m("BinanceMD", "snapshot fetch done: symbol={} ok={} err='{}'",
                         symbol, (snap ? "yes" : "no"), err);
        // shutdown 中放弃，不 post、不重入队（让 worker 尽快退出 join）。
        if (snapshot_should_exit_.load(std::memory_order_acquire)) {
            XtLogger::info_m("BinanceMD", "snapshot worker exiting early (shutdown signaled)");
            return;
        }
        if (!snap) {
            // 失败：backoff 后把本 symbol 重入队（depth 状态机仍 AwaitingSnapshot，io 线程
            // 不会重复入队）。保留无限重试语义但不阻塞 io、不卡其它 symbol。
            // backoff 走 cv.wait_for（持 snapshot_queue_mu_，与 worker loop 同锁）而非裸
            // sleep_for：stop() 的 snapshot_should_exit_ + notify_all 能即时打断这 1s 等待，
            // 否则 stop join worker 最长被卡 ~1s。
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
        // 成功：book apply + drain 回 io 线程（单写者）。
        net::post(io_ctx_.ioc(), [this, &st, snap = std::move(snap)]() mutable {
            on_snapshot_ready(st, std::move(snap));
        });
    }

    /// @brief snapshot 落地（ws_thread）：apply + drain 决策在 BinanceDepthSync；
    ///        这里只做日志、freshness witness、mark seed 与 first-ready on_ready。
    /// @note nullopt 不应出现（worker 失败走重入队不 post）；防御性走重入队。
    void on_snapshot_ready(SymbolState& st, std::optional<DepthSnapshot> snap) {
        if (!snap) {
            // 防御：理论上 worker 不会 post nullopt（失败重入队）；保险起见重排。
            enqueue_snapshot_(st);
            return;
        }
        BinanceDepthSync::DrainStats stats;
        const auto res = st.depth_.on_snapshot(*snap, &stats);
        XtLogger::info_m("BinanceMD", "snapshot applied: symbol={} lastUpdateId={}, bids={}, asks={}",
                         st.symbol, snap->last_update_id, stats.bid_levels, stats.ask_levels);
        if (res == BinanceDepthSync::SnapshotResult::NeedResnapshot) {
            // drain 中仍出现 gap，说明 snapshot 与 buffer 不可拼接，只 reset 该 symbol 重排。
            enqueue_snapshot_(st);
            XtLogger::warn_m("BinanceMD",
                             "snapshot drain mid-gap symbol={}: stale={}, applied={}, retry",
                             st.symbol, stats.stale_dropped, stats.applied);
            return;
        }
        // 本 symbol 的 WS + snapshot 已到达可用态，刷新它的第二个 freshness witness
        // （per-symbol）。
        st.freshness_.note_ready(static_cast<uint32_t>(XtSteadyClock::steady_now_ms()));
        // trade-only 模式：标记下一笔 bookTicker 推一帧 mark seed（TradeAndQuote 模式恒推，
        // 此 flag 被消费但无副作用）。
        st.mark_seed_pending_ = true;
        // first-ready：首个 book-ready 即 fire on_ready 一次让引擎控制环转起来。
        // 不等 all-ready（坏 symbol 永拉不到 snapshot 会扣住所有健康 symbol → 卡死）。
        // 每 symbol push_tick 仍受 depth_.is_live() gate，半 ready 不误交易。
        if (!on_ready_fired_) {
            on_ready_fired_ = true;
            if (cb.on_ready) cb.on_ready();
        }
        XtLogger::info_m("BinanceMD", "book ready: symbol={} stale_dropped={}, applied={}, gapped={}",
                         st.symbol, stats.stale_dropped, stats.applied, stats.gapped);
    }

    // ── 配置与回调 ─────────────────────────────────────────────────
    BinanceMdCfg binance_cfg;
    IMdGateway::Config common_cfg;
    IMdGateway::Callbacks cb;

    // ── WS / snapshot 线程资源 ─────────────────────────────────────
    // io_context + ssl_ctx + WS 独占线程 由 IoContextThread 持有（lifecycle 样板）。
    xt_live::gateway::IoContextThread io_ctx_;
    std::atomic<bool> running{false};
    // shutdown 期间通过 curl progress callback 中断 in-flight snapshot fetch，
    // 并唤醒 snapshot worker 退出 wait。
    std::atomic<bool> snapshot_should_exit_{false};

    // ── snapshot worker（常驻线程 + pending 队列）────────────────────
    // io 线程只 enqueue_snapshot_（非阻塞入队 states_ 下标）；worker drain 队列，
    // 逐个阻塞 REST 拉 snapshot，成功/失败都 net::post 回 io 线程（book 单写者）。
    std::thread snapshot_worker_thread_;
    std::deque<size_t> snapshot_queue_;       ///< states_ 下标队列（worker drain）。
    std::mutex snapshot_queue_mu_;            ///< 保护 snapshot_queue_。
    std::condition_variable snapshot_queue_cv_;  ///< 队列非空/should_exit 唤醒 worker。

    std::unique_ptr<xt_live::gateway::XtWsConnection> ws_connection;

    // ── 连接 target 与 snapshot host ───────────────────────────────
    std::string ws_host;
    std::string ws_port;
    std::string ws_target;
    std::string snapshot_host;

    // ── per-symbol 盘口/recovery 状态（ws_thread 访问）─────────────
    // init() 时 reserve(subs.size()) 锁地址后逐 sub 构造；reserve 保证后续持有的
    // SymbolState* 不因扩容悬空。
    std::vector<SymbolState> states_;
    std::unordered_map<std::string, size_t> symbol_index_;  ///< symbol → states_ 下标。

    // ── detached aggTrade backfill 的 io 存活门 ────────────────────
    // 全部 backfill 控制块共享同一 gate（ioc/owner 指针 + io_alive + 注册表 + in-flight
    // 计数）；per-launch cancel flag 在各自 BackfillCtx（见结构体注释）。
    std::shared_ptr<BackfillGate> backfill_gate_;

    // ── on_ready 一次性触发（first-ready）──────────────────────────
    // 首个 symbol book-ready（snapshot drain 成功）即 fire on_ready 一次让引擎控制环转起来；
    // 不等 all-ready（坏 symbol 永拉不到 snapshot 会扣住所有健康 symbol → 卡死）。
    bool on_ready_fired_ = false;

    // ── per-stream rate counters（ws_thread 访问）──────────────────
    uint64_t cnt_bookticker_ = 0;
    uint64_t cnt_aggtrade_   = 0;
    uint64_t cnt_depth_      = 0;
    uint64_t cnt_tick_push_  = 0;
    uint64_t cnt_aggtrade_dropped_ = 0;  ///< 非有限 p/q 丢弃累计（不随窗口清零）。
    int64_t  rate_log_last_ms_ = 0;
    int64_t  last_unknown_symbol_log_ms_ = 0;  ///< 未知/缺失 symbol drop warn 的 5s rate-limit。

    // stale reconnect 节流：控制线程按周期调用 check_health。
    uint32_t stale_last_close_ms_ = 0;

#ifdef XT_BUILD_TESTS
public:
    // ── 集成 seam（仅 2 类）：depth/queue 状态机与 freshness 判定已组件化，由
    // BinanceDepthSync / PerSymbolFreshnessWitness 公开 API 直测；这里只留跨子系统
    // 闭环无法从组件外驱动的两个入口（demux inject 在 handle_message，直接可调）。

    /// @brief 单测入口：复刻 worker 成功 handoff——走真 on_snapshot_ready 路径
    ///        （drain + witness + mark seed + first-ready on_ready）让 symbol book ready。
    /// @note 绕过真 REST snapshot；symbol 须已订阅（未订阅则 no-op）。同步、不起线程。
    ///       lastUpdateId=1：后续注入 U>1 的 diff 即可构造真 gap。
    void test_make_book_ready(const std::string& symbol) {
        SymbolState* st = state_for_ptr(to_upper(symbol));
        if (st == nullptr) return;
        DepthSnapshot snap;
        snap.last_update_id = 1;
        snap.bids = {{100.0, 1.0}};
        snap.asks = {{101.0, 1.0}};
        on_snapshot_ready(*st, std::move(snap));
    }

    /// @brief 复刻 WS 整断：驱动 on_connection_disconnect（reset 所有 symbol + cancel
    ///        在途 backfill + 置 reconnect_pending_）。stop-while-backfill-inflight 专项
    ///        测试靠它经生产路径（重连后首笔 aggTrade 检出 gap）真 launch detached 线程。
    void test_disconnect() { on_connection_disconnect(); }

    /// @brief 在跑 backfill 线程数（gate in-flight 计数）。防 stop 专项测试 vacuous：
    ///        launch 后断言 =1（线程真起）、stop 收口后断言归零。
    int test_backfill_inflight() const {
        return backfill_gate_->inflight.load(std::memory_order_acquire);
    }

    /// @brief agg 序列观测：symbol 高水位 + 非有限 p/q 丢弃累计（symbol 未订阅返 false）。
    bool test_agg_state(const std::string& symbol, int64_t& out_last_agg_id,
                        uint64_t& out_dropped) {
        SymbolState* st = state_for_ptr(to_upper(symbol));
        if (st == nullptr) return false;
        out_last_agg_id = st->last_agg_id_;
        out_dropped = cnt_aggtrade_dropped_;
        return true;
    }
#endif  // XT_BUILD_TESTS
};

// ===========================================================================
// MdBinanceUGateway: thin forwarding to Impl
// ===========================================================================

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
// ── 单测入口（XT_BUILD_TESTS only）──────────────────────────────────────────
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

bool md_binance_test_agg_state(void* impl_void, const char* symbol,
                               long long* out_last_agg_id,
                               unsigned long long* out_dropped) {
    auto* impl = static_cast<MdBinanceUGateway::Impl*>(impl_void);
    int64_t last_agg_id = 0;
    uint64_t dropped = 0;
    if (!impl->test_agg_state(symbol, last_agg_id, dropped)) return false;
    if (out_last_agg_id) *out_last_agg_id = static_cast<long long>(last_agg_id);
    if (out_dropped) *out_dropped = static_cast<unsigned long long>(dropped);
    return true;
}
#endif  // XT_BUILD_TESTS

}  // namespace binance_u
