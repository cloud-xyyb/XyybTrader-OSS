#include "BinanceUserDataStream.h"

#include "BinanceTdWireUtil.h"
#include "rest/BinanceRestClient.h"
#include "xt_live/gateway/common/IoContextThread.h"
#include "xt_live/gateway/common/XtWsConnection.h"
#include "xt_identity/OrderAttributionStore.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_core/session/XtTradingDay.h"

#include <array>
#include <atomic>
#include <cmath>  // std::isfinite（P15 safe_stod 防线）
#include <chrono>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/error_code.hpp>

#include <nlohmann/json.hpp>

#include "xt_logger/XtLogger.h"

namespace binance_u {

namespace net       = boost::asio;
namespace ssl       = net::ssl;

namespace {

thread_local nlohmann::json g_uds_json;

/// @brief 取 json 字段 string_view（零拷贝）。返回 default_view 当字段不存在或非 string。
/// @note 返回的 string_view 生命周期绑定到 `j`，不可跨函数返回或跨线程派发。
inline std::string_view value_or_view(const nlohmann::json& j, const char* key,
                                      std::string_view dflt = std::string_view()) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return dflt;
    const std::string& s = it->get_ref<const std::string&>();
    return std::string_view(s);
}

/// @brief 安全 stod，从 json string 字段解析，失败返回 dflt。
/// @note 使用 get_ref 直接读取 json 内部 string，避免为临时 std::string 分配。
///       P15：stod("nan"/"inf") 不抛，非有限同样返 dflt（UDS 是 fill/资金钱路入口）。
double safe_stod(const nlohmann::json& j, const char* key, double dflt = 0.0) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return dflt;
    try {
        const std::string& s = it->get_ref<const std::string&>();
        const double v = std::stod(s);
        return std::isfinite(v) ? v : dflt;
    } catch (...) { return dflt; }
}

/// @brief 安全取 int64，从 json 数字字段获取，失败返回 dflt。
inline int64_t value_int64_or(const nlohmann::json& j, const char* key, int64_t dflt = 0) {
    auto it = j.find(key);
    if (it == j.end()) return dflt;
    if (it->is_number_integer()) return it->get<int64_t>();
    if (it->is_number_unsigned()) return static_cast<int64_t>(it->get<uint64_t>());
    return dflt;
}

/// @brief 安全取 bool。
inline bool value_bool_or(const nlohmann::json& j, const char* key, bool dflt = false) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_boolean()) return dflt;
    return it->get<bool>();
}

/// @brief process-monotonic 微秒时间戳；与 TraderBinanceUGateway latency 域同源。
inline uint64_t lat_now_us() noexcept {
    return XtSteadyClock::steady_now_us();
}

}  // anonymous namespace

// ===========================================================================
// Impl
// ===========================================================================
class BinanceUserDataStream::Impl {
public:
    Impl(Config cfg, BinanceRestClient& rest)
        : cfg_(std::move(cfg))
        , rest_(rest)
        , keepalive_timer_(io_ctx_.ioc()) {}

    ~Impl() { stop(); }

    void bind_callbacks(OnOrderUpdate on_o, OnTrade on_t, OnAccount on_a,
                        OnPosition on_p, ParseLocalId parse) {
        on_order_ = std::move(on_o);
        on_trade_ = std::move(on_t);
        on_account_ = std::move(on_a);
        on_position_ = std::move(on_p);
        parse_local_id_ = std::move(parse);
        if (!parse_local_id_) parse_local_id_ = &parse_local_id;
    }

    void set_unattributed_fill_handler(BinanceUserDataStream::OnUnattributedFill h) {
        on_unattributed_fill_ = std::move(h);
    }

    void set_fill_dedup(BinanceUserDataStream::FillDedup fn) {
        fill_dedup_ = std::move(fn);
    }

    bool start() {
        if (running_.exchange(true)) return true;
        // UDS 独占 io_context 线程；listenKey acquire 完成后再建立 WS 连接。
        // 首个连接任务由 IoContextThread 在 ioc.run() 前 post（非阻塞建连）。
        io_ctx_.start([this] { acquire_and_connect(); });
        return true;
    }

    void stop() {
        if (!running_.exchange(false)) return;
        // teardown（timer cancel / WS 关闭 / listenKey 关闭）在 io 线程执行，
        // 控制线程等其跑完再停 ioc + join（IoContextThread wait_for_teardown 语义）。
        io_ctx_.stop([this] { stop_on_io_thread(); });
    }

    bool check_health(uint32_t now_ms) {
        // stale WS 检测：last_msg_ms > 某阈值触发重连（binance 自己每 3 分钟发 ping）
        if (ws_connection_ && !ws_connection_->check_stale(now_ms)) {
            net::post(io_ctx_.ioc(), [this] {
                if (ws_connection_) ws_connection_->request_reconnect();
            });
            return false;
        }
        return true;
    }

    /// @brief 外部要求重建 listenKey 与 WS 连接。
    void request_reconnect() {
        net::post(io_ctx_.ioc(), [this] {
            renew_listen_key_and_connect("external reconnect requested");
        });
    }

#ifdef XT_BUILD_TESTS
    void force_keepalive_for_test() {
        net::post(io_ctx_.ioc(), [this] { schedule_keepalive(); });
    }
#endif

    void inject_test_message(std::string_view payload) {
        handle_message(payload);
    }

private:
    void acquire_and_connect() {
        std::string err;
        listen_key_ = rest_.acquire_listen_key(&err);
        if (listen_key_.empty()) {
            XtLogger::warn_m("BinanceUDS", "acquire_listen_key failed: {}; 5s 后重试", err);
            schedule_reconnect();
            return;
        }
        if (!running_.load()) return;
        XtLogger::info_m("BinanceUDS", "listenKey acquired ({} chars)", listen_key_.size());
        // listenKey 是 WS target 的一部分；每次重新申请后必须重建底层连接。
        const std::string target = "/ws/" + listen_key_;
        xt_live::gateway::XtWsConnection::Config ws_cfg;
        ws_cfg.host = cfg_.host;
        ws_cfg.port = cfg_.port;
        ws_cfg.target = target;
        ws_cfg.log_tag = "BinanceUDS";
        ws_cfg.stale_threshold_ms = 5 * 60 * 1000;
        ws_connection_ = std::make_unique<xt_live::gateway::XtWsConnection>(
            io_ctx_.ioc(), io_ctx_.ssl_ctx(), std::move(ws_cfg));
        ws_connection_->bind_callbacks(
            [this](std::string_view sv) { handle_message(sv); },
            [this, target] {
                XtLogger::info_m("BinanceUDS", "ws connected: {}{}", cfg_.host, target);
                schedule_keepalive();
            },
            [] {});
        ws_connection_->start();
    }

    void schedule_reconnect() {
        if (!running_.load()) return;
        XtLogger::warn_m("BinanceUDS", "reconnect in 5000ms (listenKey acquire retry)");
        keepalive_timer_.expires_after(std::chrono::milliseconds(5000));
        keepalive_timer_.async_wait([this](boost::system::error_code ec) {
            if (ec || !running_.load()) return;
            listen_key_.clear();
            acquire_and_connect();
        });
    }

    void schedule_keepalive() {
        if (!running_.load()) return;
        keepalive_timer_.expires_after(std::chrono::milliseconds(cfg_.keepalive_ms));
        keepalive_timer_.async_wait([this](boost::system::error_code ec) {
            if (ec || !running_.load()) return;
            std::string err;
            if (!rest_.keepalive_listen_key(&err)) {
                XtLogger::warn_m("BinanceUDS",
                                 "keepalive failed: {}; rotate listenKey",
                                 err);
                renew_listen_key_and_connect("listenKey keepalive failed");
                return;
            }
            schedule_keepalive();
        });
    }

    void renew_listen_key_and_connect(const char* reason) {
        if (!running_.load()) return;
        XtLogger::warn_m("BinanceUDS",
                         "{}; acquiring fresh listenKey",
                         reason ? reason : "listenKey refresh");
        keepalive_timer_.cancel();
        if (ws_connection_) {
            ws_connection_->stop();
            ws_connection_.reset();
        }
        close_current_listen_key();
        acquire_and_connect();
    }

    void close_current_listen_key() {
        if (listen_key_.empty()) return;
        // stop/rotate 期间关闭 listenKey 使用短超时，避免 shutdown 被网络故障拖长。
        std::string err;
        (void)rest_.close_listen_key(&err, /*timeout_ms=*/3000);
        listen_key_.clear();
    }

    void stop_on_io_thread() {
        keepalive_timer_.cancel();
        if (ws_connection_) {
            ws_connection_->stop();
            ws_connection_.reset();
        }
        close_current_listen_key();
    }

public:
    void handle_message(std::string_view payload) {
        // 复用 thread_local json 实例，稳态 burst 下避免每帧 root + nested 节点 heap alloc。
        // g_uds_json 仅 ws_thread 使用，下面 handle_order_trade / handle_account
        // 的子 json (o / a) 是同一实例的引用视图，函数返回前完全消费。
        g_uds_json = nlohmann::json::parse(payload, nullptr, false);
        if (g_uds_json.is_discarded()) return;
        auto evt_it = g_uds_json.find("e");
        if (evt_it == g_uds_json.end() || !evt_it->is_string()) return;
        // string_view alias 进 g_uds_json 内部存储，仅在本函数体内使用 — 安全。
        const std::string_view evt(evt_it->get_ref<const std::string&>());
        static const auto empty_obj = nlohmann::json::object();
        if (evt == "ORDER_TRADE_UPDATE") {
            auto o_it = g_uds_json.find("o");
            handle_order_trade((o_it != g_uds_json.end() && o_it->is_object())
                                   ? *o_it : empty_obj);
        } else if (evt == "ACCOUNT_UPDATE") {
            auto a_it = g_uds_json.find("a");
            handle_account((a_it != g_uds_json.end() && a_it->is_object())
                               ? *a_it : empty_obj);
        } else if (evt == "listenKeyExpired") {
            renew_listen_key_and_connect("listenKey expired event");
        }
        // 其他事件（MARGIN_CALL / ACCOUNT_CONFIG_UPDATE 等）不处理
    }

private:
    /// @brief 解析 binance execution status 字符串 → XtOrderStatus（zero-copy 接口）。
    /// @note 已知状态表走 BinanceTdWireUtil SSOT；未知串在流式路径兜底 Submitted
    ///       （视为仍活的非终态更新，不误销 ADL/insurance 等未来状态单）。
    static XtOrderStatus parse_status(std::string_view s) {
        return parse_binance_status_opt(s).value_or(XtOrderStatus::Submitted);
    }

    void handle_order_trade(const nlohmann::json& o) {
        if (!parse_local_id_) return;
        // client_order_id alias 到 json 内部 std::string；parse_local_id_ 接 string_view，零拷贝。
        const std::string_view client_order_id_sv = value_or_view(o, "c");
        const uint64_t local_id = parse_local_id_(client_order_id_sv);
        if (local_id == 0) {
            // local_id=0 的 TRADE 无法更新本地增量链路；只触发未归因成交回调。
            // 5s gate 吸收强平/ADL burst。
            if (on_unattributed_fill_ && value_or_view(o, "x") == "TRADE") {
                const uint64_t now_us = lat_now_us();
                // 首次触发不依赖 steady-clock 已运行超过 5s。
                if (last_unattributed_fill_us_ == 0 ||
                    now_us - last_unattributed_fill_us_ >= 5'000'000ULL) {
                    last_unattributed_fill_us_ = now_us;
                    on_unattributed_fill_();
                }
            }
            return;  // 未知 id：不是本 gateway 生成的订单。
        }
        // BinanceLat t2：WS executionReport 到达本地的 steady 时刻。
        // 1/100 采样：t2 实测是健康满速期 journal 最大单项（~1300 行/分钟），
        // 延迟真值已有 prom histogram + live.jsonl 双通道。
        const std::string_view lat_exec_type = value_or_view(o, "x");
        const std::string_view lat_status    = value_or_view(o, "X");
        if (lat_t2_log_seq_++ % kLatT2LogSampleN == 0) {
            XtLogger::info_m("BinanceLat",
                             "kind=t2 local_id={} ts_us={} exec={} status={} sampled={}",
                             local_id, lat_now_us(),
                             lat_exec_type, lat_status, kLatT2LogSampleN);
        }

        XtOrderExecUpdate upd;
        upd.id = local_id;
        const StrategySid owner_sid = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id);
        upd.sid_id = owner_sid.raw();
        // string_view 比较，无 std::string 临时构造。
        upd.is_buy = (value_or_view(o, "S") == "BUY");
        const bool reduce_only = value_bool_or(o, "R", false);
        upd.close_offset = reduce_only ? XtOffset::Close : XtOffset::Open;
        // status default "NEW"：lat_status 已是 string_view，empty 即缺字段
        upd.status = parse_status(lat_status.empty() ? std::string_view("NEW") : lat_status);
        upd.price_fp = order_exec_to_fp(safe_stod(o, "p"));
        upd.qty_fp   = order_exec_to_fp(safe_stod(o, "q"));
        upd.filled_fp = order_exec_to_fp(safe_stod(o, "z"));
        upd.leaves_fp = upd.qty_fp - upd.filled_fp;
        upd.last_fill_fp = order_exec_to_fp(safe_stod(o, "l"));
        upd.exch_ts_us = static_cast<uint64_t>(value_int64_or(o, "T", 0)) * 1000;
        // local_cb_us 必须与 latency tracker 使用同一个 steady_clock 域。
        upd.local_cb_us = lat_now_us();
        if (on_order_) on_order_(upd);

        // 成交事件（x=TRADE）同时 emit XtTradeRecord；复用 lat_exec_type 避免重复取值。
        if (lat_exec_type == "TRADE" && on_trade_) {
            XtTradeRecord tr;
            tr.order_id = local_id;
            // 与 strategy_id 同源 stamp（复用本回调已算的 attribution lookup）。
            tr.sid_id = owner_sid.raw();
            if (const char* sid = xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(
                    owner_sid.raw())) {
                tr.strategy_id = sid;
            }
            // tr.code 是 std::string；回调返回后 g_uds_json 可被下一帧覆盖，必须拷贝。
            tr.code = std::string(value_or_view(o, "s"));
            tr.is_buy = upd.is_buy;
            tr.qty_fp = order_exec_to_fp(safe_stod(o, "l"));  // last filled qty
            tr.price = safe_stod(o, "L");  // last filled price
            tr.exec_price = tr.price;
            tr.fee = safe_stod(o, "n");
            const int64_t binance_trade_id = value_int64_or(o, "t", 0);
            tr.trade_id = std::to_string(binance_trade_id);
            tr.order_sys_id = std::to_string(value_int64_or(o, "i", 0));
            // live UDS 与 REST gap-fill 共享同一 fill 去重委托（ledger 记账判定）。
            // 只有首次记账的 fill 才更新 high-water、累计手续费并派发 on_trade。
            const bool tid_newly_seen =
                fill_first_seen(local_id, tr.code, binance_trade_id);
            if (tid_newly_seen) {
                update_last_seen_trade_id(tr.code, binance_trade_id);
                // ACCOUNT_UPDATE 不携带 commission；首次成交在 producer 侧累计 fee。
                cumulative_commission_ += tr.fee;
            }
            tr.is_close = reduce_only;
            tr.is_today = false;  // binance 无今昨之分
            // Binance 字段 m=true 表示 maker fill；透传给 runner maker/taker 统计。
            tr.is_passive = value_bool_or(o, "m", false);
            tr.fill_ts_us = upd.exch_ts_us;
            // 显式记录 steady 回调时刻，避免 latency tracker 在 submit/UDS 竞争时 fallback。
            tr.local_cb_us = lat_now_us();
            // Binance UDS 使用 UTC 自然日派生交易日与 action time。
            const int64_t event_ms = value_int64_or(o, "T", 0);  // binance fill transaction time
            tr.trade_date = xt_core::derive_trading_day(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            tr.trade_time = xt_core::derive_action_time(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            tr.trading_day = xt_core::derive_trading_day(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            // 仅在 trade_id 首次出现时 emit；重复帧已经由另一条路径应用过。
            if (tid_newly_seen) {
                on_trade_(tr, XtLatencyEligibility::kEligible);  // live UDS 实时
            } else {
                ++uds_trade_dedup_skips_;
                const uint64_t now_us = lat_now_us();
                if (now_us - last_dedup_skip_log_us_ >= 5'000'000ULL) {
                    XtLogger::warn_m("BinanceUDS",
                        "live UDS trade id={} already applied via gap-fill resync; "
                        "suppress re-emit to avoid account_store double-count "
                        "(total_skips={})",
                        binance_trade_id,
                        static_cast<unsigned long long>(uds_trade_dedup_skips_));
                    last_dedup_skip_log_us_ = now_us;
                }
            }
        }
    }

    void handle_account(const nlohmann::json& a) {
        // Binance ACCOUNT_UPDATE 的 P/B 数组只包含变化项。close_profit(cr) 需要按
        // symbol 保存最新值，再汇总成组合级 venue snapshot。
        auto p_it = a.find("P");
        const bool has_p = (p_it != a.end() && p_it->is_array());
        if (has_p) {
            for (const auto& p : *p_it) {
                const std::string_view sym_sv = value_or_view(p, "s");
                if (!sym_sv.empty()) {
                    // unordered_map<std::string,...> 当前不做 transparent lookup；
                    // 只有插入/更新 per-symbol cr 时 materialize key。
                    per_symbol_cr_[std::string(sym_sv)] = safe_stod(p, "cr");
                }
            }
        }
        double sum_cr = 0.0;
        for (const auto& kv : per_symbol_cr_) sum_cr += kv.second;
        // Balances (B) → AccountSnapshot。
        auto b_it = a.find("B");
        const bool has_b = (b_it != a.end() && b_it->is_array());
        if (has_b && on_account_) {
            double wallet_balance = 0.0;
            double cross_wallet = 0.0;
            bool usdt_seen = false;
            for (const auto& b : *b_it) {
                if (value_or_view(b, "a") == "USDT") {
                    wallet_balance = safe_stod(b, "wb");
                    cross_wallet = safe_stod(b, "cw");
                    usdt_seen = true;
                }
            }
            // B 数组只含「有变化」的资产；无 USDT 行（如纯 BNB 手续费扣减）时
            // wallet_balance 停在 0，emit 会让下游 SetCash(0) 把本地 cash 清零
            // （margin precheck available_cash 塌缩）。跳过 emit 保留上次锚定值。
            if (usdt_seen) {
                ITradeGateway::AccountSnapshot snap;
                snap.balance = wallet_balance;   // total wallet balance。
                snap.available = cross_wallet;   // cross wallet balance。
                snap.margin = wallet_balance - cross_wallet;  // 已占用保证金近似值。
                // ACCOUNT_UPDATE 自身不携带 commission；这里输出 producer 侧累计的 venue 快照。
                snap.close_profit = sum_cr;
                snap.commission   = cumulative_commission_;
                // frozen_margin / pre_balance 在该 event 中拿不到，保留默认 0。
                on_account_(snap);
            }
        }
        // Positions (P) → XtPositionSnapshot[]。
        // `m=ORDER` 的仓位变化已由去重后的 x=TRADE 增量驱动，不能再 emit absolute
        // position 覆盖/叠加。FUNDING_FEE、手动调整等带外 reason 仍通过 on_position
        // 同步 absolute snapshot。
        const std::string_view account_reason = value_or_view(a, "m");
        const bool is_order_reason = (account_reason == "ORDER");
        if (has_p && on_position_ && !is_order_reason) {
            std::vector<XtPositionSnapshot> positions;
            positions.reserve(p_it->size());
            for (const auto& p : *p_it) {
                XtPositionSnapshot s;
                // s.code 是 std::string；跨回调派发后 g_uds_json 会被覆盖，必须拷贝。
                s.code = std::string(value_or_view(p, "s"));
                const double pa = safe_stod(p, "pa");
                const double ep = safe_stod(p, "ep");
                if (pa > 0) {
                    s.long_qty = pa;
                    s.long_avg = ep;
                } else if (pa < 0) {
                    s.short_qty = -pa;
                    s.short_avg = ep;
                }
                positions.push_back(std::move(s));
            }
            on_position_(positions);
        }
    }

public:
    // ── 构造期依赖 ─────────────────────────────────────────────────
    Config cfg_;
    BinanceRestClient& rest_;  ///< listenKey lifecycle 使用的 REST client；不拥有。

    // ── UDS IO 线程资源（io_context + ssl_ctx + 独占线程由 IoContextThread 持有）──
    xt_live::gateway::IoContextThread io_ctx_;
    std::atomic<bool> running_{false};

    // ── listenKey 与底层 WS 连接（ws_thread 访问）──────────────────
    std::unique_ptr<xt_live::gateway::XtWsConnection> ws_connection_;
    net::steady_timer keepalive_timer_;
    std::string listen_key_;

    // ── 外部回调（正常路径在 ws_thread 触发）───────────────────────
    OnOrderUpdate on_order_;
    OnTrade on_trade_;
    OnAccount on_account_;
    OnPosition on_position_;
    ParseLocalId parse_local_id_;
    BinanceUserDataStream::OnUnattributedFill on_unattributed_fill_;

    // ── venue accounting 快照（ws_thread 写）───────────────────────
    double cumulative_commission_ = 0.0;
    std::unordered_map<std::string, double> per_symbol_cr_;

    // ── live UDS vs REST gap-fill 去重日志（ws_thread 写）───────────
    uint64_t uds_trade_dedup_skips_ = 0;
    uint64_t last_dedup_skip_log_us_ = 0;
    uint64_t last_unattributed_fill_us_ = 0;
    // BinanceLat kind=t2 采样（ws_thread 私有）；同 gateway kind=trace/t0 的 1/100 口径。
    static constexpr uint64_t kLatT2LogSampleN = 100;
    uint64_t lat_t2_log_seq_ = 0;

    // ── fill 去重委托（ws 线程 + reconcile 注入线程并发调用）───────────
    // authority 在 gateway 的 BinanceOrderLedger（per-order fills 集 +
    // orderless `symbol|trade_id` 复合窗），live UDS 与 REST gap-fill 共用。
    BinanceUserDataStream::FillDedup fill_dedup_;
    std::atomic<bool> fill_dedup_unbound_warned_{false};

    /// @brief fill 去重判定：委托未绑定时按首次处理（去重禁用）+ error 一次。
    /// @return true 表示首次记账；false 表示重复，调用方应跳过 emit。
    bool fill_first_seen(uint64_t order_id, const std::string& symbol, int64_t tid) {
        if (!fill_dedup_) {
            if (!fill_dedup_unbound_warned_.exchange(true)) {
                XtLogger::error_m("BinanceUDS",
                    "fill dedup delegate not bound; live/gap-fill dedup DISABLED "
                    "(double-count risk) — production wiring must call set_fill_dedup");
            }
            return true;
        }
        return fill_dedup_(order_id, symbol, tid);
    }

    // ── gap-fill bookkeeping ───────────────────────────────────────
    // last_trade_id_per_symbol_ 记录每个 symbol 的 Binance trade id 高水位；
    // reconcile gap-fill 用 last_seen+1 作 fromId 起点。
    mutable std::mutex trade_highwater_mu_;
    std::unordered_map<std::string, std::atomic<int64_t>> last_trade_id_per_symbol_;

    /// @brief 更新 symbol 的 trade id 高水位。
    void update_last_seen_trade_id(const std::string& symbol, int64_t tid) {
        if (tid <= 0 || symbol.empty()) return;
        // 新 symbol 插入需要 mutex；已有 slot 用 atomic CAS 提升高水位。
        std::lock_guard<std::mutex> lk(trade_highwater_mu_);
        auto& slot = last_trade_id_per_symbol_[symbol];
        int64_t prev = slot.load(std::memory_order_relaxed);
        while (tid > prev && !slot.compare_exchange_weak(prev, tid,
                                                         std::memory_order_release,
                                                         std::memory_order_relaxed)) {
            // CAS 失败时 prev 已更新，继续尝试直到 tid 不再更高。
        }
    }
};

// ===========================================================================
// Forwarding impl
// ===========================================================================

BinanceUserDataStream::BinanceUserDataStream(Config cfg, BinanceRestClient& rest)
    : impl_(std::make_unique<Impl>(std::move(cfg), rest)) {}

BinanceUserDataStream::~BinanceUserDataStream() = default;

void BinanceUserDataStream::bind_callbacks(OnOrderUpdate on_o, OnTrade on_t,
                                           OnAccount on_a, OnPosition on_p,
                                           ParseLocalId parse) {
    impl_->bind_callbacks(std::move(on_o), std::move(on_t), std::move(on_a),
                          std::move(on_p), std::move(parse));
}

void BinanceUserDataStream::set_unattributed_fill_handler(OnUnattributedFill handler) {
    impl_->set_unattributed_fill_handler(std::move(handler));
}

void BinanceUserDataStream::set_fill_dedup(FillDedup fn) {
    impl_->set_fill_dedup(std::move(fn));
}

bool BinanceUserDataStream::start() { return impl_->start(); }
void BinanceUserDataStream::stop()  { impl_->stop(); }

bool BinanceUserDataStream::check_health(uint32_t now_ms) {
    return impl_->check_health(now_ms);
}

void BinanceUserDataStream::request_reconnect() {
    impl_->request_reconnect();
}

int64_t BinanceUserDataStream::last_seen_trade_id(const std::string& symbol) const {
    std::lock_guard<std::mutex> lk(impl_->trade_highwater_mu_);
    auto it = impl_->last_trade_id_per_symbol_.find(symbol);
    return it == impl_->last_trade_id_per_symbol_.end()
        ? 0
        : it->second.load(std::memory_order_acquire);
}

std::vector<std::string> BinanceUserDataStream::tracked_trade_symbols() const {
    std::lock_guard<std::mutex> lk(impl_->trade_highwater_mu_);
    std::vector<std::string> out;
    out.reserve(impl_->last_trade_id_per_symbol_.size());
    for (const auto& kv : impl_->last_trade_id_per_symbol_) out.push_back(kv.first);
    return out;
}

bool BinanceUserDataStream::inject_gap_fill_trade(const XtTradeRecord& tr) {
    int64_t tid = 0;
    try { tid = std::stoll(tr.trade_id); } catch (...) { tid = 0; }
    // 与 live UDS 共享同一 fill 去重委托；tid<=0 无法去重，保守允许 emit
    //（委托侧同语义）。order_id=0 的归因失败行由 orderless 复合窗兜底。
    if (!impl_->fill_first_seen(tr.order_id, tr.code, tid)) {
        return false;
    }
    impl_->update_last_seen_trade_id(tr.code, tid);
    // REST gap-fill 补齐 = 延迟重放，时序与 venue 脱钩 → kIneligible。
    if (impl_->on_trade_) impl_->on_trade_(tr, XtLatencyEligibility::kIneligible);
    return true;
}

#ifdef XT_BUILD_TESTS
void BinanceUserDataStream::inject_test_message(std::string_view payload) {
    impl_->inject_test_message(payload);
}

void BinanceUserDataStream::force_keepalive_for_test() {
    impl_->force_keepalive_for_test();
}
#endif

}  // namespace binance_u
