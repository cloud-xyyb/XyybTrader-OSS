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
#include <cmath>
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

inline std::string_view value_or_view(const nlohmann::json& j, const char* key,
                                      std::string_view dflt = std::string_view()) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return dflt;
    const std::string& s = it->get_ref<const std::string&>();
    return std::string_view(s);
}

double safe_stod(const nlohmann::json& j, const char* key, double dflt = 0.0) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return dflt;
    try {
        const std::string& s = it->get_ref<const std::string&>();
        const double v = std::stod(s);
        return std::isfinite(v) ? v : dflt;
    } catch (...) { return dflt; }
}

inline int64_t value_int64_or(const nlohmann::json& j, const char* key, int64_t dflt = 0) {
    auto it = j.find(key);
    if (it == j.end()) return dflt;
    if (it->is_number_integer()) return it->get<int64_t>();
    if (it->is_number_unsigned()) return static_cast<int64_t>(it->get<uint64_t>());
    return dflt;
}

inline bool value_bool_or(const nlohmann::json& j, const char* key, bool dflt = false) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_boolean()) return dflt;
    return it->get<bool>();
}

inline uint64_t lat_now_us() noexcept {
    return XtSteadyClock::steady_now_us();
}

}

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

        io_ctx_.start([this] { acquire_and_connect(); });
        return true;
    }

    void stop() {
        if (!running_.exchange(false)) return;

        io_ctx_.stop([this] { stop_on_io_thread(); });
    }

    bool check_health(uint32_t now_ms) {

        if (ws_connection_ && !ws_connection_->check_stale(now_ms)) {
            net::post(io_ctx_.ioc(), [this] {
                if (ws_connection_) ws_connection_->request_reconnect();
            });
            return false;
        }
        return true;
    }

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

        std::string err;
        (void)rest_.close_listen_key(&err,  3000);
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

        g_uds_json = nlohmann::json::parse(payload, nullptr, false);
        if (g_uds_json.is_discarded()) return;
        auto evt_it = g_uds_json.find("e");
        if (evt_it == g_uds_json.end() || !evt_it->is_string()) return;

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

    }

private:

    static XtOrderStatus parse_status(std::string_view s) {
        return parse_binance_status_opt(s).value_or(XtOrderStatus::Submitted);
    }

    void handle_order_trade(const nlohmann::json& o) {
        if (!parse_local_id_) return;

        const std::string_view client_order_id_sv = value_or_view(o, "c");
        const uint64_t local_id = parse_local_id_(client_order_id_sv);
        if (local_id == 0) {

            if (on_unattributed_fill_ && value_or_view(o, "x") == "TRADE") {
                const uint64_t now_us = lat_now_us();

                if (last_unattributed_fill_us_ == 0 ||
                    now_us - last_unattributed_fill_us_ >= 5'000'000ULL) {
                    last_unattributed_fill_us_ = now_us;
                    on_unattributed_fill_();
                }
            }
            return;
        }

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

        upd.is_buy = (value_or_view(o, "S") == "BUY");
        const bool reduce_only = value_bool_or(o, "R", false);
        upd.close_offset = reduce_only ? XtOffset::Close : XtOffset::Open;

        upd.status = parse_status(lat_status.empty() ? std::string_view("NEW") : lat_status);
        upd.price_fp = order_exec_to_fp(safe_stod(o, "p"));
        upd.qty_fp   = order_exec_to_fp(safe_stod(o, "q"));
        upd.filled_fp = order_exec_to_fp(safe_stod(o, "z"));
        upd.leaves_fp = upd.qty_fp - upd.filled_fp;
        upd.last_fill_fp = order_exec_to_fp(safe_stod(o, "l"));
        upd.exch_ts_us = static_cast<uint64_t>(value_int64_or(o, "T", 0)) * 1000;

        upd.local_cb_us = lat_now_us();
        if (on_order_) on_order_(upd);

        if (lat_exec_type == "TRADE" && on_trade_) {
            XtTradeRecord tr;
            tr.order_id = local_id;

            tr.sid_id = owner_sid.raw();
            if (const char* sid = xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(
                    owner_sid.raw())) {
                tr.strategy_id = sid;
            }

            tr.code = std::string(value_or_view(o, "s"));
            tr.is_buy = upd.is_buy;
            tr.qty_fp = order_exec_to_fp(safe_stod(o, "l"));
            tr.price = safe_stod(o, "L");
            tr.exec_price = tr.price;
            tr.fee = safe_stod(o, "n");
            const int64_t binance_trade_id = value_int64_or(o, "t", 0);
            tr.trade_id = std::to_string(binance_trade_id);
            tr.order_sys_id = std::to_string(value_int64_or(o, "i", 0));

            const bool tid_newly_seen =
                fill_first_seen(local_id, tr.code, binance_trade_id);
            if (tid_newly_seen) {
                update_last_seen_trade_id(tr.code, binance_trade_id);

                cumulative_commission_ += tr.fee;
            }
            tr.is_close = reduce_only;
            tr.is_today = false;

            tr.is_passive = value_bool_or(o, "m", false);
            tr.fill_ts_us = upd.exch_ts_us;

            tr.local_cb_us = lat_now_us();

            const int64_t event_ms = value_int64_or(o, "T", 0);
            tr.trade_date = xt_core::derive_trading_day(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            tr.trade_time = xt_core::derive_action_time(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            tr.trading_day = xt_core::derive_trading_day(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);

            if (tid_newly_seen) {
                on_trade_(tr, XtLatencyEligibility::kEligible);
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

        auto p_it = a.find("P");
        const bool has_p = (p_it != a.end() && p_it->is_array());
        if (has_p) {
            for (const auto& p : *p_it) {
                const std::string_view sym_sv = value_or_view(p, "s");
                if (!sym_sv.empty()) {

                    per_symbol_cr_[std::string(sym_sv)] = safe_stod(p, "cr");
                }
            }
        }
        double sum_cr = 0.0;
        for (const auto& kv : per_symbol_cr_) sum_cr += kv.second;

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

            if (usdt_seen) {
                ITradeGateway::AccountSnapshot snap;
                snap.balance = wallet_balance;
                snap.available = cross_wallet;
                snap.margin = wallet_balance - cross_wallet;

                snap.close_profit = sum_cr;
                snap.commission   = cumulative_commission_;

                on_account_(snap);
            }
        }

        const std::string_view account_reason = value_or_view(a, "m");
        const bool is_order_reason = (account_reason == "ORDER");
        if (has_p && on_position_ && !is_order_reason) {
            std::vector<XtPositionSnapshot> positions;
            positions.reserve(p_it->size());
            for (const auto& p : *p_it) {
                XtPositionSnapshot s;

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

    Config cfg_;
    BinanceRestClient& rest_;

    xt_live::gateway::IoContextThread io_ctx_;
    std::atomic<bool> running_{false};

    std::unique_ptr<xt_live::gateway::XtWsConnection> ws_connection_;
    net::steady_timer keepalive_timer_;
    std::string listen_key_;

    OnOrderUpdate on_order_;
    OnTrade on_trade_;
    OnAccount on_account_;
    OnPosition on_position_;
    ParseLocalId parse_local_id_;
    BinanceUserDataStream::OnUnattributedFill on_unattributed_fill_;

    double cumulative_commission_ = 0.0;
    std::unordered_map<std::string, double> per_symbol_cr_;

    uint64_t uds_trade_dedup_skips_ = 0;
    uint64_t last_dedup_skip_log_us_ = 0;
    uint64_t last_unattributed_fill_us_ = 0;

    static constexpr uint64_t kLatT2LogSampleN = 100;
    uint64_t lat_t2_log_seq_ = 0;

    BinanceUserDataStream::FillDedup fill_dedup_;
    std::atomic<bool> fill_dedup_unbound_warned_{false};

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

    mutable std::mutex trade_highwater_mu_;
    std::unordered_map<std::string, std::atomic<int64_t>> last_trade_id_per_symbol_;

    void update_last_seen_trade_id(const std::string& symbol, int64_t tid) {
        if (tid <= 0 || symbol.empty()) return;

        std::lock_guard<std::mutex> lk(trade_highwater_mu_);
        auto& slot = last_trade_id_per_symbol_[symbol];
        int64_t prev = slot.load(std::memory_order_relaxed);
        while (tid > prev && !slot.compare_exchange_weak(prev, tid,
                                                         std::memory_order_release,
                                                         std::memory_order_relaxed)) {

        }
    }
};

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

    if (!impl_->fill_first_seen(tr.order_id, tr.code, tid)) {
        return false;
    }
    impl_->update_last_seen_trade_id(tr.code, tid);

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

}
