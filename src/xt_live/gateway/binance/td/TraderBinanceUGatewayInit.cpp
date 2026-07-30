#include "TraderBinanceUGatewayImpl.h"

#include <chrono>
#include <map>
#include <utility>
#include <thread>

#include <nlohmann/json.hpp>

#include "transport/BinanceTdWireUtil.h"
#include "td/BinanceTdParse.h"
#include "rest/BinanceRestClient.h"
#include "transport/BinanceWsUrl.h"
#include "rest/BinanceSign.h"
#include "BinancePlainCloseFallback.h"
#include "transport/RestTradeTransport.h"
#include "transport/WsTradeTransport.h"
#include "refdata/BinanceExchangeInfo.h"
#include "XtFixedPoint.h"
#include "xt_logger/XtLogger.h"

namespace binance_u {

bool TraderBinanceUGateway::Impl::init(const ITradeGateway::Config& cfg) {

    if (td_cfg.base_rest_url.empty()) {
        XtLogger::error_m("BinanceTD", "binance.base_rest_url 为空");
        return false;
    }
    if (td_cfg.api_key.empty() || td_cfg.api_secret.empty()) {
        XtLogger::error_m("BinanceTD", "binance.api_key / api_secret 为空");
        return false;
    }

    if (!local_id_gen_) {
        XtLogger::error_m("BinanceTD",
            "init rejected: local_id generator unavailable: {}",
            local_id_gen_error_.empty() ? std::string("configure() not called")
                                        : local_id_gen_error_);
        return false;
    }

    XtLogger::info_m("BinanceTD", "local_id domain: instance='{}' tag={}",
                     td_cfg.local_id_instance, local_id_gen_->instance_tag());

    if (!cfg.expected_api_key_sha8.empty()) {
        std::string got;
        try {
            got = sign::api_key_sha8(td_cfg.api_key);
        } catch (const std::exception& e) {

            XtLogger::error_m("BinanceTD", "init rejected: api_key_sha8 digest failed: {}", e.what());
            return false;
        }
        if (got != cfg.expected_api_key_sha8) {
            XtLogger::error_m("BinanceTD",
                "init rejected: api_key 指纹不符 expected_api_key_sha8={} got={} — "
                "凭证与部署身份错配 (multibook isolation)",
                cfg.expected_api_key_sha8, got);
            return false;
        }
        XtLogger::info_m("BinanceTD", "api_key identity 校验通过 sha8={}", got);
    }

    BinanceRestClient::Config rcfg;
    rcfg.base_url = td_cfg.base_rest_url;
    rcfg.api_key = td_cfg.api_key;
    rcfg.api_secret = td_cfg.api_secret;
    rcfg.recv_window_ms = td_cfg.recv_window_ms;
    rcfg.timeout_sec = 10;
    rest = std::make_unique<BinanceRestClient>(std::move(rcfg));

    if (!rest->sync_server_time()) {
        XtLogger::error_m("BinanceTD",
            "init: sync_server_time failed; check network / base_rest_url={}",
            td_cfg.base_rest_url);
        return false;
    }
    std::string lk_err;
    std::string lk = rest->acquire_listen_key(&lk_err);
    if (lk.empty()) {
        XtLogger::error_m("BinanceTD",
            "init: api_key invalid (acquire_listen_key failed): {}", lk_err);
        return false;
    }

    std::string close_err;
    (void)rest->close_listen_key(&close_err);

    auto account_resp = rest->get_signed("/fapi/v2/account", {});
    if (!account_resp.ok()) {
        XtLogger::error_m("BinanceTD",
            "init: signed GET /fapi/v2/account failed (HTTP {}): {} — "
            "可能 api_secret 错配 / 未勾 Futures Trading / IP 白名单",
            account_resp.http_status, account_resp.body);
        return false;
    }
    XtLogger::info_m("BinanceTD",
        "init validated: sync_time + listenKey + signed account 全 OK");

    {
        std::unordered_map<std::string, SymbolFilter> filters;
        std::string ex_err;
        bool loaded = false;
        for (int attempt = 1; attempt <= 3 && !loaded; ++attempt) {
            if (attempt > 1) std::this_thread::sleep_for(std::chrono::milliseconds(500));
            auto resp = rest->get_public("/fapi/v1/exchangeInfo");
            if (!resp.ok()) {
                XtLogger::warn_m("BinanceTD",
                    "init: GET /fapi/v1/exchangeInfo failed (HTTP {} attempt {}/3): {}",
                    resp.http_status, attempt, resp.curl_err);
                continue;
            }
            if (!parse_exchange_info(resp.body, filters, &ex_err)) {
                XtLogger::warn_m("BinanceTD",
                    "init: exchangeInfo parse failed (attempt {}/3): {}", attempt, ex_err);
                continue;
            }
            loaded = true;
        }
        if (!loaded || filters.empty()) {
            XtLogger::error_m("BinanceTD",
                "init: exchangeInfo 加载失败/为空（{} symbols）— fail-closed，不启动以免发 off-tick 单",
                filters.size());
            return false;
        }
        const std::size_t n = filters.size();
        BinanceSymbolFilters::global().load(std::move(filters));
        XtLogger::info_m("BinanceTD", "init: exchangeInfo loaded ({} symbols)", n);
    }

    if (!create_trade_transport()) return false;
    td_transport->bind_ack_handler(
        [this](const TransportAck& ack, const TransportLatencyTrace& trace) {
            this->on_transport_ack(ack, trace);
        });
    if (!td_transport->start()) {
        XtLogger::error_m("BinanceTD", "trade transport start failed");
        return false;
    }
    return true;
}

bool TraderBinanceUGateway::Impl::start() {
    if (running.exchange(true)) return true;
    reset_td_readiness();

    {
        std::lock_guard<std::mutex> lk(cold_worker_mu);
        cold_worker_tasks.push_back(ColdWorkerTask{ColdWorkerTask::kTimeSync});
        cold_worker_tasks.push_back(ColdWorkerTask{ColdWorkerTask::kBootstrapSnapshot});
    }
    cold_worker_thread = std::thread([this] { cold_worker_loop(); });

    BinanceUserDataStream::Config uds_cfg;
    uds_cfg.host = parse_binance_ws_url(td_cfg.base_ws_url, "443").host;
    uds_cfg.port = "443";
    uds_cfg.keepalive_ms = td_cfg.listen_key_refresh_ms;
    uds = std::make_unique<BinanceUserDataStream>(uds_cfg, *rest);

    uds->set_fill_dedup([this](uint64_t order_id, std::string_view symbol, int64_t tid) {
        return ledger_.note_fill_seen(order_id, symbol, tid);
    });
    uds->bind_callbacks(
        [this](const XtOrderExecUpdate& upd) {
            this->on_uds_order_update(upd);
        },
        [this](const XtTradeRecord& tr, XtLatencyEligibility elig) {

            forward_uds_trade(tr, elig);
        },
        [this](const ITradeGateway::AccountSnapshot& a) { if (cb.on_account)  cb.on_account(a); },
        [this](const std::vector<XtPositionSnapshot>& ps) {
            forward_uds_position(ps);
        },
        [this](std::string_view s) { return parse_local_id_from_client_order_id(s); });

    uds->set_unattributed_fill_handler([this]() {
        const uint64_t now_us = lat_now_us();
        if (last_unattributed_resync_us_ != 0 &&
            now_us - last_unattributed_resync_us_ < 30'000'000ULL) {
            return;
        }
        if (queue_reconcile_resync_if_absent_()) {
            last_unattributed_resync_us_ = now_us;
        }
    });
    uds->start();

    keepalive_thread = std::thread([this] {
        uint64_t tick = 0;
        while (running.load()) {

            {
                std::unique_lock<std::mutex> lk(shutdown_mu_);
                shutdown_cv_.wait_for(lk, std::chrono::seconds(30),
                    [this] { return !running.load(std::memory_order_acquire); });
            }
            if (!running.load(std::memory_order_acquire)) break;
            ++tick;
            const auto pick = keepalive_pick_(static_cast<int64_t>(lat_now_us() / 1000));
            if (!pick.has_value()) continue;
            {
                std::lock_guard<std::mutex> lk(cold_worker_mu);
                cold_worker_tasks.push_back(ColdWorkerTask{*pick});
                cold_worker_cv.notify_one();
            }
            XtLogger::info_m("BinanceTD", "keepalive tick={} → push {}", tick,
                             *pick == ColdWorkerTask::kBootstrapSnapshot
                                 ? "kBootstrapSnapshot (td not ready, bootstrap retry)"
                                 : "kRefreshSnapshot");
        }
        XtLogger::warn_m("BinanceTD",
            "keepalive thread exiting (running={})", running.load());
    });

    return true;
}

void TraderBinanceUGateway::Impl::stop() {
    const bool was_running = running.exchange(false);
    if (was_running) {
        cold_worker_cv.notify_all();

        {
            std::lock_guard<std::mutex> lk(shutdown_mu_);
        }
        shutdown_cv_.notify_all();
    }
    if (td_transport) td_transport->stop();

    if (uds) uds->stop();
    if (keepalive_thread.joinable()) keepalive_thread.join();
    if (cold_worker_thread.joinable()) cold_worker_thread.join();

    {
        std::lock_guard<std::mutex> lk(uds_mu_);
        uds.reset();
    }
}

bool TraderBinanceUGateway::Impl::query_wallet_balance_sync(double& out_balance) {
    if (!rest) return false;
    auto ar = rest->get_signed("/fapi/v2/account", {});
    if (!ar.ok()) {
        XtLogger::warn_m("BinanceTD",
            "query_wallet_balance_sync: GET account failed http={} err='{}'",
            ar.http_status, ar.curl_err);
        return false;
    }
    const auto bal = parse_wallet_balance_body(ar.body);
    if (!bal) {
        XtLogger::warn_m("BinanceTD",
            "query_wallet_balance_sync: totalWalletBalance parse failed");
        return false;
    }
    out_balance = *bal;
    return true;
}

uint64_t TraderBinanceUGateway::Impl::parse_local_id_from_client_order_id(std::string_view s) {
    if (const auto local_id = ledger_.local_for_client(s)) return *local_id;
    return parse_local_id(s);
}

bool TraderBinanceUGateway::Impl::check_health(uint32_t now_ms) {
    const uint32_t last = last_time_sync_ms.load(std::memory_order_relaxed);
    if (last == 0 || now_ms - last > td_cfg.time_sync_interval_ms) {
        last_time_sync_ms.store(now_ms, std::memory_order_relaxed);

        {
            std::lock_guard<std::mutex> lk(cold_worker_mu);
            cold_worker_tasks.push_back(ColdWorkerTask{ColdWorkerTask::kTimeSync});
        }
        cold_worker_cv.notify_one();
    }

    bool uds_healthy = true;
    {
        std::lock_guard<std::mutex> lk(uds_mu_);
        if (uds) uds_healthy = uds->check_health(now_ms);
    }
    check_trade_transport_health(now_ms);
    return uds_healthy;
}

bool TraderBinanceUGateway::Impl::create_trade_transport() {
    if (td_cfg.transport == BinanceTdCfg::Transport::kRest) {
        td_transport = std::make_unique<RestTradeTransport>(*rest);
        return true;
    }
    if (td_cfg.base_ws_trade_url.empty()) {
        XtLogger::error_m("BinanceTD", "binance.base_ws_trade_url 为空");
        return false;
    }
    const auto url = parse_binance_ws_url(td_cfg.base_ws_trade_url, "443");
    if (url.host.empty()) {
        XtLogger::error_m("BinanceTD",
                          "binance.base_ws_trade_url host 为空: {}",
                          td_cfg.base_ws_trade_url);
        return false;
    }

    WsTradeTransport::Config ws_cfg;
    ws_cfg.host = url.host;
    ws_cfg.port = url.port;
    ws_cfg.target = url.path_root.empty() ? "/ws-fapi/v1" : url.path_root;
    ws_cfg.submit_timeout_ms = td_cfg.ws_submit_timeout_ms;
    ws_cfg.stale_drop_ms = td_cfg.ws_stale_drop_ms;
    ws_cfg.send_queue_cap = td_cfg.ws_send_queue_cap;
    ws_cfg.reconnect_min_ms = td_cfg.ws_reconnect_min_ms;
    ws_cfg.reconnect_max_ms = td_cfg.ws_reconnect_max_ms;

    WsTradeCredentials cred;
    cred.api_key = td_cfg.api_key;
    cred.api_secret = td_cfg.api_secret;
    cred.recv_window_ms = td_cfg.recv_window_ms;
    td_transport = std::make_unique<WsTradeTransport>(
        std::move(ws_cfg), *rest, std::move(cred));
    return true;
}

void TraderBinanceUGateway::Impl::check_trade_transport_health(uint32_t now_ms) {
    if (!td_transport || td_cfg.transport != BinanceTdCfg::Transport::kWs) return;
    const auto h = td_transport->health();
    if (h.connected) {
        transport_down_since_ms.store(0, std::memory_order_relaxed);
        return;
    }
    uint32_t since = transport_down_since_ms.load(std::memory_order_relaxed);
    if (since == 0) {
        transport_down_since_ms.store(now_ms, std::memory_order_relaxed);
        return;
    }
    if (now_ms - since <= 30000) return;
    const uint32_t last_warn = transport_down_warn_ms.load(std::memory_order_relaxed);
    if (last_warn == 0 || now_ms - last_warn > 30000) {
        transport_down_warn_ms.store(now_ms, std::memory_order_relaxed);
        XtLogger::warn_m("BinanceTD",
            "ws trade transport disconnected for {}ms; keep health=true, reconcile will guard drift",
            now_ms - since);
    }
}

void TraderBinanceUGateway::Impl::reset_td_readiness() {
    readiness_gate_.reset("binance_u");
    readiness_gate_.set_not_required(xt_live::gateway::TdReadinessStage::Settlement,
                                     "binance_u has no daily settlement confirmation");
    ready_emitted_.store(false, std::memory_order_release);
    reconciler_.reset_bootstrap_backoff();
}

void TraderBinanceUGateway::Impl::emit_ready_if_td_readiness_satisfied() {
    const auto report = readiness_gate_.report();
    if (!report.ready) {
        XtLogger::error_m("BinanceTD",
                          "td readiness blocked venue={} stage={} reason={}",
                          report.venue, report.blocking_stage, report.blocking_reason);
        return;
    }
    if (ready_emitted_.load(std::memory_order_acquire)) return;
    ready_emitted_.store(true, std::memory_order_release);
    if (cb.on_ready) cb.on_ready();
}

std::optional<TraderBinanceUGateway::Impl::ColdWorkerTask::Kind>
TraderBinanceUGateway::Impl::keepalive_pick_(int64_t now_ms) {

    if (ready_emitted_.load(std::memory_order_acquire)) return ColdWorkerTask::kRefreshSnapshot;
    if (now_ms < reconciler_.bootstrap_next_retry_ms()) return std::nullopt;
    return ColdWorkerTask::kBootstrapSnapshot;
}

bool TraderBinanceUGateway::Impl::queue_reconcile_resync_if_absent_() {
    std::lock_guard<std::mutex> lk(cold_worker_mu);
    for (const auto& m : cold_worker_tasks) {
        if (m.kind == ColdWorkerTask::kReconcileResync) return false;
    }
    cold_worker_tasks.push_back(ColdWorkerTask{ColdWorkerTask::kReconcileResync});
    cold_worker_cv.notify_one();
    return true;
}

void TraderBinanceUGateway::Impl::request_resnap() {

    if (!queue_reconcile_resync_if_absent_()) {
        XtLogger::info_m("BinanceTD",
            "request_resnap merged: kReconcileResync already queued");
        return;
    }

    if (uds) {
        XtLogger::warn_m("BinanceTD",
            "request_resnap → UDS reconnect + queue kReconcileResync (gap-fill + position resync)");
        uds->request_reconnect();
    }
}

void TraderBinanceUGateway::Impl::cold_worker_loop() {
    while (running.load()) {
        std::unique_lock<std::mutex> lk(cold_worker_mu);
        cold_worker_cv.wait(lk, [this] { return !cold_worker_tasks.empty() || !running.load(); });
        if (!running.load() && cold_worker_tasks.empty()) return;

        if (!cold_worker_tasks.empty()) {
            auto m = cold_worker_tasks.front();
            cold_worker_tasks.pop_front();
            lk.unlock();
            if (m.kind == ColdWorkerTask::kTimeSync) {
                (void)rest->sync_server_time();
            } else if (m.kind == ColdWorkerTask::kBootstrapSnapshot) {
                reconciler_.bootstrap_snapshot();
            } else if (m.kind == ColdWorkerTask::kRefreshSnapshot) {
                reconciler_.refresh_snapshot();
            } else if (m.kind == ColdWorkerTask::kReconcileResync) {
                reconciler_.reconcile_resync();
            } else if (m.kind == ColdWorkerTask::kPlainCloseFallback) {

                int64_t venue_pos_fp = 0;
                bool    venue_known  = false;
                {
                    auto pr = rest->get_signed("/fapi/v2/positionRisk", {{"symbol", m.fb_symbol}});
                    if (pr.ok()) {
                        auto j = nlohmann::json::parse(pr.body, nullptr,  false);
                        if (j.is_array()) {
                            for (const auto& p : j) {
                                if (p.value("symbol", std::string{}) == m.fb_symbol) {
                                    try { venue_pos_fp = xt_fp::to_fp(std::stod(p.value("positionAmt", "0"))); }
                                    catch (...) { venue_pos_fp = 0; }
                                    break;
                                }
                            }
                            venue_known = true;
                        }
                    } else {
                        XtLogger::warn_m("BinanceTD",
                            "PLAIN-CLOSE-FALLBACK positionRisk fetch failed symbol={} http={} — skip (no flip)",
                            m.fb_symbol, pr.http_status);
                    }
                }
                std::optional<PlainCloseFallbackDecision> dec;
                if (venue_known)
                    dec = decide_plain_close_fallback(m.fb_is_buy, m.fb_qty_fp, venue_pos_fp);
                if (!dec) {
                    XtLogger::warn_m("BinanceTD",
                        "PLAIN-CLOSE-FALLBACK skipped symbol={} venue_pos_fp={} frozen_qty_fp={} "
                        "venue_known={} (flat / direction-mismatch / fetch-fail → no flip)",
                        m.fb_symbol, venue_pos_fp, m.fb_qty_fp, venue_known);
                } else {
                    XtLogger::warn_m("BinanceTD",
                        "PLAIN-CLOSE-FALLBACK re-submit symbol={} side={} qty_fp={} px_fp={} "
                        "venue_pos_fp={} (reduceOnly close -2022 rejected; sized to venue truth)",
                        m.fb_symbol, dec->is_buy ? "BUY" : "SELL", dec->qty_fp, m.fb_price_fp,
                        venue_pos_fp);
                    submit_order_inq(m.fb_symbol.c_str(), dec->is_buy, m.fb_price_fp, dec->qty_fp,
                                     XtTimeInForce::IOC, "reduceonly_fallback_close",
                                     XtOffset::Open, exec_none(), 0);
                }
            }
            continue;
        }
    }
}

}
