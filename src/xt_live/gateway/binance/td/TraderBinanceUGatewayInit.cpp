#include "TraderBinanceUGatewayImpl.h"

#include <chrono>
#include <map>
#include <utility>
#include <thread>

#include <nlohmann/json.hpp>

#include "transport/BinanceTdWireUtil.h"       // parse_local_id
#include "td/BinanceTdParse.h"                 // parse_wallet_balance_body
#include "rest/BinanceRestClient.h"            // 停机对账 fresh 余额 get_signed
#include "transport/BinanceWsUrl.h"            // parse_binance_ws_url
#include "rest/BinanceSign.h"                  // api_key_sha8（identity 自证）
#include "BinancePlainCloseFallback.h" // plain-close fallback 的 no-flip 数量判定
#include "transport/RestTradeTransport.h"
#include "transport/WsTradeTransport.h"
#include "refdata/BinanceExchangeInfo.h"   // parse_exchange_info + BinanceSymbolFilters（snap 真值源）
#include "XtFixedPoint.h"            // xt_fp::to_fp(positionAmt)
#include "xt_logger/XtLogger.h"

namespace binance_u {

bool TraderBinanceUGateway::Impl::init(const ITradeGateway::Config& cfg) {
    // Binance TD 的 provider 专属配置由 configure_binance() 注入到 td_cfg；
    // ITradeGateway::Config 仅消费 venue-neutral 的 expected_api_key_sha8 identity 锚。
    //
    // 配置校验：这里只检查 signed REST client 必需的 Binance 专属字段。
    if (td_cfg.base_rest_url.empty()) {
        XtLogger::error_m("BinanceTD", "binance.base_rest_url 为空");
        return false;
    }
    if (td_cfg.api_key.empty() || td_cfg.api_secret.empty()) {
        XtLogger::error_m("BinanceTD", "binance.api_key / api_secret 为空");
        return false;
    }
    // local_id 发号域 fail-fast：claim/flock 失败（跨实例唯一性无法
    // 验证或同名双活）→ 拒起。fail-open 会让 multibook 同 ms boot race 完整复活。
    if (!local_id_gen_) {
        XtLogger::error_m("BinanceTD",
            "init rejected: local_id generator unavailable: {}",
            local_id_gen_error_.empty() ? std::string("configure() not called")
                                        : local_id_gen_error_);
        return false;
    }
    // 发号域观测：tag=0 = legacy venue 共享域（未配 instance）。
    XtLogger::info_m("BinanceTD", "local_id domain: instance='{}' tag={}",
                     td_cfg.local_id_instance, local_id_gen_->instance_tag());
    // identity 自证（multibook 凭证错配防护）：api_key 指纹与部署期望不符 →
    // 在任何网络探测之前 fail-fast。指纹单向可打印，api_key 本身不打印。
    if (!cfg.expected_api_key_sha8.empty()) {
        std::string got;
        try {
            got = sign::api_key_sha8(td_cfg.api_key);
        } catch (const std::exception& e) {
            // EVP 失败实际不可达；与其它 init 失败路径对称走 return false，不让异常逃逸启动链。
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

    // REST client：后续认证探测、listenKey、UDS keepalive、
    BinanceRestClient::Config rcfg;
    rcfg.base_url = td_cfg.base_rest_url;
    rcfg.api_key = td_cfg.api_key;
    rcfg.api_secret = td_cfg.api_secret;
    rcfg.recv_window_ms = td_cfg.recv_window_ms;
    rcfg.timeout_sec = 10;
    rest = std::make_unique<BinanceRestClient>(std::move(rcfg));

    // 启动期探测：同步验证 Binance 关键依赖，包括网络、api_key 形态、
    // 签名链路与 futures 权限。
    // 任一项失败都直接返回 false，避免 runner 进入半启动状态。
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
    // 这里的 listenKey 只用于启动期鉴权探测；UDS::start 会重新 acquire 自己的 listenKey。
    std::string close_err;
    (void)rest->close_listen_key(&close_err);
    // signed account 查询覆盖 secret 配对与 futures 权限校验。
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

    // venue tick/step filters：拉 exchangeInfo 填进程级缓存（下单/平仓 snap 的唯一真值源）。
    // TD 无 subscribed symbol 列表 → 全量拉 + retry 3；
    // 拉不到/解析空 → fail-closed return false（对齐上方各 init 探测，避免半启动 + 绝不发 off-tick）。
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

    // 下单 transport：ACK 回调必须先绑定，再 start，
    // 否则早到 ACK 可能没有统一入口处理。
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
    // 启动时投递 kTimeSync 与 kBootstrapSnapshot。
    {
        std::lock_guard<std::mutex> lk(cold_worker_mu);
        cold_worker_tasks.push_back(ColdWorkerTask{ColdWorkerTask::kTimeSync});
        cold_worker_tasks.push_back(ColdWorkerTask{ColdWorkerTask::kBootstrapSnapshot});
    }
    cold_worker_thread = std::thread([this] { cold_worker_loop(); });

    // ── 启动 User Data Stream（listenKey + WS → order/trade/account/position 回报）
    BinanceUserDataStream::Config uds_cfg;
    uds_cfg.host = parse_binance_ws_url(td_cfg.base_ws_url, "443").host;
    uds_cfg.port = "443";
    uds_cfg.keepalive_ms = td_cfg.listen_key_refresh_ms;
    uds = std::make_unique<BinanceUserDataStream>(uds_cfg, *rest);
    // fill 去重 authority 在 ledger（per-order fills 集 + orderless 复合窗）；
    // live UDS 与 REST gap-fill 注入共用同一判定入口。
    uds->set_fill_dedup([this](uint64_t order_id, std::string_view symbol, int64_t tid) {
        return ledger_.note_fill_seen(order_id, symbol, tid);
    });
    uds->bind_callbacks(
        [this](const XtOrderExecUpdate& upd) {
            this->on_uds_order_update(upd);
        },
        [this](const XtTradeRecord& tr, XtLatencyEligibility elig) {
            // venue commission (day-scoped fees 真值) 由 runner 层 XtHftChannels::on_td_trade
            // 经 note_fill_fee_fp 转发到 truth source（与 overlay 共用 bridge，去 gateway 指针）。
            // forward_uds_trade = resync 快照-流栅栏的唯一 fill emit 入口；elig 透传
            //（UDS 实时 kEligible / gap-fill replay kIneligible）。
            forward_uds_trade(tr, elig);
        },
        [this](const ITradeGateway::AccountSnapshot& a) { if (cb.on_account)  cb.on_account(a); },
        [this](const std::vector<XtPositionSnapshot>& ps) {
            forward_uds_position(ps);
        },
        [this](std::string_view s) { return parse_local_id_from_client_order_id(s); });
    // 未归因成交不能走增量持仓更新，只请求一次 reconcile resync。
    // 30s gate 与 cold_worker_tasks 查重共同限制 REST weight。
    uds->set_unattributed_fill_handler([this]() {
        const uint64_t now_us = lat_now_us();
        if (last_unattributed_resync_us_ != 0 &&
            now_us - last_unattributed_resync_us_ < 30'000'000ULL) {
            return;  // 30s gate
        }
        if (queue_reconcile_resync_if_absent_()) {
            last_unattributed_resync_us_ = now_us;
        }
    });
    uds->start();

    // Binance UDS 无 account/position heartbeat；keepalive 只投递 refresh 任务。
    // REST 查询集中在 cold worker，keepalive 线程不持有 gateway 状态锁。
    keepalive_thread = std::thread([this] {
        uint64_t tick = 0;
        while (running.load()) {
            // wait_for + shutdown_cv_ 让 stop() 能立即唤醒线程，而不是等待完整周期。
            {
                std::unique_lock<std::mutex> lk(shutdown_mu_);
                shutdown_cv_.wait_for(lk, std::chrono::seconds(30),
                    [this] { return !running.load(std::memory_order_acquire); });
            }
            if (!running.load(std::memory_order_acquire)) break;
            ++tick;
            const auto pick = keepalive_pick_(static_cast<int64_t>(lat_now_us() / 1000));
            if (!pick.has_value()) continue;  // 未 ready 且 bootstrap 退避未到期：静默省 REST
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
        // shutdown_cv_ 唤醒 keepalive 与 bootstrap wait。
        {
            std::lock_guard<std::mutex> lk(shutdown_mu_);
        }
        shutdown_cv_.notify_all();
    }
    if (td_transport) td_transport->stop();
    // uds->stop() 只停 WS io 线程与 keepalive timer，不析构内部状态，先于 join 安全。
    if (uds) uds->stop();
    if (keepalive_thread.joinable()) keepalive_thread.join();
    if (cold_worker_thread.joinable()) cold_worker_thread.join();
    // uds 析构必须晚于 cold worker join：reconciler 经 uds 槽位引用做 gap-fill
    // （tracked_trade_symbols / last_seen_trade_id / inject_gap_fill_trade），
    // join 前 reset 会让在途 resync 解引用空槽位。
    // rest / td_transport 不在 stop() 置空（活到 ~Impl），无同类竞争。
    // uds_mu_ 与 check_health 的解引用互斥（defense-in-depth，见 Impl.h 成员注释）。
    // 锁内 reset() 是 dtor（含 UDS io 线程 join）——阻塞但无死锁（UDS 内部线程
    // 不取本锁），shutdown 期至多让一次并发 check_health 等待。
    {
        std::lock_guard<std::mutex> lk(uds_mu_);
        uds.reset();
    }
}

// 停机对账冷路径：调用时内部线程已全部 join，rest 是唯一用户（活到 ~Impl，
// 见 stop() 尾注）；失败一律回 false 让调用点回退 anchor 缓存，不重试。
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
        // 投递时间同步到 worker（避免阻塞 health-check 线程）
        {
            std::lock_guard<std::mutex> lk(cold_worker_mu);
            cold_worker_tasks.push_back(ColdWorkerTask{ColdWorkerTask::kTimeSync});
        }
        cold_worker_cv.notify_one();
    }
    // User Data Stream 自身也做 stale 检测（内部 post 到 uds io_context 重连）。
    // uds_mu_ 令判空+解引用与 stop() 的 uds.reset() 互斥；锁内仅 net::post 非阻塞。
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
    // 已 ready 选择周期 refresh；未 ready 只在 bootstrap 退避窗口到期后重试启动快照。
    if (ready_emitted_.load(std::memory_order_acquire)) return ColdWorkerTask::kRefreshSnapshot;
    if (now_ms < reconciler_.bootstrap_next_retry_ms()) return std::nullopt;
    return ColdWorkerTask::kBootstrapSnapshot;
}

bool TraderBinanceUGateway::Impl::queue_reconcile_resync_if_absent_() {
    std::lock_guard<std::mutex> lk(cold_worker_mu);
    for (const auto& m : cold_worker_tasks) {
        if (m.kind == ColdWorkerTask::kReconcileResync) return false;  // 已排队，由其合并
    }
    cold_worker_tasks.push_back(ColdWorkerTask{ColdWorkerTask::kReconcileResync});
    cold_worker_cv.notify_one();
    return true;
}

void TraderBinanceUGateway::Impl::request_resnap() {
    // 仅合并仍在 cold_worker_tasks 内的 resync；已经出队执行的 resync 可能正在取样，
    // 新请求需要再排一轮兜底后续 gap。重复入队会线性放大 REST weight。
    if (!queue_reconcile_resync_if_absent_()) {
        XtLogger::info_m("BinanceTD",
            "request_resnap merged: kReconcileResync already queued");
        return;
    }
    // UDS reconnect 只修连接，不回放 WS gap；漏 fill 和 truth 覆盖由上方
    // kReconcileResync 负责。
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
                // plain-close fallback 仅用于 Binance reduceOnly 平仓拒单。
                // plain 单不带 reduceOnly，必须先按交易所最新持仓重新判定方向与数量。
                int64_t venue_pos_fp = 0;
                bool    venue_known  = false;
                {
                    auto pr = rest->get_signed("/fapi/v2/positionRisk", {{"symbol", m.fb_symbol}});
                    if (pr.ok()) {
                        auto j = nlohmann::json::parse(pr.body, nullptr, /*allow_exceptions=*/false);
                        if (j.is_array()) {
                            for (const auto& p : j) {
                                if (p.value("symbol", std::string{}) == m.fb_symbol) {
                                    try { venue_pos_fp = xt_fp::to_fp(std::stod(p.value("positionAmt", "0"))); }
                                    catch (...) { venue_pos_fp = 0; }
                                    break;
                                }
                            }
                            venue_known = true;  // symbol absent ⇒ flat (decision will skip)
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

}  // namespace binance_u
