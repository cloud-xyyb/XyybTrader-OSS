#include "td/BinanceReconciler.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

#include "td/BinanceOrderLedger.h"
#include "transport/BinanceTdWireUtil.h"  // parse_local_id / Binance 状态映射。
#include "rest/BinanceRestClient.h"
#include "rest/BinanceSign.h"        // sign::now_ms。
#include "transport/BinanceUserDataStream.h"
#include "xt_live/gateway/common/ReverseSyncHelper.h"  // reverse-sync batch emit。
#include "xt_core/session/XtTradingDay.h"
#include "xt_identity/OrderAttributionStore.h"
#include "xt_logger/XtLogger.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

namespace binance_u {

// BinanceReconciler Sweep TU：ghost 正/反 sweep、blank-sys-id 收敛、
// userTrades gap-fill 与 openOrders reverse-sync。
// bootstrap/resync、snapshot-vs-stream 栅栏与 account truth 见
// BinanceReconcilerSync.cpp。

namespace {

// P15 NaN/Inf ingress 防线（同 BinanceReconcilerSync.cpp）：stod("nan"/"inf") 不抛，
// 改抛入各簇既有 catch 分支，复用行级 fail-closed 语义（nullopt/skip/counter）。
[[nodiscard]] double stod_finite(const std::string& s) {
    const double v = std::stod(s);
    if (!std::isfinite(v)) throw std::invalid_argument("non-finite numeric field");
    return v;
}

// Binance 业务错误码在 HTTP 非 2xx body 中返回；解析失败返回 0，按 transport 失败处理。
int parse_venue_err_code(const std::string& body) {
    auto j = nlohmann::json::parse(body, nullptr, /*allow_exceptions=*/false);
    return j.is_object() ? j.value("code", 0) : 0;
}

constexpr int kErrOrderNotExist = -2013;  ///< GET /fapi/v1/order: Order does not exist。

}  // namespace

std::optional<std::vector<BinanceReconciler::VenueOpenOrderBrief>>
BinanceReconciler::fetch_open_orders_brief() {
    auto arr = get_open_orders_array_("fetch_open_orders_brief");
    // REST 失败跳过本周期；空视图会把全部本地 in-flight 误判为 ghost。
    if (!arr) return std::nullopt;
    const nlohmann::json& j = *arr;
    std::vector<VenueOpenOrderBrief> rows;
    rows.reserve(j.size());
    for (const auto& o : j) {
        VenueOpenOrderBrief row;
        row.sys_id = std::to_string(o.value("orderId", 0LL));
        row.client_order_id = o.value("clientOrderId", std::string{});
        row.symbol = o.value("symbol", std::string{});
        row.order_time_ms = static_cast<uint64_t>(o.value("time", 0LL));
        rows.push_back(std::move(row));
    }
    return rows;
}

std::size_t BinanceReconciler::cancel_leaked_venue_orders(
    const std::vector<VenueOpenOrderBrief>& venue_open, uint32_t min_age_ms) {
    // 每周期限量：控 REST weight；泄漏存量由后续周期逐批收敛。
    constexpr std::size_t kMaxCancelsPerCycle = 10;
    // age 门槛取 ghost 同源 min_age + 5s 地板：吞本地时钟与 venue 时钟的残差，
    // 也给 submit-ack 在途的新单留出落地窗口。
    const int64_t age_floor_ms =
        std::max<int64_t>(static_cast<int64_t>(min_age_ms), 5000);
    const int64_t now_ms = static_cast<int64_t>(sign::now_ms());
    std::size_t canceled = 0;
    for (const auto& row : venue_open) {
        if (canceled >= kMaxCancelsPerCycle) break;
        // 只动本进程 clientOrderId 格式的委托；外部/手工单不碰。
        const uint64_t local_id = parse_local_id(row.client_order_id);
        if (local_id == 0) continue;
        if (ledger_.is_inflight(local_id)) continue;  // 正常 tracking 中
        if (row.order_time_ms == 0 ||
            now_ms - static_cast<int64_t>(row.order_time_ms) < age_floor_ms) {
            continue;
        }
        auto resp = rest_->delete_signed("/fapi/v1/order",
                                         {{"symbol", row.symbol},
                                          {"origClientOrderId", row.client_order_id}});
        if (resp.ok()) {
            ++canceled;
            XtLogger::error_m("BinanceTD",
                "leaked venue order canceled: local_id={} coid={} sys_id={} age_ms={} "
                "(venue-alive but untracked locally — tracking was wrongly terminalized)",
                local_id, row.client_order_id, row.sys_id,
                now_ms - static_cast<int64_t>(row.order_time_ms));
        } else {
            // -2011 = 撤单竞态下已终态，下周期 openOrders 自然收敛；其余失败同样重试。
            XtLogger::warn_m("BinanceTD",
                "leaked venue order cancel failed: local_id={} coid={} http={} body='{}'",
                local_id, row.client_order_id, resp.http_status, resp.body);
        }
    }
    return canceled;
}

std::optional<XtOrderExecUpdate>
BinanceReconciler::fetch_order_status(const std::string& sys_id,
                                      const std::string& symbol,
                                      uint64_t local_id,
                                      XtLatencyEligibility& out_elig) {
    out_elig = XtLatencyEligibility::kEligible;  // venue 查单转译（新鲜数据，可采）
    auto resp = rest_->get_signed("/fapi/v1/order",
                                  {{"symbol", symbol}, {"orderId", sys_id}});
    if (!resp.ok()) {
        // -2013 是 venue 对 orderId 的权威否认，可合成 0 成交终态；curl/5xx/限流
        // 仍可能是活单查询失败，必须保持 in-flight 重试。
        if (parse_venue_err_code(resp.body) == kErrOrderNotExist) {
            out_elig = XtLatencyEligibility::kIneligible;  // 本地合成终态
            return synthesize_not_exist_terminal_(local_id, sys_id);
        }
        XtLogger::warn_m("BinanceTD",
            "GET /fapi/v1/order failed: orderId={} symbol={} http={} err='{}'",
            sys_id, symbol, resp.http_status, resp.curl_err);
        return std::nullopt;
    }
    auto j = nlohmann::json::parse(resp.body, nullptr, /*allow_exceptions=*/false);
    if (!j.is_object()) return std::nullopt;

    XtOrderExecUpdate upd;
    upd.id = local_id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
    upd.is_buy = (j.value("side", std::string("BUY")) == "BUY");
    upd.close_offset = j.value("reduceOnly", false) ? XtOffset::Close : XtOffset::Open;
    const std::string status_str = j.value("status", std::string("CANCELED"));
    const auto mapped = parse_binance_status_opt(status_str);
    if (!mapped) {
        // 未映射状态串保持 in-flight；误销 venue 活单比暂存僵尸单风险更高。
        // warn 暴露原始串，避免状态新增后静默卡住。
        XtLogger::warn_m("BinanceTD",
            "fetch_order_status unmapped status='{}': orderId={} symbol={} (kept alive)",
            status_str, sys_id, symbol);
        return std::nullopt;
    }
    if (*mapped == XtOrderStatus::Accepted || *mapped == XtOrderStatus::PartiallyFilled) {
        return std::nullopt;  // openOrders 取样竞态下仍可能是活单，不在 ghost 路径终结。
    }
    upd.status = *mapped;  // FILLED/CANCELED/EXPIRED/EXPIRED_IN_MATCH/REJECTED 终态
    try {
        upd.qty_fp    = order_exec_to_fp(stod_finite(j.value("origQty",     "0")));
        upd.filled_fp = order_exec_to_fp(stod_finite(j.value("executedQty", "0")));
        upd.leaves_fp = upd.qty_fp - upd.filled_fp;
        upd.price_fp  = order_exec_to_fp(stod_finite(j.value("price",       "0")));
    } catch (...) { return std::nullopt; }
    upd.local_cb_us = lat_now_us();
    ledger_.remember_terminal(upd.id);
    return upd;
}

std::optional<XtOrderExecUpdate>
BinanceReconciler::synthesize_not_exist_terminal_(uint64_t local_id,
                                                  const std::string& sys_id) {
    XtOrderExecUpdate upd;
    upd.id = local_id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
    upd.status = XtOrderStatus::Canceled;   // venue 无记录意味着 0 成交，按撤单口径销账。
    upd.local_cb_us = lat_now_us();
    // 原子「补齐 + 记终态」；in-flight facet 保留，由 ghost 主循环统一销账。
    const auto decision = ledger_.on_reconcile_terminal(
        local_id, BinanceOrderLedger::ReconcileTerminal::kNotExistCanceled, upd);
    if (decision.action != BinanceOrderLedger::TerminalDecision::Action::kEmit) {
        return std::nullopt;
    }
    XtLogger::warn_m("BinanceTD",
        "ghost confirmed not-exist by venue (-2013) → synthesize CANCELED: "
        "local_id={} sys_id={}", local_id, sys_id);
    return upd;
}

void BinanceReconciler::note_ghost_fetch_failure_(
    uint64_t local_id, const std::string& sys_id,
    std::unordered_map<uint64_t, uint32_t>& next_streak) {
    const auto prev = ghost_fetch_fail_streak_.find(local_id);
    const uint32_t streak =
        prev == ghost_fetch_fail_streak_.end() ? 1u : prev->second + 1u;
    next_streak[local_id] = streak;
    XtLogger::warn_m("BinanceTD",
        "ghost detected but fetch_order_status failed: local_id={} sys_id={} "
        "consecutive_cycles={}", local_id, sys_id, streak);
    // transport 类失败绝不终态化；僵尸单会占用 margin/in-flight 投影，
    // 连续多周期不收敛时升级 error 提示人工介入。
    if (streak >= kGhostFetchFailEscalateCycles &&
        streak % kGhostFetchFailEscalateCycles == 0) {
        XtLogger::error_m("BinanceTD",
            "zombie order not converging: {} consecutive fetch failures, "
            "local_id={} sys_id={} (kept alive by design)",
            streak, local_id, sys_id);
    }
}

BinanceReconciler::ClientOrderLookup
BinanceReconciler::find_open_order_by_client_id(
    const std::string& symbol, const std::string& client_order_id) {
    ClientOrderLookup out;
    if (symbol.empty() || client_order_id.empty()) return out;
    auto resp = rest_->get_signed("/fapi/v1/openOrders", {{"symbol", symbol}});
    if (!resp.ok()) {
        XtLogger::warn_m("BinanceTD",
            "GET openOrders by symbol failed: symbol={} http={} err='{}' body='{}'",
            symbol, resp.http_status, resp.curl_err, resp.body);
        return out;
    }
    out.ok = true;
    auto j = nlohmann::json::parse(resp.body, nullptr, /*allow_exceptions=*/false);
    if (!j.is_array()) return out;
    for (const auto& o : j) {
        if (o.value("clientOrderId", std::string()) != client_order_id) continue;
        out.found = true;
        out.sys_id = std::to_string(o.value("orderId", 0LL));
        return out;
    }
    return out;
}

bool BinanceReconciler::synthesize_blank_sys_id_reject(uint64_t local_id) {
    XtOrderExecUpdate upd;
    upd.id = local_id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
    upd.status = XtOrderStatus::Rejected;
    upd.local_cb_us = lat_now_us();
    // 原子判定：在途且 sys_id 为空 → 补齐 + 记终态 + 销账（含 trace 清理）；
    // emit 在 ledger 锁外。
    const auto decision = ledger_.on_reconcile_terminal(
        local_id, BinanceOrderLedger::ReconcileTerminal::kBlankSysIdReject, upd);
    const bool erased =
        decision.action == BinanceOrderLedger::TerminalDecision::Action::kEmit;
    if (erased) {
        // blank-sys-id reject = 本地合成终态（时序与 venue 脱钩）。
        emit_reconcile_terminal_update(upd, XtLatencyEligibility::kIneligible);
    }
    return erased;
}

bool BinanceReconciler::resolve_blank_sys_id_for_reconcile(
    uint64_t local_id, const std::string& symbol,
    uint64_t age_ms, uint32_t min_age_ms) {
    const std::string client_order_id = ledger_.client_order_id_for(local_id);
    const auto lookup = find_open_order_by_client_id(symbol, client_order_id);
    if (lookup.ok && lookup.found) {
        ledger_.bind_sys_id_if_blank(local_id, lookup.sys_id);
        return false;
    }
    if (!lookup.ok || age_ms <= static_cast<uint64_t>(min_age_ms) * 2) return false;
    return synthesize_blank_sys_id_reject(local_id);
}

std::optional<std::size_t>
BinanceReconciler::detect_and_resolve_ghosts(uint32_t min_age_ms) {
    auto venue_open = fetch_open_orders_brief();
    if (!venue_open.has_value()) return std::nullopt;

    std::unordered_set<std::string> exchange_open;
    exchange_open.reserve(venue_open->size());
    for (const auto& row : *venue_open) exchange_open.insert(row.sys_id);

    // 复制本地 in-flight 快照后在 ledger 锁外做 REST IO，避免阻塞 submit/cancel 回调路径。
    const auto snapshot = ledger_.inflight_snapshot();
    const uint64_t now_ms = static_cast<uint64_t>(lat_now_us() / 1000);

    std::size_t resolved_count = 0;
    std::unordered_map<uint64_t, uint32_t> next_fetch_fail_streak;
    for (const auto& inf : snapshot) {
        const uint64_t age_ms = now_ms - inf.submit_ts_ms;
        if (age_ms < min_age_ms) continue;                      // 太新，不判定 ghost。
        if (inf.sys_id.empty()) {
            if (resolve_blank_sys_id_for_reconcile(inf.local_id, inf.symbol,
                                                   age_ms, min_age_ms)) {
                ++resolved_count;
            }
            continue;
        }
        if (exchange_open.count(inf.sys_id) > 0) continue;       // 交易所仍认为是活单。
        // 本地认为 in-flight，但交易所 openOrders 不再包含该订单。
        XtLatencyEligibility term_elig = XtLatencyEligibility::kEligible;
        auto term = fetch_order_status(inf.sys_id, inf.symbol, inf.local_id, term_elig);
        if (!term.has_value()) {
            note_ghost_fetch_failure_(inf.local_id, inf.sys_id, next_fetch_fail_streak);
            continue;
        }
        XtLogger::warn_m("BinanceTD",
            "ghost order resolved: local_id={} sys_id={} status={} age_ms={}",
            inf.local_id, inf.sys_id, static_cast<int>(term->status), age_ms);
        // 带成交 ghost（含 Canceled/Expired 部分成交）：漏推的 fill 先按 orderId
        // 精确补发（保持 venue 自然序 fills→terminal），fee/realized 由补发 fill
        // 入账；游标式 per-symbol gap-fill 查不到已被游标越过的老 trade。
        // status==Filled 兜 executedQty 字段缺失/畸形的响应（FILLED 必有成交）。
        const bool has_fills =
            term->filled_fp > 0 || term->status == XtOrderStatus::Filled;
        if (has_fills) {
            resync_user_trades_for_order(inf.symbol, inf.sys_id);
        }
        // 同步销账（含 trace），避免下个周期重复 emit；终态已在 fetch 路径记录，重复登记幂等。
        // 终态走与 UDS final 相同的 dedup 入口（emit 在 ledger 锁外），再由 TD queue 串行进入 event loop。
        if (ledger_.on_reconcile_terminal(
                inf.local_id, BinanceOrderLedger::ReconcileTerminal::kVenueTerminal,
                *term).action == BinanceOrderLedger::TerminalDecision::Action::kEmit) {
            emit_reconcile_terminal_update(*term, term_elig);
        }
        ++resolved_count;

        // 带成交 ghost 说明 UDS 可能同时漏了 fill 与 account update；订单终态不足以
        // 修复 account_store，必须排队 gap-fill + positionRisk 覆盖。
        // 这里不主动 reconnect：重连会打开新的 WS gap，连接自愈留给 health/listenKey
        // 证据路径，数据修复由 REST resync 完成。
        if (has_fills) {
            XtLogger::warn_m("BinanceTD",
                "ghost with fills (filled_fp={}) → queue position resync (gap-fill + "
                "write-through, no reconnect) for local_id={}",
                term->filled_fp, inf.local_id);
            hooks_.queue_reconcile_resync();
        }
    }

    // 仅保留本周期仍 fetch-fail 的条目：已收敛/已离开 inflight 的自动剪除。
    ghost_fetch_fail_streak_ = std::move(next_fetch_fail_streak);

    // 反向对账：venue-alive 但本地已无 tracking 的本进程委托直接撤。
    // 正向 ghost 与反向泄漏必须成对处理，否则误终态化的订单会继续占用
    // venue margin / max-open-order 限额。
    resolved_count += cancel_leaked_venue_orders(*venue_open, min_age_ms);
    return resolved_count;
}

void BinanceReconciler::resync_user_trades_for_symbol(const std::string& symbol) {
    if (!uds_) return;
    const int64_t last_seen = uds_->last_seen_trade_id(symbol);
    // userTrades.fromId 是包含式；从 last_seen+1 开始，避免把 UDS 已见的最后一笔
    // 再拉一遍。冷路径从 1 开始。单轮 gap>limit 时按 fromId 游标续页拉齐，
    // 有界轮次防 REST weight 失控；触顶残量由后续 reconcile 周期收敛。
    const int64_t from_id = last_seen > 0 ? last_seen + 1 : 1;

    int64_t cursor = from_id;
    size_t emitted = 0, deduped = 0, skipped = 0;
    int rounds = 0;
    for (;;) {
        ++rounds;
        std::map<std::string, std::string> qs = {
            {"symbol", symbol},
            {"fromId", std::to_string(cursor)},
            {"limit",  std::to_string(kUserTradesPageLimit)},
        };
        auto resp = rest_->get_signed("/fapi/v1/userTrades", qs);
        if (!resp.ok()) {
            XtLogger::warn_m("BinanceTD",
                "resync_user_trades GET failed sym={} round={} http={} err='{}'",
                symbol, rounds, resp.http_status, resp.curl_err);
            return;  // 已重放的页已生效；余量留给下一周期。
        }
        auto j = nlohmann::json::parse(resp.body, nullptr, /*allow_exceptions=*/false);
        if (!j.is_array()) return;
        if (j.empty()) {
            if (rounds == 1) {
                XtLogger::info_m("BinanceTD",
                    "resync_user_trades sym={} from_id={} no missed fills (UDS healthy)",
                    symbol, from_id);
                return;
            }
            break;  // 上一页恰满 limit、本页空 = 已拉齐。
        }
        int64_t max_tid = 0;
        replay_user_trades_json_(symbol, j, emitted, deduped, skipped, max_tid);
        if (j.size() < kUserTradesPageLimit) break;  // 末页（未满）= 已拉齐。
        if (max_tid < cursor) {
            // 满页但游标未推进（全页坏行等异常）：终止防死循环。
            XtLogger::warn_m("BinanceTD",
                "resync_user_trades sym={} round={} cursor not advancing (max_tid={} cursor={})",
                symbol, rounds, max_tid, cursor);
            break;
        }
        cursor = max_tid + 1;
        if (rounds >= kUserTradesPageRoundsMax) {
            user_trades_pages_capped_total_.fetch_add(1, std::memory_order_relaxed);
            XtLogger::error_m("BinanceTD",
                "resync_user_trades sym={} page rounds capped at {} (still full page); "
                "residual left to next reconcile cycle",
                symbol, kUserTradesPageRoundsMax);
            break;
        }
    }
    XtLogger::warn_m("BinanceTD",
        "resync_user_trades sym={} from_id={} rounds={} replayed={} deduped={} skipped={} (UDS already had it)",
        symbol, from_id, rounds, emitted, deduped, skipped);
}

void BinanceReconciler::resync_user_trades_for_order(const std::string& symbol,
                                                     const std::string& sys_id) {
    if (!uds_ || !rest_ || symbol.empty() || sys_id.empty()) return;
    // 首轮只带 orderId；恰满 limit 时加 fromId 续页（有界轮次）。orderId+fromId
    // 组合官方文档未明示：续页轮被 venue 拒绝时就地终止 —— 首页已重放，不劣于
    // 单发；单笔订单 fill 数 > limit 属病态，此处是硬封顶防线而非常态路径。
    int64_t cursor = 0;  // 0 = 首轮不带 fromId。
    size_t emitted = 0, deduped = 0, skipped = 0;
    int rounds = 0;
    for (;;) {
        ++rounds;
        std::map<std::string, std::string> qs = {
            {"symbol", symbol},
            {"orderId", sys_id},
            {"limit", std::to_string(kUserTradesPageLimit)},
        };
        if (cursor > 0) qs.emplace("fromId", std::to_string(cursor));
        auto resp = rest_->get_signed("/fapi/v1/userTrades", qs);
        if (!resp.ok()) {
            XtLogger::warn_m("BinanceTD",
                "resync_user_trades(orderId) GET failed sym={} orderId={} round={} http={} err='{}'",
                symbol, sys_id, rounds, resp.http_status, resp.curl_err);
            return;  // 已重放的页已生效。
        }
        auto j = nlohmann::json::parse(resp.body, nullptr, /*allow_exceptions=*/false);
        if (!j.is_array()) return;
        if (j.empty()) {
            if (rounds == 1) {
                // venue /order 报有成交但 userTrades 空：竞态或 venue 侧滞后，留给下轮
                // reconcile（终态已合成，账面差由 position write-through 兜底）。
                XtLogger::warn_m("BinanceTD",
                    "resync_user_trades(orderId) sym={} orderId={} returned no trades",
                    symbol, sys_id);
                return;
            }
            break;  // 上一页恰满 limit、本页空 = 已拉齐。
        }
        int64_t max_tid = 0;
        replay_user_trades_json_(symbol, j, emitted, deduped, skipped, max_tid);
        if (j.size() < kUserTradesPageLimit) break;
        if (max_tid < cursor) {
            XtLogger::warn_m("BinanceTD",
                "resync_user_trades(orderId) sym={} orderId={} round={} cursor not advancing",
                symbol, sys_id, rounds);
            break;
        }
        cursor = max_tid + 1;
        if (rounds >= kUserTradesPageRoundsMax) {
            user_trades_pages_capped_total_.fetch_add(1, std::memory_order_relaxed);
            XtLogger::error_m("BinanceTD",
                "resync_user_trades(orderId) sym={} orderId={} page rounds capped at {}; "
                "residual left to next reconcile cycle",
                symbol, sys_id, kUserTradesPageRoundsMax);
            break;
        }
    }
    XtLogger::warn_m("BinanceTD",
        "resync_user_trades(orderId) sym={} orderId={} rounds={} replayed={} deduped={} skipped={}",
        symbol, sys_id, rounds, emitted, deduped, skipped);
}

void BinanceReconciler::replay_user_trades_json_(const std::string& symbol,
                                                 const nlohmann::json& trades,
                                                 size_t& emitted, size_t& deduped,
                                                 size_t& out_skipped,
                                                 int64_t& out_max_trade_id) {
    // userTrades 缺 reduceOnly / clientOrderId，需用 orderId 再查 /order 才能补
    // local_id、close_offset 与 strategy identity。
    for (const auto& t : trades) {
        try {
            XtTradeRecord tr;
            const int64_t binance_trade_id = t.value("id", 0LL);
            if (binance_trade_id <= 0) { ++out_skipped; continue; }
            out_max_trade_id = std::max(out_max_trade_id, binance_trade_id);
            const int64_t exchange_order_id = t.value("orderId", 0LL);
            tr.trade_id   = std::to_string(binance_trade_id);
            tr.order_sys_id = std::to_string(exchange_order_id);
            tr.code       = symbol;
            const std::string side = t.value("side", std::string{});
            tr.is_buy     = (side == "BUY");
            tr.price      = stod_finite(t.value("price", "0"));
            tr.exec_price = tr.price;
            const double qty = stod_finite(t.value("qty", "0"));
            tr.qty_fp     = order_exec_to_fp(qty);
            tr.fee        = stod_finite(t.value("commission", "0"));
            tr.is_passive = t.value("maker", false);
            tr.is_close   = false;  // reduceOnly 在 order 维度，trade 结果不携带。
            tr.is_today   = false;
            const int64_t event_ms = t.value("time", 0LL);
            tr.fill_ts_us = static_cast<uint64_t>(event_ms) * 1000ULL;
            tr.local_cb_us = lat_now_us();
            // 与 UDS live 路径同款 UTC 自然日派生（补发 fill 是 fee/realized 的
            // 指定载体，today-PnL/日切归属依赖这三字段）。
            tr.trade_date = xt_core::derive_trading_day(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            tr.trade_time = xt_core::derive_action_time(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            tr.trading_day = xt_core::derive_trading_day(
                event_ms, xt_core::TimezonePolicy::UtcNaturalDay);
            if (auto attr = resolve_gap_fill_order_attribution(symbol, exchange_order_id)) {
                tr.order_id = attr->local_id;
                tr.is_close = attr->is_close;
                tr.close_offset = attr->is_close ? XtOffset::Close : XtOffset::Open;
                tr.strategy_id = attr->strategy_id;
                tr.sid_id = attr->strategy_sid.raw();  // 与 strategy_id 同源（populate_gap_fill_strategy_identity）。
            } else {
                tr.order_id = 0;
            }
            if (uds_->inject_gap_fill_trade(tr)) {
                ++emitted;
            } else {
                ++deduped;
            }
        } catch (...) { ++out_skipped; continue; }
    }
}

std::optional<GapFillOrderAttribution>
BinanceReconciler::resolve_gap_fill_order_attribution(
    const std::string& symbol, int64_t exchange_order_id) {
    if (symbol.empty() || exchange_order_id <= 0 || !rest_) return std::nullopt;
    const std::string order_sys_id = std::to_string(exchange_order_id);
    if (const auto hit = ledger_.find_inflight_by_sys_id(order_sys_id)) {
        GapFillOrderAttribution attr;
        attr.local_id = hit->local_id;
        attr.is_close = hit->close_offset != XtOffset::Open;
        populate_gap_fill_strategy_identity(attr);  // ledger 锁外做 identity 查表。
        return attr;
    }

    auto resp = rest_->get_signed("/fapi/v1/order",
                                  {{"symbol", symbol}, {"orderId", order_sys_id}});
    if (!resp.ok()) {
        XtLogger::warn_m("BinanceTD",
            "gap-fill attribution GET /order failed sym={} orderId={} http={} err='{}'",
            symbol, order_sys_id, resp.http_status, resp.curl_err);
        return std::nullopt;
    }
    auto attr = parse_gap_fill_order_attribution_body(resp.body);
    if (!attr) {
        XtLogger::warn_m("BinanceTD",
            "gap-fill attribution skipped sym={} orderId={} body missing local clientOrderId",
            symbol, order_sys_id);
    }
    return attr;
}

// Reverse-sync 交易所仍存活的本进程订单。
//
// 只接收能从 clientOrderId 反解 local_id 的订单；外部/手工单不进入本地
// ledger 的 in-flight 记录，避免 gateway 接管非本进程委托。

bool BinanceReconciler::rebuild_from_open_orders_body(
    const std::string& body, bool is_cold_start) {
    auto j = nlohmann::json::parse(body, nullptr, /*allow_exceptions=*/false);
    if (!j.is_array()) {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::OpenOrders,
                             "binance openOrders response was not an array");
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "binance openOrders response was not an array");
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance()
            .inc_recovery_rebuild_alive_total("binance_u", "failure");
#endif
        XtLogger::warn_m("BinanceTD",
            "rebuild_alive_inflight: openOrders body not array, skipping");
        return false;
    }

    std::vector<AliveInflightOrder> orders;
    std::vector<uint64_t> ids_to_clear;
    orders.reserve(static_cast<size_t>(j.size()));
    ids_to_clear.reserve(static_cast<size_t>(j.size()));

    for (const auto& o : j) {
        const std::string client_order_id = o.value("clientOrderId", std::string{});
        if (client_order_id.empty()) {
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance()
                .inc_recovery_rebuild_alive_skipped_foreign_total("binance_u");
#endif
            continue;
        }
        // parse_local_id 只接受本进程 clientOrderId 格式；0 表示外部单或无法反解。
        const uint64_t local_id = parse_local_id(client_order_id);
        if (local_id == 0) {
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance()
                .inc_recovery_rebuild_alive_skipped_foreign_total("binance_u");
#endif
            continue;
        }

        AliveInflightOrder a;
        a.local_id = local_id;
        a.code = o.value("symbol", std::string{});
        a.is_buy = (o.value("side", std::string("BUY")) == "BUY");
        try {
            a.total_qty_fp =
                order_exec_to_fp(stod_finite(o.value("origQty",     std::string("0"))));
            a.filled_qty_fp =
                order_exec_to_fp(stod_finite(o.value("executedQty", std::string("0"))));
            a.price_fp =
                order_exec_to_fp(stod_finite(o.value("price",       std::string("0"))));
        } catch (...) {
            // 单条解析失败只跳过该订单，不破坏整个 batch readiness。
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance()
                .inc_recovery_rebuild_alive_skipped_foreign_total("binance_u");
#endif
            continue;
        }
        a.close_offset = o.value("reduceOnly", false) ? XtOffset::Close : XtOffset::Open;
        a.strategy_sid = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id);

        // 本地未记录的活单回灌 in-flight 并回填 cancel 路由（client/symbol）；
        // 已存在的只参与 batch emit（emplace 语义不覆盖本进程 submit 条目）。
        // cross-restart 订单缺撤单路由会在 cancel_order_inq 被判 unknown。
        {
            BinanceOrderLedger::OrderRecord rec;
            rec.client_order_id = client_order_id;
            rec.symbol = a.code;
            rec.sys_id = std::to_string(o.value("orderId", 0LL));
            rec.status = XtOrderStatus::Accepted;
            rec.is_buy = a.is_buy;
            rec.price_fp = a.price_fp;
            rec.qty_fp = a.total_qty_fp;
            rec.close_offset = a.close_offset;
            rec.submit_ts_ms = static_cast<uint64_t>(o.value("time", 0LL));
            ledger_.upsert_recovered(local_id, std::move(rec));
        }
        tracked_symbols_.note(a.code);   // reverse-sync 活单播种 tracked_symbols_（ledger 调用后锁外）。

        ids_to_clear.push_back(local_id);
        orders.push_back(std::move(a));
    }

    // warm reconnect 需要清理这些订单的 dedup state；cold-start 时表本为空。
    if (!is_cold_start && !ids_to_clear.empty()) {
        hooks_.erase_dedup_for_local_ids(ids_to_clear);
    }

    readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::OpenOrders,
                            "binance openOrders snapshot parsed");
    if (!cb_.on_reverse_sync_batch) {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "binance reverse-sync handler not bound");
        return false;
    }
    // reverse-sync batch 统一走共享 helper，保持 metric/log/handler 语义一致。
    xyybtrader::recovery::emit_reverse_sync_batch_with_metric(
        std::move(orders), is_cold_start, "binance_u", cb_.on_reverse_sync_batch);
    readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::ReverseSync,
                            "binance reverse-sync batch emitted");
    return true;
}

void BinanceReconciler::rebuild_alive_inflight_and_emit(bool is_cold_start) {
    // openOrders 失败只短重试一次；再失败则 fail readiness，等待下一轮 reconcile。
    constexpr int kMaxAttempts = 2;
    constexpr int kRetryDelayMs = 1000;
    for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
        auto resp = rest_->get_signed("/fapi/v1/openOrders", {});
        if (resp.ok()) {
            if (attempt > 1) {
                XtLogger::info_m("BinanceTD",
                    "rebuild_alive_inflight GET openOrders succeeded after retry attempt={}",
                    attempt);
            }
            (void)rebuild_from_open_orders_body(resp.body, is_cold_start);
            return;
        }
        // 非最后一次失败进入短重试。
        if (attempt < kMaxAttempts) {
            XtLogger::warn_m("BinanceTD",
                "rebuild_alive_inflight GET openOrders failed (attempt {}): http={} err='{}', retrying after {}ms",
                attempt, resp.http_status, resp.curl_err, kRetryDelayMs);
            // wait 可被 shutdown 打断，避免 stop() 等完整 retry 延迟。
            if (hooks_.wait_shutdown(kRetryDelayMs)) {
                XtLogger::info_m("BinanceTD",
                    "rebuild_alive_inflight retry interrupted by shutdown (attempt {})", attempt);
                return;
            }
            continue;
        }
        // 最后一次仍失败则保持 fail-closed，避免以缺失 openOrders 的状态宣告 ready。
#ifdef XT_HAVE_METRICS
        ct::metrics::XtMetricRegistry::instance()
            .inc_recovery_rebuild_alive_total("binance_u", "failure");
#endif
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::OpenOrders,
                             "binance openOrders request failed");
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "binance openOrders request failed");
        XtLogger::warn_m("BinanceTD",
            "rebuild_alive_inflight GET openOrders failed after {} attempts: http={} err='{}',"
            " 30s 后 reconcile 周期重试",
            kMaxAttempts, resp.http_status, resp.curl_err);
        return;
    }
}

}  // namespace binance_u
