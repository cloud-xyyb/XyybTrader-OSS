#include "td/BinanceReconciler.h"

#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

#include "rest/BinanceRestClient.h"
#include "rest/BinanceSign.h"        // sign::now_ms。
#include "td/ResyncFillReplayer.h"   // partition_resync_replay 纯函数分区核
#include "transport/BinanceUserDataStream.h"
#include "xt_logger/XtLogger.h"

namespace binance_u {

// BinanceReconciler Sync TU：REST truth fetch（tracked-symbol 减负）、
// snapshot-vs-stream 栅栏、bootstrap snapshot、reconcile resync 与 account
// truth 锚定/refresh。ghost/leak sweep、gap-fill、reverse-sync 与 blank-sys-id
// 见 BinanceReconcilerSweep.cpp。

namespace {

// account re-anchor 与 keepalive refresh 共用 REST weight 预算；短时间内只需
// 一个 account truth 覆盖，重复拉取会提高 testnet/demo IP ban 风险。
constexpr uint64_t kAccountAnchorMinIntervalMs = 10'000;

[[nodiscard]] bool account_anchor_due(uint64_t now_ms,
                                      uint64_t last_fetch_ms,
                                      uint64_t min_interval_ms) noexcept {
    if (last_fetch_ms == 0) return true;
    return now_ms >= last_fetch_ms && now_ms - last_fetch_ms >= min_interval_ms;
}

// 解析失败行的防御性 symbol 提取：任何字段形态下不抛，取不到返回空串。
[[nodiscard]] std::string defensive_symbol(const nlohmann::json& p) {
    if (p.is_object()) {
        auto it = p.find("symbol");
        if (it != p.end() && it->is_string()) return it->get<std::string>();
    }
    return {};
}

// P15 NaN/Inf ingress 防线：stod("nan"/"inf") 不抛，非有限仓位/资金数据会污染
// 钱路；改抛入既有 catch 分支，复用各簇的行级 fail-closed 语义（计数/播种/告警）。
[[nodiscard]] double stod_finite(const std::string& s) {
    const double v = std::stod(s);
    if (!std::isfinite(v)) throw std::invalid_argument("non-finite numeric field");
    return v;
}

}  // namespace

BinanceReconciler::BinanceReconciler(std::unique_ptr<BinanceRestClient>& rest,
                                     std::unique_ptr<BinanceUserDataStream>& uds,
                                     BinanceOrderLedger& ledger,
                                     ITradeGateway::Callbacks& cb,
                                     xt_live::gateway::TdReadinessGate& readiness_gate,
                                     TrackedSymbolSet& tracked_symbols,
                                     ImplHooks hooks)
    : rest_(rest),
      uds_(uds),
      ledger_(ledger),
      cb_(cb),
      readiness_gate_(readiness_gate),
      tracked_symbols_(tracked_symbols),
      hooks_(std::move(hooks)) {}

// REST truth fetch 优先按 tracked symbol 过滤。
//
// tracked_symbols_ 播种闭包契约见 TrackedSymbolSet 声明处 @note（任何使本地
// 仓位非零的路径必须先 note()）。过滤失败或集合过大时必须退回全量；
// resync snapshot 是 reset+set 语义，部分结果会把缺行 symbol 误清零。

std::optional<nlohmann::json>
BinanceReconciler::get_position_risk_array_(const char* tag) {
    const auto symbols = tracked_symbols_.snapshot();
    if (!symbols.empty() && symbols.size() <= kSymbolFilterMax) {
        nlohmann::json merged = nlohmann::json::array();
        bool all_ok = true;
        for (const auto& sym : symbols) {
            auto pr = rest_->get_signed("/fapi/v2/positionRisk", {{"symbol", sym}});
            if (!pr.ok()) { all_ok = false; break; }
            auto j = nlohmann::json::parse(pr.body, nullptr, /*allow_exceptions=*/false);
            if (!j.is_array()) { all_ok = false; break; }
            for (auto& row : j) merged.push_back(std::move(row));
        }
        if (all_ok) return merged;
        XtLogger::warn_m("BinanceTD",
            "{}: per-symbol positionRisk failed, fallback to full fetch", tag);
    }
    auto pr = rest_->get_signed("/fapi/v2/positionRisk", {});
    if (!pr.ok()) {
        XtLogger::warn_m("BinanceTD",
            "{}: GET positionRisk failed: http={} err='{}'",
            tag, pr.http_status, pr.curl_err);
        return std::nullopt;
    }
    auto j = nlohmann::json::parse(pr.body, nullptr, /*allow_exceptions=*/false);
    if (!j.is_array()) {
        XtLogger::warn_m("BinanceTD", "{}: positionRisk body not array", tag);
        return std::nullopt;
    }
    return j;
}

std::optional<nlohmann::json>
BinanceReconciler::get_open_orders_array_(const char* tag) {
    const auto symbols = tracked_symbols_.snapshot();
    if (!symbols.empty() && symbols.size() <= kSymbolFilterMax) {
        nlohmann::json merged = nlohmann::json::array();
        bool all_ok = true;
        for (const auto& sym : symbols) {
            auto resp = rest_->get_signed("/fapi/v1/openOrders", {{"symbol", sym}});
            if (!resp.ok()) { all_ok = false; break; }
            auto j = nlohmann::json::parse(resp.body, nullptr, /*allow_exceptions=*/false);
            if (!j.is_array()) { all_ok = false; break; }
            for (auto& row : j) merged.push_back(std::move(row));
        }
        if (all_ok) return merged;
        XtLogger::warn_m("BinanceTD",
            "{}: per-symbol openOrders failed, fallback to full fetch", tag);
    }
    auto resp = rest_->get_signed("/fapi/v1/openOrders", {});
    if (!resp.ok()) {
        XtLogger::warn_m("BinanceTD",
            "{}: GET openOrders failed: http={} err='{}'",
            tag, resp.http_status, resp.curl_err);
        return std::nullopt;
    }
    auto j = nlohmann::json::parse(resp.body, nullptr, /*allow_exceptions=*/false);
    if (!j.is_array()) {
        XtLogger::warn_m("BinanceTD", "{}: openOrders body not array", tag);
        return std::nullopt;
    }
    return j;
}

// 行级解析失败 fail-closed（与 fetch 级失败对齐）：tracked/身份不明的坏行若被
// 静默丢弃，缺席会被 reconcile 反向 sweep 误判 phantom（缺席⇒flat 推断）并沿
// ladder 升级到 resnap 抹仓；宁可整轮跳过（caller 视同 fetch 失败，age gauge 增长
// → XtReconcileSnapshotStale 告警）。全量 fallback 模式下账户里其它 symbol 的坏行
// 只告警不拦，避免无关行把本 loop 永久 fail-closed。
std::optional<std::vector<XtPositionSnapshot>>
BinanceReconciler::fetch_positions() {
    auto arr = get_position_risk_array_("fetch_positions");
    if (!arr) return std::nullopt;
    const nlohmann::json& j = *arr;
    std::vector<XtPositionSnapshot> positions;
    std::size_t dropped_tracked = 0;   // tracked 或身份不明（从严）
    std::size_t dropped_other = 0;
    if (j.is_array()) {
        for (const auto& p : j) {
            const std::string symbol = defensive_symbol(p);
            if (symbol.empty()) {
                ++dropped_tracked;
                XtLogger::warn_m("BinanceTD",
                    "fetch_positions: row without parsable symbol dropped (fail-closed)");
                continue;
            }
            try {
                const double amt = stod_finite(p.value("positionAmt", "0"));
                if (amt == 0.0) continue;
                XtPositionSnapshot ps;
                ps.code = symbol;
                const double entry = stod_finite(p.value("entryPrice", "0"));
                if (amt > 0) { ps.long_qty = amt; ps.long_avg = entry; }
                else         { ps.short_qty = -amt; ps.short_avg = entry; }
                positions.push_back(std::move(ps));
            } catch (...) {
                if (tracked_symbols_.contains(symbol)) ++dropped_tracked;
                else                                   ++dropped_other;
                XtLogger::warn_m("BinanceTD",
                    "fetch_positions: unparsable positionRisk row dropped symbol={}",
                    symbol);
            }
        }
    }
    if (dropped_tracked > 0) {
        XtLogger::warn_m("BinanceTD",
            "fetch_positions: {} tracked/unknown unparsable row(s) → treat whole fetch "
            "as failed (fail-closed, no absent=flat inference this cycle)",
            dropped_tracked);
        return std::nullopt;
    }
    if (dropped_other > 0) {
        XtLogger::warn_m("BinanceTD",
            "fetch_positions: {} untracked unparsable row(s) skipped", dropped_other);
    }
    return positions;
}

// Resync snapshot-vs-stream fence.
//
// positionRisk 是绝对仓位 truth，UDS / gap-fill trade 是增量。REST snapshot
// 窗口内只靠 td queue 到达顺序会出现双计或被覆盖丢失。
// 栅栏暂存窗口内 fill 和终态 order update，用 Binance venue time
// (trade time vs positionRisk updateTime) 把 fill 分到 snapshot 前/后：
// snapshot 前的 fill 先重放再被绝对仓位吸收，snapshot 后的 fill 后重放。
// fill 不能像 depth event 一样丢弃，因为它还驱动 fee、realized PnL 和 strategy 归因。

void BinanceReconciler::forward_uds_trade(const XtTradeRecord& tr,
                                          XtLatencyEligibility elig) {
    {
        std::lock_guard<std::mutex> lk(resync_gate_mu_);
        if (resync_buffering_) {
            // 暂存 fill 一律延迟重放（flush 时 kIneligible）；此处 elig 不需保留。
            resync_fill_buffer_.push_back(tr);
            return;
        }
    }
    if (cb_.on_trade) cb_.on_trade(tr, elig);
}

void BinanceReconciler::forward_order_update_gated(
    const XtOrderExecUpdate& upd, EffectiveSignalDedup::Source src,
    XtLatencyEligibility elig) {
    // 只拦终态：非终态 ACK 保持实时。终态若先于暂存 fill 到达策略，
    // per-order 消账会先定稿后见 fill，导致 filled_qty / fill_sum 误报。
    if (order_status_is_final(upd.status)) {
        std::lock_guard<std::mutex> lk(resync_gate_mu_);
        if (resync_buffering_) {
            resync_terminal_buffer_.push_back(ResyncTerminal{upd, src, elig});
            return;
        }
    }
    hooks_.emit_through_dedup(upd, src, elig);
}

void BinanceReconciler::forward_uds_position(
    const std::vector<XtPositionSnapshot>& positions) {
    // 播种 tracked 集：外部/手工成交打在本进程从未 submit 过的 symbol 上时，
    // per-symbol positionRisk 拉取必须覆盖它，否则快照缺席会被 reconcile 反向
    // sweep 误判 phantom → resnap 抹真仓。insert-only；在 gate 锁外调用（锁纪律 X3）。
    for (const auto& ps : positions) {
        if (ps.long_qty != 0.0 || ps.short_qty != 0.0) tracked_symbols_.note(ps.code);
    }
    {
        std::lock_guard<std::mutex> lk(resync_gate_mu_);
        if (resync_buffering_) {
            ++resync_pos_dropped_total_;
            XtLogger::warn_m("BinanceTD",
                "UDS absolute position snapshot dropped during resync gate "
                "(superseded by incoming recovery snapshot), total={}",
                resync_pos_dropped_total_);
            return;
        }
    }
    if (cb_.on_position) cb_.on_position(positions);
}

void BinanceReconciler::emit_reconcile_terminal_update(const XtOrderExecUpdate& upd,
                                                       XtLatencyEligibility elig) {
    forward_order_update_gated(upd, EffectiveSignalDedup::Source::kAck, elig);
}

BinanceReconciler::ResyncFillGate::ResyncFillGate(BinanceReconciler& reconciler)
    : reconciler_(reconciler) {
    std::lock_guard<std::mutex> lk(reconciler_.resync_gate_mu_);
    reconciler_.resync_buffering_ = true;
}

BinanceReconciler::ResyncFillGate::~ResyncFillGate() {
    if (!finished_) {
        reconciler_.finish_resync_fill_gate_(nullptr, {}, 0, /*mark_recovering=*/false);
    }
}

void BinanceReconciler::ResyncFillGate::finish_with_snapshot(
    const std::vector<XtPositionSnapshot>& positions,
    const std::unordered_map<std::string, int64_t>& fence_update_ms,
    int64_t fence_floor_ms, bool mark_recovering) {
    finished_ = true;
    reconciler_.finish_resync_fill_gate_(&positions, fence_update_ms, fence_floor_ms,
                                         mark_recovering);
}

void BinanceReconciler::finish_resync_fill_gate_(
    const std::vector<XtPositionSnapshot>* positions,
    const std::unordered_map<std::string, int64_t>& fence_update_ms,
    int64_t fence_floor_ms, bool mark_recovering) {
    // 分区 emit 期间持锁：并发 fill 会短暂阻塞在 forward_uds_trade。栅栏关闭后
    // 直发的 fill 必然晚于快照读取，排在 snapshot 后正确。分区计算（纯函数）与
    // emit 全程在同一临界区内完成（X2：临界区内不取 ledger 锁）。
    std::lock_guard<std::mutex> lk(resync_gate_mu_);
    std::vector<XtTradeRecord> buffered;
    buffered.swap(resync_fill_buffer_);
    std::vector<ResyncTerminal> terminals;
    terminals.swap(resync_terminal_buffer_);
    resync_buffering_ = false;

    const std::size_t buffered_total = buffered.size();
    ReplayPlan plan = partition_resync_replay(
        std::move(buffered), std::move(terminals), fence_update_ms, fence_floor_ms,
        /*has_snapshot=*/positions != nullptr);

    // 暂存的终态 order update 排在全部 fill 之后重放：保证策略 per-order
    // 消账先见 fill 后见终态（两分支共用，dedup 语义与实时路径一致）。
    // eligibility 透传构造时值（L-1.2 binance）。
    const auto drain_terminals = [&] {
        for (const auto& t : plan.terminals) {
            hooks_.emit_through_dedup(t.upd, t.src, t.elig);
        }
        if (!plan.terminals.empty()) {
            XtLogger::warn_m("BinanceTD",
                "resync gate drained {} buffered terminal order update(s) after fills",
                plan.terminals.size());
        }
    };

    // 快照前 fill 先重放，让绝对仓位快照吸收其效果（PnL/fee 记账保留不双计）。
    // 栅栏重放 = 延迟脱钩 venue 到达时刻 → kIneligible（L-1.3 binance :309/:327）。
    for (const auto& tr : plan.pre_fills) {
        if (cb_.on_trade) cb_.on_trade(tr, XtLatencyEligibility::kIneligible);
    }

    if (!plan.emit_position) {
        // 失败路径：无快照可锚定，pre_fills 已按到达序放行，无 position/post 段。
        drain_terminals();
        return;
    }

    if (mark_recovering) {
        const bool prev = reconcile_in_progress_.exchange(true, std::memory_order_release);
        if (cb_.on_position) cb_.on_position(*positions);
        reconcile_in_progress_.store(prev, std::memory_order_release);
    } else if (cb_.on_position) {
        cb_.on_position(*positions);
    }

    for (const auto& tr : plan.post_fills) {
        if (cb_.on_trade) cb_.on_trade(tr, XtLatencyEligibility::kIneligible);
    }

    drain_terminals();

    if (buffered_total > 0) {
        XtLogger::warn_m("BinanceTD",
            "resync fill gate drained: buffered={} pre_snapshot={} post_snapshot={} "
            "fence_floor_ms={}",
            buffered_total, plan.pre_fills.size(), plan.post_fills.size(), fence_floor_ms);
    }
}

int64_t BinanceReconciler::sample_position_fence_floor_ms_() {
    auto t = rest_->get_public("/fapi/v1/time");
    if (t.ok()) {
        auto tj = nlohmann::json::parse(t.body, nullptr, /*allow_exceptions=*/false);
        if (tj.is_object()) {
            const int64_t server_ms = tj.value("serverTime", 0LL);
            if (server_ms > 0) return server_ms;
        }
    }
    // /time 失败时用本地时钟 + server offset，再减 1s 保守取低；floor 偏低只会
    // 缩小缺行 symbol 的覆盖，不会把未含于 snapshot 的 fill 误判为已含。
    return static_cast<int64_t>(sign::now_ms()) + rest_->server_time_offset_ms() - 1000;
}

void BinanceReconciler::reconcile_resync() {
    // reconnect 后给已在 WS/handler 管道内的事件一个落地窗口；stop() 可打断等待。
    (void)hooks_.wait_shutdown(500);

    // 栅栏开启：此后到达的 fill（live UDS + gap-fill 重放）暂存，待快照落地后
    // 按 venue 时间分区重放（见本文件「resync 快照-流栅栏」节）。
    ResyncFillGate fill_gate(*this);

    // 按 UDS 追踪过的 symbol 做 userTrades gap-fill。
    if (uds_) {
        const auto symbols = uds_->tracked_trade_symbols();
        for (const auto& sym : symbols) {
            resync_user_trades_for_symbol(sym);
        }
    }

    // fence floor 必须在 positionRisk 前采样，保证 T <= floor 的 fill 必然已含于快照。
    const int64_t fence_floor_ms = sample_position_fence_floor_ms_();

    // positionRisk 是持仓真值源；emit 前切到 Recovering，让 account_store reset+set。
    auto arr = get_position_risk_array_("reconcile_resync");
    if (!arr) {
        return;  // fill_gate 析构按到达序 flush 暂存 fill。
    }
    const nlohmann::json& j = *arr;

    std::vector<XtPositionSnapshot> positions;
    std::unordered_map<std::string, int64_t> fence_update_ms;
    positions.reserve(static_cast<size_t>(j.size()));
    fence_update_ms.reserve(static_cast<size_t>(j.size()));
    // 行级解析失败 fail-closed：resync 是 reset+set 绝对覆盖，快照缺 tracked 行
    // = 该 symbol 本地真仓被抹零，比 fetch_positions 侧更致命 → 放弃本次快照。
    std::size_t dropped_tracked = 0;   // tracked 或身份不明（从严）
    std::size_t dropped_other = 0;
    for (const auto& p : j) {
        const std::string symbol = defensive_symbol(p);
        if (symbol.empty()) {
            ++dropped_tracked;
            XtLogger::warn_m("BinanceTD",
                "reconcile_resync: row without parsable symbol dropped (fail-closed)");
            continue;
        }
        try {
            // fence 采集所有行（含 positionAmt=0）；刚平为 flat 的 symbol 仍需要
            // updateTime 来吸收对应平仓 fill。
            fence_update_ms[symbol] = p.value("updateTime", 0LL);
            const double amt = stod_finite(p.value("positionAmt", "0"));
            // snapshot emit 只保留非零持仓，空仓由 reset+set 语义自然清除。
            if (amt == 0.0) continue;
            XtPositionSnapshot ps;
            ps.code = symbol;
            if (amt > 0) {
                ps.long_qty   = amt;
                ps.long_today = amt;          // Binance 不区分今昨仓
                ps.long_avg   = stod_finite(p.value("entryPrice", "0"));
            } else if (amt < 0) {
                ps.short_qty   = -amt;
                ps.short_today = -amt;
                ps.short_avg   = stod_finite(p.value("entryPrice", "0"));
            }
            // 全量 fallback 导入的非零行补播种：per-symbol 拉取模式必须覆盖任何
            // 本地可能持仓的 symbol，否则缺席会被反向 sweep 误判 phantom。
            tracked_symbols_.note(symbol);
            positions.push_back(std::move(ps));
        } catch (...) {
            if (tracked_symbols_.contains(symbol)) ++dropped_tracked;
            else                                   ++dropped_other;
            XtLogger::warn_m("BinanceTD",
                "reconcile_resync: unparsable positionRisk row dropped symbol={}",
                symbol);
        }
    }
    if (dropped_tracked > 0) {
        XtLogger::warn_m("BinanceTD",
            "reconcile_resync: {} tracked/unknown unparsable row(s) → abort snapshot "
            "(reset+set would wipe symbols missing from a partial snapshot)",
            dropped_tracked);
        return;  // fill_gate 析构按到达序 flush 暂存 fill（同 fetch 失败路径）。
    }
    if (dropped_other > 0) {
        XtLogger::warn_m("BinanceTD",
            "reconcile_resync: {} untracked unparsable row(s) skipped", dropped_other);
    }

    // 暂存 fill 分区重放 + recovery 快照 emit（is_recovery 语义由 gate 内切换）。
    fill_gate.finish_with_snapshot(positions, fence_update_ms, fence_floor_ms,
                                   /*mark_recovering=*/true);

    XtLogger::warn_m("BinanceTD",
        "reconcile_resync emitted {} position(s) is_recovery=true (overwrites local)",
        positions.size());

    // 末尾重建交易所仍存活的本进程订单；cold-start 与 warm reconnect 共用入口。
    const bool cold = is_first_recovery_.exchange(false);
    rebuild_alive_inflight_and_emit(/*is_cold_start=*/cold);

    // positionRisk 不携带资金字段，末尾补 account truth；emit 顺序固定 position → account。
    anchor_account_truth_after_resync_();
}

// account re-anchor 共享 fetch_account_detail 节流；失败只跳过本次 resync，
// 后续 keepalive refresh 会再次覆盖 cash truth。
void BinanceReconciler::anchor_account_truth_after_resync_() {
    const uint64_t now_ms = lat_now_us() / 1000;
    if (!account_anchor_due(now_ms,
                            last_account_fetch_ms_.load(std::memory_order_relaxed),
                            kAccountAnchorMinIntervalMs)) {
        return;
    }
    auto det = fetch_account_detail();
    if (!det) return;  // fetch_account_detail 已 warn。
    if (cb_.on_account) cb_.on_account(det->acct);
    XtLogger::info_m("BinanceTD",
        "reconcile_resync account re-anchor balance={} available={}",
        det->acct.balance, det->acct.available);
}

// 拉取 account venue truth；REST 成功即消耗节流预算，解析失败也不能立即重刷。
// body 解析见 td/BinanceTdParse.h::parse_account_detail_body（纯函数）。
std::optional<AccountTruthDetail> BinanceReconciler::fetch_account_detail() {
    auto ar = rest_->get_signed("/fapi/v2/account", {});
    if (!ar.ok()) {
        XtLogger::warn_m("BinanceTD",
            "GET account failed: http={} err='{}' body='{}'",
            ar.http_status, ar.curl_err, ar.body);
        return std::nullopt;
    }
    last_account_fetch_ms_.store(lat_now_us() / 1000, std::memory_order_relaxed);
    return parse_account_detail_body(ar.body);
}

// account 接口不提供 markPrice；positionRisk 只作为 markPrice 补充源。
std::unordered_map<std::string, int64_t> BinanceReconciler::fetch_position_marks() {
    auto arr = get_position_risk_array_("fetch_position_marks");
    if (!arr) return {};
    // per-symbol merged array 复用同一个 body parser；过滤后序列化成本在冷路径可接受。
    return parse_position_marks_body(arr->dump());
}

void BinanceReconciler::refresh_snapshot() {
    // keepalive 只刷新 account/broker truth，不发 position snapshot；position snapshot
    // 仍由 UDS ACCOUNT_UPDATE 或 recovery/resync 路径负责。account 失败时不 emit，
    // 避免用空 truth 覆盖有效 overlay。
    const auto t0 = sign::now_ms();
    auto det = fetch_account_detail();
    if (!det) {
        XtLogger::warn_m("BinanceTD",
            "refresh_snapshot done acct=FAIL elapsed_ms={}",
            sign::now_ms() - t0);
        return;
    }
    if (cb_.on_account) cb_.on_account(det->acct);

    // markPrice merge 是 best-effort；失败时 mark_price_fp 保持 0，不阻塞 account emit。
    auto marks = fetch_position_marks();
    for (auto& pos : det->per_symbol) {
        auto it = marks.find(pos.symbol);
        if (it != marks.end()) pos.mark_price_fp = it->second;
    }

    // cb.on_account 与 broker venue-truth overlay 并行发布：
    // 前者服务账户快照消费者，后者服务 read-side truth source。
    if (cb_.on_broker_account_truth) {
        BrokerVenueTruth bt;
        bt.available_balance_fp = det->available_balance_fp;
        bt.maint_margin_fp = det->maint_margin_fp;
        bt.total_margin_balance_fp = det->total_margin_balance_fp;
        bt.per_symbol = std::move(det->per_symbol);
        bt.per_symbol_is_broker_truth = true;
        cb_.on_broker_account_truth(bt);
    }

    XtLogger::info_m("BinanceTD",
        "refresh_snapshot done acct=ok elapsed_ms={}",
        sign::now_ms() - t0);
}

void BinanceReconciler::bootstrap_snapshot() {
    // 先写下一次 bootstrap 重试时间；任一 readiness 失败分支 early-return 后，
    // keepalive 只能按该退避窗口重推，避免 418/-1003 ban 期间高频 REST 重试。
    {
        const uint32_t attempt = bootstrap_attempts_.fetch_add(1, std::memory_order_relaxed);
        int64_t backoff_ms = 30000LL << (attempt < 4u ? attempt : 4u);
        if (backoff_ms > 300000LL) backoff_ms = 300000LL;
        bootstrap_next_retry_ms_.store(static_cast<int64_t>(lat_now_us() / 1000) + backoff_ms,
                                       std::memory_order_relaxed);
    }

    // 1. positionRisk：启动持仓 truth。
    // UDS 已并行连接，跨重启在途单可能继续成交，因此 bootstrap 也使用
    // snapshot-vs-stream fence，而不是直接 emit position。
    ResyncFillGate fill_gate(*this);
    const int64_t fence_floor_ms = sample_position_fence_floor_ms_();
    std::unordered_map<std::string, int64_t> fence_update_ms;
    std::vector<XtPositionSnapshot> positions;
    auto pr = rest_->get_signed("/fapi/v2/positionRisk", {});
    if (pr.ok()) {
        auto j = nlohmann::json::parse(pr.body, nullptr, /*allow_exceptions=*/false);
        if (j.is_array()) {
            fence_update_ms.reserve(static_cast<size_t>(j.size()));
            for (const auto& p : j) {
                try {
                    const std::string symbol = p.value("symbol", std::string{});
                    if (symbol.empty()) continue;
                    // 空仓行也要采 fence updateTime，否则刚平仓 fill 可能被二次 apply。
                    fence_update_ms[symbol] = p.value("updateTime", 0LL);
                    const double amt = stod_finite(p.value("positionAmt", "0"));
                    if (amt == 0.0) continue;
                    XtPositionSnapshot ps;
                    ps.code = symbol;
                    const double entry = stod_finite(p.value("entryPrice", "0"));
                    if (amt > 0) { ps.long_qty = amt; ps.long_avg = entry; }
                    else         { ps.short_qty = -amt; ps.short_avg = entry; }
                    // positionSide 仅用于诊断 one-way/hedge 模式与 reduceOnly 拒单。
                    XtLogger::warn_m("BinanceTD",
                        "bootstrap position detail symbol={} positionAmt={} positionSide='{}' entry={}",
                        ps.code, amt, p.value("positionSide", std::string("?")), entry);
                    tracked_symbols_.note(ps.code);   // 继承持仓播种 tracked_symbols_。
                    positions.push_back(std::move(ps));
                } catch (...) {
                    // 坏行也播种：让 reconcile loop 的 per-symbol 拉取覆盖该 symbol
                    // 并按行级 fail-closed 暴露（age 告警），而不是从此全链路静默
                    // （不播种则 per-symbol 拉取永不查它、本地又无仓 → 反向 sweep
                    // 也不扫，venue 真仓 vs 本地无仓的分歧无任何信号）。
                    const std::string symbol = defensive_symbol(p);
                    if (!symbol.empty()) tracked_symbols_.note(symbol);
                    // 收窄 fail-closed：仅当能独立证明该坏行是真实非零仓位时才闩死
                    // Position readiness——真仓 malformed 若被 satisfy 放行，本地按
                    // flat 启动会有重复开仓风险。反之无关 flat symbol 的 junk 行只
                    // 播种放行，避免账户级 ~700 symbol 的坏行 DoS 整个 TD 就绪与
                    // divergence 检测（positionRisk 返全账户，非零仓才是 money-path）。
                    // 持续坏行下 :544 退避重试永不到达行末 satisfy → TD 永不 ready。
                    bool provable_nonzero_position = false;
                    try {
                        // 字符串（venue 现行 wire 形态）与数值型 JSON 两种形态都能证真仓。
                        const auto it_amt = p.find("positionAmt");
                        if (it_amt != p.end() && it_amt->is_number()) {
                            provable_nonzero_position = it_amt->get<double>() != 0.0;
                        } else {
                            provable_nonzero_position =
                                stod_finite(p.value("positionAmt", "0")) != 0.0;
                        }
                    } catch (...) {
                        // positionAmt 缺失或字符串不可解析 → 无法证明真仓，
                        // 维持现状播种 + 继续，不闩 readiness。
                    }
                    if (provable_nonzero_position) {
                        XtLogger::error_m("BinanceTD",
                            "bootstrap: unparsable non-zero positionRisk row symbol='{}'; "
                            "hold Position readiness fail-closed", symbol);
                        readiness_gate_.fail(
                            xt_live::gateway::TdReadinessStage::Position,
                            "unparsable non-zero positionRisk row symbol=" + symbol);
                        return;
                    }
                    XtLogger::warn_m("BinanceTD",
                        "bootstrap: unparsable positionRisk row dropped symbol='{}' "
                        "(seeded to tracked set)", symbol);
                }
            }
            readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::Position,
                                    "binance positionRisk snapshot parsed");
        } else {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Position,
                                 "binance positionRisk response was not an array");
            XtLogger::error_m("BinanceTD",
                "bootstrap positionRisk body not array; hold TD readiness fail-closed");
            return;
        }
        XtLogger::info_m("BinanceTD",
            "bootstrap positions: {} non-zero detected",
            positions.size());
        if (!positions.empty()) {
            XtLogger::warn_m("BinanceTD",
                "bootstrap importing {} inherited position(s) via position snapshot recovery",
                positions.size());
        }
    } else {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Position,
                             "binance positionRisk request failed");
        XtLogger::warn_m("BinanceTD",
            "bootstrap positionRisk failed: http={} err='{}' body='{}'",
            pr.http_status, pr.curl_err, pr.body);
        return;
    }

    // 2. account：资金与保证金。
    // emit 顺序固定 account → position；position snapshot 可能触发 trading_ready，
    // reporter baseline 必须先看到真实资金。
    ITradeGateway::AccountSnapshot acct{};
    auto ar = rest_->get_signed("/fapi/v2/account", {});
    bool account_truth_ok = false;
    if (ar.ok()) {
        auto j = nlohmann::json::parse(ar.body, nullptr, /*allow_exceptions=*/false);
        if (j.is_object()) {
            try {
                // wallet 口径喂 set_cash（与 UDS wb 锚/共享 parser 同口径）。
                acct.balance = stod_finite(j.value("totalWalletBalance", "0"));
                acct.available = stod_finite(j.value("availableBalance", "0"));
                acct.margin = stod_finite(j.value("totalInitialMargin", "0"));
                acct.position_profit = stod_finite(j.value("totalUnrealizedProfit", "0"));
                XtLogger::info_m("BinanceTD",
                    "bootstrap account: wallet={:.2f} available={:.2f} margin={:.2f} unrealized={:.2f}",
                    acct.balance, acct.available, acct.margin, acct.position_profit);
                account_truth_ok = true;
            } catch (...) {
                readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                                     "binance bootstrap account parse error");
                XtLogger::warn_m("BinanceTD", "bootstrap account parse error: {}", ar.body);
            }
        } else {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                                 "binance bootstrap account response was not an object");
        }
    } else {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                             "binance bootstrap account request failed");
        XtLogger::warn_m("BinanceTD",
            "bootstrap account failed: http={} err='{}' body='{}'",
            ar.http_status, ar.curl_err, ar.body);
    }
    if (!account_truth_ok) return;
    readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::Account,
                            "binance account snapshot parsed");
    if (cb_.on_account) cb_.on_account(acct);

    // 再通过 fence emit position。bootstrap 不置 reconcile_in_progress_：冷启 store 为空，
    // position set 本身就是全量导入，不需要 recovery reset 标记。
    fill_gate.finish_with_snapshot(positions, fence_update_ms, fence_floor_ms,
                                   /*mark_recovering=*/false);

    // 末尾 reverse-sync 活单；is_first_recovery_ 决定 cold-start / warm reconnect 语义。
    const bool cold = is_first_recovery_.exchange(false);
    rebuild_alive_inflight_and_emit(/*is_cold_start=*/cold);
    hooks_.emit_ready_if_td_readiness_satisfied();
}

}  // namespace binance_u
