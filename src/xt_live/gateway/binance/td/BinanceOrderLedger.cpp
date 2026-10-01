#include "td/BinanceOrderLedger.h"

#include <algorithm>
#include <utility>

#include "transport/BinanceTdWireUtil.h"  // parse_binance_status：Accepted ack 原文状态映射。
#include "transport/ITradeTransport.h"    // TransportAck：on_ack 的终态判定输入。

namespace binance_u {

// ── 登记 / 状态推进 ──────────────────────────────────────────────

void BinanceOrderLedger::register_submit(uint64_t local_id, OrderRecord rec) {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    rec.has_pending = true;
    rec.has_inflight = true;
    client_to_local_[rec.client_order_id] = local_id;
    records_[local_id] = std::move(rec);
}

void BinanceOrderLedger::upsert_recovered(uint64_t local_id, OrderRecord venue_rec) {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    if (it == records_.end()) {
        venue_rec.has_pending = false;
        venue_rec.has_inflight = true;
        records_.emplace(local_id, std::move(venue_rec));
        return;
    }
    OrderRecord& rec = it->second;
    // emplace 语义：只回填空缺路由，不覆盖本进程 submit 登记的既有字段。
    if (rec.client_order_id.empty()) rec.client_order_id = std::move(venue_rec.client_order_id);
    if (rec.symbol.empty()) rec.symbol = std::move(venue_rec.symbol);
    if (!rec.has_inflight) {
        // 已销账但 venue 仍 alive：用 venue 字段复活 in-flight facet。
        rec.sys_id = std::move(venue_rec.sys_id);
        rec.status = venue_rec.status;
        rec.is_buy = venue_rec.is_buy;
        rec.price_fp = venue_rec.price_fp;
        rec.qty_fp = venue_rec.qty_fp;
        rec.close_offset = venue_rec.close_offset;
        rec.submit_ts_ms = venue_rec.submit_ts_ms;
        rec.has_inflight = true;
    }
}

void BinanceOrderLedger::bind_sys_id_if_blank(uint64_t local_id, const std::string& sys_id) {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    if (it == records_.end() || !it->second.has_inflight) return;
    if (!it->second.sys_id.empty()) return;
    it->second.sys_id = sys_id;
    it->second.status = XtOrderStatus::Accepted;
}

// ── 字段补齐 helper ─────────────────────────────────────────────

void BinanceOrderLedger::apply_record_fields_(const OrderRecord& rec,
                                              XtOrderExecUpdate& upd) {
    upd.is_buy = rec.is_buy;
    upd.close_offset = rec.close_offset;
    upd.price_fp = rec.price_fp;
    upd.qty_fp = rec.qty_fp;
    if (upd.filled_fp <= 0) upd.filled_fp = 0;
    if (upd.leaves_fp <= 0) upd.leaves_fp = std::max<int64_t>(0, upd.qty_fp - upd.filled_fp);
}

void BinanceOrderLedger::erase_inflight_unlocked_(OrderRecord& rec) {
    rec.has_inflight = false;
    rec.trace_id = 0;
}

bool BinanceOrderLedger::erase_client_route_unlocked_(OrderRecord& rec) {
    if (rec.client_order_id.empty()) return false;
    client_to_local_.erase(rec.client_order_id);
    rec.client_order_id.clear();
    return true;
}

void BinanceOrderLedger::settle_ack_terminal_unlocked_(OrderRecord& rec) {
    erase_inflight_unlocked_(rec);
    if (erase_client_route_unlocked_(rec)) {
        route_erased_ack_.fetch_add(1, std::memory_order_relaxed);
    }
}

// ── 终态判定 / 销账 ─────────────────────────────────────────────

BinanceOrderLedger::TerminalDecision
BinanceOrderLedger::on_ack(const TransportAck& ack, uint64_t local_cb_us) {
    TerminalDecision d;
    // ── cancel 请求的 ack 只认 kCanceled 为终态 ───────────────────────────
    // 其余一律保持在途、不补齐 update、不做终态登记，订单可能仍 live 在交易所：
    //   - transport 失败（stale/timeout/queue-overflow/disconnect/internal）；
    //   - 限流类业务拒绝（-1003/-1008 → codec kRejected）；
    //   - kCancelNoop（-2011 Unknown order）：cancel 紧跟 submit 发出时会跑赢
    //     venue 订单注册（订单随后仍照常挂出）——合成 Canceled 会让本地销账而
    //     委托留在 venue 吃 margin / max-open-order 限额。
    // 真死单由 UDS 终态 / reconcile ghost 检测收敛；活单由策略 cancel 冷却重试补撤。
    if (ack.req_kind == TransportAck::ReqKind::kCancel &&
        ack.kind != TransportAck::Kind::kCanceled) {
        d.action = TerminalDecision::Action::kKeepInflight;
        return d;
    }

    d.action = TerminalDecision::Action::kEmit;
    XtOrderExecUpdate& upd = d.update;
    upd.id = ack.local_id;
    upd.local_cb_us = local_cb_us;

    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(ack.local_id);

    // 字段补齐：pending 短窗（一次性）优先，其后回退 in-flight 记录；
    // 两者都缺时恢复缺省成交字段。
    if (it != records_.end() && it->second.has_pending) {
        it->second.has_pending = false;
        const OrderRecord& rec = it->second;
        upd.is_buy = rec.is_buy;
        upd.close_offset = rec.close_offset;
        upd.price_fp = rec.price_fp;
        upd.qty_fp = rec.qty_fp;
        upd.filled_fp = ack.filled_fp;
        upd.leaves_fp = rec.qty_fp - ack.filled_fp;
        upd.last_fill_fp = ack.filled_fp;
    } else if (it != records_.end() && it->second.has_inflight) {
        upd.filled_fp = ack.filled_fp;
        upd.last_fill_fp = ack.filled_fp;
        apply_record_fields_(it->second, upd);
    } else {
        upd.filled_fp = 0;
        upd.last_fill_fp = 0;
    }

    d.terminal_seen_before =
        terminal_orders_.find(ack.local_id) != terminal_orders_.end();

    switch (ack.kind) {
    case TransportAck::Kind::kAccepted:
        upd.status = parse_binance_status(ack.raw_status);
        // 先绑 exchange orderId 再判终态；已终态/已销账的迟到 Accepted 不回绑。
        if (!ack.sys_id.empty() && !d.terminal_seen_before &&
            it != records_.end() && it->second.has_inflight) {
            it->second.sys_id = ack.sys_id;
            it->second.status = upd.status;
        }
        if (order_status_is_final(upd.status)) {
            upd.leaves_fp = 0;
            remember_terminal_unlocked_(ack.local_id);
            if (it != records_.end()) settle_ack_terminal_unlocked_(it->second);
        }
        break;
    case TransportAck::Kind::kCanceled:
        // 仅 venue 确认的 kCanceled 会走到此处（早返回短路已拦下 cancel 请求的其余
        // kind）；非 cancel 请求收到 kCancelNoop 理论不可达，落 default
        //（Rejected）暴露假设破裂而非静默终态化。
        upd.status = XtOrderStatus::Canceled;
        remember_terminal_unlocked_(ack.local_id);
        if (it != records_.end()) settle_ack_terminal_unlocked_(it->second);
        break;
    case TransportAck::Kind::kRejected:
        upd.status = XtOrderStatus::Rejected;
        upd.leaves_fp = 0;
        remember_terminal_unlocked_(ack.local_id);
        if (it != records_.end()) settle_ack_terminal_unlocked_(it->second);
        break;
    case TransportAck::Kind::kTransportError_Stale:
    case TransportAck::Kind::kTransportError_TimeoutBeforeSend:
    case TransportAck::Kind::kTransportError_QueueOverflow:
    case TransportAck::Kind::kTransportError_Internal:
        // submit 未挂出，可安全终态化。
        upd.status = XtOrderStatus::Rejected;
        upd.leaves_fp = 0;
        remember_terminal_unlocked_(ack.local_id);
        if (it != records_.end()) settle_ack_terminal_unlocked_(it->second);
        break;
    case TransportAck::Kind::kTransportError_TimeoutAfterSend:
    case TransportAck::Kind::kTransportError_Disconnect:
        // 已 write：venue 可能已挂单/成交，保持 Submitted 交给 reconcile。
        upd.status = XtOrderStatus::Submitted;
        break;
    default:
        upd.status = XtOrderStatus::Rejected;
        upd.leaves_fp = 0;
        remember_terminal_unlocked_(ack.local_id);
        if (it != records_.end()) settle_ack_terminal_unlocked_(it->second);
        break;
    }
    return d;
}

BinanceOrderLedger::TerminalDecision
BinanceOrderLedger::on_uds_terminal(const XtOrderExecUpdate& upd) {
    TerminalDecision d;
    d.action = TerminalDecision::Action::kEmit;  // UDS 信号无论终态与否放行；去重在下游。
    if (!order_status_is_final(upd.status)) return d;
    std::lock_guard<std::mutex> lk(ledger_mu_);
    remember_terminal_unlocked_(upd.id);
    auto it = records_.find(upd.id);
    if (it == records_.end()) return d;
    erase_inflight_unlocked_(it->second);
    erase_client_route_unlocked_(it->second);
    return d;
}

BinanceOrderLedger::TerminalDecision
BinanceOrderLedger::on_reconcile_terminal(uint64_t local_id, ReconcileTerminal kind,
                                          XtOrderExecUpdate& upd) {
    TerminalDecision d;
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    switch (kind) {
    case ReconcileTerminal::kNotExistCanceled:
        // 原子「补齐 + 记终态」；in-flight facet 保留，由 ghost 主循环统一销账。
        if (it == records_.end() || !it->second.has_inflight) return d;  // 并发已销账 → kSkip
        apply_record_fields_(it->second, upd);
        remember_terminal_unlocked_(local_id);
        break;
    case ReconcileTerminal::kBlankSysIdReject:
        // 在途且 sys_id 仍为空才合成 Rejected；否则交由正常 ACK/ghost 路径收敛。
        if (it == records_.end() || !it->second.has_inflight) return d;
        if (!it->second.sys_id.empty()) return d;
        apply_record_fields_(it->second, upd);
        remember_terminal_unlocked_(local_id);
        erase_inflight_unlocked_(it->second);
        if (erase_client_route_unlocked_(it->second)) {
            route_erased_reconcile_.fetch_add(1, std::memory_order_relaxed);
        }
        break;
    case ReconcileTerminal::kVenueTerminal:
        // 字段已来自 venue REST；记终态 + 销账 + 清撤单路由（重复登记幂等）。
        remember_terminal_unlocked_(local_id);
        if (it != records_.end()) {
            erase_inflight_unlocked_(it->second);
            if (erase_client_route_unlocked_(it->second)) {
                route_erased_reconcile_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        break;
    }
    d.action = TerminalDecision::Action::kEmit;
    return d;
}

// ── 终态去重窗口 ────────────────────────────────────────────────

bool BinanceOrderLedger::terminal_seen(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    return terminal_orders_.find(local_id) != terminal_orders_.end();
}

void BinanceOrderLedger::remember_terminal_unlocked_(uint64_t local_id) {
    if (local_id == 0) return;
    if (terminal_orders_.insert(local_id).second) {
        terminal_order_fifo_.push_back(local_id);
    }
    while (terminal_orders_.size() > kTerminalOrderCacheCap && !terminal_order_fifo_.empty()) {
        const uint64_t old = terminal_order_fifo_.front();
        terminal_order_fifo_.pop_front();
        terminal_orders_.erase(old);
    }
}

void BinanceOrderLedger::remember_terminal(uint64_t local_id) {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    remember_terminal_unlocked_(local_id);
}

// ── fill 去重 ───────────────────────────────────────────────────

std::string BinanceOrderLedger::make_orderless_fill_key_(std::string_view symbol,
                                                        int64_t trade_id) {
    std::string key;
    key.reserve(symbol.size() + 21);
    key.append(symbol);
    key.push_back('|');
    key.append(std::to_string(trade_id));
    return key;
}

bool BinanceOrderLedger::orderless_fill_contains_unlocked_(const std::string& key) const {
    return orderless_fill_keys_.find(key) != orderless_fill_keys_.end();
}

void BinanceOrderLedger::insert_orderless_fill_unlocked_(std::string key, uint64_t owner) {
    // 幂等：key 已在窗内 → 不覆盖 owner、不重复入 FIFO（首写者保留；自成交
    // 另一腿同记不夺首腿归属）。
    if (orderless_fill_keys_.find(key) != orderless_fill_keys_.end()) return;
    orderless_fill_keys_.emplace(key, owner);
    orderless_fill_fifo_.push_back(std::move(key));
    while (orderless_fill_keys_.size() > kOrderlessFillWindowCap &&
           !orderless_fill_fifo_.empty()) {
        orderless_fill_keys_.erase(orderless_fill_fifo_.front());
        orderless_fill_fifo_.pop_front();
    }
}

bool BinanceOrderLedger::note_orderless_fill_unlocked_(std::string_view symbol,
                                                       int64_t trade_id) {
    orderless_fill_routed_.fetch_add(1, std::memory_order_relaxed);
    std::string key = make_orderless_fill_key_(symbol, trade_id);
    if (orderless_fill_contains_unlocked_(key)) {
        // orderless 呈现无法区分自成交腿，任何命中一律判重（与旧全局窗同口径的
        // 保守抑制——此残留由 order_id=0 入口无 owner 上下文所致）。
        orderless_fill_dups_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    insert_orderless_fill_unlocked_(std::move(key), /*owner=*/0);
    return true;
}

bool BinanceOrderLedger::note_fill_seen(uint64_t order_id, std::string_view symbol,
                                        int64_t trade_id) {
    if (trade_id <= 0) return true;  // 无 trade id 无法去重，按新成交处理。
    std::lock_guard<std::mutex> lk(ledger_mu_);
    if (order_id != 0) {
        auto it = records_.find(order_id);
        if (it != records_.end()) {
            OrderRecord& rec = it->second;
            if (rec.fill_tids.count(trade_id) > 0) {
                per_order_fill_dups_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            // 跨域互查（owner-aware）：同笔 (symbol, trade_id) 可能先以 order_id=0
            // （归因失败）记入全局窗，或被另一单先记——venue 自成交两腿是不同
            // order_id、同 (symbol, trade_id) 的合法成交。按窗内 owner 区分：
            std::string key = make_orderless_fill_key_(symbol, trade_id);
            const auto wit = orderless_fill_keys_.find(key);
            if (wit != orderless_fill_keys_.end()) {
                const uint64_t owner = wit->second;
                if (owner == 0 || owner == order_id) {
                    // orderless 起源 / 本单已同记 → 判重抑制；同时回写 fill_tids，
                    // 使抑制耐久性不再依赖全局窗 FIFO 存活（该 fill 已记账）。
                    cross_domain_fill_dups_.fetch_add(1, std::memory_order_relaxed);
                    rec.fill_tids.insert(trade_id);
                    return false;
                }
                // owner 为其它订单 → venue 自成交另一腿，直接耐久接受（绕过 cap，
                // 与上方回写同口径）：避免触顶订单的自成交腿落入 overflow→全局窗
                // 命中被误抑制；窗内 key 已在（owner=首腿），无需同记。
                self_trade_leg_passthrough_.fetch_add(1, std::memory_order_relaxed);
                rec.fill_tids.insert(trade_id);
                return true;
            }
            if (rec.fill_tids.size() < kMaxFillKeysPerOrder) {
                rec.fill_tids.insert(trade_id);
                // 双域同记（owner=本单）：per-order 接受反向同记进全局窗，封住后续
                // 同笔以 order_id=0 归因失败的二次放行。全局窗同记一份 → 窗占用与
                // 全局去重口径同基线（kOrderlessFillWindowCap FIFO 兜底）。key 已在
                // 窗内时 insert 幂等 → 首写者保留（自成交另一腿不夺首腿 owner）。
                insert_orderless_fill_unlocked_(std::move(key), order_id);
                return true;
            }
            // 容器触顶（单笔订单 fill 数病态多）：不淘汰已记账 key（防重交付），
            // 新 key 降级走全局窗继续去重。
            per_order_fill_overflow_.fetch_add(1, std::memory_order_relaxed);
        }
        // 无记录（order_id 反解成功但 ledger 未登记，如跨重启残余）→ 全局窗兜底。
    }
    return note_orderless_fill_unlocked_(symbol, trade_id);
}

BinanceOrderLedger::FillDedupStats BinanceOrderLedger::fill_dedup_stats() const {
    FillDedupStats s;
    s.per_order_dups = per_order_fill_dups_.load(std::memory_order_relaxed);
    s.cross_domain_dups = cross_domain_fill_dups_.load(std::memory_order_relaxed);
    s.self_trade_leg_passthrough =
        self_trade_leg_passthrough_.load(std::memory_order_relaxed);
    s.per_order_overflow = per_order_fill_overflow_.load(std::memory_order_relaxed);
    s.orderless_routed = orderless_fill_routed_.load(std::memory_order_relaxed);
    s.orderless_dups = orderless_fill_dups_.load(std::memory_order_relaxed);
    s.route_erased_ack = route_erased_ack_.load(std::memory_order_relaxed);
    s.route_erased_reconcile = route_erased_reconcile_.load(std::memory_order_relaxed);
    return s;
}

// ── venue 终态查询 ──────────────────────────────────────────────

bool BinanceOrderLedger::is_order_terminal(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    // in-flight 优先判非终态：reverse-sync 复活单（venue openOrders 报 alive）
    // 即使终态窗仍残留旧 local_id，也不得让引擎按终态合成销账。
    if (it != records_.end() && it->second.has_inflight) return false;
    if (terminal_orders_.count(local_id) > 0) return true;
    // 终态窗 FIFO cap 淘汰后的兜底：record 终身存活，in-flight 已销 ⇔ 走过
    // 某个终态销账站点。无记录返回 false（未知 id 不得判终态）。
    return it != records_.end();
}

bool BinanceOrderLedger::lookup_terminal(uint64_t local_id, XtOrderStatus& out_status,
                                         int64_t& out_filled_fp) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    // payload 需要记录作载体；in-flight 优先判非终态（与 is_order_terminal 同序）。
    if (it == records_.end() || it->second.has_inflight) return false;
    const OrderRecord& rec = it->second;
    // 记录 status 是最近一次回写值，终态销账站点不全回写（kCanceled/UDS 终态
    // 不回写）→ 非终态值时 payload 不可信，交还布尔探测兜底。
    if (!order_status_is_final(rec.status)) return false;
    out_status = rec.status;
    out_filled_fp = (rec.status == XtOrderStatus::Filled) ? rec.qty_fp : -1;
    return true;
}

// ── 查询面 ─────────────────────────────────────────────────────

bool BinanceOrderLedger::is_inflight(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    return it != records_.end() && it->second.has_inflight;
}

std::string BinanceOrderLedger::sys_id_for(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    if (it == records_.end() || !it->second.has_inflight) return std::string();
    return it->second.sys_id;
}

std::string BinanceOrderLedger::client_order_id_for(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    return it == records_.end() ? std::string() : it->second.client_order_id;
}

std::optional<uint64_t>
BinanceOrderLedger::local_for_client(std::string_view client_order_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    const auto it = client_to_local_.find(std::string(client_order_id));
    if (it == client_to_local_.end()) return std::nullopt;
    return it->second;
}

uint64_t BinanceOrderLedger::trace_id_for(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    return it == records_.end() ? 0 : it->second.trace_id;
}

std::string BinanceOrderLedger::symbol_for(uint64_t local_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    auto it = records_.find(local_id);
    return it == records_.end() ? std::string() : it->second.symbol;
}

std::vector<BinanceOrderLedger::InflightBrief>
BinanceOrderLedger::inflight_snapshot() const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    std::vector<InflightBrief> out;
    out.reserve(records_.size());
    for (const auto& [local_id, rec] : records_) {
        if (!rec.has_inflight) continue;
        InflightBrief b;
        b.local_id = local_id;
        b.sys_id = rec.sys_id;
        b.symbol = rec.symbol;
        b.submit_ts_ms = rec.submit_ts_ms;
        b.close_offset = rec.close_offset;
        out.push_back(std::move(b));
    }
    return out;
}

std::optional<BinanceOrderLedger::InflightBrief>
BinanceOrderLedger::find_inflight_by_sys_id(std::string_view sys_id) const {
    std::lock_guard<std::mutex> lk(ledger_mu_);
    for (const auto& [local_id, rec] : records_) {
        if (!rec.has_inflight || rec.sys_id != sys_id) continue;
        InflightBrief b;
        b.local_id = local_id;
        b.sys_id = rec.sys_id;
        b.symbol = rec.symbol;
        b.submit_ts_ms = rec.submit_ts_ms;
        b.close_offset = rec.close_offset;
        return b;
    }
    return std::nullopt;
}

}  // namespace binance_u
