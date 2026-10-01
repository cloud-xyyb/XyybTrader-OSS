#include "xt_engine/sim/XtCtaMatchEngine.h"

#include <climits>
#include <cstring>

#include "xt_util/time/XtTime.h"

// ── Order Management ────────────────────────────────────────────────────

bool XtCtaMatchEngine::cancel(uint64_t order_id) {
    for (auto it = pending_.begin(); it != pending_.end(); ++it) {
        if (it->order_id != order_id) {
            continue;
        }
        // 本地撤单没有交易所时间戳；仍需输出终态 update，避免上层在途状态悬挂。
        publish_order_update_(it->order_id, it->code, it->is_buy, it->offset,
                              it->limit_price_fp, it->qty_fp,
                              XtOrderStatus::Canceled,
                              /*filled_fp=*/0,
                              /*leaves_fp=*/it->qty_fp,
                              /*last_fill_fp=*/0,
                              /*date=*/0,
                              /*time_ms=*/0);
        pending_.erase(it);
        return true;
    }
    return false;
}

int XtCtaMatchEngine::process_pending(const char* code, int64_t price_fp,
                                      uint32_t date, uint32_t time_ms) {
    int fills = 0;
    for (auto it = pending_.begin(); it != pending_.end(); ) {
        if (std::strcmp(it->code, code) != 0) { ++it; continue; }
        // 限价单只能在不劣于 limit price 的价格成交。
        if (it->limit_price_fp > 0) {
            if ( it->is_buy && price_fp > it->limit_price_fp) { ++it; continue; }
            if (!it->is_buy && price_fp < it->limit_price_fp) { ++it; continue; }
        }
        if (do_fill_(it->code, it->is_buy, it->qty_fp, price_fp,
                     it->tag, it->offset, date, time_ms, it->order_id)) {
            ++fills;
        }
        it = pending_.erase(it);
    }
    return fills;
}

int XtCtaMatchEngine::process_pending_ignore_limit(const char* code, int64_t price_fp,
                                                   uint32_t date, uint32_t time_ms) {
    int fills = 0;
    for (auto it = pending_.begin(); it != pending_.end(); ) {
        if (std::strcmp(it->code, code) != 0) { ++it; continue; }
        if (do_fill_(it->code, it->is_buy, it->qty_fp, price_fp,
                     it->tag, it->offset, date, time_ms, it->order_id)) {
            ++fills;
        }
        it = pending_.erase(it);
    }
    return fills;
}

bool XtCtaMatchEngine::fill_immediate(const char* code, bool is_buy, int64_t qty_fp,
                                      int64_t price_fp, const char* tag, XtOffset offset,
                                      uint32_t date, uint32_t time_ms) {
    return do_fill_(code, is_buy, qty_fp, price_fp, tag, offset, date, time_ms);
}

int XtCtaMatchEngine::flush_session_end(uint32_t date, uint32_t time_ms,
                                        bool allow_market_fill) {
    int fills = 0;
    for (const auto& pf : pending_) {
        if (pf.limit_price_fp > 0) {
            // 日结终止限价单必须输出 Canceled，否则上层订单视图会保留在途状态。
            publish_order_update_(pf.order_id, pf.code, pf.is_buy, pf.offset,
                                  pf.limit_price_fp, pf.qty_fp,
                                  XtOrderStatus::Canceled,
                                  /*filled_fp=*/0,
                                  /*leaves_fp=*/pf.qty_fp,
                                  /*last_fill_fp=*/0,
                                  date, time_ms);
            continue;
        }
        auto it = last_price_fp_.find(pf.code);
        // 时间戳非法（调用方判定落在交易时段外）时不得成交：venue 在休市窗内不撮合，
        // 成交即凭空造出 live 不存在的时刻的 fill。与下方「无法定价」分支同走 Canceled。
        if (allow_market_fill && it != last_price_fp_.end() && it->second > 0) {
            if (do_fill_(pf.code, pf.is_buy, pf.qty_fp, it->second,
                         pf.tag, pf.offset, date, time_ms, pf.order_id)) {
                ++fills;
            }
        } else {
            // 市价单缺 last price（session 内无该合约 tick）无法定价成交；必须发 Canceled
            // 终态，否则订单被静默丢弃 → 上层在途视图悬挂 + accept 侧冻结泄漏。
            publish_order_update_(pf.order_id, pf.code, pf.is_buy, pf.offset,
                                  pf.limit_price_fp, pf.qty_fp,
                                  XtOrderStatus::Canceled,
                                  /*filled_fp=*/0,
                                  /*leaves_fp=*/pf.qty_fp,
                                  /*last_fill_fp=*/0,
                                  date, time_ms);
        }
    }
    pending_.clear();
    // 收盘结清：force-drain 双延迟队列（含本 flush 产物与 session 内已在途的延迟事件）。
    // 延迟建模的是回报可见延迟；交易所侧已发生的事件不得越过 session 边界——
    // 否则 fill_latency_ns / cancel_latency_ns > 0 时中间日事件泄漏进下一 session
    // （在策略 on_session_end 结算后才被 promote 派发）、末日事件永久丢失。
    // latency=0 时两队列恒空，此调用为结构性 no-op。
    promote_due_(INT64_MAX);
    return fills;
}

// ── Clock ───────────────────────────────────────────────────────────────

int XtCtaMatchEngine::advance_clock(uint32_t date, uint32_t time_ms) {
    // date = 自然日（event 轴，与 enqueue base 的 update_market_price action 对
    // 同轴，P1 已对齐）。旧 1h 回跳重标绗带（T2.BT-4）已随混合轴退役：
    // event 轴跨午夜单调；倒流脏数据 max-clamp 冻结（只晚释不早释，与
    // OrderManager 入口 clamp 同款语义）。
    int64_t now_ns = xt_time::to_ns(date, time_ms);
    if (cur_clock_ns_ != INT64_MIN && now_ns < cur_clock_ns_) {
        now_ns = cur_clock_ns_;
    }
    cur_clock_ns_ = now_ns;
    if (now_ns < earliest_due_ns_) return 0;
    return promote_due_(now_ns);
}

// ── Event Construction ─────────────────────────────────────────────────

void XtCtaMatchEngine::publish_order_update_(uint64_t order_id,
                                             const char* code,
                                             bool is_buy,
                                             XtOffset offset,
                                             int64_t price_fp,
                                             int64_t qty_fp,
                                             XtOrderStatus status,
                                             int64_t filled_fp,
                                             int64_t leaves_fp,
                                             int64_t last_fill_fp,
                                             uint32_t date,
                                             uint32_t time_ms) {
    if (order_id == 0) return;
    PendingOrderUpdate u{};
    u.order_id     = order_id;
    std::strncpy(u.code, code ? code : "", sizeof(u.code) - 1);
    u.is_buy       = is_buy;
    u.offset       = offset;
    u.price_fp     = price_fp;
    u.qty_fp       = qty_fp;
    u.status       = status;
    u.filled_fp    = filled_fp;
    u.leaves_fp    = leaves_fp;
    u.last_fill_fp = last_fill_fp;
    u.date         = date;
    u.time_ms      = time_ms;
    // cancel() 路径传入 date=0，无法换算物理时间；回退到当前撮合时钟。
    const int64_t now_ns = (date != 0)
                         ? xt_time::to_ns(date, time_ms)
                         : cur_clock_ns_;
    // fill 驱动的 update（Filled/PartiallyFilled）与同源 trade 同回程到达 →
    // fill_latency；旧行为一律 cancel_latency（默认 0）使 Filled update 先于
    // trade 500ms 可见，策略/账本在滞后窗口读到「已终态但仓位未变」分裂态。
    const int64_t latency_ns = (status == XtOrderStatus::Filled ||
                                status == XtOrderStatus::PartiallyFilled)
                                   ? cfg_.fill_latency_ns
                                   : cfg_.cancel_latency_ns;
    enqueue_delayed_update_(u, now_ns, latency_ns);
}

bool XtCtaMatchEngine::do_fill_(const char* code, bool is_buy, int64_t qty_fp,
                                int64_t price_fp, const char* tag, XtOffset offset,
                                uint32_t date, uint32_t time_ms,
                                uint64_t order_id) {
    // 输出 raw trade；close 归因和成本落账由 Gateway drain 后处理。
    XtTradeRecord tr;
    tr.code         = code ? code : "";
    tr.is_buy       = is_buy;
    tr.qty_fp       = qty_fp;
    tr.price        = order_exec_from_fp(price_fp);
    tr.exec_price   = order_exec_from_fp(price_fp);
    tr.order_date   = date;
    tr.order_time   = time_ms;
    tr.trade_date   = date;
    tr.trade_time   = time_ms;
    tr.user_tag     = tag ? tag : "";
    tr.is_close     = false;      // raw close flag，Gateway apply 阶段补全
    tr.close_offset = offset;     // raw offset，Gateway apply 阶段可修正
    tr.is_today     = (offset == XtOffset::CloseToday);
    tr.order_id     = (order_id != 0) ? order_id : next_order_id_++;
    // 合成 fill timestamp：date * 1e9 + HHMMSSmmm * 1e3。
    tr.fill_ts_us   = static_cast<uint64_t>(date)    * 1'000'000'000ULL
                    + static_cast<uint64_t>(time_ms) * 1000ULL;

    const int64_t fill_ns = xt_time::to_ns(date, time_ms);
    enqueue_delayed_trade_(tr, fill_ns);
    // 成交同步发 per-order Filled update（T2.SIM-2）：驱动 account order-freeze
    // 释放链（AccountState::on_order_update 按 filled_delta 增量释放 + 终态释放）
    // 与 book/executer 终态收敛。order_id==0（未跟踪合成单，如 fill_immediate
    // 无 id 路径）由 publish_order_update_ 入口滤掉，不对 book 发未知 id 终态。
    publish_order_update_(order_id, code, is_buy, offset, price_fp, qty_fp,
                          XtOrderStatus::Filled, /*filled_fp=*/qty_fp,
                          /*leaves_fp=*/0, /*last_fill_fp=*/qty_fp,
                          date, time_ms);
    return true;
}

// ── Delay Queue ─────────────────────────────────────────────────────────

void XtCtaMatchEngine::enqueue_delayed_trade_(const XtTradeRecord& tr, int64_t now_ns) {
    if (cfg_.fill_latency_ns <= 0) {
        visible_trades_.push_back(tr);
        return;
    }
    const int64_t visible_at = now_ns + cfg_.fill_latency_ns;
    if (visible_at <= cur_clock_ns_) {
        visible_trades_.push_back(tr);
        return;
    }
    delay_trades_.push_back({tr, visible_at});
    if (visible_at < earliest_due_ns_) earliest_due_ns_ = visible_at;
}

void XtCtaMatchEngine::enqueue_delayed_update_(const PendingOrderUpdate& upd,
                                               int64_t now_ns, int64_t latency_ns) {
    if (now_ns == INT64_MIN || latency_ns <= 0) {
        visible_updates_.push_back(upd);
        return;
    }
    const int64_t visible_at = now_ns + latency_ns;
    if (visible_at <= cur_clock_ns_) {
        visible_updates_.push_back(upd);
        return;
    }
    delay_updates_.push_back({upd, visible_at});
    if (visible_at < earliest_due_ns_) earliest_due_ns_ = visible_at;
}

int XtCtaMatchEngine::promote_due_(int64_t now_ns) {
    int released = 0;
    // 保序 compaction：同期到期事件按入队序释放（同单 Partial→Filled 等
    // 序敏感对不得倒序）；未到期事件维持相对序，后续 promote 同样稳定。
    size_t w = 0;
    for (size_t i = 0; i < delay_trades_.size(); ++i) {
        if (delay_trades_[i].visible_at_ns <= now_ns) {
            visible_trades_.push_back(std::move(delay_trades_[i].trade));
            ++released;
        } else {
            if (w != i) delay_trades_[w] = std::move(delay_trades_[i]);
            ++w;
        }
    }
    delay_trades_.resize(w);
    w = 0;
    for (size_t i = 0; i < delay_updates_.size(); ++i) {
        if (delay_updates_[i].visible_at_ns <= now_ns) {
            visible_updates_.push_back(std::move(delay_updates_[i].update));
            ++released;
        } else {
            if (w != i) delay_updates_[w] = std::move(delay_updates_[i]);
            ++w;
        }
    }
    delay_updates_.resize(w);
    recompute_earliest_due_();
    return released;
}

void XtCtaMatchEngine::recompute_earliest_due_() {
    int64_t e = INT64_MAX;
    for (const auto& d : delay_trades_)  if (d.visible_at_ns < e) e = d.visible_at_ns;
    for (const auto& d : delay_updates_) if (d.visible_at_ns < e) e = d.visible_at_ns;
    earliest_due_ns_ = e;
}
