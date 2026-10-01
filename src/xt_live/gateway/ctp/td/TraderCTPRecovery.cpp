// TraderCTP recovery 分片：登录后查询超时守卫、recovery pipeline entry 与 reverse-sync。
// 本文件只编排 readiness gate 前的 account/position/order/trade 查询结果和 alive in-flight 灌入。
// query 发送和 retry 控制在 TraderCTPQuery.cpp；订单/成交行的 CTP 解析在 TraderCTP.cpp/OrderOps 中完成。
// timeout token 使用 per-kind 原子序号失效过期回调，保证 stop/reset/recovery restart 后不会消费过期 timer。

#include "ctp/td/TraderCTP.h"

#include "ctp/common/CTPUtil.h"
#include "ctp/recovery/CtpRecoveryRearm.h"
#include "xt_live/gateway/common/ReverseSyncHelper.h"
#include "XtLogger.h"
#include "common/XtLiveRecoveryError.h"

#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif

#include <algorithm>
#include <cstdio>

namespace {

// 文件内日志辅助：保持 recovery timeout / reverse-sync reason 输出一致。
const char* rc(XtLiveRecoveryError code) { return live_recovery_error_str(code); }

}  // namespace

// ── 恢复查询超时守卫 ──────────────────────────────

void TraderCTP::clear_recovery_query_timeout(QueryKind kind) {
    auto& q = qry_[qry_idx(kind)];
    q.timeout_token.fetch_add(1, std::memory_order_acq_rel);
    q.timeout_count.store(0, std::memory_order_release);
}

void TraderCTP::clear_recovery_query_timeout_states() {
    for (auto& q : qry_) {
        q.timeout_token.fetch_add(1, std::memory_order_acq_rel);
        q.timeout_count.store(0, std::memory_order_release);
    }
}

// ── 恢复查询 fail-closed 后的有界 re-arm ──────────────

void TraderCTP::set_recovery_wedged_(bool wedged) {
    recovery_wedged_.store(wedged, std::memory_order_release);
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().set_td_recovery_wedged("ctp", wedged);
#endif
}

void TraderCTP::reset_query_ctrl_for_rearm_(QueryKind kind) {
    // query_once 的 inflight CAS 在「已发送但无回包」时恒失败 → 重发前必须解开 stuck inflight，
    // 否则 query_once no-op、wedge 不解。qry_ 全 atomic，从 timer 线程触碰安全；不碰 state_。
    auto& q = qry_[qry_idx(kind)];
    q.inflight.store(false, std::memory_order_release);
    // 使旧轮 request_id 绑定失效：新发送覆盖前（query_once 可能被 throttle 推迟），
    // 被放弃轮的迟到回包（含 bIsLast）由 generation fence 丢弃。不能置 0——
    // 0 是「未绑定 fail-open 全收」语义。
    q.active_request_id.store(kQueryRequestIdInvalidated, std::memory_order_release);
    // 查询会话代际推进：rearm = 放弃旧代（provenance 消费与观测）。
    query_session_gen_.fetch_add(1, std::memory_order_acq_rel);
    q.dirty.store(false, std::memory_order_release);
    q.next_allowed_ms.store(0, std::memory_order_release);
    q.backoff_level.store(0, std::memory_order_release);
    q.retry_pending.store(false, std::memory_order_release);
    clear_recovery_query_timeout(kind);  // bump timeout_token（杀 stuck inner timer）+ count=0（fresh inner 预算）
}

void TraderCTP::maybe_schedule_recovery_rearm_(QueryKind kind) {
    if (recovery_rearm_backoff_ms_ <= 0) return;              // 关闭 re-arm（回滚开关）
    if (stopping_.load(std::memory_order_acquire)) return;
    if (!connected_.load(std::memory_order_acquire)) return;  // session 死，OnFront* 接管
    if (!recovery_.is_recovering()) return;                   // 非 recovery（post-ready refresh / done）→ 不打扰

    int      round;
    int      backoff_ms;
    uint64_t token;
    {
        std::lock_guard<std::mutex> lk(recovery_rearm_mu_);
        if (recovery_rearm_pending_) return;                 // 单轮去重（timeout+rsp-error 同查询 race / 单查询多阶段）
        recovery_rearm_pending_ = true;
        round = ++recovery_rearm_round_;
        const auto d = ctp_recovery::recovery_rearm_decision(
            round, recovery_rearm_alert_rounds_, recovery_rearm_backoff_ms_,
            ctp_recovery::kRecoveryRearmBackoffMaxMs);
        backoff_ms = d.backoff_ms;
        token      = recovery_rearm_token_.load(std::memory_order_acquire);
        if (d.raise_alert) set_recovery_wedged_(true);       // 超阈 paging，但 re-arm 继续（page-then-continue）
        recovery_rearm_scheduled_total_.fetch_add(1, std::memory_order_acq_rel);
    }
    const QueryMeta meta = query_meta(kind);
    XtLogger::warn_m("TD",
                     "event=recovery_rearm_scheduled stage={} round={} alert_rounds={} backoff_ms={}",
                     meta.name, round, recovery_rearm_alert_rounds_, backoff_ms);

    schedule_after_ms(backoff_ms, [this, kind, token]() {
        if (stopping_.load(std::memory_order_acquire)) return;
        {
            std::lock_guard<std::mutex> lk(recovery_rearm_mu_);
            // 跨 session 旧回调：token 不匹配则 no-op，且不动新 session 的 pending（由 reset_session_state 管）。
            if (recovery_rearm_token_.load(std::memory_order_acquire) != token) return;
            recovery_rearm_pending_ = false;
        }
        if (stopping_.load(std::memory_order_acquire)) return;
        if (!connected_.load(std::memory_order_acquire)) return;
        if (!recovery_.is_recovering()) return;              // recovery 已完成 → 不打扰 ready session
        const QueryMeta m = query_meta(kind);
        XtLogger::warn_m("TD", "event=recovery_rearm_fire stage={} re-issuing query", m.name);
        reset_query_ctrl_for_rearm_(kind);  // 解 stuck inflight
        query_once(kind);                   // 唯一动作；既有 inner-retry 已安全从 timer 线程调；不重入 pipeline / 不碰 state_
    });
}

void TraderCTP::arm_recovery_query_timeout(QueryKind kind) {
    if (recovery_query_timeout_ms_ <= 0) return;
    if (!recovery_.is_recovering()) return;

    auto&          q         = qry_[qry_idx(kind)];
    const uint64_t token     = q.timeout_token.fetch_add(1, std::memory_order_acq_rel) + 1;
    const int      timeout_ms = recovery_query_timeout_ms_;

    schedule_after_ms(timeout_ms, [this, kind, token, timeout_ms]() {
        if (!is_session_alive()) return;
        if (!recovery_.is_recovering()) return;
        auto& q = qry_[qry_idx(kind)];
        if (q.timeout_token.load(std::memory_order_acquire) != token) return;
        const int timeout_hit = q.timeout_count.fetch_add(1, std::memory_order_acq_rel) + 1;
        const QueryMeta meta  = query_meta(kind);

        // position/account 是交易正确性的硬门槛，超时优先重试。
        // order/trade 超时直接标记失败，避免无限卡住 ready。
        if (kind == QueryKind::Position || kind == QueryKind::AccountState) {
            if (timeout_hit >= recovery_query_max_timeouts_) {
                XtLogger::error_m("TD",
                                 "event=recovery_query_timeout stage={} reason_code={} "
                                 "timeout_ms={} hit={}/{} fail closed",
                                 meta.name, rc(XtLiveRecoveryError::RecoveryQueryTimeout),
                                 timeout_ms, timeout_hit, recovery_query_max_timeouts_);
                if (kind == QueryKind::Position) {
                    readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Position,
                                         "ctp position query timeout");
                } else {
                    readiness_gate_.fail(xt_live::gateway::TdReadinessStage::Account,
                                         "ctp account query timeout");
                }
                maybe_schedule_recovery_rearm_(kind);  // 有界 backoff 后重发，不再永久 fail-closed
                clear_recovery_query_timeout(kind);
                return;
            }
            XtLogger::warn_m("TD",
                             "event=recovery_query_timeout stage={} reason_code={} "
                             "timeout_ms={} hit={}/{} retry query",
                             meta.name, rc(XtLiveRecoveryError::RecoveryQueryTimeout),
                             timeout_ms, timeout_hit, recovery_query_max_timeouts_);
            const uint64_t prev = q.timeout_token.load(std::memory_order_acquire);
            query_once(kind);
            // query_once 可能因 throttle 跳过；未挂新 timeout 时补挂一次。
            if (q.timeout_token.load(std::memory_order_acquire) == prev) {
                arm_recovery_query_timeout(kind);
            }
            return;
        }

        XtLogger::error_m("TD",
                         "event=recovery_query_timeout stage={} reason_code={} "
                         "timeout_ms={} hit={} fail closed",
                         meta.name, rc(XtLiveRecoveryError::RecoveryQueryTimeout),
                         timeout_ms, timeout_hit);
        if (kind == QueryKind::XtOrder) {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::OpenOrders,
                                 "ctp order query timeout");
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                                 "ctp order query timeout before reverse-sync");
        } else if (kind == QueryKind::Trade) {
            readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                                 "ctp trade query timeout before recovery ready");
        }
        // order/trade 内层本无重试，由 re-arm 兜底（有界 backoff 后重发）。
        maybe_schedule_recovery_rearm_(kind);
        clear_recovery_query_timeout(kind);
    });
}

// ── 会话恢复入口 ────────────────────────────────

void TraderCTP::enter_recovery_pipeline(bool settlement_confirmed) {
    // identity mismatch 闩锁是进程级终态：任何路径（含未来新增调用点）都不得再推进
    // 恢复流程；正常入口已在 OnRspUserLogin 的 verify_login_identity_ 处被拦截。
    if (identity_mismatch_latched_.load(std::memory_order_acquire)) {
        XtLogger::error_m("TD",
                          "enter_recovery_pipeline blocked: identity mismatch latched "
                          "(expected_investor_id={})", expected_investor_id_);
        return;
    }
    settlement_confirmed_ = settlement_confirmed;
    clear_recovery_query_timeout_states();
    reset_query_control_states();
    // 成交交付记忆（exec ledger fill 记账）跨 recovery 保留：重放行由投影
    // exactly-once 抑制，无需清任何去重缓存。
    // 清空上一轮未 emit 的 alive order 累积。
    {
        std::lock_guard<std::mutex> lk(recovery_alive_mu_);
        recovery_alive_orders_.clear();
        recovery_alive_routing_.clear();
    }
    // 本轮 recovery 的暂存快照与发射分区锚点重新开始（login retry 同 session 重入）。
    {
        std::lock_guard<std::mutex> lk(recovery_staged_pos_mu_);
        recovery_staged_positions_.clear();
        recovery_staged_pos_valid_ = false;
    }
    recovery_position_anchor_us_.store(0, std::memory_order_release);
    recovery_skipped_foreign_ = 0;
    // 关窗口成交 gate：窗口内待交付成交入缓冲，recovery finish 分区发射。
    // recovery 关闭的构型不关（保持直发 = 原行为，login→ready 窗口极短）。
    // 判据直读 FSM 单一存储（init 与 wrapper 两条 enabled 写入路径共用）。
    if (recovery_.recovery_enabled()) {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        recovery_window_trades_.clear();
        recovery_window_gate_closed_ = true;
        recovery_window_overflow_ = 0;
    }
    set_session_phase(SessionPhase::Recovering);
    recovery_.on_login();
}

// ── Reverse sync 重建 ─────────────────────────

void TraderCTP::rebuild_alive_inflight_and_emit(bool is_cold_start) {
    std::vector<AliveInflightOrder> orders;
    std::unordered_map<uint64_t, RecoveredCancelRouting> routing;
    uint64_t skipped_foreign = 0;
    {
        std::lock_guard<std::mutex> lk(recovery_alive_mu_);
        orders.swap(recovery_alive_orders_);
        routing.swap(recovery_alive_routing_);
    }
    skipped_foreign = recovery_skipped_foreign_;
    recovery_skipped_foreign_ = 0;

    // 先清空 recovery batch，避免 handler 缺失或失败后重复 emit。
    // cold-start 活跃单可能不存在本地映射，需回填后续 OnRtnOrder/OnRtnTrade
    // 归一化所需 metadata。
    for (const auto& a : orders) {
        char ref_buf[24];
        std::snprintf(ref_buf, sizeof(ref_buf), "%llu",
                      static_cast<unsigned long long>(a.local_id));
        if (!state_.has_local_order_ref(ref_buf)) {
            const bool is_close = (a.close_offset != XtOffset::Open);
            const bool is_today = (a.close_offset == XtOffset::CloseToday);
            const double price = order_exec_from_fp(a.price_fp);
            // absorbed_watermark_fp = venue 已成交量：跨重启单 pre-restart 成交
            // 的量口径封印（error-26 推断 + O7 泄漏观测），逐笔重放不再交付。
            state_.register_order(a.local_id, ref_buf, a.code.c_str(),
                                  a.is_buy, price, a.total_qty_fp,
                                  /*user_tag*/ "",
                                  is_close, is_today,
                                  /*submit_begin_us*/ 0,
                                  /*submit_end_us*/ 0,
                                  /*absorbed_watermark_fp*/ a.filled_qty_fp);
        }
        // 新注册与已注册都回填：缺 OrderSysID/原始 front/session 时，
        // cancel 会 fallback 到新 session 三元组 → 交易所匹配不到
        // （跨重启孤儿不可撤的根因）。
        const auto rit = routing.find(a.local_id);
        if (rit != routing.end()) {
            state_.backfill_cancel_routing(a.local_id,
                                           rit->second.exchange_id,
                                           rit->second.order_sys_id,
                                           rit->second.front_id,
                                           rit->second.session_id);
        }
    }

    // CTP 在查询阶段累积 foreign skipped 数量，这里逐个补发 metric。
#ifdef XT_HAVE_METRICS
    for (uint64_t i = 0; i < skipped_foreign; ++i) {
        ct::metrics::XtMetricRegistry::instance()
            .inc_recovery_rebuild_alive_skipped_foreign_total("ctp");
    }
#endif
    // 通用 emit helper 不包含 CTP skipped_foreign 统计，先单独记录。
    XtLogger::info_m("TD",
                     "event=recovery_rebuild_alive_emit venue=ctp orders={} skipped_foreign={} is_cold_start={}",
                     orders.size(), skipped_foreign, is_cold_start ? 1 : 0);

    if (!callbacks_.on_reverse_sync_batch) {
        readiness_gate_.fail(xt_live::gateway::TdReadinessStage::ReverseSync,
                             "ctp reverse-sync handler not bound");
        return;
    }
    xyybtrader::recovery::emit_reverse_sync_batch_with_metric(
        std::move(orders), is_cold_start, "ctp", callbacks_.on_reverse_sync_batch);
    readiness_gate_.satisfy(xt_live::gateway::TdReadinessStage::ReverseSync,
                            "ctp recovery alive-order batch emitted");
    // pending terminals 由 emit_recovery_finish_sequence_ 在 post fills 之后
    // flush（发射序：终态销账不得先于同单 post fill 交付）。
}

// ── Recovery finish 发射分区（§4.4）─────────────────────────

void TraderCTP::stage_recovery_positions_(std::vector<PositionSnapshot> out) {
    // 发射分区锚点 = 本轮 position 查询 bIsLast 的本地墙钟（与 venue TradeTime
    // 同为墙钟域，秒级 skew 由 kFillSealMarginUs 吸收）；多轮 rearm 最新轮覆盖。
    recovery_position_anchor_us_.store(wall_now_epoch_us_(), std::memory_order_release);
    std::lock_guard<std::mutex> lk(recovery_staged_pos_mu_);
    recovery_staged_positions_ = std::move(out);
    recovery_staged_pos_valid_ = true;
}

void TraderCTP::emit_staged_recovery_positions_(bool is_cold_start) {
    std::vector<PositionSnapshot> out;
    bool valid = false;
    {
        std::lock_guard<std::mutex> lk(recovery_staged_pos_mu_);
        valid = recovery_staged_pos_valid_;
        out.swap(recovery_staged_positions_);
        recovery_staged_pos_valid_ = false;
    }
    if (!valid) return;
    // 空 batch 也发射（= 旧 bIsLast 即发行为，消费侧以空快照收敛仓位）。
    // dedicated callback 与 legacy fallback 恰好择一，避免 account/order reducer 双灌。
    if (callbacks_.on_recovery_position) {
        callbacks_.on_recovery_position(out, is_cold_start);
    } else if (callbacks_.on_position) {
        callbacks_.on_position(out);
    }
}

void TraderCTP::reopen_recovery_window_gate_and_drain_() {
    // gate 重开与残量取出同临界区；二次 drain 覆盖发射期间新入缓冲的 fill
    // （SPI 单回调线程下恒空，多回调线程构型的防御性收口）。
    std::vector<XtTradeRecord> residual;
    {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        recovery_window_gate_closed_ = false;
        residual.swap(recovery_window_trades_);
    }
    if (!callbacks_.on_trade) return;
    // recovery 重放 = 延迟脱钩 venue 到达时刻 → kIneligible。
    for (auto& tr : residual) callbacks_.on_trade(tr, XtLatencyEligibility::kIneligible);
}

void TraderCTP::emit_recovery_finish_sequence_(bool is_cold_start) {
    // 取出窗口缓冲；gate 保持关闭直到快照 + post fills 发射完（检查点 iv）。
    std::vector<XtTradeRecord> pending;
    uint64_t overflow = 0;
    {
        std::lock_guard<std::mutex> lk(recovery_window_mu_);
        pending.swap(recovery_window_trades_);
        overflow = recovery_window_overflow_;
        recovery_window_overflow_ = 0;
    }
    if (!callbacks_.on_reverse_sync_batch) {
        // 归因/book 无法就位（rebuild 内部 fail ReverseSync readiness）：快照
        // 照发（与旧 bIsLast 即发行为对齐），缓冲整批不下发（= 旧行为的窗口
        // fill 丢弃），gate 开——之后 fill 直发（td_ready 已被 fail 挡住新单）。
        emit_staged_recovery_positions_(is_cold_start);
        rebuild_alive_inflight_and_emit(is_cold_start);
        {
            std::lock_guard<std::mutex> lk(recovery_window_mu_);
            recovery_window_gate_closed_ = false;
            // 发射期间新入缓冲的残量与整批同语义丢弃，不遗留到下轮 flush。
            recovery_window_trades_.clear();
        }
        if (!pending.empty() || overflow > 0) {
            XtLogger::error_m("TD",
                "event=recovery_window_trades_dropped reason=reverse_sync_not_ready "
                "dropped={} overflow={}",
                pending.size(), overflow);
        }
        return;
    }
    // 分区：venue TradeTime vs 锚点。≤ anchor+margin → pre（大概率已含于快照，
    // 先交付再由快照绝对覆盖吸收仓位效应，fee/realized/on_fill 不丢）；
    // > anchor+margin → post（快照之上的增量，发射在快照/reverse-sync 之后，
    // 仓位效应不得被吸收——检查点 v）。anchor==0（本轮无快照）→ 全 pre。
    const int64_t anchor =
        recovery_position_anchor_us_.load(std::memory_order_acquire);
    std::vector<XtTradeRecord> pre;
    std::vector<XtTradeRecord> post;
    pre.reserve(pending.size());
    for (auto& tr : pending) {
        const auto fill_us = static_cast<int64_t>(tr.fill_ts_us);
        if (anchor > 0 && fill_us > anchor + kFillSealMarginUs) {
            post.push_back(std::move(tr));
        } else {
            pre.push_back(std::move(tr));
        }
    }
    // pre fills 按 venue 时间序发射（秒级同刻保到达序）。
    std::stable_sort(pre.begin(), pre.end(),
                     [](const XtTradeRecord& a, const XtTradeRecord& b) {
                         return a.fill_ts_us < b.fill_ts_us;
                     });
    // recovery 分区重放 = 延迟脱钩 venue 到达时刻 → kIneligible（pre/post 同）。
    if (callbacks_.on_trade) {
        for (auto& tr : pre) callbacks_.on_trade(tr, XtLatencyEligibility::kIneligible);
    }
    emit_staged_recovery_positions_(is_cold_start);
    rebuild_alive_inflight_and_emit(is_cold_start);
    if (callbacks_.on_trade) {
        for (auto& tr : post) callbacks_.on_trade(tr, XtLatencyEligibility::kIneligible);
    }
    reopen_recovery_window_gate_and_drain_();
    flush_recovery_pending_terminals_();
    if (!pre.empty() || !post.empty() || overflow > 0) {
        XtLogger::info_m("TD",
            "event=recovery_window_trades_flush pre={} post={} dropped_overflow={}",
            pre.size(), post.size(), overflow);
    }
}

// ── Recovery 终态补发 ─────────────────────

void TraderCTP::flush_recovery_pending_terminals_() {
    std::unordered_map<uint64_t, RecoveryPendingTerminal> pending;
    {
        std::lock_guard<std::mutex> lk(recovery_term_mu_);
        pending.swap(recovery_pending_terminals_);
    }
    if (pending.empty()) return;
    for (auto& [lid, entry] : pending) {
        // 在册归属已在缓冲时判定；这里直接补发（含 cum-filled completion 暂存项）。
        // eligibility 透传构造时值（写点 :367 kEligible / :491 kIneligible）。
        XtLogger::warn_m("TD",
                         "event=td_recovery_terminal_flush local_id={} status={}",
                         lid, static_cast<int>(entry.upd.status));
        if (callbacks_.on_order_update)
            callbacks_.on_order_update(entry.upd, entry.elig);
    }
}
