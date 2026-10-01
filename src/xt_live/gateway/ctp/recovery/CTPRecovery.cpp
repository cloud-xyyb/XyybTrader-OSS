#include "ctp/recovery/CTPRecovery.h"

#include <cstdio>

#include "recording/XtLiveRecorder.h"
#include "xt_util/time/XtSteadyClock.h"

namespace {

double clamp_non_negative(double v) { return v > 0.0 ? v : 0.0; }

bool is_pos_today(char d) {
    // 部分 CTP 前置返回原始字符 '1'/'2'，与 SDK 常量同时兼容。
    return d == THOST_FTDC_PSD_Today || d == '1';
}

bool is_pos_history(char d) {
    return d == THOST_FTDC_PSD_History || d == '2';
}

double clamp_today(double today, double total) {
    if (today <= 0.0 || total <= 0.0) return 0.0;
    return today < total ? today : total;
}

void merge_side_qty_avg(double row_qty, double row_avg,
                        double& side_qty, double& side_avg) {
    if (row_qty <= 0.0) return;
    const double total = side_qty + row_qty;
    if (total > 0.0)
        side_avg = (side_avg * side_qty + row_avg * row_qty) / total;
    side_qty = total;
}

}  // namespace

CtpRecovery::CtpRecovery(HostPorts ports)
    : publish_positions_(std::move(ports.publish_positions)),
      start_position_query_(std::move(ports.start_position_query)),
      start_account_query_(std::move(ports.start_account_query)),
      start_order_query_(std::move(ports.start_order_query)),
      start_trade_query_(std::move(ports.start_trade_query)),
      publish_ready_(std::move(ports.publish_ready)) {}

void CtpRecovery::set_recovery_enabled(bool enabled) {
    std::lock_guard<std::mutex> guard(mu_);
    recovery_enabled_ = enabled;
    recovering_.store(enabled && !recovery_done_, std::memory_order_release);
}

bool CtpRecovery::recovery_enabled() const {
    std::lock_guard<std::mutex> guard(mu_);
    return recovery_enabled_;
}

bool CtpRecovery::is_recovering() const {
    // risk 热路径无锁读取；状态机写入时使用 release-store。
    return recovering_.load(std::memory_order_acquire);
}

void CtpRecovery::reset() {
    std::lock_guard<std::mutex> guard(mu_);
    recovery_done_       = false;
    recovered_positions_ = false;
    recovered_account_   = false;
    recovered_orders_    = false;
    recovered_trades_    = false;
    ready_emitted_       = false;
    // 递增世代，使正在锁外执行的旧 ready 发布失效。
    ++reset_gen_;
    pos_cache_.clear();
    pos_bucket_cache_.clear();
    pos_query_owner_req_ = 0;  // 新 session 清持仓查询归属，旧轮回包一律判过期
    recovering_.store(recovery_enabled_, std::memory_order_release);
}

void CtpRecovery::on_login() {
    QueryStarter start_query;
    bool         enabled;
    bool         account_done;
    {
        std::lock_guard<std::mutex> guard(mu_);
        enabled      = recovery_enabled_;
        account_done = recovered_account_;
        // 恢复模式从持仓开始；非恢复模式只需要资金 gate。
        start_query  = enabled ? start_position_query_ : start_account_query_;
    }
    if (enabled) {
        if (start_query) start_query();
        return;
    }
    // 非恢复模式下重连可能已有资金快照，此时直接 ready。
    if (!account_done && start_query) start_query();
    else                             emit_ready();
}

void CtpRecovery::begin_position_query(int request_id) {
    // 新一轮持仓查询独占聚合缓存：绑定 request_id 归属 + 清残留半截行。
    // 上一轮超时重发前的半截 row 在此清掉，配合 on_position 的归属判定，
    // 杜绝新旧两轮 row 叠加造成的持仓翻倍/混轮。send 端在 ReqQry* 之前调用
    // （bind-before-send），令回包早于绑定的窄窗不存在。
    std::lock_guard<std::mutex> guard(mu_);
    pos_query_owner_req_ = request_id;
    pos_cache_.clear();
    pos_bucket_cache_.clear();
}

bool CtpRecovery::position_query_owns(int request_id) const {
    std::lock_guard<std::mutex> guard(mu_);
    // owner==0 = 无绑定（含单测直调）→ 全收；request_id<=0 = 柜台不回显 nRequestID，
    // 无法比对 → fail-open 归属（栅栏不得成为可用性单点）；否则仅当前轮归属。
    return pos_query_owner_req_ == 0 || request_id <= 0 ||
           request_id == pos_query_owner_req_;
}

void CtpRecovery::note_stale_position_frame() {
    stale_position_frame_dropped_.fetch_add(1, std::memory_order_relaxed);
}

uint64_t CtpRecovery::stale_position_frame_dropped() const {
    return stale_position_frame_dropped_.load(std::memory_order_relaxed);
}

uint64_t CtpRecovery::stale_position_frame_unfenceable() const {
    return stale_position_frame_unfenceable_.load(std::memory_order_relaxed);
}

bool CtpRecovery::on_position(const CThostFtdcInvestorPositionField* pPosition,
                              bool bIsLast, int request_id) {
    PositionsPublisher      publish_positions;
    QueryStarter            start_account_query;
    std::vector<PositionSnapshot> out;
    {
        std::lock_guard<std::mutex> guard(mu_);
        if (!recovery_enabled_) return true;  // 非恢复模式：聚合不参与，语义视为已处理
        // 归属栅栏与 merge 同锁原子判定（无 TOCTOU）。fail-open：request_id<=0
        // （柜台不回显 nRequestID / 直调默认参）无法比对 → 接受 + unfenceable 计数，
        // 栅栏不得成为可用性单点。仅双方 id 均有效（owner!=0 且 request_id>0）才比对：
        // 不匹配 = 过期轮次回包（超时重发后旧轮迟到）→ 丢弃，不进 accum / 不 emit /
        // 不动当前在途。
        if (pos_query_owner_req_ != 0) {
            if (request_id <= 0) {
                stale_position_frame_unfenceable_.fetch_add(1, std::memory_order_relaxed);
            } else if (request_id != pos_query_owner_req_) {
                stale_position_frame_dropped_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
        }
        if (pPosition && pPosition->InstrumentID[0])
            merge_position_row(pPosition);
        if (!bIsLast) return true;
        recovered_positions_ = true;
        out                 = collect_snapshots();
        pos_query_owner_req_ = 0;  // 本轮完成，释放归属；下一轮 send 重新绑定
        publish_positions   = publish_positions_;
        start_account_query = start_account_query_;
    }
    // 锁外按顺序推进：先发布持仓，再查询资金。
    if (publish_positions)   publish_positions(out);
    if (start_account_query) start_account_query();
    return true;
}

void CtpRecovery::on_account_done(bool bIsLast) {
    if (!bIsLast) return;
    QueryStarter start_order_query;
    bool         do_ready = false;
    {
        std::lock_guard<std::mutex> guard(mu_);
        recovered_account_ = true;
        if (recovery_enabled_) {
            if (recovered_positions_ && !recovered_orders_)
                start_order_query = start_order_query_;
        } else {
            recovery_done_ = true;
            do_ready = true;
            recovering_.store(false, std::memory_order_release);
        }
    }
    // 锁外推进下一阶段，避免查询入口反入时持有 recovery 锁。
    if (start_order_query) { start_order_query(); return; }
    if (do_ready) emit_ready();
}

void CtpRecovery::on_order_done(bool bIsLast) {
    if (!bIsLast) return;
    QueryStarter start_trade_query;
    bool         done = false;
    {
        std::lock_guard<std::mutex> guard(mu_);
        recovered_orders_ = true;
        if (recovery_enabled_) {
            if (!recovered_trades_)
                start_trade_query = start_trade_query_;
            else {
                // CTP 不保证 orders/trades 回包顺序，兼容 trades 先完成。
                recovery_done_ = true;
                done = true;
                recovering_.store(false, std::memory_order_release);
            }
        }
    }
    // 锁外推进成交查询；若成交阶段已先完成，则收尾 ready。
    if (start_trade_query) { start_trade_query(); return; }
    if (done) emit_ready();
}

void CtpRecovery::on_trade_done(bool bIsLast) {
    if (!bIsLast) return;
    bool done = false;
    {
        std::lock_guard<std::mutex> guard(mu_);
        recovered_trades_ = true;
        // 若 orders 尚未完成，由 on_order_done 收尾。
        if (recovery_enabled_ && recovered_orders_) {
            recovery_done_ = true;
            done = true;
            recovering_.store(false, std::memory_order_release);
        }
    }
    // order/trade 回包顺序不稳定；只有两个阶段都完成后才 emit ready。
    if (done) emit_ready();
}

// ── 持仓聚合 ──────────────────────────────────

void CtpRecovery::accumulate_side(SideBucketAgg& side,
                                   double qty, double today_field,
                                   double yd, char pos_date) {
    side.total += qty;
    if (today_field > 0.0) {
        side.today_by_field += today_field;
        side.saw_today_field = true;
    }
    if (yd > 0.0) {
        side.yd_sum      += yd;
        side.saw_yd_field = true;
    }
    if (is_pos_today(pos_date)) {
        side.today_by_date += qty;
        side.saw_pos_date   = true;
    } else if (is_pos_history(pos_date)) {
        // history 行不贡献今量，但证明 PositionDate 口径可用。
        side.saw_pos_date = true;
    }
}

double CtpRecovery::resolve_today_qty(const SideBucketAgg& s) {
    // 不同 CTP 前置填充策略不同，按 TodayPosition > 总量减 YdPosition >
    // PositionDate 口径多级回退。
    if (s.total <= 0.0) return 0.0;
    if (s.saw_today_field) return clamp_today(s.today_by_field, s.total);
    if (s.saw_yd_field)    return s.total - clamp_today(s.yd_sum, s.total);
    if (s.saw_pos_date)    return clamp_today(s.today_by_date, s.total);
    return 0.0;
}

void CtpRecovery::merge_position_row(const CThostFtdcInvestorPositionField* row) {
    // CTP 会下发 Position=0 占位行；过滤后避免恢复不存在的合约。
    const double qty = clamp_non_negative(static_cast<double>(row->Position));
    if (qty <= 0.0) return;

    const std::string code = row->InstrumentID;
    auto& snap = pos_cache_[code];
    auto& agg  = pos_bucket_cache_[code];
    snap.code  = code;

    const double today_field = clamp_non_negative(static_cast<double>(row->TodayPosition));
    const double yd          = clamp_non_negative(static_cast<double>(row->YdPosition));
    const double avg         = row->PositionCost > 0.0
                               ? row->PositionCost / qty : 0.0;
    const char   pos_date    = row->PositionDate;

    // broker-side 冻结量不分今/昨；同一 code 多行需要累加。
    const double long_frozen  = clamp_non_negative(static_cast<double>(row->LongFrozen));
    const double short_frozen = clamp_non_negative(static_cast<double>(row->ShortFrozen));

    if (row->PosiDirection == THOST_FTDC_PD_Long) {
        merge_side_qty_avg(qty, avg, snap.long_qty, snap.long_avg);
        accumulate_side(agg.long_side, qty, today_field, yd, pos_date);
        snap.long_frozen  += long_frozen;
        snap.short_frozen += short_frozen;
    } else if (row->PosiDirection == THOST_FTDC_PD_Short) {
        merge_side_qty_avg(qty, avg, snap.short_qty, snap.short_avg);
        accumulate_side(agg.short_side, qty, today_field, yd, pos_date);
        snap.long_frozen  += long_frozen;
        snap.short_frozen += short_frozen;
    }
}

std::vector<CtpRecovery::PositionSnapshot> CtpRecovery::collect_snapshots() {
    std::vector<PositionSnapshot> out;
    out.reserve(pos_cache_.size());
    for (auto& [code, snap] : pos_cache_) {
        const auto it = pos_bucket_cache_.find(code);
        if (it != pos_bucket_cache_.end()) {
            const PositionBucketAgg& bkt = it->second;
            snap.long_today        = resolve_today_qty(bkt.long_side);
            snap.short_today       = resolve_today_qty(bkt.short_side);
        }
        out.push_back(snap);
    }
    pos_cache_.clear();
    pos_bucket_cache_.clear();
    return out;
}

void CtpRecovery::emit_ready() {
    ReadyPublisher publish_ready;
    uint32_t gen_at_emit;
    {
        std::lock_guard<std::mutex> guard(mu_);
        if (ready_emitted_) return;
        ready_emitted_ = true;
        gen_at_emit = reset_gen_;
        publish_ready = publish_ready_;
    }
    if (auto* rec = XtLiveRecorder::global()) {
        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();
        rec->emit_conn_state(ts_local_ns, "td", "ready", "");
    }
    // ready 发布锁外执行前复查 reset generation，防止旧 session ready 穿透。
    {
        std::lock_guard<std::mutex> guard(mu_);
        if (reset_gen_ != gen_at_emit) return;
    }
    if (publish_ready) publish_ready();
}
