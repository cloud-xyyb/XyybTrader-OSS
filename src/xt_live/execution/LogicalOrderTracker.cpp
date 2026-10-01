#include "LogicalOrderTracker.h"

#include <cassert>
#include <cstring>

#include "xt_util/time/XtSteadyClock.h"

// ---------------------------------------------------------------------------
// Entry 辅助
// ---------------------------------------------------------------------------

bool LogicalOrderTracker::Entry::all_done() const noexcept {
    for (uint8_t i = 0; i < slice_count; ++i) {
        if (!LogicalOrderTracker::is_terminal(slice_status[i])) return false;
    }
    return slice_count > 0;
}

int64_t LogicalOrderTracker::Entry::total_filled_fp() const noexcept {
    int64_t sum = 0;
    for (uint8_t i = 0; i < slice_count; ++i) {
        sum += slice_filled_fp[i];
    }
    return sum;
}

int64_t LogicalOrderTracker::Entry::working_leaves_fp() const noexcept {
    int64_t sum = 0;
    for (uint8_t i = 0; i < slice_count; ++i) {
        if (LogicalOrderTracker::is_terminal(slice_status[i])) continue;
        // 逐切片 >0 clamp：防御性超额成交（slice_filled > slice_qty）的负残量
        // 不冲抵其他活切片（旧 total−filled 全局口径会净掉）。
        const int64_t leaves = slice_qty_fp[i] - slice_filled_fp[i];
        if (leaves > 0) sum += leaves;
    }
    return sum;
}

// ---------------------------------------------------------------------------
// LogicalOrderTracker 实现
// ---------------------------------------------------------------------------

bool LogicalOrderTracker::is_terminal(XtOrderStatus s) noexcept {
    return s == XtOrderStatus::Filled
        || s == XtOrderStatus::Canceled
        || s == XtOrderStatus::Rejected;
}

// 从 N 个物理切片状态合成一个逻辑订单状态——策略只看到逻辑订单。
XtOrderStatus LogicalOrderTracker::aggregate_status(const Entry& e,
                                                     int64_t total_filled_fp) noexcept {
    // (1) 扫描切片终态
    bool all_terminal   = true;
    bool any_rejected   = false;
    for (uint8_t i = 0; i < e.slice_count; ++i) {
        if (!is_terminal(e.slice_status[i])) {
            all_terminal = false;
        }
        if (e.slice_status[i] == XtOrderStatus::Rejected) {
            any_rejected = true;
        }
    }

    // (2) 全终态路径
    if (all_terminal) {
        // >= 处理超额防御（理论上不应超额，但 TD 回报不可完全信任）
        if (total_filled_fp >= e.total_qty_fp) {
            return XtOrderStatus::Filled;
        }
        if (total_filled_fp == 0) {
            // 零成交时 Rejected 优先上报：比 Canceled 更保守，策略会重新评估
            return any_rejected ? XtOrderStatus::Rejected : XtOrderStatus::Canceled;
        }
        // 部分成交后全切片终态（IOC 余量过期 / binance reduceOnly amend-down /
        // 部分切片被拒）：FIX 语义 = 携带累计成交的 Canceled 终态。有成交时
        // 不报 Rejected（策略会误判全拒重下）；返回非终态会让策略永远收不到
        // 逻辑单终态（book 物理切片已消、策略侧挂死）。
        // 此处 Canceled = 「带残量的终态」，不代表操作员/venue 主动撤单动作。
        return XtOrderStatus::Canceled;
    }

    // (3) 活跃路径
    if (total_filled_fp > 0) {
        return XtOrderStatus::PartiallyFilled;
    }
    // Accepted 是 Submitted 的子状态，统一归 Submitted
    return XtOrderStatus::Submitted;
}

uint64_t LogicalOrderTracker::register_multi_slice(const uint64_t* phys_ids,
                                                    const int64_t*  slice_qty_fps,
                                                    uint8_t         slice_count,
                                                    int64_t         total_qty_fp,
                                                    bool            is_buy,
                                                    int64_t         price_fp,
                                                    XtOffset        close_offset,
                                                    uint64_t        symbol_id) {
    assert(slice_count >= 2 && "单切片不需要注册，直接透传");
    assert(slice_count <= kMaxSlices);

    std::lock_guard<std::mutex> lock(mu_);

    // (1) 分配逻辑订单 ID
    const uint64_t clord_id = next_clord_id_++;

    // (2) 构建 Entry
    Entry e;
    e.clord_id     = clord_id;
    e.total_qty_fp = total_qty_fp;
    e.slice_count  = slice_count;
    e.is_buy       = is_buy;
    e.price_fp     = price_fp;
    e.close_offset = close_offset;
    e.symbol_id    = symbol_id;

    e.register_ms = XtSteadyClock::steady_now_ms();

    for (uint8_t i = 0; i < slice_count; ++i) {
        e.phys_ids[i]        = phys_ids[i];
        e.slice_qty_fp[i]    = slice_qty_fps[i];
        e.slice_filled_fp[i] = 0;
        e.slice_status[i]    = XtOrderStatus::Submitted;
    }

    // (3) 双向映射：entries_ 用于正向查询，phys_to_clord_ 用于回报反查
    entries_.emplace(clord_id, e);
    for (uint8_t i = 0; i < slice_count; ++i) {
        phys_to_clord_.emplace(phys_ids[i], clord_id);
    }

    return clord_id;
}

void LogicalOrderTracker::translate(const XtOrderExecUpdate& phys_upd,
                                    XtOrderExecUpdate&       out) {
    std::lock_guard<std::mutex> lock(mu_);
    // 快路径：单切片不在映射表中，直接透传（~5ns hash miss）
    auto clord_it = phys_to_clord_.find(phys_upd.id);
    if (clord_it == phys_to_clord_.end()) {
        out = phys_upd;
        return;
    }

    const uint64_t clord_id = clord_it->second;
    auto entry_it = entries_.find(clord_id);
    if (entry_it == entries_.end()) {
        // 防御：映射存在但 Entry 已被 cleanup_done 清理（理论不应发生）
        out = phys_upd;
        return;
    }
    Entry& e = entry_it->second;

    // 定位切片。slice_count 作为"未找到"哨兵。
    uint8_t slice_idx = e.slice_count;
    for (uint8_t i = 0; i < e.slice_count; ++i) {
        if (e.phys_ids[i] == phys_upd.id) {
            slice_idx = i;
            break;
        }
    }
    if (slice_idx == e.slice_count) {
        // 映射与 Entry 不一致，防御性透传
        out = phys_upd;
        return;
    }

    // filled_fp 语义是累计成交量（snapshot），非增量
    e.slice_filled_fp[slice_idx] = phys_upd.filled_fp;
    e.slice_status[slice_idx]    = phys_upd.status;

    // 合成逻辑视图：先拷贝物理回报（保留时间戳等），再覆写逻辑层字段
    const int64_t agg_filled = e.total_filled_fp();
    out = phys_upd;

    out.id        = clord_id;
    out.filled_fp = agg_filled;
    // FIX LeavesQty 语义：leaves 只计活切片的未成交量。被撤/被拒切片的残量
    // 不再 working（qty − filled − leaves = 被撤量，三者允许不相等）。
    out.leaves_fp = e.working_leaves_fp();
    out.qty_fp    = e.total_qty_fp;
    out.status    = aggregate_status(e, agg_filled);
    // last_fill_fp 保留物理回报值（已通过 out = phys_upd 透传），
    // 策略用它算本次成交 PnL/滑点，不做跨切片聚合。

    // 覆写为策略原始意图：物理切片的 close_offset 是路由后方向（如 CloseToday），
    // 但策略下单时传的是 Close。
    out.is_buy       = e.is_buy;
    out.price_fp     = e.price_fp;
    out.close_offset = e.close_offset;
    out.symbol_id    = e.symbol_id != 0 ? e.symbol_id : phys_upd.symbol_id;
}

uint8_t LogicalOrderTracker::expand_to_phys(uint64_t  clord_id,
                                             uint64_t* phys_out,
                                             uint8_t   max_count) const {
    std::lock_guard<std::mutex> lock(mu_);
    // 返回 0 = 单切片（未注册），调用方直接用 clord_id 作为物理 ID
    auto it = entries_.find(clord_id);
    if (it == entries_.end()) return 0;

    const Entry& e = it->second;
    const uint8_t n = (e.slice_count < max_count) ? e.slice_count : max_count;
    for (uint8_t i = 0; i < n; ++i) {
        phys_out[i] = e.phys_ids[i];
    }
    return n;
}

uint64_t LogicalOrderTracker::lookup_done_clord(uint64_t phys_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto map_it = phys_to_clord_.find(phys_id);
    if (map_it == phys_to_clord_.end()) return 0;
    const auto entry_it = entries_.find(map_it->second);
    if (entry_it == entries_.end()) return 0;
    return entry_it->second.all_done() ? entry_it->second.clord_id : 0;
}

// 全终态条目两段回收：首见 stamp，超 grace 才删——尾随 fill（终态 rtn 之后
// 到达的同笔 trade）在窗口内仍能 lookup_clord 命中。
void LogicalOrderTracker::cleanup_done() {
    cleanup_done_at(XtSteadyClock::steady_now_ms());
}

size_t LogicalOrderTracker::cleanup_done_at(uint64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    size_t reaped = 0;
    auto it = entries_.begin();
    while (it != entries_.end()) {
        Entry& e = it->second;
        if (!e.all_done()) {
            // 终态后乱序非终态更新（venue 重放）会让条目复活：清掉旧 stamp，
            // 再次全终态时重新计 grace（否则旧 stamp 已超窗 → 立即回收，
            // 第二次终态化的尾随 fill 又会反查落空）。
            e.all_done_since_ms = 0;
            ++it;
            continue;
        }
        if (e.all_done_since_ms == 0) {
            e.all_done_since_ms = now_ms;
            ++it;
            continue;
        }
        // now < stamp 仅注入时钟可构造（steady clock 单调）；防无符号回绕误判超窗。
        if (now_ms < e.all_done_since_ms ||
            now_ms - e.all_done_since_ms < kDoneEntryGraceMs) {
            ++it;
            continue;
        }
        for (uint8_t i = 0; i < e.slice_count; ++i) {
            phys_to_clord_.erase(e.phys_ids[i]);
        }
        it = entries_.erase(it);
        ++reaped;
    }
    return reaped;
}

size_t LogicalOrderTracker::translate_open_orders(XtOrderExecUpdate* rows, size_t n) const {
    if (!rows || n == 0) return 0;
    std::lock_guard<std::mutex> lock(mu_);
    if (phys_to_clord_.empty()) return n;  // 无多切片在途，零开销路径

    // 同一 clord 只合成一行；缓冲区内 distinct 多切片单极少，线性查重足够。
    constexpr size_t kMaxDistinctClords = 64;
    uint64_t seen[kMaxDistinctClords];
    size_t   seen_count = 0;
    size_t   w = 0;
    for (size_t i = 0; i < n; ++i) {
        const auto map_it = phys_to_clord_.find(rows[i].id);
        if (map_it == phys_to_clord_.end()) {
            rows[w++] = rows[i];
            continue;
        }
        const uint64_t clord    = map_it->second;
        const auto     entry_it = entries_.find(clord);
        if (entry_it == entries_.end()) {  // 防御：映射悬空，按未注册透传
            rows[w++] = rows[i];
            continue;
        }
        bool dup = false;
        for (size_t k = 0; k < seen_count; ++k) {
            if (seen[k] == clord) {
                dup = true;
                break;
            }
        }
        if (dup) continue;  // 同 clord 的其余切片已并入首行
        if (seen_count >= kMaxDistinctClords) {  // 容量耗尽：保守透传物理行
            rows[w++] = rows[i];
            continue;
        }
        seen[seen_count++] = clord;

        const Entry&  e          = entry_it->second;
        const int64_t agg_filled = e.total_filled_fp();
        XtOrderExecUpdate out    = rows[i];  // 保留 book 行基底（sid_id 等）
        out.id           = clord;
        out.qty_fp       = e.total_qty_fp;
        out.filled_fp    = agg_filled;
        out.leaves_fp    = e.working_leaves_fp();
        out.status       = aggregate_status(e, agg_filled);
        out.is_buy       = e.is_buy;
        out.price_fp     = e.price_fp;
        out.close_offset = e.close_offset;
        if (e.symbol_id != 0) out.symbol_id = e.symbol_id;
        rows[w++] = out;
    }
    return w;
}

// 清理超时未到达终态的逻辑订单，防止网络分区等异常场景下映射表无限增长。
size_t LogicalOrderTracker::cleanup_stale(uint64_t now_ms, uint64_t stale_threshold_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    size_t removed = 0;
    auto it = entries_.begin();
    while (it != entries_.end()) {
        const Entry& e = it->second;
        if (!e.all_done() && e.register_ms > 0 &&
            now_ms > e.register_ms &&
            (now_ms - e.register_ms) > stale_threshold_ms) {
            for (uint8_t i = 0; i < e.slice_count; ++i) {
                phys_to_clord_.erase(e.phys_ids[i]);
            }
            it = entries_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    return removed;
}
