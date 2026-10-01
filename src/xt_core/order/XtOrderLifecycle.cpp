#include "XtOrderLifecycle.h"

#include <cassert>
#include <cstdlib>

#include "XtFixedPoint.h"

namespace order_lifecycle {

namespace {

// 进度只能向“更多成交/更少剩余”方向推进；venue race 或重复终态不能让 open 表倒退。
bool progress_regressed(const XtOrderExecUpdate& cur, const XtOrderExecUpdate& prev) {
    if (cur.filled_fp < prev.filled_fp) return true;
    return effective_remaining_fp(cur) > effective_remaining_fp(prev);
}

}  // namespace

int64_t effective_remaining_fp(const XtOrderExecUpdate& upd) {
    int64_t remain_fp = upd.leaves_fp;
    if (remain_fp <= 0) {
        remain_fp = upd.qty_fp - upd.filled_fp;
    }
    return xt_fp::clamp_nonneg(remain_fp);
}

bool is_final_update(const XtOrderExecUpdate& upd) {
    return order_status_is_final(upd.status) || effective_remaining_fp(upd) <= 0;
}

bool same_update_progress(const XtOrderExecUpdate& lhs, const XtOrderExecUpdate& rhs) {
    return lhs.status == rhs.status &&
           lhs.filled_fp == rhs.filled_fp &&
           lhs.last_fill_fp == rhs.last_fill_fp &&
           effective_remaining_fp(lhs) == effective_remaining_fp(rhs);
}

bool apply_open_order_update(std::unordered_map<uint64_t, XtOrderExecUpdate>& open_orders,
                             const XtOrderExecUpdate& upd) {
    assert(upd.id != 0);
    // id=0 会把所有无效回报折叠到同一 key，release 构建也必须 fail-fast。
    if (upd.id == 0) std::abort();
    if (is_final_update(upd)) {
        open_orders.erase(upd.id);
        return false;
    }
    open_orders[upd.id] = upd;
    return true;
}

bool XtOrderUpdateDeduper::should_emit(uint64_t order_id,
                                       const XtOrderExecUpdate& upd,
                                       StatusRankFn rank_fn) {
    assert(order_id != 0);
    // 去重器以 order_id 为主键；0 不是合法订单号，不能继续进入状态表。
    if (order_id == 0) std::abort();
    const bool final = is_final_update(upd);
    if (final_seen_.find(order_id) != final_seen_.end()) {
        return false;
    }

    auto it = last_update_.find(order_id);
    if (it == last_update_.end()) {
        if (final) {
            remember_final(order_id);
        } else {
            last_update_[order_id] = upd;
        }
        return true;
    }

    const XtOrderExecUpdate& prev = it->second;
    // venue-specific rank 可保留“终态强于非终态”等规则，防止乱序回报降低状态等级。
    if (rank_fn && rank_fn(upd.status) < rank_fn(prev.status)) {
        return false;
    }
    if (progress_regressed(upd, prev)) {
        return false;
    }
    if (same_update_progress(upd, prev)) {
        return false;
    }
    if (final) {
        last_update_.erase(it);
        remember_final(order_id);
    } else {
        it->second = upd;
    }
    return true;
}

void XtOrderUpdateDeduper::forget(uint64_t order_id) {
    assert(order_id != 0);
    if (order_id == 0) std::abort();
    last_update_.erase(order_id);
    // fifo 中的 stale 条目由 remember_final 淘汰时惰性跳过。
    final_seen_.erase(order_id);
}

bool XtOrderUpdateDeduper::last_update(uint64_t order_id, XtOrderExecUpdate& out) const {
    const auto it = last_update_.find(order_id);
    if (it == last_update_.end()) return false;
    out = it->second;
    return true;
}

void XtOrderUpdateDeduper::clear() {
    last_update_.clear();
    final_seen_.clear();
    final_seen_fifo_.clear();
}

size_t XtOrderUpdateDeduper::size() const {
    return last_update_.size() + final_seen_.size();
}

void XtOrderUpdateDeduper::remember_final(uint64_t order_id) {
    assert(order_id != 0);
    if (order_id == 0) std::abort();
    if (final_seen_.insert(order_id).second) {
        final_seen_fifo_.push_back(order_id);
    }
    // 0 表示无限窗口；用于需要完整终态去重的长生命周期测试或特殊运行模式。
    if (final_seen_cap_ == 0) return;
    // 淘汰时跳过已被 forget() 移除的 stale 条目。
    while (final_seen_.size() > final_seen_cap_) {
        const uint64_t old = final_seen_fifo_.front();
        final_seen_fifo_.pop_front();
        final_seen_.erase(old);  // stale 条目 erase 返回 0，无副作用
    }
}

}  // namespace order_lifecycle
