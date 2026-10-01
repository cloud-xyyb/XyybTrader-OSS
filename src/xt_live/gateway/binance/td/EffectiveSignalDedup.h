#pragma once

#include <deque>
#include <utility>
#include <unordered_map>
#include <vector>

#include "xt_core/order/XtOrderTypes.h"

namespace binance_u {

/// @brief `EffectiveSignalDedup` 针对单个 `local_id` 保存的上游通知判定缓存。
///
/// 记录 UDS/ACK 到达情况，以及上一次已向上游发出的 `status` 和 `filled_fp`。
/// `note_signal()` 仅在首次到达、status rank 升级或累计成交量增加时放行。
/// 终态发出后短期保留 entry，用于过滤迟到的同义终态重复信号。
struct EffectiveSignalState {
    bool          uds_arrived      = false;  ///< UDS push 已到达；只作为来源标记，不单独触发上游回调。
    bool          ack_arrived      = false;  ///< ACK/reconcile update 已到达；只作为来源标记，不要求双路到齐。
    bool          emitted          = false;  ///< 已有至少一个信号向上游发出；false 表示下一信号必发出。
    bool          terminal_retained = false; ///< 终态已发出后仍在 TTL/cap 内保留，用于拦截迟到重复。
    XtOrderStatus last_status      = XtOrderStatus::Submitted; ///< 最近一次已发出的 status，用于比较 rank 升级。
    int64_t       last_filled_fp   = 0;      ///< 最近一次已发出的累计成交量；更大值表示 fill 进展。
    uint64_t      terminal_seen_us = 0;      ///< 终态保留时间戳；0 表示只受 cap 清理，不按 TTL 过期。
};

/// @brief 同一笔订单的 UDS 与 ACK 两路信号去重：只把带新信息的通知放行给上游。
///
/// 首次到达、状态升级或成交量增加才放行，其余当重复丢弃。
/// EffectiveSignalDedup 不内置锁；调用方持外部 mutex 序列化访问，
/// 以便显式控制它与 BinanceOrderLedger 之间的锁顺序。
class EffectiveSignalDedup {
public:
    /// @brief 订单状态信号来源。
    enum class Source { kUds, kAck };

    /// @brief 记录新信号到达，返回是否需要向上游发出。
    /// @param local_id  订单本地 id
    /// @param src       信号来源（UDS or ACK），用于 state.<src>_arrived 标记
    /// @param status    最新 status
    /// @param filled_fp 累计成交量定点（XtOrderExecUpdate.filled_fp，不是 last_fill_fp）
    /// @return true 表示调用方应调 cb.on_order_update；false 表示已被去重
    /// @note 首次到达 || rank 升级 || filled_fp 升级 → 返回 true
    bool note_signal(uint64_t      local_id,
                     Source        src,
                     XtOrderStatus status,
                     int64_t       filled_fp,
                     uint64_t      now_us = 0) {
        prune_terminal(now_us);
        auto& state = state_[local_id];
        if (src == Source::kUds) state.uds_arrived = true;
        else                     state.ack_arrived = true;

        const bool is_first   = !state.emitted;
        const bool rank_up    = order_status_rank(status) >
                                order_status_rank(state.last_status);
        const bool filled_up  = filled_fp > state.last_filled_fp;
        const bool should_emit = is_first || rank_up || filled_up;

        if (should_emit) {
            state.emitted        = true;
            state.last_status    = status;
            state.last_filled_fp = filled_fp;
            if (order_status_is_final(status)) {
                retain_terminal(local_id, state, now_us);
            }
        }
        return should_emit;
    }

    /// @brief 清掉指定 local_id 的状态（显式生命周期收口 / 测试用）。
    /// @note 终态发出后默认保留短 TTL，用来吞掉另一信号源的迟到重复。
    void erase(uint64_t local_id) {
        auto it = state_.find(local_id);
        if (it == state_.end()) return;
        if (it->second.terminal_retained && terminal_count_ > 0) --terminal_count_;
        state_.erase(it);
    }

    /// @brief 批量清除指定 local_id 的 dedup state（reverse-sync 路径用）。
    ///
    /// warm reconnect 反向灌 in-flight order 前调用，清掉本地残留的
    /// `last_filled_fp` / `last_status`，避免真实 UDS update 被旧状态吞掉。
    /// cold-start 路径下 dedup 表本为空，不需要调用。
    ///
    /// @param ids  要清的 local_id 列表；空列表是合法输入。
    ///
    /// @note 调用方持外锁；重复清理同一 id 是幂等操作。
    /// @note 终态已发出（terminal_retained）的 entry 保留不清：擦掉会让另一信号源
    ///       迟到的同义终态在 rebuild 窗口二次放行（double_terminal）；保留不挡
    ///       真实进展 —— rank/filled 升级仍按 note_signal 规则放行。
    void erase_for_local_ids(const std::vector<uint64_t>& ids) {
        for (uint64_t id : ids) {
            auto it = state_.find(id);
            if (it != state_.end() && it->second.terminal_retained) continue;
            erase(id);
        }
    }

    /// @brief 测试 / 调试入口。
    bool has(uint64_t local_id) const {
        return state_.count(local_id) > 0;
    }

    /// @brief 总 entry 数（health 监控 / 内存累积告警用）。
    std::size_t size() const noexcept {
        return state_.size();
    }

private:
    static constexpr uint64_t kTerminalRetentionUs =
        5ULL * 60ULL * 1000ULL * 1000ULL; ///< 终态重复拦截 TTL。
    static constexpr std::size_t kTerminalRetentionCap = 8192; ///< 终态保留 entry 数量上限。

    void retain_terminal(uint64_t local_id, EffectiveSignalState& state, uint64_t now_us) {
        if (!state.terminal_retained) ++terminal_count_;
        state.terminal_retained = true;
        state.terminal_seen_us = now_us;
        terminal_fifo_.emplace_back(local_id, now_us);
        prune_terminal(now_us);
    }

    void prune_terminal(uint64_t now_us) {
        while (!terminal_fifo_.empty()) {
            const auto [local_id, seen_us] = terminal_fifo_.front();
            auto it = state_.find(local_id);
            if (it == state_.end() ||
                !it->second.terminal_retained ||
                it->second.terminal_seen_us != seen_us) {
                terminal_fifo_.pop_front();
                continue;
            }
            const bool expired = now_us != 0 && seen_us != 0 &&
                                 now_us >= seen_us &&
                                 now_us - seen_us >= kTerminalRetentionUs;
            const bool over_cap = terminal_count_ > kTerminalRetentionCap;
            if (!expired && !over_cap) break;
            terminal_fifo_.pop_front();
            if (terminal_count_ > 0) --terminal_count_;
            state_.erase(it);
        }
    }

    std::unordered_map<uint64_t, EffectiveSignalState> state_; ///< local_id 到去重状态的主表。
    std::deque<std::pair<uint64_t, uint64_t>> terminal_fifo_;  ///< 终态保留 FIFO，用于 TTL/cap 清理。
    std::size_t terminal_count_ = 0; ///< 当前仍处于终态保留窗口的 entry 数。
};

}  // namespace binance_u
