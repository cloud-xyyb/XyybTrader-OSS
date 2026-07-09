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
    bool          uds_arrived      = false;
    bool          ack_arrived      = false;
    bool          emitted          = false;
    bool          terminal_retained = false;
    XtOrderStatus last_status      = XtOrderStatus::Submitted;
    int64_t       last_filled_fp   = 0;
    uint64_t      terminal_seen_us = 0;
};

/// @brief 同一笔订单的 UDS 与 ACK 两路信号去重：只把带新信息的通知放行给上游。
///
/// 首次到达、状态升级或成交量增加才放行，其余当重复丢弃。
/// EffectiveSignalDedup 不内置锁；调用方持外部 mutex 序列化访问，
/// 以便显式控制它与 BinanceOrderLedger 之间的锁顺序。
class EffectiveSignalDedup {
public:

    enum class Source { kUds, kAck };

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

    void erase(uint64_t local_id) {
        auto it = state_.find(local_id);
        if (it == state_.end()) return;
        if (it->second.terminal_retained && terminal_count_ > 0) --terminal_count_;
        state_.erase(it);
    }

    void erase_for_local_ids(const std::vector<uint64_t>& ids) {
        for (uint64_t id : ids) {
            auto it = state_.find(id);
            if (it != state_.end() && it->second.terminal_retained) continue;
            erase(id);
        }
    }

    bool has(uint64_t local_id) const {
        return state_.count(local_id) > 0;
    }

    std::size_t size() const noexcept {
        return state_.size();
    }

private:
    static constexpr uint64_t kTerminalRetentionUs =
        5ULL * 60ULL * 1000ULL * 1000ULL;
    static constexpr std::size_t kTerminalRetentionCap = 8192;

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

    std::unordered_map<uint64_t, EffectiveSignalState> state_;
    std::deque<std::pair<uint64_t, uint64_t>> terminal_fifo_;
    std::size_t terminal_count_ = 0;
};

}
