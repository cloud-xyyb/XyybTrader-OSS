#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "BinanceOrderBook.h"
#include "BinanceSnapshotFetcher.h"

namespace binance_u {

/// @brief Binance L2 order book 的同步状态机
///
/// BinanceDepthSync 负责把 Binance 的 REST depth snapshot 和 WS depth diff 拼成可信的本地 order book。
///
/// per-symbol 隔离：每 symbol 各持一实例，一个 symbol 的 gap/overflow 只 reset 自己。
class BinanceDepthSync {
public:
    enum class State {
        NeedSnapshot,
        AwaitingSnapshot,
        Live,
    };

    enum class DiffResult {
        Applied,
        Buffered,
        NeedResnapshot,
    };

    enum class SnapshotResult {
        Ready,
        NeedResnapshot,
    };

    enum class ResnapshotCause {
        None,
        SequenceGap,
        BufferOverflow,
    };

    /// @brief 一条 futures `@depth` diff 的解析结果（snapshot 未就绪时排队，drain 时回放）。
    struct Diff {
        uint64_t U  = 0;
        uint64_t u  = 0;
        uint64_t pu = 0;
        std::vector<std::pair<double, double>> bids;
        std::vector<std::pair<double, double>> asks;
    };

    static constexpr size_t kMaxDiffBuffer = 5000;

    /// @brief on_snapshot 的 drain 统计（调用方日志用）。
    struct DrainStats {
        size_t stale_dropped = 0;
        size_t applied       = 0;
        size_t gapped        = 0;
        size_t bid_levels    = 0;
        size_t ask_levels    = 0;
    };

    State state() const noexcept {
        if (awaiting_snapshot_) return State::AwaitingSnapshot;
        return book_.is_ready() ? State::Live : State::NeedSnapshot;
    }
    bool is_live() const noexcept { return book_.is_ready(); }
    bool awaiting_snapshot() const noexcept { return awaiting_snapshot_; }
    size_t buffered_diffs() const noexcept { return diff_buffer_.size(); }
    const BinanceOrderBook& book() const noexcept { return book_; }

    bool begin_await() noexcept {
        if (awaiting_snapshot_) return false;
        awaiting_snapshot_ = true;
        return true;
    }

    DiffResult on_diff(Diff d,
                       ResnapshotCause* cause = nullptr,
                       uint64_t* pre_reset_update_id = nullptr) {
        if (cause) *cause = ResnapshotCause::None;
        if (!book_.is_ready()) {
            if (diff_buffer_.size() >= kMaxDiffBuffer) {
                book_.reset();
                diff_buffer_.clear();
                if (cause) *cause = ResnapshotCause::BufferOverflow;
                return DiffResult::NeedResnapshot;
            }
            diff_buffer_.push_back(std::move(d));
            return DiffResult::Buffered;
        }
        if (!book_.apply_diff(d.U, d.u, d.pu, d.bids, d.asks)) {
            if (pre_reset_update_id) *pre_reset_update_id = book_.last_update_id();
            book_.reset();
            diff_buffer_.clear();
            diff_buffer_.push_back(std::move(d));
            if (cause) *cause = ResnapshotCause::SequenceGap;
            return DiffResult::NeedResnapshot;
        }
        return DiffResult::Applied;
    }

    SnapshotResult on_snapshot(const DepthSnapshot& snap, DrainStats* out_stats = nullptr) {
        awaiting_snapshot_ = false;
        book_.apply_snapshot(snap.last_update_id, snap.bids, snap.asks);
        DrainStats stats;
        stats.bid_levels = book_.bid_levels();
        stats.ask_levels = book_.ask_levels();
        for (auto& d : diff_buffer_) {
            if (d.u < book_.last_update_id()) {
                ++stats.stale_dropped;
                continue;
            }
            if (!book_.apply_diff(d.U, d.u, d.pu, d.bids, d.asks)) {
                ++stats.gapped;
                book_.reset();
                diff_buffer_.clear();
                if (out_stats) *out_stats = stats;
                return SnapshotResult::NeedResnapshot;
            }
            ++stats.applied;
        }
        diff_buffer_.clear();
        if (out_stats) *out_stats = stats;
        return SnapshotResult::Ready;
    }

    void reset_for_reconnect() {
        book_.reset();
        diff_buffer_.clear();
        awaiting_snapshot_ = false;
    }

private:
    BinanceOrderBook book_;
    std::vector<Diff> diff_buffer_;
    bool awaiting_snapshot_ = false;
};

}
