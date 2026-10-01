#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "BinanceOrderBook.h"
#include "BinanceSnapshotFetcher.h"  // DepthSnapshot

namespace binance_u {

/// @brief Binance L2 order book 的同步状态机
///
/// BinanceDepthSync 负责把 Binance 的 REST depth snapshot 和 WS depth diff 拼成可信的本地 order book。
///
/// per-symbol 隔离：每 symbol 各持一实例，一个 symbol 的 gap/overflow 只 reset 自己。
class BinanceDepthSync {
public:
    enum class State {
        NeedSnapshot,      ///< book 未就绪且未入队 snapshot。
        AwaitingSnapshot,  ///< 已入 snapshot 队列或 worker 正拉取（begin_await 置位）。
        Live,              ///< snapshot 已 apply，diff 直接进 book。
    };

    enum class DiffResult {
        Applied,         ///< diff 已进 book（Live 态正常路径）。
        Buffered,        ///< snapshot 未就绪，diff 已入 buffer 待 drain。
        NeedResnapshot,  ///< 本地状态不可信已 reset；调用方应 enqueue snapshot。
    };

    enum class SnapshotResult {
        Ready,           ///< snapshot apply + buffer drain 成功，book 进入 Live。
        NeedResnapshot,  ///< drain 中出现 gap（snapshot 与 buffer 不可拼接），已 reset。
    };

    /// NeedResnapshot 的成因（调用方日志分支用）。
    enum class ResnapshotCause {
        None,
        SequenceGap,     ///< futures U/u/pu 链式校验失败（Live 态或 drain 中）。
        BufferOverflow,  ///< snapshot 未到、diff_buffer 触 kMaxDiffBuffer 上限。
    };

    /// @brief 一条 futures `@depth` diff 的解析结果（snapshot 未就绪时排队，drain 时回放）。
    struct Diff {
        uint64_t U  = 0;  ///< First update ID
        uint64_t u  = 0;  ///< Final update ID
        uint64_t pu = 0;  ///< Previous final update ID（futures 特有，链式校验）
        std::vector<std::pair<double, double>> bids;
        std::vector<std::pair<double, double>> asks;
    };

    /// @brief snapshot 未到前 diff_buffer 可堆积的上限。
    /// 超限说明该 symbol 的 snapshot 长期卡住（坏 symbol / REST 持续失败）；继续 buffer
    /// 会无界吃内存并连累其它 symbol，触上限即 reset + re-snapshot 丢弃陈旧 buffer。
    static constexpr size_t kMaxDiffBuffer = 5000;

    /// @brief on_snapshot 的 drain 统计（调用方日志用）。
    struct DrainStats {
        size_t stale_dropped = 0;  ///< u < book last_update_id 的陈旧 diff（丢弃）。
        size_t applied       = 0;  ///< 成功回放进 book 的 diff。
        size_t gapped        = 0;  ///< drain 中撞 gap（触发 NeedResnapshot）。
        size_t bid_levels    = 0;  ///< apply_snapshot 后、drain 前的 bid 档数。
        size_t ask_levels    = 0;  ///< 同上 ask 档数。
    };

    State state() const noexcept {
        if (awaiting_snapshot_) return State::AwaitingSnapshot;
        return book_.is_ready() ? State::Live : State::NeedSnapshot;
    }
    bool is_live() const noexcept { return book_.is_ready(); }
    bool awaiting_snapshot() const noexcept { return awaiting_snapshot_; }
    size_t buffered_diffs() const noexcept { return diff_buffer_.size(); }
    const BinanceOrderBook& book() const noexcept { return book_; }

    /// @brief 标记进入 AwaitingSnapshot（调用方入队前调；幂等守卫）。
    /// @return true = 状态已转移，调用方应真正入队；false = 已 awaiting（重复入队，跳过）。
    /// @note 该守卫与「全局 worker 忙不忙」解耦：B 的 re-snapshot 不被 A 的拉取吞。
    bool begin_await() noexcept {
        if (awaiting_snapshot_) return false;
        awaiting_snapshot_ = true;
        return true;
    }

    /// @brief 喂一条 depth diff（ws_thread）。
    ///
    /// 转移规则：
    ///   - 未就绪：buffer 触上限 → reset + 清 buffer + NeedResnapshot（**触发 diff 本身
    ///     也丢弃**，走全新 snapshot-before-buffer 重建）；否则入 buffer → Buffered。
    ///   - Live：apply 成功 → Applied；sequence gap → reset + 清 buffer + **re-buffer
    ///     当前 diff**（snapshot 后 drain）→ NeedResnapshot。
    /// @note overflow 不清 awaiting：snapshot 可能已在队/在拉，begin_await 幂等防双入队。
    /// @param cause 非空时写 NeedResnapshot 成因（其余结果写 None）。
    /// @param pre_reset_update_id 非空时在 SequenceGap 分支写 reset 前 book 的
    ///        last_update_id（调用方 gap 日志用）。
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
            diff_buffer_.push_back(std::move(d));  // re-buffer 当前 diff，snapshot 后 drain
            if (cause) *cause = ResnapshotCause::SequenceGap;
            return DiffResult::NeedResnapshot;
        }
        return DiffResult::Applied;
    }

    /// @brief snapshot 落地：apply + drain diff_buffer（丢弃 stale，余下按 U/u/pu 回放）。
    ///
    /// 无条件先清 awaiting（无论成败——NeedResnapshot 时调用方 enqueue 会经 begin_await
    /// 重新置位）。drain 中撞 gap → snapshot 与 buffer 不可拼接，reset + NeedResnapshot。
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

    /// @brief WS 整断后的全重置：book/buffer/awaiting 全清，回 NeedSnapshot。
    /// @note 清 awaiting 让重连后 on-connect 的 enqueue 能重新入队（断线前可能正
    ///       awaiting，旧 worker post 会被 reset 后的 drain 自然纠正）。
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

}  // namespace binance_u
