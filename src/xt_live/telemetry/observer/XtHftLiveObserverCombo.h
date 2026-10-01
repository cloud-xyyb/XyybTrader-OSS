#pragma once

#include <cmath>
#include <cstring>
#ifndef NDEBUG
#include <cassert>
#include <thread>
#endif

#include "api/IHftEngineObserver.h"
#include "xt_account/api/AccountSnapshot.h"
#include "api/XtRunnerSignals.h"
#include "latency/XtLiveLatencyTracker.h"
#include "xt_core/container/XtLockFreeQueue.h"
#include "XtFixedPoint.h"
#include "xt_util/time/XtSteadyClock.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"

namespace xt_live {

/// @brief HFT engine 执行回报的 live observer
///
/// 成交/订单回报到达后依次推进延迟统计、runner signals 和诊断队列。
/// 构造方指针均为 non-owning，生命周期由调用方保证覆盖 engine。
class XtHftLiveObserverCombo final : public xt_runtime::IHftEngineObserver {
public:
    XtHftLiveObserverCombo(XtLiveLatencyTracker* latency,
                           XtRunnerSignals*      signals,
                           XtLockFreeQueue<XtTradeRecord>*     trade_q,
                           XtLockFreeQueue<XtOrderExecUpdate>* order_q)
        : latency_(latency), signals_(signals),
          trade_q_(trade_q), order_q_(order_q) {}

    void on_trade(const XtTradeRecord& tr, XtLatencyEligibility elig,
                  uint64_t logical_order_id) override {
        if (signals_) {
            signals_->trade_fill_cnt.fetch_add(1, std::memory_order_relaxed);
            // 成交已由 account_store apply，可作为 position freshness witness。
            // Binance keepalive 抖动时，这能降低 close path 误触 PositionSyncStale。
            signals_->last_position_snapshot_ms.store(
                static_cast<uint64_t>(XtSteadyClock::steady_now_ms()),
                std::memory_order_release);
        }
        // latency tracker feed 仅 kEligible（延迟重放/合成的 kIneligible 不喂，防假尾延迟）。
        if (latency_ && elig == XtLatencyEligibility::kEligible) {
            // L-2 fill-link：logical_order_id 非 0 时经 on_trade 的 order_id_override 参落
            // logical 域键（与 gateway submit track 同域配对），零拷贝——不复制整条 record
            // （broker-ID string heap 分配，I-4）；engine 已算出 logical id，不再二次 lookup。
            // logical_order_id==0 时 override 为 0，退化为按 tr.order_id 落键。
            latency_->on_trade(tr, logical_order_id);
        }
        // 诊断通道满队列静默丢弃（诊断数据可接受；用原始 tr，order_id 语义不变）
        if (trade_q_) trade_q_->push(tr);
    }

    void on_order_update(const XtOrderExecUpdate& upd, XtLatencyEligibility elig) override {
        if (latency_ && elig == XtLatencyEligibility::kEligible) latency_->on_order_update(upd);
        if (order_q_) order_q_->push(upd);
    }

    void on_position_snapshot_begin(bool is_recovery) override {
        if (is_recovery && signals_) {
            signals_->trading_ready.store(false, std::memory_order_release);
        }
    }

    void on_position_snapshot_end(bool is_recovery, bool position_applied,
                                  uint32_t session_gen) override {
        if (!signals_) return;
        assert_worker_thread_();
        // freshness witness：无条件更新（语义 = "已处理过一帧 position snapshot / reconcile"，
        // 不要求 apply 成功；stale guard 与 closeout 兜底读它）。gen 无关。
        signals_->last_position_snapshot_ms.store(
            static_cast<uint64_t>(XtSteadyClock::steady_now_ms()),
            std::memory_order_release);
        // gen fence：仅对"当前 session"的 snapshot 置 readiness latch。断连前 enqueue 的 stale
        // snapshot 带旧 gen（!= 当前 gen）→ 跳过 → 即便断连 ChannelLost 物理满被丢也不 fail-open。
        const bool cur_gen =
            (session_gen == signals_->td_session_gen.load(std::memory_order_acquire));
        // readiness 诚实契约：仅当快照真正灌入 account_store（position_applied）且属当前 gen 时
        // 置 applied，并与 TD 通道 up 配对置 td_position_ready。td_channel_up_ 是 worker-local
        // （由 on_channel_ready/lost 同线程维护）—— readiness latch 收成 worker 单写，无 Dekker。
        // 置位后 validate-after-set 重验 gen（见 revoke_if_gen_moved_），关闭「gen 检查通过后、
        // store 落地前 SDK 恰好断连」的 check-then-store 窗口。
        if (position_applied && cur_gen) {
            last_applied_gen_ = session_gen;
            signals_->position_snapshot_applied.store(true, std::memory_order_release);
            if (td_channel_up_)
                signals_->td_position_ready.store(true, std::memory_order_release);
            revoke_if_gen_moved_(session_gen);
        }
        if (is_recovery) signals_->refresh_trading_ready();
    }

    // TD 通道 ready/lost 由 engine.on_channel_ready/lost fan 到此（worker 线程）。
    // 只处理 "TD"（MD ready 由 SDK 侧 md_ready 直接管，不进 position readiness）。
    void on_channel_ready(const char* channel) override {
        if (!signals_ || std::strcmp(channel, "TD") != 0) return;
        assert_worker_thread_();
        td_channel_up_ = true;
        // 补 ready-before-snapshot 乱序：apply 已到位则 td 通道 up 即放行 position-ready。
        // re-latch 要求 applied 来自当前 gen（last_applied_gen_ 等值校验）；
        // applied 若是断连竞争窗口残留的 stale-true，gen 已推进则不放行。
        const uint32_t cur = signals_->td_session_gen.load(std::memory_order_acquire);
        if (last_applied_gen_ == cur &&
            signals_->position_snapshot_applied.load(std::memory_order_relaxed)) {
            signals_->td_position_ready.store(true, std::memory_order_release);
            revoke_if_gen_moved_(cur);
        }
    }

    void on_channel_lost(const char* channel, int /*reason*/) override {
        if (!signals_ || std::strcmp(channel, "TD") != 0) return;
        assert_worker_thread_();
        td_channel_up_ = false;
        // 权威 clear：本事件在 td_event_queue 里 FIFO 排在断连前入队的 stale snapshot 之后，
        // worker 先 re-latch 再在此清，确保断连后的旧快照不能重新放行 readiness。
        signals_->position_snapshot_applied.store(false, std::memory_order_release);
        signals_->td_position_ready.store(false, std::memory_order_release);
    }

    void on_account_snapshot(const ITradeGateway::AccountSnapshot& snap,
                              const xt_account::AccountSnapshot& metrics) override {
        if (!signals_) return;
        const int64_t close_profit_fp = std::isfinite(snap.close_profit)
            ? xt_fp::to_fp(snap.close_profit) : 0;
        const int64_t external_cashflow_fp =
            (std::isfinite(snap.deposit) && std::isfinite(snap.withdraw))
                ? xt_fp::to_fp(snap.deposit - snap.withdraw)
                : 0;
        signals_->latest_close_profit_fp.store(close_profit_fp, std::memory_order_release);
        signals_->latest_balance_fp.store(metrics.cash, std::memory_order_release);
        signals_->latest_external_cashflow_today_fp.store(external_cashflow_fp, std::memory_order_release);
        signals_->latest_external_cashflow_trading_day.store(
            static_cast<int64_t>(signals_->last_tick_date.load(std::memory_order_acquire)),
            std::memory_order_release);
        signals_->latest_account_snapshot_ms.store(
            static_cast<uint64_t>(XtSteadyClock::steady_now_ms()),
            std::memory_order_release);
        signals_->has_account_snapshot.store(true, std::memory_order_release);
    }

private:
    // Debug-only 单线程不变量：所有 observer 回调（含 readiness latch 与 td_channel_up_）
    // 均由 engine 在 event-loop worker 线程 fan，故 td_channel_up_ 用 plain bool 免原子。
    // first-touch 捕获 worker tid，后续断言同线程；未来若有非 worker caller 立即暴露。
#ifndef NDEBUG
    void assert_worker_thread_() {
        const auto tid = std::this_thread::get_id();
        if (worker_tid_ == std::thread::id{}) worker_tid_ = tid;
        else assert(worker_tid_ == tid && "XtHftLiveObserverCombo must run on the single worker thread");
    }
    std::thread::id worker_tid_{};
#else
    void assert_worker_thread_() {}
#endif

    /// @brief validate-after-set：置位 readiness 后重读 gen，已推进则无条件撤销两 flag。
    /// 关闭 check-then-store 窗口：worker 的 gen 检查通过后、store 落地前，SDK 断连恰好
    /// gen++ + clear（若叠加 ChannelLost 物理丢，stale-true 将无人再清）。任一交错终态均
    /// false——gen++ 在重读前可见 → 此处撤销；在重读后 → SDK clear（程序序在 gen++ 后）
    /// 最后落地覆盖为 false（x86-64 TSO 下严密；C++ MM 层面与 gen fence 同口径：
    /// equality-compare + 单调递增，详 XtRunnerSignals.h td_session_gen 注释）。
    /// 撤销无条件（不看 td_channel_up_），只会多 fail-closed，不引入 fail-open。
    void revoke_if_gen_moved_(uint32_t expect_gen) {
        if (signals_->td_session_gen.load(std::memory_order_acquire) != expect_gen) {
            signals_->position_snapshot_applied.store(false, std::memory_order_release);
            signals_->td_position_ready.store(false, std::memory_order_release);
        }
    }

    XtLiveLatencyTracker*               latency_ = nullptr;
    XtRunnerSignals*                    signals_ = nullptr;
    XtLockFreeQueue<XtTradeRecord>*     trade_q_ = nullptr;
    XtLockFreeQueue<XtOrderExecUpdate>* order_q_ = nullptr;
    bool                                td_channel_up_ = false;  ///< worker-local：TD 通道是否 up（配对置 td_position_ready）
    /// worker-local：最后一次成功 apply 的 snapshot gen。on_channel_ready re-latch 要求其等于
    /// 当前 td_session_gen。on_channel_lost 不重置（applied 门已被权威清，重置无必要）；
    /// binance 不发 on_front → gen 恒 0，`0==0` 恒真，行为与 gen fence 前一致（inert）。
    uint32_t                            last_applied_gen_ = 0;
};

} // namespace xt_live
