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

    void on_trade(const XtTradeRecord& tr) override {
        if (signals_) {
            signals_->trade_fill_cnt.fetch_add(1, std::memory_order_relaxed);

            signals_->last_position_snapshot_ms.store(
                static_cast<uint64_t>(XtSteadyClock::steady_now_ms()),
                std::memory_order_release);
        }
        if (latency_) latency_->on_trade(tr);

        if (trade_q_) trade_q_->push(tr);
    }

    void on_order_update(const XtOrderExecUpdate& upd) override {
        if (latency_) latency_->on_order_update(upd);
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

        signals_->last_position_snapshot_ms.store(
            static_cast<uint64_t>(XtSteadyClock::steady_now_ms()),
            std::memory_order_release);

        const bool cur_gen =
            (session_gen == signals_->td_session_gen.load(std::memory_order_acquire));

        if (position_applied && cur_gen) {
            last_applied_gen_ = session_gen;
            signals_->position_snapshot_applied.store(true, std::memory_order_release);
            if (td_channel_up_)
                signals_->td_position_ready.store(true, std::memory_order_release);
            revoke_if_gen_moved_(session_gen);
        }
        if (is_recovery) signals_->refresh_trading_ready();
    }

    void on_channel_ready(const char* channel) override {
        if (!signals_ || std::strcmp(channel, "TD") != 0) return;
        assert_worker_thread_();
        td_channel_up_ = true;

        const uint32_t cur = signals_->td_session_gen.load(std::memory_order_acquire);
        if (last_applied_gen_ == cur &&
            signals_->position_snapshot_applied.load(std::memory_order_relaxed)) {
            signals_->td_position_ready.store(true, std::memory_order_release);
            revoke_if_gen_moved_(cur);
        }
    }

    void on_channel_lost(const char* channel, int  ) override {
        if (!signals_ || std::strcmp(channel, "TD") != 0) return;
        assert_worker_thread_();
        td_channel_up_ = false;

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
    bool                                td_channel_up_ = false;

    uint32_t                            last_applied_gen_ = 0;
};

}
