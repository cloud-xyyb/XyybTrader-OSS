#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "api/IReplayRuntime.h"
#include "api/ITradeGateway.h"
#include "xt_core/order/XtTradeRecord.h"

class XtHftEngine;
class XtHftBacktestOrderGateway;

namespace xt_backtest::hft {

/// @brief HFT 回测的 runtime adapter：让通用 Replayer 不需要知道 HFT 的 engine + gateway 协作细节。
class XtHftBacktestDriver final : public IReplayRuntime {
public:
    XtHftBacktestDriver(XtHftEngine& engine, XtHftBacktestOrderGateway& gw)
        : engine_(&engine), gw_(&gw) {}

    // ── IReplayRuntime ────────────────────────────────────────────────────

    void on_tick(const XtTick& tick) override;

    void on_order_queue(const XtOrderQueue& oq) override;
    void on_order_detail(const XtOrderDetail& od) override;
    void on_transaction(const XtTransaction& tr) override;

    /// @brief 处理 replayer 注入的 grid clock，并派发该时刻已可见的 fill。
    ///
    /// `date/time_ms` 使用 YYYYMMDD 自然日 + HHMMSSmmm。该入口只表示时间流逝，
    /// 不表示新的 market tick；gateway 不递增 tick_idx_，也不消耗
    /// ack_delay_ticks / ttl_ticks 的 tick 预算。
    void on_clock(uint32_t action_date, uint32_t action_time_ms, uint32_t trading_date) override;

    /// @brief 返回 gateway 当前仍 alive 的 backtest orders 数量。
    size_t active_orders() const override;

    // ── Run 生命周期 ───────────────────────────────────────────────────────

    /// @brief 回放结束时按 EOF cutoff 收尾 pending fill 和 in-transit order。
    ///
    /// cutoff = 最近一次 gateway clock + `grace_ms`。
    ///
    /// 只派发 `visible_at_ns <= cutoff` 的 pending fill；更晚才可见的 fill
    /// 直接丢弃，不进入 strategy / reporter / PnL。尚在 order-delay wheel、
    /// 未到 match engine 的订单不生成回报，只计入 lost-in-transit。
    void flush_pending(std::uint32_t grace_ms);

private:
    // non-owning；构造时绑定，生命周期由 runner / test fixture 保证。
    XtHftEngine*                engine_ = nullptr;
    XtHftBacktestOrderGateway*  gw_     = nullptr;
};

} // namespace xt_backtest::hft
