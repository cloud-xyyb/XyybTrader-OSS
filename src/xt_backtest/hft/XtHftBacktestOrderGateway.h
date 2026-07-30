#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "api/IHftGateway.h"
#include "xt_engine/sim/HFTMatchEngine.h"
#include "xt_risk/engine/XtRiskEngine.h"
#include "xt_risk/gateway/XtRiskGateway.h"
#include "xt_risk/live/XtSymbolPriceBoundSTP.h"
#include "xt_risk/config/XtRiskCfg.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_account/AccountStoreView.h"
#include "xt_cost/XtFillCostApplier.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_config/catalog/XtFeeRuleBook.h"
#include "xt_cost/XtTradeCostConfig.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/market/XtTick.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "api/IHftEngineObserver.h"

class IRejectSink;
namespace xt_runtime { class XtRuntimeMarketCache; }

using xt_runtime::IHftGateway;
using xt_runtime::SubmitResult;

/// @brief HFT 回测执行链的订单 gateway。
///
/// 策略侧通过 `IHftGateway` 提交/撤单；本类持有 match、risk、cost 与 order sidecar，
/// 把撮合结果按回放时钟转成 fill/update 队列。
/// account、catalog、order book 由 Runner 持有并通过 non-owning 指针注入
class XtHftBacktestOrderGateway final : public IHftGateway {
public:

    /// @brief 执行链初始化参数，由 Runner 组装后传入一次。
    /// @details
    /// - 配置字段用于重建 gateway-owned match/risk/cost 组件。
    /// - raw ptr 字段均为 required non-owning 依赖，生命周期必须覆盖 gateway。
    struct InitParams {
        XtRiskCfg risk;
        XtRiskPolicy risk_policy;
        XtHftMatchEngine::Config match;
        XtTradeCostConfig cost;
        XtFeeRuleBook::Config fees;
        int64_t max_order_qty_fp = 0;
        bool self_match_check = false;
        bool margin_call_enabled = false;
        double maintenance_ratio = 0.9;

        xt_account::AccountStateStore* account = nullptr;
        const XtCommodityCatalog* catalog = nullptr;
        xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;
    };

    XtHftBacktestOrderGateway() = default;

    void init(InitParams p);

    void set_order_delay_ticks(uint32_t n) { match_engine_.set_order_delay_ticks(n); }

    void set_market_cache(const xt_runtime::XtRuntimeMarketCache* cache) noexcept {
        market_cache_ = cache;
    }

    void set_latency_model(std::unique_ptr<ILatencyModel> m) {
        match_engine_.set_latency_model(std::move(m));
    }

    void set_queue_model(std::unique_ptr<IQueueModel> m) {
        match_engine_.set_queue_model(std::move(m));
    }

    void add_observer(xt_runtime::IHftEngineObserver* obs) {
        if (obs) observers_.push_back(obs);
    }

    void clear_observers() { observers_.clear(); }

    void bind_reject_sink(IRejectSink* sink) { reject_sink_ = sink; }

    void advance_clock(uint32_t action_date, uint32_t action_time_ms,
                       uint32_t trading_date) override;

    void advance_clock_only(uint32_t action_date, uint32_t action_time_ms,
                            uint32_t trading_date);

    void process_tick(const XtTick& tick) override;

    size_t drain_fills(uint32_t date, uint32_t time_ms,
                       std::vector<XtTradeRecord>& out) override;

    size_t drop_pending_fills() override;

    /// @brief post-fill 风险观测快照（纯观测，不产生拒单）。
    /// @details
    /// - `position_breach_count`：某 code 的 fill 净仓绝对值穿透 `risk.max_pos_qty`
    ///   的进入次数（edge-triggered：进入超限记一次 + warn log，回到限内再穿透
    ///   才记下次）。pre-trade 的 max_pos_qty 是 confirmed 净仓语义（resting
    ///   不计入），批量成交可越限——本观测让越限对策略作者显式可见。
    ///   只观测账户级 `risk.max_pos_qty`；`per_strategy_max_pos_qty` 配额
    ///   不在观测范围。
    /// - `peak_abs_position_fp`：fill 净仓绝对值峰值（跨全部 code 取最大）。
    /// - `peak_live_orders`：在途挂单数峰值（submit 成功时刻采样撮合簿
    ///   live 数；`order_delay_ticks > 0` 时 delay wheel 内在途单不计入，
    ///   该形态下为下界观测）。
    /// - 净仓按 drained fill 带符号全额累计（BT 起始仓位恒 0，正常路径与
    ///   confirmed 同值；over-close 被账本 clamp 的角落 fill 全额计入 →
    ///   观测偏保守 over-report，对告警信号 fail-safe）；EOF 被 drop 的
    ///   未可见 fill 不计（与 P&L 口径一致）。
    struct RiskObservability {
        uint32_t position_breach_count = 0;
        int64_t  peak_abs_position_fp  = 0;
        size_t   peak_live_orders      = 0;
    };
    const RiskObservability& risk_observability() const noexcept { return risk_obs_; }

    uint32_t last_clock_date() const noexcept { return last_date_; }

    uint32_t last_clock_time_ms() const noexcept { return last_time_ms_; }

    uint32_t last_clock_action_date() const noexcept { return last_action_date_; }

    std::uint64_t lost_in_transit_count() const noexcept {
        return match_engine_.lost_in_transit_count();
    }

    std::size_t discard_in_transit_orders() {
        const std::size_t n = match_engine_.discard_in_transit_orders();
        if (n > 0) match_engine_.drain_updates(updates_buf_);
        return n;
    }

    void begin_eof_closing() noexcept { eof_closing_ = true; }

    std::size_t synthesize_eof_delivered_cancels();

    std::size_t complete_eof_delivered_terminal_fills(std::vector<XtTradeRecord>& out);

    uint64_t margin_call_count() const noexcept { return margin_call_count_; }

    uint64_t event_forward_spike_count() const noexcept { return event_forward_spike_count_; }

    size_t drain_order_updates(std::vector<XtOrderExecUpdate>& out) override;

    void set_fill_latency_ns(int64_t ns) noexcept { fill_latency_ns_ = ns; }

    void set_dispatch_clock(uint32_t date, uint32_t time_ms) override;

    void on_session_begin(uint32_t trading_date) override;

    void notify_order_terminal(uint64_t order_id) override;

    xt_runtime::IAccountView*                      account_view();

    xt_runtime::order::XtOrderRuntimeBook*         order_book();
    const xt_runtime::order::XtOrderRuntimeBook*   order_book()        const;

    const XtCommodityCatalog*                      commodity_catalog() const;

    xt_account::AccountStateStore*       account()       { return account_; }
    const xt_account::AccountStateStore* account() const { return account_; }

    int64_t get_cash_fp()       const;

    int64_t get_equity_fp()     const;

    int64_t get_unrealized_fp() const;

    int64_t get_frozen_fp()     const;

    size_t  live_orders()       const override;

    int64_t current_ts() const;

    SubmitResult submit(const char* code,
                        bool is_buy,
                        int64_t price_fp,
                        int64_t qty_fp,
                        XtTimeInForce tif,
                        std::string_view user_tag,
                        XtOffset offset,
                        ExecFlags exec_flags,
                        std::string_view strategy_id = "") override;

    bool cancel(uint64_t id) override;

    void stop(std::chrono::milliseconds timeout = std::chrono::seconds(5)) noexcept override {
        (void)timeout;
    }

private:

    SubmitResult submit_single_(const char* code,
                                bool is_buy,
                                int64_t price_fp,
                                int64_t qty_fp,
                                XtTimeInForce tif,
                                std::string_view user_tag,
                                XtOffset offset,
                                std::string_view strategy_id = "",
                                bool is_forced_close = false);

    uint64_t submit_one_slice_(const char* code, bool is_buy, int64_t price_fp,
                               XtTimeInForce tif, std::string_view user_tag,
                               XtOffset sl_offset, int64_t sl_qty,
                               const xt_account::CloseableView& closeable_for_book,
                               StrategySid sid);

    void check_margin_call_();

    void execute_forced_liquidation_();

    bool positions_flat_() const;

    void note_event_forward_spike_(int64_t event_ns) noexcept;

    int64_t compute_maintenance_fp_() const;

    XtHftMatchEngine match_engine_;

    XtFeeRuleBook fee_book_;
    xt_fill_cost::FillCostApplier cost_applier_;
    std::unique_ptr<XtRiskEngine> risk_engine_;
    XtRiskPolicy risk_policy_;
    XtRiskGateway risk_gw_;

    xt_account::AccountStateStore* account_ = nullptr;
    const XtCommodityCatalog* commodities_ = nullptr;
    xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr;
    const xt_runtime::XtRuntimeMarketCache* market_cache_ = nullptr;
    bool price_limit_enabled_ = false;
    bool self_match_check_ = false;
    XtSymbolPriceBoundSTP stp_checker_;
    std::unique_ptr<xt_account::AccountStoreView> account_view_;

    int64_t max_order_qty_fp_ = 0;
    int64_t session_begin_realized_fp_ = 0;

    void note_fill_for_risk_obs_(const XtTradeRecord& tr);

    RiskObservability risk_obs_;
    std::unordered_map<std::string, int64_t> fill_net_fp_;
    std::unordered_map<std::string, bool> breach_state_;
    int64_t obs_max_pos_qty_fp_ = 0;

    bool margin_call_enabled_ = false;
    int64_t maintenance_ratio_fp_ = 0;
    bool daily_loss_check_enabled_ = false;
    int64_t risk_max_order_qty_fp_ = 0;
    bool in_liquidation_ = false;
    uint64_t margin_call_count_ = 0;

    std::vector<xt_runtime::IHftEngineObserver*> observers_;
    IRejectSink* reject_sink_ = nullptr;

    uint64_t tick_idx_ = 0;
    uint32_t last_date_ = 0;
    uint32_t last_time_ms_ = 0;
    uint32_t last_action_date_ = 0;
    int64_t  last_event_ns_ = 0;
    uint64_t event_forward_spike_count_ = 0;
    uint32_t pending_dispatch_date_ = 0;
    uint32_t pending_dispatch_time_ms_ = 0;

    std::vector<XtTradeRecord> trades_buf_;
    std::vector<XtOrderExecUpdate> updates_buf_;

    // multi-strategy routing sidecar：submit 写入 order_id -> strategy id，
    // drain_fills/drain_order_updates 反查后补回回报对象。backtest 单线程运行，不加锁。
    // 终态订单按 trading_date 标记，跨日 session_begin 清理上一日终态 entries，避免长期回测无界增长。
    /// @brief multi-strategy 回报路由的 order sidecar entry。
    struct OrderStrategyEntry {
        std::string sid;
        uint32_t sid_id = 0;
        uint32_t terminal_at_date = 0;
    };
    std::unordered_map<uint64_t, OrderStrategyEntry> order_strategy_;
    uint32_t current_trading_date_ = 0;

    struct DeliveredOrderState {
        bool     is_buy = true;
        XtOffset close_offset = XtOffset::Open;
        int64_t  price_fp = 0;
        int64_t  qty_fp = 0;
        int64_t  delivered_filled_fp = 0;
        bool     terminal_delivered = false;
        int64_t  claimed_filled_fp = 0;
    };
    std::unordered_map<uint64_t, DeliveredOrderState> delivered_proj_;
    bool eof_closing_ = false;

    struct PendingFill {
        XtTradeRecord trade;
        int64_t visible_at_ns;
    };
    std::deque<PendingFill> pending_fills_;

    /// @brief fill 驱动的 order update（Filled/PartiallyFilled）与对应 fill 同
    ///        visible_at 释放——update 立即可见而 fill 延迟会让策略在滞后窗口
    ///        读到「已 Filled 但仓位/冻结未变」的分裂状态。
    struct PendingUpdate {
        XtOrderExecUpdate update;
        int64_t visible_at_ns;
    };
    std::deque<PendingUpdate> pending_updates_;

    void release_due_updates_(int64_t now_ns);

    bool order_has_unvisible_pending_fill_(uint64_t id) const;

    int64_t fill_latency_ns_ = 0;

    bool initialized_ = false;
};
