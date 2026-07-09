#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "api/StrategySid.h"
#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#include "xt_core/order/XtOrderRequest.h"
#include "api/XtLiveRejectReason.h"
#include "xt_risk/gateway/XtRiskGateway.h"
#include "xt_risk/engine/XtRiskEngine.h"
#include "live/XtLiveRiskGuardContext.h"
#include "live/XtLiveRejectSink.h"
#include "live/XtSymbolPriceBoundSTP.h"
#include "live/XtVelocityLimiter.h"
#include "api/IExecutionGateway.h"
#include "api/IHftGateway.h"
#include "api/ICtaGateway.h"
#include "XtLiveOrderGatewayCfg.h"
#include "RiskStatsSnapshot.h"
#include "LogicalOrderTracker.h"
#include "xt_util/runtime/XtAtomicWindowLimiter.h"
#include "xt_strategy_engine/IStrategyExecSink.h"

// 构造期/装配期依赖：这里只暴露指针或引用，具体行为在 .cpp 中 include。
class XtLiveParamDomain;
class XtLiveLatencyTracker;
class XtCommodityCatalog;
namespace xt_account { class AccountStateStore; }
namespace xt_identity { class StrategySidRegistry; }

namespace xt_risk { class XtRiskMonitorState; }
namespace ct::metrics {
class XtStrategyPnlAggregator;
class XtStrategyPnlBaseline;
}

namespace xt_runtime::order { class XtOrderRuntimeBook; }

/// @brief 实盘下单/撤单前置网关。
///
/// `submit()`/`cancel()` 先经过 trading gate、risk gate 和 sidecar metadata，
/// 再调用 TD 通道发送请求；订单终态以 TD 回报为准。
/// 内部不加锁，`single_threaded_submit=true` 时调用方必须保证无并发提交。
class XtLiveOrderGateway final : public xt_runtime::IHftGateway,
                                 public xt_runtime::ICtaGateway,
                                 private ILiveRejectOutput {
public:
    using RiskStatsSnapshot = ::RiskStatsSnapshot;

    using Config = XtLiveOrderGatewayCfg;

    XtLiveOrderGateway(const Config& cfg,
                       xt_strategy_engine::IStrategyExecSink* exec_sink,
                       std::atomic<bool>* trading_ready,
                       std::atomic<uint32_t>* last_tick_action_time,
                       std::atomic<bool>* td_position_ready,
                       XtLiveParamDomain* param_domain,
                       XtLiveLatencyTracker* latency_tracker,
                       std::atomic<bool>* kill_switch = nullptr,
                       XtRiskEngine::InitDeps risk_engine_deps = XtRiskEngine::InitDeps());

    size_t live_orders() const override;

    [[nodiscard]] bool is_order_terminal(uint64_t local_id) const override;

    void bind_td(ITradeGateway* td);

    void bind_md(IMdGateway* md);

    void set_exec_sinks(xt_strategy_engine::IStrategyExecSink* fallback,
                        const std::vector<xt_strategy_engine::IStrategyExecSink*>& sinks,
                        const xt_identity::StrategySidRegistry* sid_registry) noexcept {
        exec_sink_ = fallback;
        exec_sinks_ = sinks;
        sid_registry_ = sid_registry;
    }

    void clear_exec_sinks() noexcept {
        exec_sink_ = nullptr;
        exec_sinks_.clear();
        sid_registry_ = nullptr;
    }

    void insert_self_match_blacklist(const char* code, const char* reason) noexcept;

    XtRiskCfg risk_config() const;

    void set_risk_config(const XtRiskCfg& cfg);

    using AnnualLatchSnapshot = XtRiskEngine::AnnualLatchSnapshot;
    AnnualLatchSnapshot annual_latch_snapshot() const;

    void restore_annual_latch(int year, int64_t year_start_equity_fp, bool cutoff_active,
                              int64_t year_start_net_pnl_fp, int64_t risk_fund_balance_fp);

    void arm_annual_loss_on_session_begin(const xt_account::AccountStateStore* account_store,
                                          uint32_t trading_date, int64_t equity_fp) noexcept;

    void set_risk_monitor_state(const xt_risk::XtRiskMonitorState* state) noexcept {

        risk_monitor_state_.store(state, std::memory_order_release);
    }

    void set_strategy_pnl_aggregator(
        const ct::metrics::XtStrategyPnlAggregator* aggr) noexcept;

    void set_strategy_pnl_baseline(
        const ct::metrics::XtStrategyPnlBaseline* baseline) noexcept;

    void set_per_strategy_risk_monitor_state(
        const xt_risk::XtRiskMonitorState* monitor_state) noexcept;

    void inject_components(xt_runtime::order::XtOrderRuntimeBook* order_book,
                           xt_account::AccountStateStore* account_store,
                           std::atomic<int64_t>* close_profit_fp = nullptr,
                           const XtCommodityCatalog* catalog = nullptr,
                           const std::atomic<uint64_t>* last_position_snapshot_ms = nullptr,
                           bool stale_guard_enabled = true,
                           int  stale_ms = 120000);

    std::string validate_risk_config();

    xt_runtime::SubmitResult submit(const char* code,
                                    bool is_buy,
                                    int64_t price_fp,
                                    int64_t qty_fp,
                                    XtTimeInForce tif,
                                    std::string_view user_tag,
                                    XtOffset close_offset,
                                    ExecFlags exec_flags,
                                    std::string_view strategy_id = "") override;

    xt_runtime::SubmitResult submit_with_intent_trace(const char* code,
                                    bool is_buy,
                                    int64_t price_fp,
                                    int64_t qty_fp,
                                    XtTimeInForce tif,
                                    std::string_view user_tag,
                                    XtOffset close_offset,
                                    ExecFlags exec_flags,
                                    std::string_view strategy_id,
                                    uint64_t intent_trace_id) override;

    xt_runtime::SubmitResult submit(const xt_core::XtOrderRequest& req,
                                    ExecFlags exec_flags) override;

    bool cancel(uint64_t id) override;

    void stop(std::chrono::milliseconds timeout =
                  std::chrono::seconds(5)) noexcept override;

    RiskStatsSnapshot risk_stats_snapshot() const;

    LogicalOrderTracker& logical_order_tracker() { return logical_order_tracker_; }

    uint64_t lookup_trace_id(uint64_t order_id) const noexcept;

    void pre_warm_risk(const std::vector<std::string>& symbols);

    void notify_order_terminal(const char* code, bool is_buy) override;

    void record_order_terminal(XtOrderStatus status) override;

    void erase_order_tracking(uint64_t order_id) override;

    std::size_t prune_terminal_strategy_ids(int64_t cutoff_us);

    std::size_t order_strategy_size() const {
        std::shared_lock<std::shared_mutex> lock(order_strategy_mu_);
        return order_strategy_.size();
    }

    int64_t order_strategy_grace_us() const noexcept {
        return order_strategy_grace_us_;
    }

    std::string resolve_strategy_id(uint64_t logical_order_id) const noexcept {
        std::shared_lock<std::shared_mutex> lock(order_strategy_mu_);
        const auto it = order_strategy_.find(logical_order_id);
        return it != order_strategy_.end() ? it->second.sid : std::string{};
    }

    const char* resolve_route_status(uint64_t logical_order_id,
                                     ITradeGateway::TdState td_state) const noexcept {
        std::shared_lock<std::shared_mutex> lock(order_strategy_mu_);
        const auto it = order_strategy_.find(logical_order_id);
        if (it == order_strategy_.end()) {
            if (td_state == ITradeGateway::TdState::Recovering) return "fallback_recovery";
            return logical_order_id == 0 ? "unknown" : "unmatched";
        }
        switch (it->second.source) {
            case xt_core::XtOrderSource::Strategy:
                return it->second.sid.empty() ? "unmatched" : "routed";
            case xt_core::XtOrderSource::Manual:
            case xt_core::XtOrderSource::Closeout:
                return "operator";
            case xt_core::XtOrderSource::Recovery:
                return "fallback_recovery";
        }
        return "unknown";
    }

    std::string lookup_strategy_id(uint64_t order_id) const override {
        return resolve_strategy_id(order_id);
    }

    size_t translate_open_orders(XtOrderExecUpdate* rows, size_t n) const override {
        return logical_order_tracker_.translate_open_orders(rows, n);
    }

    void process_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& pos,
                                   ITradeGateway::TdState td_state);

private:
    struct SubmitSlice {
        XtOffset close_offset = XtOffset::Open;
        int64_t qty_fp = 0;
    };

    static constexpr size_t kMaxSubmitSlices = 4;
    struct SubmitSliceArray {
        std::array<SubmitSlice, kMaxSubmitSlices> slices{};
        uint8_t count = 0;
        bool truncated = false;
        bool deferred = false;
        void push(XtOffset offset, int64_t qty) {
            assert(count < kMaxSubmitSlices);
            slices[count++] = {offset, qty};
        }
    };

    static int64_t now_ms();
    static bool is_valid_code(const char* code);

    static bool is_valid_order_input(const char* code, int64_t price_fp, int64_t qty_fp);

    xt_runtime::SubmitResult submit_with_metadata(const char* code,
                                                  bool is_buy,
                                                  int64_t price_fp,
                                                  int64_t qty_fp,
                                                  XtTimeInForce tif,
                                                  std::string_view user_tag,
                                                  XtOffset close_offset,
                                                  ExecFlags exec_flags,
                                                  std::string_view strategy_id,
                                                  xt_core::XtOrderSource source,
                                                  uint64_t audit_trace_id,
                                                  uint64_t intent_trace_id);

    bool build_close_slices(const char* code,
                            bool is_buy,
                            int64_t qty_fp,
                            bool prefer_today,
                            SubmitSliceArray& out) const;

    SubmitSliceArray build_submit_slices(const char* code,
                                         bool is_buy,
                                         int64_t qty_fp,
                                         XtOffset close_offset,
                                         xt_core::XtOrderSource source) const;

    void emit_margin_reject(bool is_buy,
                            int64_t price_fp,
                            int64_t qty_fp,
                            int64_t tnow,
                            std::string_view strategy_id = std::string_view{});

    uint64_t submit_slices(const char* code,
                           bool is_buy,
                           int64_t price_fp,
                           XtTimeInForce tif,
                           std::string_view user_tag,
                           const SubmitSliceArray& slices,
                           XtOffset orig_close_offset,
                           ExecFlags exec_flags,
                           int64_t tnow,
                           uint64_t decision_us,
                           IRejectSink& reject_sink,
                           bool is_forced_close = false,
                           std::string_view strategy_id = std::string_view{},
                           uint64_t intent_trace_id = 0);

    enum class SliceOutcome : uint8_t {
        kSubmitted,
        kRejectAll,
        kStopPartial,
    };

    /// @brief submit_slices 跨切片累积状态（栈上，零堆开销）。
    struct SliceRunState {
        uint64_t phys_ids[kMaxSubmitSlices] = {};
        int64_t  phys_qty_fps[kMaxSubmitSlices] = {};
        uint8_t  phys_count = 0;
        int64_t  total_submitted_qty_fp = 0;
        uint64_t first_phys_id = 0;
    };

    SliceOutcome submit_one_slice_(const char* code,
                                   bool is_buy,
                                   int64_t price_fp,
                                   XtTimeInForce tif,
                                   std::string_view user_tag,
                                   const SubmitSliceArray& slices,
                                   uint8_t slice_index,
                                   ExecFlags exec_flags,
                                   int64_t tnow,
                                   uint64_t decision_us,
                                   IRejectSink& reject_sink,
                                   bool is_forced_close,
                                   std::string_view strategy_id,
                                   uint64_t intent_trace_id,
                                   StrategySid owner_sid,
                                   bool owner_sid_valid,
                                   SliceRunState& st);

    uint64_t finalize_multi_slice_(const SliceRunState& st,
                                   bool is_buy,
                                   int64_t price_fp,
                                   XtOffset orig_close_offset,
                                   uint64_t intent_trace_id);

    void register_submitted_order_(uint64_t phys_id,
                                   const char* code,
                                   bool is_buy,
                                   int64_t qty_fp,
                                   int64_t price_fp,
                                   XtOffset close_offset,
                                   StrategySid owner_sid,
                                   int64_t required_margin_fp,
                                   int64_t per_unit_margin_fp);

    void record_reject(XtLiveRejectReason code);
    void maybe_enter_cooldown_on_reject(int64_t now_ms);

    void publish_margin_reject(const xt_core::XtOrder& req,
                               int64_t now_ms,
                               std::string_view strategy_id = std::string_view{}) override;
    void publish_reject_reason(XtLiveRejectReason code) override;

    Config cfg_;
    xt_runtime::order::XtOrderRuntimeBook* order_book_direct_ = nullptr;
    const XtCommodityCatalog* catalog_direct_ = nullptr;
    xt_account::AccountStateStore*                    account_store_direct_ = nullptr;
    std::atomic<int64_t>*                  close_profit_fp_direct_ = nullptr;
    xt_strategy_engine::IStrategyExecSink* exec_sink_ = nullptr;
    std::atomic<bool>* trading_ready_ = nullptr;
    std::atomic<uint32_t>* last_tick_action_time_ = nullptr;
    std::atomic<bool>* td_position_ready_ = nullptr;
    std::atomic<bool>* kill_switch_ = nullptr;
    std::atomic<ITradeGateway*> td_{nullptr};
    std::atomic<IMdGateway*> md_{nullptr};
    XtLiveParamDomain* param_domain_ = nullptr;
    XtLiveLatencyTracker* latency_tracker_ = nullptr;
    std::atomic<uint64_t> gate_seen_version_{0};
    std::atomic<bool> gate_submit_enabled_{true};
    std::atomic<bool> gate_cancel_enabled_{true};

    AtomicWindowLimiter order_limiter_{1000};
    AtomicWindowLimiter cancel_limiter_{1000};
    AtomicWindowLimiter reject_limiter_{60000};
    std::atomic<int64_t> block_until_ms_{0};

    std::atomic<bool> stopping_{false};
    mutable std::mutex submit_mu_;
#ifndef NDEBUG
    std::atomic<bool> submit_reentry_guard_{false};
#endif
    XtRiskGateway risk_gateway_;
    XtRiskEngine risk_engine_;
    XtRiskPolicy risk_policy_;
    XtLiveRiskGuardContext::OrderRateLimitFn order_rate_limit_fn_;
    XtLiveRiskGuardContext risk_guard_ctx_;
    XtSymbolPriceBoundSTP stp_checker_;
    XtVelocityLimiter velocity_limiter_;
    std::atomic<const xt_risk::XtRiskMonitorState*> risk_monitor_state_{nullptr};

    /// logical_order_id → submit-time metadata sidecar。
    ///
    /// 终态时 mark `terminal_at_us` 而非立即删除，grace window 后统一 prune。
    struct OrderStrategyEntry {
        std::string sid;
        int64_t terminal_at_us = 0;

        xt_core::XtOrderSource source = xt_core::XtOrderSource::Strategy;
    };
    mutable std::shared_mutex order_strategy_mu_;
    std::unordered_map<uint64_t, OrderStrategyEntry> order_strategy_;

    int64_t order_strategy_grace_us_ = 60'000'000;

    /// phys_id / clord_id → intent_trace_id；终态后同 strategy sidecar grace 统一保留。
    struct OrderTraceEntry {
        uint64_t trace_id = 0;
        int64_t terminal_at_us = 0;
    };
    mutable std::shared_mutex order_trace_mu_;
    std::unordered_map<uint64_t, OrderTraceEntry> order_trace_;
    void note_trace_attribution_(uint64_t order_id, uint64_t trace_id);
    void mark_order_tracking_terminal_(uint64_t order_id, int64_t now_us);

    mutable std::atomic<uint64_t> dropped_margin_reject_total_{0};

    std::vector<xt_strategy_engine::IStrategyExecSink*> exec_sinks_;

    const xt_identity::StrategySidRegistry* sid_registry_ = nullptr;
public:

    uint64_t dropped_margin_reject_count() const noexcept {
        return dropped_margin_reject_total_.load(std::memory_order_relaxed);
    }
private:

    static int64_t xt_steady_now_us_();
    std::atomic<uint64_t> last_halt_log_version_{0};

    std::atomic<uint64_t> submit_attempts_{0};
    std::atomic<uint64_t> submit_success_{0};
    std::atomic<uint64_t> cancel_attempts_{0};
    std::atomic<uint64_t> cancel_success_{0};
    std::atomic<uint64_t> canceled_terminal_total_{0};

    std::array<std::atomic<uint64_t>, kLiveRejectReasonCount> reject_counts_{};

    LogicalOrderTracker logical_order_tracker_;

    void refresh_param_gate_cache();
};
