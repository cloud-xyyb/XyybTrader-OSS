#pragma once

/// @file BinanceReconciler.h
/// @brief Binance TD 的对账组件：把本地订单/资金/持仓和交易所拉回一致
///
/// - 启动时拉全量快照建立基线；
/// - 运行中发现对不上（幽灵单、漏 fill、仓位漂移）就用 REST 重新核对。
/// - REST 失败时跳过本轮

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nlohmann/json_fwd.hpp>

#include "api/ITradeGateway.h"
#include "td/BinanceTdParse.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_live/gateway/binance/td/EffectiveSignalDedup.h"
#include "xt_live/gateway/binance/td/ResyncFillReplayer.h"
#include "xt_live/gateway/common/TdReadinessGate.h"
#include "xt_util/time/XtSteadyClock.h"

namespace binance_u {

class BinanceOrderLedger;
class BinanceRestClient;
class BinanceUserDataStream;

inline uint64_t lat_now_us() noexcept {
    return XtSteadyClock::steady_now_us();
}

/// @brief 「可能有仓位或成交」的 symbol 集合；REST 对账时只查这些 symbol 减负。
///
/// 对账靠「没登记过的 symbol 本地必定是零仓」这个前提工作——
/// 任何让本地仓位变非零的路径都必须先 note()，漏了会让对账把真仓当零仓抹掉。
/// insert-only，自带小锁；临界区内不获取其它锁。
class TrackedSymbolSet {
public:

    void note(const std::string& symbol) {
        if (symbol.empty()) return;
        std::lock_guard<std::mutex> lk(mu_);
        symbols_.insert(symbol);
    }

    std::vector<std::string> snapshot() const {
        std::lock_guard<std::mutex> lk(mu_);
        return {symbols_.begin(), symbols_.end()};
    }

    bool contains(const std::string& symbol) const {
        std::lock_guard<std::mutex> lk(mu_);
        return symbols_.count(symbol) > 0;
    }

private:
    mutable std::mutex mu_;
    std::unordered_set<std::string> symbols_;
};

/// @brief Binance TD 对账的执行组件：启动快照、定期核对、幽灵单清理和漏单回补。
///
/// BinanceReconciler 跑在冷路径（cold worker / reconcile 线程），不占下单热路径。
/// 对账期间到达的 fill 经栅栏按时间分区重放，快照和实时流不会互相覆盖。
/// 实现分在 BinanceReconcilerSync.cpp 和 BinanceReconcilerSweep.cpp 两个文件。
class BinanceReconciler {
public:
    /// @brief reconciler 需要、但仍归 gateway Impl 管的能力回调集合。
    ///
    /// 字段顺序就是初始化顺序；Impl 构造时全部绑定，不可为空。
    struct ImplHooks {

        std::function<void(const XtOrderExecUpdate&, EffectiveSignalDedup::Source,
                           XtLatencyEligibility)>
            emit_through_dedup;

        std::function<bool()> queue_reconcile_resync;

        std::function<bool(int64_t wait_ms)> wait_shutdown;

        std::function<void(const std::vector<uint64_t>&)> erase_dedup_for_local_ids;

        std::function<void()> emit_ready_if_td_readiness_satisfied;
    };

    BinanceReconciler(std::unique_ptr<BinanceRestClient>& rest,
                      std::unique_ptr<BinanceUserDataStream>& uds,
                      BinanceOrderLedger& ledger,
                      ITradeGateway::Callbacks& cb,
                      xt_live::gateway::TdReadinessGate& readiness_gate,
                      TrackedSymbolSet& tracked_symbols,
                      ImplHooks hooks);

    BinanceReconciler(const BinanceReconciler&) = delete;
    BinanceReconciler& operator=(const BinanceReconciler&) = delete;

    bool is_recovering() const {
        return reconcile_in_progress_.load(std::memory_order_acquire);
    }

    int64_t bootstrap_next_retry_ms() const {
        return bootstrap_next_retry_ms_.load(std::memory_order_relaxed);
    }

    void reset_bootstrap_backoff() {
        bootstrap_attempts_.store(0, std::memory_order_relaxed);
        bootstrap_next_retry_ms_.store(0, std::memory_order_relaxed);
    }

    void bootstrap_snapshot();

    void refresh_snapshot();

    void reconcile_resync();

    std::optional<std::vector<XtPositionSnapshot>> fetch_positions();

    std::optional<std::size_t> detect_and_resolve_ghosts(uint32_t min_age_ms);

    bool synthesize_blank_sys_id_reject(uint64_t local_id);

    bool rebuild_from_open_orders_body(const std::string& body, bool is_cold_start);

    uint64_t user_trades_pages_capped_total() const {
        return user_trades_pages_capped_total_.load(std::memory_order_relaxed);
    }

    void forward_uds_trade(const XtTradeRecord& tr, XtLatencyEligibility elig);

    void forward_order_update_gated(const XtOrderExecUpdate& upd,
                                    EffectiveSignalDedup::Source src,
                                    XtLatencyEligibility elig);

    void forward_uds_position(const std::vector<XtPositionSnapshot>& positions);

    void emit_reconcile_terminal_update(const XtOrderExecUpdate& upd,
                                        XtLatencyEligibility elig);

    /// @brief 对账快照落地期间，把并发到达的 fill 暂存、排好序再放行的栅栏。
    ///
    /// 栅栏开着时 fill 先暂存；快照落地后按 venue 时间把暂存 fill 分成
    /// 快照前/快照后两段重放，保证既不双计仓位也不丢 PnL/fee 记账。
    /// 任何 fill 都不丢弃。未走到 finish（REST 失败早退）时，
    /// 析构按到达顺序放行并关闭栅栏。
    class ResyncFillGate {
    public:
        explicit ResyncFillGate(BinanceReconciler& reconciler);
        ~ResyncFillGate();
        ResyncFillGate(const ResyncFillGate&) = delete;
        ResyncFillGate& operator=(const ResyncFillGate&) = delete;

        void finish_with_snapshot(const std::vector<XtPositionSnapshot>& positions,
                                  const std::unordered_map<std::string, int64_t>& fence_update_ms,
                                  int64_t fence_floor_ms,
                                  bool mark_recovering);

    private:
        BinanceReconciler& reconciler_;
        bool finished_ = false;
    };

private:

    struct VenueOpenOrderBrief {
        std::string sys_id;
        std::string client_order_id;
        std::string symbol;
        uint64_t    order_time_ms = 0;
    };

    /// @brief 用 client_order_id 反查 open order 的结果。
    struct ClientOrderLookup {
        bool ok = false;
        bool found = false;
        std::string sys_id;
    };

    std::optional<std::vector<VenueOpenOrderBrief>> fetch_open_orders_brief();

    std::size_t cancel_leaked_venue_orders(const std::vector<VenueOpenOrderBrief>& venue_open,
                                           uint32_t min_age_ms);

    std::optional<XtOrderExecUpdate> fetch_order_status(const std::string& sys_id,
                                                        const std::string& symbol,
                                                        uint64_t local_id,
                                                        XtLatencyEligibility& out_elig);

    std::optional<XtOrderExecUpdate>
        synthesize_not_exist_terminal_(uint64_t local_id, const std::string& sys_id);

    void note_ghost_fetch_failure_(uint64_t local_id, const std::string& sys_id,
                                   std::unordered_map<uint64_t, uint32_t>& next_streak);

    ClientOrderLookup find_open_order_by_client_id(const std::string& symbol,
                                                   const std::string& client_order_id);

    bool resolve_blank_sys_id_for_reconcile(uint64_t local_id,
                                            const std::string& symbol,
                                            uint64_t age_ms,
                                            uint32_t min_age_ms);

    void resync_user_trades_for_symbol(const std::string& symbol);

    void resync_user_trades_for_order(const std::string& symbol, const std::string& sys_id);

    void replay_user_trades_json_(const std::string& symbol, const nlohmann::json& trades,
                                  size_t& emitted, size_t& deduped, size_t& out_skipped,
                                  int64_t& out_max_trade_id);

    [[nodiscard]] std::optional<GapFillOrderAttribution>
        resolve_gap_fill_order_attribution(const std::string& symbol,
                                           int64_t exchange_order_id);

    void rebuild_alive_inflight_and_emit(bool is_cold_start);

    [[nodiscard]] std::optional<AccountTruthDetail> fetch_account_detail();

    [[nodiscard]] std::unordered_map<std::string, int64_t> fetch_position_marks();

    void anchor_account_truth_after_resync_();

    void finish_resync_fill_gate_(const std::vector<XtPositionSnapshot>* positions,
                                  const std::unordered_map<std::string, int64_t>& fence_update_ms,
                                  int64_t fence_floor_ms,
                                  bool mark_recovering);

    int64_t sample_position_fence_floor_ms_();

    std::optional<nlohmann::json> get_position_risk_array_(const char* tag);

    std::optional<nlohmann::json> get_open_orders_array_(const char* tag);

    std::unique_ptr<BinanceRestClient>& rest_;
    std::unique_ptr<BinanceUserDataStream>& uds_;
    BinanceOrderLedger& ledger_;
    ITradeGateway::Callbacks& cb_;
    xt_live::gateway::TdReadinessGate& readiness_gate_;
    TrackedSymbolSet& tracked_symbols_;
    ImplHooks hooks_;

    std::atomic<bool> reconcile_in_progress_{false};

    std::atomic<bool> is_first_recovery_{true};

    std::atomic<uint32_t> bootstrap_attempts_{0};
    std::atomic<int64_t>  bootstrap_next_retry_ms_{0};

    std::atomic<uint64_t> last_account_fetch_ms_{0};

    std::mutex resync_gate_mu_;
    bool resync_buffering_ = false;
    std::vector<XtTradeRecord> resync_fill_buffer_;

    std::vector<ResyncTerminal> resync_terminal_buffer_;
    uint64_t resync_pos_dropped_total_ = 0;

    static constexpr uint32_t kGhostFetchFailEscalateCycles = 10;
    std::unordered_map<uint64_t, uint32_t> ghost_fetch_fail_streak_;

    static constexpr std::size_t kSymbolFilterMax = 8;

    static constexpr std::size_t kUserTradesPageLimit = 500;
    static constexpr int kUserTradesPageRoundsMax = 8;

    std::atomic<uint64_t> user_trades_pages_capped_total_{0};
};

}
