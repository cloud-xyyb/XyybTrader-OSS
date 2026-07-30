#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "api/XtPositionSnapshot.h"
#include "state/AccountEvent.h"
#include "state/AccountState.h"
#include "state/MarkPriceTable.h"
#include "state/detail/AccountFullSnapshot.h"
#include "xt_account/api/AccountMarking.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_util/sync/XtHazardDomain.h"
#include "xt_util/sync/XtSnapshotPool.h"

class XtCommodityCatalog;

namespace account_audit {

/// @brief AccountStateStore 冻结资金一致性审计结果。
///
/// 审计算法属于 store 内部；本 DTO 只承载调用方需要记录/告警的结果。
struct XtFrozenAuditReport {
    bool ok = true;
    double frozen_cash = 0.0;
    double frozen_cash_by_orders = 0.0;
    std::string reason;
};

}

namespace xt_account {

namespace detail {
struct AccountStoreSymbolTestSeam;
}

/// @brief 账户账本的唯一写入口与快照发布门面。
///
/// 交易线程在不停地改账户——成交入账、下单冻结保证金、行情刷新持仓市值——而风控、策略
/// 和上报随时要读一份自洽的账户视图。AccountStateStore 把写和读隔开：写侧由单一 worker
/// 线程串行更新 AccountState 账本和 MarkPriceTable，读侧通过 snapshot_*() 拿到已发布的
/// 不可变快照，两者互不阻塞，读者也绝不会看到写到一半的中间态。
///
/// 硬契约：所有写入口（apply / on_position_snapshot）只能在绑定的 worker 线程调用。
/// Store 不持久化账本；进程恢复时由 broker 持仓快照重建仓位域、由 SetCash 重锚资金域。
class AccountStateStore {
public:

    explicit AccountStateStore(const AccountState::Config& cfg);

    void bind_worker_thread() noexcept;

    void bind_worker_thread(std::thread::id tid) noexcept;

    void unbind_worker_thread() noexcept;

    void enter_quiescent() noexcept;

    void apply(const XtAccountEvent& ev);

    void on_position_snapshot(const std::vector<XtPositionSnapshot>& positions,
                              bool is_recovery,
                              const XtCommodityCatalog& catalog);

    template <class CodesRange>
    void configure_symbols(const CodesRange& codes) {
        for (const auto& code : codes) {
            register_symbol_(code);
        }
        if (symbols_sorted_.empty()) {
            throw std::invalid_argument(
                "AccountStateStore::configure_symbols: empty symbol set");
        }
        freeze_symbols_();
    }

    [[nodiscard]] bool symbols_frozen() const noexcept;

    [[nodiscard]] bool futures_mode() const noexcept;

    [[nodiscard]] bool is_registered_symbol(std::string_view code) const noexcept;

    [[nodiscard]] std::vector<std::string> snapshot_tracked_codes() const;

    [[nodiscard]] std::size_t quarantined_symbol_count() const noexcept;

    [[nodiscard]] uint64_t quarantined_overflow_count() const noexcept;

    [[nodiscard]] std::vector<std::string> snapshot_quarantined_codes() const;

    void on_session_begin_hook(int64_t equity_fp) noexcept;

    [[nodiscard]] int64_t starting_balance_fp() const noexcept;

    void set_closeable_known(bool known) noexcept;

    [[nodiscard]] AccountSnapshot snapshot_account() const noexcept;

    [[nodiscard]] int64_t realized_pnl() const noexcept;

    [[nodiscard]] PositionSnapshot snapshot_position(std::string_view code) const noexcept;

    /// @brief 同一发布边界下的账户 + 单 symbol 持仓快照。
    struct AccountPositionFrame {
        AccountSnapshot account;
        PositionSnapshot position;
    };

    [[nodiscard]] AccountPositionFrame snapshot_account_and_position(
        std::string_view code) const noexcept;

    void frozen_audit_report(account_audit::XtFrozenAuditReport& out) const;

    void frozen_audit_from_snapshot(account_audit::XtFrozenAuditReport& out) const;

private:
    friend struct detail::AccountStoreSymbolTestSeam;

    void register_symbol_(std::string_view code);

    void freeze_symbols_();

    bool insert_symbol_sorted_(std::string_view code);

    AccountState account_;
    MarkPriceTable mark_cache_;

    xt_util::sync::XtSnapshotPool<PositionSnapshot, kMaxSymbols * 4> position_pool_;
    xt_util::sync::XtSnapshotPool<FullSnapshot, 64> full_pool_;
    xt_util::sync::HazardDomain hp_domain_;

    std::atomic<const FullSnapshot*> current_{nullptr};

    std::atomic<bool> worker_bound_{false};
    std::thread::id   worker_tid_{};
    mutable std::atomic<std::thread::id> write_first_touch_tid_{};

    std::atomic<bool> worker_quiescent_{false};

    std::vector<std::string> symbols_sorted_;

    std::atomic<bool> symbols_frozen_{false};

    bool suppress_partial_publish_ = false;

    bool publish_dirty_ = false;

    alignas(64) std::atomic<int64_t> starting_balance_fp_{0};

    alignas(64) std::atomic<bool> closeable_known_{true};

    static constexpr std::size_t kMaxQuarantinedCodes = 32;
    mutable std::mutex quarantine_mu_;
    std::vector<std::string> quarantined_codes_;
    std::atomic<std::size_t> quarantined_count_{0};
    std::atomic<uint64_t> quarantined_overflow_{0};

    xt_account::XtPositionMarkInput make_mark_input(const char* code) const;

    xt_account::XtPositionMarkInput make_mark_input(
        const char* code, const AccountState::PositionReadout& readout) const;

    void on_event_before_apply(const XtAccountEvent& ev);

    void note_symbol_if_open_(const char* code, bool tracked);

    [[nodiscard]] bool is_in_universe_(std::string_view code) const noexcept;

    void quarantine_if_unknown_(const char* code, const char* event_tag);

    void guard_worker_thread_() const noexcept;

    void on_cross_thread_write_() const noexcept;

    void publish_partial_(const XtAccountEvent& ev) noexcept;

    void publish_all_snapshots_() noexcept;

    void publish_account_snapshot_() noexcept;

    void publish_event_snapshot_(std::string_view code) noexcept;

    [[nodiscard]] AccountSnapshot build_account_snapshot_() const noexcept;

    [[nodiscard]] PositionSnapshot build_position_snapshot_(std::string_view code) const noexcept;
};

}

namespace xt_account_store_drops {

std::atomic<uint64_t>& cross_thread_write();

std::atomic<uint64_t>& snapshot_publish_skipped();

#ifdef XT_BUILD_TESTS

inline void reset_for_test() {
    cross_thread_write().store(0, std::memory_order_relaxed);
    snapshot_publish_skipped().store(0, std::memory_order_relaxed);
}
#endif
}
