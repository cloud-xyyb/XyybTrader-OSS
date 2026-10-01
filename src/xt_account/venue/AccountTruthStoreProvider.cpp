#include "xt_account/venue/AccountTruthStoreProvider.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>

#include "XtFixedPoint.h"
#include "api/XtRunnerSignals.h"
#include "xt_account/state/AccountStateStore.h"

namespace {

uint64_t wall_clock_now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

}  // namespace

namespace xt_account {

AccountTruth read_store_account_truth(const AccountStateStore& account_store,
                                      XtRunnerSignals& signals,
                                      xt_core::TimezonePolicy trading_day_policy,
                                      bool fill_store_per_symbol) {
    AccountTruth truth;
    truth.ts_ns = wall_clock_now_ns();
    const auto account = account_store.snapshot_account();
    truth.wallet_balance_fp = account.cash;
    truth.unrealized_pnl_fp = account.unrealized_pnl;
    truth.fees_today_fp = account.fees;

    // 账户级持仓市值直接取同一 AccountSnapshot 的聚合值：与 unrealized 同源同帧，
    // 且含有 mark id 的 quarantined 仓（tracked_codes 只覆盖 slot 持有者，按它求和
    // 会漏计）。per-symbol 行仍按 tracked 枚举（展示镜像，非账户级真值）。
    const int64_t position_value_fp = account.market_value;
    const auto tracked_codes = account_store.snapshot_tracked_codes();
    truth.runtime_per_symbol_marks_fp.reserve(tracked_codes.size());
    for (const auto& code : tracked_codes) {
        if (code.empty()) continue;
        const auto pos = account_store.snapshot_position(code);
        // per-symbol 展示镜像保留逐 code 循环；账户级 position_value_fp 已改取聚合值。
        // account_store mark 与 venue truth mark 分开上报，避免 mark_cache 置零时
        // 只能看到聚合权益漂移。
        truth.runtime_per_symbol_marks_fp.emplace_back(code, pos.mark_price);
        // CTP/XTP 等无 REST venue per-symbol truth 的路径可用 account_store 活值
        // 填展示镜像；该镜像不是 broker truth，不参与 store-vs-broker 背离比对。
        if (fill_store_per_symbol && (pos.qty != 0 || pos.unrealized_pnl != 0)) {
            PerSymbolTruth row{};
            row.symbol = code;
            row.qty_fp = pos.qty;
            row.avg_price_fp = pos.avg_price;
            row.mark_price_fp = pos.mark_price;
            row.unrealized_pnl_fp = pos.unrealized_pnl;
            truth.per_symbol.push_back(std::move(row));
        }
    }
    truth.position_value_fp = position_value_fp;

    int64_t trading_day = signals.last_tick_date.load(std::memory_order_acquire);
    if (trading_day <= 0) {
        const int64_t epoch_ms = static_cast<int64_t>(truth.ts_ns / 1'000'000ULL);
        trading_day = static_cast<int64_t>(
            xt_core::derive_trading_day(epoch_ms, trading_day_policy));
    }
    truth.trading_day_id = trading_day;

    const int64_t cashflow_day =
        signals.latest_external_cashflow_trading_day.load(std::memory_order_acquire);
    if (cashflow_day == 0 || cashflow_day == trading_day) {
        truth.external_cashflow_today_fp =
            signals.latest_external_cashflow_today_fp.load(std::memory_order_acquire);
    }
    return truth;
}

}  // namespace xt_account
