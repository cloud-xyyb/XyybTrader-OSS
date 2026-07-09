#pragma once

/// @file
/// @brief xt_account 对外发布的账户、持仓、风控和可平量快照类型。
///
/// DTO 均按值传递；跨线程读取由 AccountStateStore 的 hazard snapshot API
/// 负责生命周期保护。

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "xt_core/order/XtOffset.h"
#include "api/XtRiskSnapshot.h"

namespace xt_account {

inline constexpr std::size_t kMaxSymbols = 100;
inline constexpr std::size_t kInvalidSymbolIndex = static_cast<std::size_t>(-1);

using RiskSnapshot = ::XtRiskSnapshot;

/// @brief AccountStateStore 发布的账户级资金快照。
///
/// 金额字段统一为 fixed-point int64；快照按值返回，读侧不持有 store
/// 内部对象生命周期。`frozen_by_orders` 与 `frozen_cash` 来自两条维护路径，
/// 供 frozen audit 比对同一发布边界下的账本一致性。
struct AccountSnapshot {
    int64_t cash = 0;
    int64_t frozen_cash = 0;
    int64_t available_cash = 0;
    int64_t equity = 0;
    int64_t unrealized_pnl = 0;
    int64_t realized_pnl = 0;
    int64_t fees = 0;
    int64_t market_value = 0;
    int64_t frozen_by_orders = 0;

};

/// @brief AccountStateStore 发布的单 symbol 持仓快照。
///
/// 数量、价格和盈亏字段统一为 fixed-point int64；broker frozen 字段来自
/// venue 原始冻结口径，缺失时保持 0。
///
/// @warning `closeable_known=false` 时，可平量字段只能展示，风控不得据此直接放行。
struct PositionSnapshot {
    int64_t qty = 0;
    int64_t long_qty = 0;
    int64_t short_qty = 0;
    int64_t long_today_qty = 0;
    int64_t short_today_qty = 0;
    int64_t avg_price = 0;
    int64_t mark_price = 0;
    int64_t unrealized_pnl = 0;
    int64_t available_sell_today_qty = 0;
    int64_t available_sell_total_qty = 0;
    int64_t available_cover_today_qty = 0;
    int64_t available_cover_total_qty = 0;
    int64_t broker_long_frozen_fp = 0;
    int64_t broker_short_frozen_fp = 0;
    bool has_today_split = false;
    bool closeable_known = true;
    int64_t broker_frozen_last_update_ms = 0;
};

/// @brief 账户级 hazard 发布帧，承载 account 快照和 per-symbol slot 表。
///
/// `tracked_codes` 按字典序固定；`position_slots` / `risk_slots` 与其同下标。
/// 本对象的 hazard guard 只保护容器和 slot 数组本身，不保护 slot 指向的
/// `PositionSnapshot` / `RiskSnapshot` 生命周期。
/// @warning 跨线程读取单合约快照必须走 AccountStateStore::snapshot_position() /
///          snapshot_risk()；这两个 API 会做二级 hazard protect 和 current re-confirm。
struct FullSnapshot {
    int64_t generation = 0;
    AccountSnapshot account{};
    mutable std::array<std::atomic<const PositionSnapshot*>, kMaxSymbols> position_slots{};
    mutable std::array<std::atomic<const RiskSnapshot*>, kMaxSymbols> risk_slots{};
    std::vector<std::string> tracked_codes;

    FullSnapshot() noexcept {
        reset_slots();
    }

    void reset_slots() noexcept {
        for (auto& slot : position_slots) {
            slot.store(nullptr, std::memory_order_relaxed);
        }
        for (auto& slot : risk_slots) {
            slot.store(nullptr, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] std::size_t find_symbol_idx(std::string_view code) const noexcept {
        const auto it = std::lower_bound(
            tracked_codes.begin(),
            tracked_codes.end(),
            code,
            [](const std::string& lhs, std::string_view rhs) {
                return std::string_view(lhs) < rhs;
            });
        if (it == tracked_codes.end() || std::string_view(*it) != code) {
            return kInvalidSymbolIndex;
        }
        const auto idx = static_cast<std::size_t>(std::distance(tracked_codes.begin(), it));
        return idx < kMaxSymbols ? idx : kInvalidSymbolIndex;
    }

    [[nodiscard]] const PositionSnapshot* find_position(std::string_view code) const noexcept {
        const auto idx = find_symbol_idx(code);
        if (idx == kInvalidSymbolIndex) {
            return nullptr;
        }
        return position_slots[idx].load(std::memory_order_acquire);
    }

    [[nodiscard]] const RiskSnapshot* find_risk(std::string_view code) const noexcept {
        const auto idx = find_symbol_idx(code);
        if (idx == kInvalidSymbolIndex) {
            return nullptr;
        }
        return risk_slots[idx].load(std::memory_order_acquire);
    }
};

static_assert(std::is_trivially_copyable<AccountSnapshot>::value,
              "AccountSnapshot must stay trivially copyable");
static_assert(std::is_trivially_copyable<PositionSnapshot>::value,
              "PositionSnapshot must stay trivially copyable");
static_assert(std::is_trivially_copyable<RiskSnapshot>::value,
              "RiskSnapshot must stay trivially copyable");
static_assert(sizeof(AccountSnapshot) == 72, "AccountSnapshot size changed");
static_assert(sizeof(PositionSnapshot) == 128, "PositionSnapshot size changed");

static_assert(sizeof(RiskSnapshot) == 96, "RiskSnapshot (=::XtRiskSnapshot) size changed");
static_assert(alignof(AccountSnapshot) == alignof(int64_t), "AccountSnapshot alignment changed");
static_assert(alignof(PositionSnapshot) == alignof(int64_t), "PositionSnapshot alignment changed");
static_assert(alignof(RiskSnapshot) == alignof(int64_t), "RiskSnapshot alignment changed");

/// @brief 单侧平仓可用量视图，由 PositionSnapshot 的 sell/cover 字段投影得到。
///
/// today_fp 与 total_fp 会 clamp 至非负；yesterday_fp = max(total_fp - today_fp, 0)。
struct CloseableView {
    int64_t today_fp     = 0;
    int64_t yesterday_fp = 0;
    int64_t total_fp     = 0;
};

inline int64_t clamp_qty_nonneg(int64_t v) noexcept {
    return v > 0 ? v : 0;
}

inline CloseableView make_closeable_view(int64_t today_raw, int64_t total_raw) noexcept {
    CloseableView out;
    out.today_fp = clamp_qty_nonneg(today_raw);
    out.total_fp = clamp_qty_nonneg(total_raw);
    out.yesterday_fp = out.total_fp > out.today_fp ? (out.total_fp - out.today_fp) : 0;
    return out;
}

inline CloseableView closeable_view(const PositionSnapshot& pos, bool is_buy_to_close) noexcept {
    return is_buy_to_close
        ? make_closeable_view(pos.available_cover_today_qty, pos.available_cover_total_qty)
        : make_closeable_view(pos.available_sell_today_qty,  pos.available_sell_total_qty);
}

inline int64_t closeable_for_offset(const CloseableView& v, XtOffset offset) noexcept {
    switch (offset) {
    case XtOffset::CloseToday:     return v.today_fp;
    case XtOffset::CloseYesterday: return v.yesterday_fp;
    case XtOffset::Close:
    default:                       return v.total_fp;
    }
}

static_assert(std::is_trivially_copyable<CloseableView>::value,
              "CloseableView must stay trivially copyable");

}
