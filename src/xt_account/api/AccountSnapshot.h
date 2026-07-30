#pragma once

/// @file
/// @brief xt_account 对外发布的账户、持仓、风控快照 DTO。
///
/// DTO 均按值传递；跨线程读取由 AccountStateStore 的 hazard snapshot API
/// 负责生命周期保护。可平量投影 helper 见 api/AccountCloseableView.h；
/// hazard 发布帧（FullSnapshot）属 store 实现细节，见 state/detail/AccountFullSnapshot.h。

#include <cstdint>
#include <type_traits>

namespace xt_account {

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

static_assert(std::is_trivially_copyable<AccountSnapshot>::value,
              "AccountSnapshot must stay trivially copyable");
static_assert(std::is_trivially_copyable<PositionSnapshot>::value,
              "PositionSnapshot must stay trivially copyable");
static_assert(sizeof(AccountSnapshot) == 72, "AccountSnapshot size changed");
static_assert(sizeof(PositionSnapshot) == 128, "PositionSnapshot size changed");
static_assert(alignof(AccountSnapshot) == alignof(int64_t), "AccountSnapshot alignment changed");
static_assert(alignof(PositionSnapshot) == alignof(int64_t), "PositionSnapshot alignment changed");

}
