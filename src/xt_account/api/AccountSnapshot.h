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
///
/// @warning **新鲜度契约（无硬实时上界）**：行情（MarketTick）不触发发布，只更新
///          store 内部的 mark 表。因此 `equity` / `unrealized_pnl` / `market_value`
///          这些随行情变动的字段**不是 tick 级实时值**：它们在持仓/资金事件的即时
///          发布上刷新，此外由控制线程每秒一次的兜底发布请求推进。目标节拍 1s、
///          典型滞后 ≤2 个控制帧周期；无上界保证，实际滞后由 `frame_age`
///          （store 的 `last_publish_steady_ms()`）观测、超阈值告警兜底。
///          需要「读到不早于此刻的帧」的调用方（如 session 起点冻结基准）必须走
///          `AccountStateStore::request_publish()` + ticket 等待，不能假设直读即最新。
struct AccountSnapshot {
    int64_t cash = 0;
    int64_t frozen_cash = 0;
    int64_t available_cash = 0;
    int64_t equity = 0;
    int64_t unrealized_pnl = 0;
    int64_t realized_pnl = 0;
    int64_t fees = 0;
    int64_t market_value = 0;
    int64_t frozen_by_orders = 0;  ///< 订单冻结 ledger 影子总额（≡ Σ slot.frozen 于同一 publish 边界）；
                                   ///< 与 frozen_cash 分源维护，两者分歧 = frozen audit 不一致信号。
};

/// @brief AccountStateStore 发布的单 symbol 持仓快照。
///
/// 数量、价格和盈亏字段统一为 fixed-point int64；broker frozen 字段来自
/// venue 原始冻结口径，缺失时保持 0。
///
/// @warning `closeable_known=false` 时，可平量字段只能展示，风控不得据此直接放行。
/// @note `available_*_today` 是该方向的今仓**持仓量**（≡ `long_today_qty` /
///       `short_today_qty`），不是今仓可平量——T+1 账本下当日买入不可卖。今昨可平量
///       的唯一拆分入口是 api/AccountCloseableView.h 的 `closeable_view()`；
///       `total − today` 不是合法拆分（T+1 下今仓被减两次）。
struct PositionSnapshot {
    int64_t qty = 0;              ///< 净持仓，多头为正、空头为负。
    int64_t long_qty = 0;
    int64_t short_qty = 0;
    int64_t long_today_qty = 0;
    int64_t short_today_qty = 0;
    int64_t avg_price = 0;
    int64_t mark_price = 0;       ///< 估值口径（venue 优先、次成本价）；随帧刷新，非 tick 级实时——
                                  ///< 新鲜度见 AccountSnapshot 的契约警告。出单定价禁用本字段，
                                  ///< 唯一合法来源是 `AccountStateStore::venue_mark_price()`。
    int64_t unrealized_pnl = 0;
    int64_t available_sell_today_qty = 0;     ///< 多头今仓持仓量；口径见类注 @note。
    int64_t available_sell_total_qty = 0;     ///< 多头总可平量（venue 口径权威）；T+1 账本下已扣今仓。
    int64_t available_cover_today_qty = 0;    ///< 空头今仓持仓量；口径见类注 @note。
    int64_t available_cover_total_qty = 0;    ///< 空头总可平量（venue 口径权威）；T+1 账本下已扣今仓。
    int64_t broker_long_frozen_fp = 0;        ///< broker 已冻结的多头平仓量；CTP LongFrozen，单位 fp。
    int64_t broker_short_frozen_fp = 0;       ///< broker 已冻结的空头平仓量；CTP ShortFrozen，单位 fp。
    bool has_today_split = false;             ///< true 表示 long_today/short_today 可用于今昨仓拆单。
    bool closeable_known = true;              ///< false 表示可平量未知；风控不得按本快照放行。
    int64_t broker_frozen_last_update_ms = 0; ///< broker frozen 的 steady-ms 更新时间；0 表示缺失。
};

// Layout 受 hazard snapshot / 测试 ABI 约束保护；新增字段必须同步审视 size guard。
static_assert(std::is_trivially_copyable<AccountSnapshot>::value,
              "AccountSnapshot must stay trivially copyable");
static_assert(std::is_trivially_copyable<PositionSnapshot>::value,
              "PositionSnapshot must stay trivially copyable");
static_assert(sizeof(AccountSnapshot) == 72, "AccountSnapshot size changed");
static_assert(sizeof(PositionSnapshot) == 128, "PositionSnapshot size changed");
static_assert(alignof(AccountSnapshot) == alignof(int64_t), "AccountSnapshot alignment changed");
static_assert(alignof(PositionSnapshot) == alignof(int64_t), "PositionSnapshot alignment changed");

}  // namespace xt_account
