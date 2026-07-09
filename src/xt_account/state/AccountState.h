#pragma once

#include <string>
#include <unordered_map>
#include <cstdint>
#include <memory>
#include "xt_config/catalog/XtCommodityCatalog.h"

struct XtAccountEvent;

namespace xt_account {
class AccountStateStore;
}

/// @brief reducer 背后的可变账户账本对象。
///
/// 保存账户资金、持仓表和订单冻结 ledger；不负责线程同步、snapshot 发布或
/// mark price 管理。写入口只暴露给 `apply_event()`，调用方通过
/// `AccountStateStore` 提交事件并读取账户/持仓/风控快照。
///
/// @note 非线程安全；live 路径由 AccountStateStore worker 线程串行写入，再发布
///       immutable snapshot 给读侧。
class AccountState {
public:
    /// @brief AccountState 构造配置。
    struct Config {
        double initial_cash = 1000000.0;
        bool t1_enabled = false;
        bool futures_mode = false;

        const XtCommodityCatalog* catalog = nullptr;
    };

    /// @brief 内部构造令牌；限制 AccountState 只能由 AccountStateStore 创建。
    struct CtorToken {
    private:
        CtorToken() = default;
        friend class xt_account::AccountStateStore;
    };

    /// @brief 单订单冻结资金 ledger entry，用于维护账户级 frozen_cash 不变量。
    ///
    /// 该结构不表达订单生命周期；订单状态归 XtOrderRuntimeBook。这里仅记录
    /// AccountState 释放冻结保证金所需的剩余冻结额、已处理成交量和每手保证金。
    struct FrozenOrderSlot {
        int64_t frozen   = 0;
        int64_t filled   = 0;
        int64_t per_unit = 0;
    };

    explicit AccountState(const Config& cfg, CtorToken);

    [[nodiscard]] int64_t cash() const;

    [[nodiscard]] int64_t frozen_cash() const;

    [[nodiscard]] int64_t frozen_by_orders() const;

    [[nodiscard]] int64_t available_cash() const;

    [[nodiscard]] int64_t realized_pnl() const;

    [[nodiscard]] int64_t fees() const;

    [[nodiscard]] bool futures_mode() const;

    [[nodiscard]] const XtCommodityCatalog* commodity_catalog() const;

    const std::unordered_map<uint64_t, FrozenOrderSlot>& order_slots() const;

    [[nodiscard]] bool has_today_split() const;

    /// @brief position_readout() 返回的单合约持仓只读视图。
    ///
    /// 包含内部 Position 的数量、均价、broker frozen 原始字段，也带出净持仓、
    /// 净方向均价和今昨可平量等派生口径。调用方拿到的是值拷贝，不持有内部
    /// Position 指针；未知 code 返回全默认值。
    struct PositionReadout {
        int64_t qty = 0;
        int64_t avg_price = 0;
        int64_t long_qty = 0;
        int64_t long_avg_price = 0;
        int64_t short_qty = 0;
        int64_t short_avg_price = 0;
        int64_t long_today_qty = 0;
        int64_t short_today_qty = 0;
        int64_t broker_long_frozen_fp = 0;
        int64_t broker_short_frozen_fp = 0;
        int64_t broker_frozen_last_update_ms = 0;
        int64_t available_sell_today_qty = 0;
        int64_t available_sell_total_qty = 0;
        int64_t available_cover_today_qty = 0;
        int64_t available_cover_total_qty = 0;
    };

    [[nodiscard]] PositionReadout position_readout(const char* code) const;

private:

    friend void apply_event(AccountState& account, const XtAccountEvent& ev);

    struct Position {
        int64_t long_qty       = 0;
        int64_t long_today     = 0;
        int64_t long_avg_price = 0;
        int64_t short_qty      = 0;
        int64_t short_today    = 0;
        int64_t short_avg_price= 0;
        int64_t broker_long_frozen_fp  = 0;
        int64_t broker_short_frozen_fp = 0;
        int64_t broker_frozen_last_update_ms = 0;
    };

    Position& get_pos(const char* code);

    const Position* find_pos(const char* code) const;

    static int64_t avg_price_of_(const Position& p);

    static int64_t available_sell_of_(const Position& p, bool is_today, bool t1_enabled);

    static int64_t available_cover_of_(const Position& p, bool is_today, bool t1_enabled);

    void on_session_begin(uint32_t action_date);

    void reset_positions();

    void set_cash(int64_t cash);

    void set_position(const char* code,
                      int64_t long_qty,
                      int64_t long_avg_price,
                      int64_t short_qty,
                      int64_t short_avg_price,
                      int64_t long_today = 0,
                      int64_t short_today = 0,
                      int64_t broker_long_frozen_fp  = 0,
                      int64_t broker_short_frozen_fp = 0,
                      int64_t broker_frozen_last_update_ms = 0);

    void on_order_accepted(uint64_t order_id, int64_t required_margin, int64_t per_unit_margin);

    void on_order_update(uint64_t order_id, int64_t filled, int64_t remaining, bool is_final);

    void on_daily_settlement(int64_t realized_delta, int64_t settlement_fee);

    void on_cash_transfer(int64_t amount);

    void on_trade_ex(const char* code,
                     bool is_buy,
                     int64_t qty,
                     int64_t price,
                     int64_t fee,
                     bool is_close,
                     bool is_today);

    int64_t cash_ = 0;
    int64_t frozen_cash_ = 0;
    int64_t frozen_by_orders_ = 0;
    int64_t realized_pnl_ = 0;
    int64_t fees_ = 0;
    bool t1_enabled_ = false;
    bool futures_mode_ = false;
    uint32_t last_action_date_ = 0;

    std::unordered_map<std::string, Position> pos_;

    const XtCommodityCatalog* catalog_ = nullptr;

    std::unordered_map<uint64_t, FrozenOrderSlot> order_slots_;
};
