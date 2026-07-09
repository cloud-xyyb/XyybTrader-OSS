#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "api/XtStrategyRuntimeTypes.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_account/api/AccountSnapshot.h"

namespace xt_runtime {

/// @brief 账户级快照（cash/equity/unrealized 一次性读取）。
struct XtAccountSnapshotFp {
    int64_t cash = 0;
    int64_t equity = 0;
    int64_t unrealized_pnl = 0;
};

/**
 * @brief 账户视图接口（position/account read model）。
 */
class IAccountView {
public:
    virtual ~IAccountView() = default;

    [[nodiscard]] virtual xt_account::AccountSnapshot account() const {
        return {};
    }

    [[nodiscard]] virtual xt_account::PositionSnapshot position(std::string_view code) const {
        (void)code;
        return {};
    }

    [[nodiscard]] virtual xt_account::RiskSnapshot risk(std::string_view code) const {
        (void)code;
        return {};
    }

    [[nodiscard]] virtual std::vector<std::string> tracked_codes() const {
        return {};
    }

    [[nodiscard]] virtual int64_t pos_qty_fp(const char* code) const {
        return position(code ? std::string_view(code) : std::string_view{}).qty;
    }

    [[nodiscard]] virtual int64_t pos_avg_fp(const char* code) const {
        return position(code ? std::string_view(code) : std::string_view{}).avg_price;
    }

    [[nodiscard]] virtual int64_t pos_unrealized_fp(const char* code) const {
        return position(code ? std::string_view(code) : std::string_view{}).unrealized_pnl;
    }

    [[nodiscard]] virtual bool pos_detail(const char* code, XtStraPositionDetail& out) const;

    [[nodiscard]] virtual int64_t cash_fp() const {
        return account().cash;
    }

    [[nodiscard]] virtual int64_t equity_fp() const {
        return account().equity;
    }

    [[nodiscard]] virtual int64_t unrealized_fp() const {
        return account().unrealized_pnl;
    }

    [[nodiscard]] virtual XtAccountSnapshotFp account_snapshot_fp() const {
        const auto snap = account();
        return {snap.cash, snap.equity, snap.unrealized_pnl};
    }
};

inline bool IAccountView::pos_detail(const char* code, XtStraPositionDetail& out) const {
    if (!code || code[0] == '\0') {
        return false;
    }
    const auto snap = position(code);
    if (snap.long_qty == 0 && snap.short_qty == 0) {
        return false;
    }
    out = {};
    out.long_side.position = order_exec_from_fp(snap.long_qty);
    out.long_side.today = order_exec_from_fp(snap.long_today_qty);
    out.long_side.yesterday = out.long_side.position - out.long_side.today;
    out.long_side.closeable_today = order_exec_from_fp(snap.available_sell_today_qty);
    out.long_side.closeable_yesterday = order_exec_from_fp(
        snap.available_sell_total_qty - snap.available_sell_today_qty);
    out.long_side.has_today_split = snap.has_today_split;
    out.short_side.position = order_exec_from_fp(snap.short_qty);
    out.short_side.today = order_exec_from_fp(snap.short_today_qty);
    out.short_side.yesterday = out.short_side.position - out.short_side.today;
    out.short_side.closeable_today = order_exec_from_fp(snap.available_cover_today_qty);
    out.short_side.closeable_yesterday = order_exec_from_fp(
        snap.available_cover_total_qty - snap.available_cover_today_qty);
    out.short_side.has_today_split = snap.has_today_split;
    return true;
}

}
