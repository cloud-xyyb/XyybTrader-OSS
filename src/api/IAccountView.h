#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "api/XtStrategyRuntimeTypes.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_account/api/AccountCloseableView.h"
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

    /// @brief 账户侧已登记的合约列表。
    /// @warning 成员面是账户实现的内部登记集，**不是**「可交易 symbol 全集」：调用方
    ///          不得假设返回值覆盖全部 declared symbol，也不得把「不在返回值里」当作
    ///          「该 symbol 不可交易 / 无需处理」。需要遍历可交易集合的调用方按 config
    ///          声明的 universe 枚举，再逐 code 查 position()。
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

    /// @brief 一次性读取 cash/equity/unrealized（同一账户快照保证一致性）。
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
    // 可平今昨走 xt_account::closeable_view 单源：手写 `total - today` 在 T+1 账本下
    // 会把今仓减两次（total 本身已扣今仓），昨仓低估、今仓虚报可平。下游 stop 逻辑
    // 把两分量相加当总可平量用，故拆分必须保持 today + yesterday == venue total。
    const auto long_close  = xt_account::closeable_view(snap, /*is_buy_to_close=*/false);
    const auto short_close = xt_account::closeable_view(snap, /*is_buy_to_close=*/true);
    out.long_side.position = order_exec_from_fp(snap.long_qty);
    out.long_side.today = order_exec_from_fp(snap.long_today_qty);
    out.long_side.yesterday = out.long_side.position - out.long_side.today;
    out.long_side.closeable_today = order_exec_from_fp(long_close.today_fp);
    out.long_side.closeable_yesterday = order_exec_from_fp(long_close.yesterday_fp);
    out.long_side.has_today_split = snap.has_today_split;
    out.short_side.position = order_exec_from_fp(snap.short_qty);
    out.short_side.today = order_exec_from_fp(snap.short_today_qty);
    out.short_side.yesterday = out.short_side.position - out.short_side.today;
    out.short_side.closeable_today = order_exec_from_fp(short_close.today_fp);
    out.short_side.closeable_yesterday = order_exec_from_fp(short_close.yesterday_fp);
    out.short_side.has_today_split = snap.has_today_split;
    return true;
}

} // namespace xt_runtime
