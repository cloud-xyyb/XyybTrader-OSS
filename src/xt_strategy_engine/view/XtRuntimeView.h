#pragma once

#include <cassert>
#include <cstdint>
#include <string>

#include "xt_runtime/market/XtRuntimeMarketCache.h"
#include "api/IAccountView.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/market/XtBarStruct.h"
#include "api/XtStrategyRuntimeTypes.h"

#include "api/StrategySid.h"

class XtCommodityCatalog;
namespace xt_runtime::order {
class XtOrderRuntimeBook;
}

namespace xt_strategy_engine {

using XtStraContractInfo  = ::xt_runtime::XtStraContractInfo;
using XtStraPositionDetail = ::xt_runtime::XtStraPositionDetail;

/// @brief 策略查询行情、账户和在途单的统一只读 view
///
/// 策略回调里的所有查询（bars/持仓/资金/open orders）都通过 XtRuntimeView
/// HFT/CTA/回测共用，差异由构造期注入的 market/account/order 组件决定。
class XtRuntimeView final {
public:
    struct Components {
        ::xt_runtime::XtRuntimeMarketCache*            market = nullptr;
        const ::xt_runtime::IAccountView*              account = nullptr;
        const ::xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;
        const XtCommodityCatalog*                      catalog = nullptr;
    };

    explicit XtRuntimeView(const Components& comp)
        : market_(comp.market)
        , account_(comp.account)
        , order_book_(comp.order_book)
        , catalog_(comp.catalog) {
        assert(market_ && "XtRuntimeView requires non-null market");
        assert(account_ && "XtRuntimeView requires non-null account");
    }

    void set_clock(uint32_t date, uint32_t hhmm,
                   uint32_t action_time_packed, uint32_t trading_date) {
        date_               = date;
        hhmm_               = hhmm;
        action_time_packed_ = action_time_packed;
        trading_date_       = trading_date;
    }

    void set_clock(uint32_t date, uint32_t time_ms, uint32_t trading_date) {
        date_               = date;
        hhmm_               = time_ms / 100000;
        action_time_packed_ = time_ms;
        trading_date_       = trading_date;
    }

    const XtTick* last_tick(const char* code) const;

    size_t ticks(const char* code, XtTick* out, size_t max) const;

    size_t bars(const char* code, const char* period, XtBar* out, size_t max);

    int64_t last_price_fp(const char* code) const;

    bool contract_info(const char* code, XtStraContractInfo& out) const;

    void set_bar_capacity(const char* code, const char* period, size_t max);

    ::xt_runtime::XtRuntimeMarketCache* market_cache() { return market_; }

    const ::xt_runtime::IAccountView* account_view_ptr() const { return account_; }

    size_t open_orders(XtOrderExecUpdate* out, size_t max, uint32_t sid_filter = 0) const;

    size_t open_orders(const char* code, XtOrderExecUpdate* out, size_t max,
                       uint32_t sid_filter = 0) const;

    int64_t undone_qty_fp(const char* code) const;

    int64_t signed_inflight_fp(StrategySid sid, const char* code) const;

    int64_t pos_qty_fp(const char* code) const;

    int64_t pos_avg_fp(const char* code) const;

    int64_t pos_unrealized_fp(const char* code) const;

    bool pos_detail(const char* code, XtStraPositionDetail& out) const;

    int64_t cash_fp() const;

    int64_t equity_fp() const;

    uint32_t date() const { return date_; }
    uint32_t trading_date() const { return trading_date_; }
    uint32_t hhmm() const { return hhmm_; }

    uint32_t time_ms() const { return action_time_packed_; }

private:

    ::xt_runtime::XtRuntimeMarketCache*            market_ = nullptr;
    const ::xt_runtime::IAccountView*              account_ = nullptr;
    const ::xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr;
    const XtCommodityCatalog*                      catalog_ = nullptr;

    uint32_t date_               = 0;
    uint32_t hhmm_               = 0;
    uint32_t action_time_packed_ = 0;
    uint32_t trading_date_       = 0;
};

}
