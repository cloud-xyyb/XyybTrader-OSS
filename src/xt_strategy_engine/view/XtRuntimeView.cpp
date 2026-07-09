#include "XtRuntimeView.h"

#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "xt_core/order/XtOrderTypes.h"

namespace xt_strategy_engine {

const XtTick* XtRuntimeView::last_tick(const char* code) const {
    return market_->last_tick(code);
}

size_t XtRuntimeView::ticks(const char* code, XtTick* out, size_t max) const {
    return market_->ticks(code, out, max);
}

size_t XtRuntimeView::bars(const char* code, const char* period, XtBar* out, size_t max) {
    return market_->bars(code, period, out, max);
}

int64_t XtRuntimeView::last_price_fp(const char* code) const {
    return market_->last_price_fp(code);
}

bool XtRuntimeView::contract_info(const char* code, XtStraContractInfo& out) const {
    if (!catalog_) {
        out = XtStraContractInfo{};
        return false;
    }
    const XtCommodityInfo* info = catalog_->find(code);
    if (!info) {
        out = XtStraContractInfo{};
        return false;
    }
    out = *info;
    return true;
}

void XtRuntimeView::set_bar_capacity(const char* code, const char* period, size_t max) {
    if (market_) market_->set_bar_capacity(code, period, max);
}

size_t XtRuntimeView::open_orders(XtOrderExecUpdate* out, size_t max,
                                  uint32_t sid_filter) const {

    return order_book_ ? order_book_->collect_open_orders(out, max, nullptr, sid_filter) : 0;
}

size_t XtRuntimeView::open_orders(const char* code, XtOrderExecUpdate* out, size_t max,
                                  uint32_t sid_filter) const {
    return order_book_ ? order_book_->collect_open_orders(out, max, code, sid_filter) : 0;
}

int64_t XtRuntimeView::undone_qty_fp(const char* code) const {
    if (!order_book_) return 0;
    return order_book_->undone_qty_fp(code);
}

int64_t XtRuntimeView::signed_inflight_fp(StrategySid sid, const char* code) const {
    return order_book_ ? order_book_->signed_inflight_fp(sid, code) : 0;
}

int64_t XtRuntimeView::pos_qty_fp(const char* code) const {
    return account_->pos_qty_fp(code);
}

int64_t XtRuntimeView::pos_avg_fp(const char* code) const {
    return account_->pos_avg_fp(code);
}

int64_t XtRuntimeView::pos_unrealized_fp(const char* code) const {
    return account_->pos_unrealized_fp(code);
}

bool XtRuntimeView::pos_detail(const char* code, XtStraPositionDetail& out) const {
    if (!account_->pos_detail(code, out)) return false;

    if (order_book_) {
        out.long_side.open_no_trade  = order_exec_from_fp(order_book_->open_no_trade_qty_fp(code, true));
        out.short_side.open_no_trade = order_exec_from_fp(order_book_->open_no_trade_qty_fp(code, false));
    }
    return true;
}

int64_t XtRuntimeView::cash_fp() const {
    return account_->cash_fp();
}

int64_t XtRuntimeView::equity_fp() const {
    return account_->equity_fp();
}

}
