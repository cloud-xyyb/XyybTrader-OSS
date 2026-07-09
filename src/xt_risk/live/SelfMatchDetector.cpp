#include "SelfMatchDetector.h"

namespace xt_risk {

bool SelfMatchDetector::on_trade(const XtTradeRecord& tr) {
    if (tr.trade_id.empty()) return false;
    auto [it, inserted] = seen_trade_ids_.emplace(tr.trade_id, tr.order_id);
    if (inserted) return false;

    return it->second != tr.order_id;
}

void SelfMatchDetector::reset() noexcept {
    seen_trade_ids_.clear();
}

}
