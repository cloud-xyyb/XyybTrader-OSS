#include "SelfMatchDetector.h"

namespace xt_risk {

bool SelfMatchDetector::on_trade(const XtTradeRecord& tr) {
    if (tr.trade_id.empty()) return false;  // 回测或 trade_id 缺失场景跳过
    auto [it, inserted] = seen_trade_ids_.emplace(tr.trade_id, tr.order_id);
    if (inserted) return false;
    // 同一 trade_id 映射到不同 order_id 表示 post-trade self-match。
    return it->second != tr.order_id;
}

void SelfMatchDetector::reset() noexcept {
    seen_trade_ids_.clear();
}

}  // namespace xt_risk
