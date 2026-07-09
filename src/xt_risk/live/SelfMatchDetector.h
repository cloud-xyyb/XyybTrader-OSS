#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "xt_core/order/XtTradeRecord.h"

namespace xt_risk {

/// @brief 基于成交编号（trade_id）的 post-trade 自成交兜底检测器。
///
/// 同一非空 `trade_id` 若先后绑定到两个不同 `order_id`，说明交易所一笔成交
/// 同时命中了本地两笔订单，调用方应按 self-match 处理。空 `trade_id` 不参与检测：
/// 回测、加密或 broker 缺字段场景保持 fail-open，避免把不可判定路径误判为自成交。
///
/// @warning 非线程安全；必须由单一事件线程按成交回报顺序调用。
/// @note 状态按交易 session 累积；session_begin 必须调用 reset() 清空已记录 trade_id。
class SelfMatchDetector {
public:

    bool on_trade(const XtTradeRecord& tr);

    void reset() noexcept;

    std::size_t size() const noexcept { return seen_trade_ids_.size(); }

private:

    std::unordered_map<std::string, uint64_t> seen_trade_ids_;
};

}
