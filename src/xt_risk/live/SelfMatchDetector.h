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
    /// @brief 观察一条成交回报并更新当前 session 的 trade_id 索引。
    /// @param tr 只读取 `trade_id` 与 `order_id`；其他成交字段不参与判定。
    /// @return true 表示检测到 self-match；调用方应触发对应合约黑名单或等价风控动作。
    /// @note 非 `noexcept`：内部 `unordered_map::emplace` 分配失败会抛 `std::bad_alloc`。
    ///       调用方（worker 线程）必须在异常 guard 内调用，让 bad_alloc 升级为 fail-stop，
    ///       而非 `std::terminate`（`noexcept` 下抛异常会直接终止进程）。
    bool on_trade(const XtTradeRecord& tr);

    /// @brief 清空当前 session 的 trade_id 索引，通常在 session_begin 调用。
    void reset() noexcept;

    /// @brief 当前 session 已记录的非空 trade_id 数；仅用于诊断和测试。
    std::size_t size() const noexcept { return seen_trade_ids_.size(); }

private:
    // 当前 session 的成交编号索引；value 固定为首次观测到的本地 order_id，
    // 用于把同一订单的重复成交回报与跨订单 self-match 区分开。
    std::unordered_map<std::string, uint64_t> seen_trade_ids_;
};

}  // namespace xt_risk
