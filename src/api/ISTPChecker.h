#pragma once

namespace xt_core {
struct XtOrder;
}

/**
 * @brief 自成交防护检查接口（Self-Trade Prevention checker）。
 *
 * @details
 * `would_self_trade()` 在 submit 热路径、订单被接受前执行。实现必须
 * non-blocking，目标复杂度为 O(1) 或 O(log n)；不得在热路径扫描全量
 * open orders。
 *
 * @warning 实现必须在订单状态更新（submit/ack/fill/cancel/terminal）并发发生时
 * 保持一致性，不得观察到部分更新的 price bound 或 open-order state。
 */
class ISTPChecker {
public:
    virtual ~ISTPChecker() = default;

    /**
     * @brief 检查该订单是否会与本账户已有挂单发生自成交。
     *
     * @param req 即将提交的委托请求。
     * @return true = 存在自成交风险，应拒绝该单。
     *
     * @note 实现应为 O(1) 或 O(log n) 级别，不得阻塞热路径。
     */
    [[nodiscard]] virtual bool would_self_trade(const xt_core::XtOrder& req) const = 0;
};
