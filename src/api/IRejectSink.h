#pragma once

#include <cstdint>
#include <string_view>

namespace xt_core {
struct XtOrder;
}
struct XtRiskResult;

/**
 * @brief 拒单动作落地接口（reject sink）。
 *
 * @details
 * 仅接收领域输入（OrderRequest/RiskDecision），
 * 不暴露 live/backtest 专属 reason 类型。
 *
 * `strategy_id` 参数 (default empty)：multi-strategy live 部署下 sink 实现可
 * 按 sid 路由 reject 到对应 strategy 的 bridge。default empty =
 * global/single-strategy 路径。Backtest sink 忽略 sid。
 */
class IRejectSink {
public:
    virtual ~IRejectSink() = default;
    virtual void on_margin_reject(const xt_core::XtOrder& req,
                                  const XtRiskResult& decision,
                                  int64_t now_ms,
                                  std::string_view strategy_id = std::string_view{}) = 0;
    virtual void on_reject(const xt_core::XtOrder& req,
                           const XtRiskResult& decision,
                           std::string_view strategy_id = std::string_view{}) = 0;
};
