#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "api/IRejectSink.h"
#include "api/XtRiskTypes.h"

/// @file
/// @brief `XtRiskResult` 到 `IRejectSink` 的拒单路由 helper。
///
/// 本文件提供无状态 inline helper；日志、指标和事件发布等副作用由传入的
/// `IRejectSink` 实现负责。
namespace risk_action {

/// @brief 判断拒单是否属于保证金类拒绝（margin-class reject）。
///
/// `MarginPrecheck` 来自 submit-resource 阶段的现金/保证金检查；
/// `InsufficientMargin` 来自 CoreRule 阶段的静态保证金规则。两者共享
/// `on_margin_reject` 路径，因为下游需要同一组 margin diagnostic fields 和拒绝时间戳。
inline bool is_margin_reject(const XtRiskResult& decision) {
    return decision.stage == XtRiskStage::MarginPrecheck ||
           decision.reason_code == XtRiskReasonCode::InsufficientMargin;
}

/// @brief 将 submit 拒绝结果路由到对应的 IRejectSink 接口。
///
/// @param now_ms 拒绝发生时间戳（ms）；传给 `on_margin_reject`。
/// @param strategy_id 策略归属；empty 表示 global/single-strategy 路径。multi-strategy live
///                    sink 可按该值路由到对应 strategy bridge，backtest sink 可忽略。
inline void dispatch_reject(IRejectSink& sink,
                            const xt_core::XtOrder& req,
                            const XtRiskResult& decision,
                            int64_t now_ms,
                            std::string_view strategy_id = std::string_view{}) {
    if (is_margin_reject(decision)) {
        sink.on_margin_reject(req, decision, now_ms, strategy_id);
        return;
    }
    sink.on_reject(req, decision, strategy_id);
}

}  // namespace risk_action
