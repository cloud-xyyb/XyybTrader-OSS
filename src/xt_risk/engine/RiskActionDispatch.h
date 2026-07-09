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

inline bool is_margin_reject(const XtRiskResult& decision) {
    return decision.stage == XtRiskStage::MarginPrecheck ||
           decision.reason_code == XtRiskReasonCode::InsufficientMargin;
}

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

}
