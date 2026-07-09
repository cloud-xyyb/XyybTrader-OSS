#pragma once

#include <cstdint>
#include <string_view>

#include "api/XtLiveRejectReason.h"

namespace xt_core {
struct XtOrder;
}

/**
 * @brief Live 拒单输出端口（落地端口）。
 *
 * 由 XtLiveRejectSink 在适配层将 IRejectSink 的 XtRiskResult 转换
 * 为 XtLiveRejectReason 后调用此端口完成实际 IO 落地（告警/Prometheus/
 * 日志写入等）。
 *
 * `strategy_id` 参数 (default empty)：multi-strategy 部署下 gateway 实现按 sid
 * 解析对应 exec sink 路由 reject callback。default empty 走 runner-wide fallback
 * sink (global/single-strategy 路径)。
 */
class ILiveRejectOutput {
public:
    virtual ~ILiveRejectOutput() = default;
    virtual void publish_margin_reject(const xt_core::XtOrder& req,
                                       int64_t now_ms,
                                       std::string_view strategy_id = std::string_view{}) = 0;
    virtual void publish_reject_reason(XtLiveRejectReason code) = 0;
};
