#pragma once

#include "api/ILiveRejectOutput.h"
#include "api/IRejectSink.h"
#include "api/XtLiveRejectReason.h"

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

/// @file
/// @brief live submit reject 的日志抑制与输出适配。
///
/// `XtLiveRejectSink` 接收 `IRejectSink` 回调，把 `XtRiskResult` 映射到
/// `ILiveRejectOutput`；`XtLiveRejectLogThrottle` 在 reject 慢路径按
/// `(strategy, reason)` 抑制重复日志。

namespace xt_core {
struct XtOrder;
}
struct XtRiskResult;

/// @brief 拒单日志窗口抑制器：同 (sid, reason) 在窗口内只放行首条完整日志。
///
/// 放行时带出上一窗口被抑制的条数，由日志行以 suppressed_since_last 字段补齐聚合计数。
/// 本类只在 reject 慢路径触达，单 mutex 足够；key 空间 = 策略数 × reason 数，天然有界。
/// 时钟由调用方注入（steady µs），便于单测控制窗口推进。
class XtLiveRejectLogThrottle {
public:
    static constexpr uint64_t kWindowUs = 5'000'000;

    bool should_log(std::string_view strategy_id,
                    int reason_code,
                    uint64_t now_us,
                    uint64_t& out_suppressed);

private:
    struct Window {
        uint64_t last_log_us = 0;
        uint64_t suppressed = 0;
    };
    std::mutex mu_;
    std::map<std::pair<std::string, int>, Window> windows_;
};

/// @brief live 风控拒单输出适配器。
///
/// `RiskActionDispatch` 将拒单送入本 sink；本类负责记录限流后的日志、映射 live
/// reject bucket，并调用 `ILiveRejectOutput` 发布给 submit 结果/事件通道。
/// 对象不拥有 output，调用方必须保证 output 生命周期覆盖本 sink。
class XtLiveRejectSink final : public IRejectSink {
public:
    explicit XtLiveRejectSink(ILiveRejectOutput& output);

    void on_margin_reject(const xt_core::XtOrder& req,
                          const XtRiskResult& decision,
                          int64_t now_ms,
                          std::string_view strategy_id = std::string_view{}) override;

    void on_reject(const xt_core::XtOrder& req,
                   const XtRiskResult& decision,
                   std::string_view strategy_id = std::string_view{}) override;

    [[nodiscard]] XtLiveRejectReason first_reject_reason() const noexcept {
        return first_reject_;
    }

private:

    ILiveRejectOutput& output_;

    XtLiveRejectReason first_reject_ = XtLiveRejectReason::Unknown;

    void note_first_(XtLiveRejectReason r) noexcept {
        if (first_reject_ == XtLiveRejectReason::Unknown) first_reject_ = r;
    }
};
