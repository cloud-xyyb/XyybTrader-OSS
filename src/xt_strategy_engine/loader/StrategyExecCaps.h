#pragma once

/// @file StrategyExecCaps.h
/// @brief 策略 capability 与部署形态的启动期 gate。
///
/// 判断通道是否符合 request 的要求

#include <cstdio>
#include <string>

#include "xt_strategy_engine/XtStrategyBase.h"

namespace xt_strategy_engine {

/// @brief 当前部署形态无法兑现 exec sink 回调时的 capability gate 判定。
inline bool exec_sink_caps_violation(uint64_t caps) {
    return (caps & kExecSinkRequiredCaps) != 0;
}

/// @brief gate 命中时的统一拒绝文案（deploy_desc 描述部署形态，如
///        "multi-strategy mode" / "multi-strategy tick-mode BT"）。
inline std::string exec_sink_caps_error(const std::string& sid, uint64_t caps,
                                          const char* deploy_desc) {
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "strategy id=%s declares caps=0x%llx requiring exec sink callback path "
                  "but %s has exec_sink=nullptr; on_order/on_fill/on_account would be "
                  "silently dropped. abort.",
                  sid.c_str(),
                  static_cast<unsigned long long>(caps & kExecSinkRequiredCaps),
                  deploy_desc);
    return std::string(buf);
}

}  // namespace xt_strategy_engine
