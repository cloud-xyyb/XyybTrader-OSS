cmake_minimum_required(VERSION 3.5)
project(XtLiveControlLib LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# ── xt_live_control: 跨引擎 runner 控制操作 lib ────────────────────────────
# PanelHelpers（cancel_all / flatten_all / force_exit / manual_order 等控制操作
# + wire_per_strategy_risk_state / overlap 校验装配 helper）与 XtKillSwitchState
# （kill-switch trip 因果链 + audit log）的唯一编译点（TU 单一所有权）。
# 调用方：3 个 runner composition root + HftRunnerControl（closeout/watchdog
# 非 gRPC 路径）+ xt_panel（gRPC handler 绑定层，单向 xt_panel → 本 lib）。
# 注：HFT 专属 runner control（HftRunnerControl / closeout / reconcile / emit）已归位
#     xt_live/hft/；本目录另留 CloseoutPricing.h + XtRunnerControlCfg.h 作 shared 定价/配置面 header。
add_library(xt_live_control STATIC
    AdvanceRiskFrame.cpp
    PanelHelpers.cpp
    RunnerFrameLoop.cpp
    XtKillSwitchState.cpp
)

target_include_directories(xt_live_control PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
)

# xt_live_execution PUBLIC：XtLiveOrderGateway.h 及其 transitive include/link
# （xt_runtime / xt_account / xt_risk / xt_paramdomain / xt_live_telemetry）。
target_link_libraries(xt_live_control PUBLIC
    xt_live_execution
    xt_live_common     # transitive include dirs (xt_live/config 等) + xt_live_config/telemetry link
    xt_stra            # XtDeclaredSymbolRegistry / XtStrategyInitConfig
    xt_config          # XtCommodityCatalog
    xt_logger
)
