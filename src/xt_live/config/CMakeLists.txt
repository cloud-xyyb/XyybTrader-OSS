cmake_minimum_required(VERSION 3.5)
project(XtLiveConfigLib LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# ── xt_live_config: live yaml 配置加载 / schema / 校验 / 解析 ──────────────
# 五个 TU 的唯一编译点（TU 单一所有权）。
add_library(xt_live_config STATIC
    XtLiveConfig.cpp
    LiveConfigSchemaRules.cpp
    LiveConfigValidators.cpp
    LiveConfigParsers.cpp
    DeclaredUniverse.cpp
)

target_include_directories(xt_live_config PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_runtime
    ${CMAKE_SOURCE_DIR}/xt_engine
    ${CMAKE_SOURCE_DIR}/xt_risk
    ${CMAKE_SOURCE_DIR}/xt_risk/live
    ${CMAKE_SOURCE_DIR}/xt_account
    ${CMAKE_SOURCE_DIR}/xt_account/core
    ${CMAKE_SOURCE_DIR}/xt_report
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine/core
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine/loader
    ${CMAKE_SOURCE_DIR}/xt_live/common
    ${CMAKE_SOURCE_DIR}/xt_live/control
    ${CMAKE_SOURCE_DIR}/xt_live/execution
    ${CMAKE_SOURCE_DIR}/xt_live/gateway
    ${CMAKE_SOURCE_DIR}/xt_live/gateway/ctp
    ${CMAKE_SOURCE_DIR}/xt_live/hft
    ${CMAKE_SOURCE_DIR}/xt_live/telemetry
)

target_link_libraries(xt_live_config PUBLIC
    xt_config
    xt_logger
    xt_core
    xt_stra
    xt_paramdomain
    xt_live_telemetry   # XtLiveConfig.h 值持有 XtLiveRecorder::Config（真直接消费，不靠传递）
)

