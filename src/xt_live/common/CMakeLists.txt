cmake_minimum_required(VERSION 3.5)
project(XtLiveCommonLib LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# ── xt_live_common: 实盘公共装配基础设施 ──────────────────────────────────
# XtLiveBootstrap（account store/view + catalog + param domain + latency
# tracker + risk monitor 公共创建）与 RunnerCommon（PositionAggregator metric
# 上报）的唯一编译点（TU 单一所有权）。
add_library(xt_live_common STATIC
    XtLiveBootstrap.cpp
    RunnerBootCommon.cpp
    RunnerCommon.cpp
    AccountTruthSourceFactory.cpp
    StrategyUserdataStore.cpp
    XtLiveStrategyTelemetrySink.cpp
)

target_include_directories(xt_live_common PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_runtime
    ${CMAKE_SOURCE_DIR}/xt_account
    ${CMAKE_SOURCE_DIR}/xt_account/core
    ${CMAKE_SOURCE_DIR}/xt_risk
    ${CMAKE_SOURCE_DIR}/xt_risk/live
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_live/control
    ${CMAKE_SOURCE_DIR}/xt_live/config
    ${CMAKE_SOURCE_DIR}/xt_live/telemetry
)

# xt_live_telemetry 必须 PUBLIC：携带 XT_HAVE_METRICS define
# （RunnerCommon.cpp 5 处 #ifdef）+ xt_metrics / xt_data transitive。
target_link_libraries(xt_live_common PUBLIC
    xt_live_config
    xt_live_telemetry
    xt_account
    xt_risk
    xt_paramdomain
    xt_stra
    xt_config
    xt_logger
)

# StrategyUserdataStore.cpp 用 nlohmann/json 做 userdata 序列化（正确 JSON 转义）；
# 与 XtCtaReconcileSnapshot 同模式。PRIVATE：JSON 是实现细节，不泄漏到 header。
target_link_libraries(xt_live_common PRIVATE nlohmann_json::nlohmann_json)
