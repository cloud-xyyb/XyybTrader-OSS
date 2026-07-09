cmake_minimum_required(VERSION 3.5)

project(ctRisk LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_risk
    engine/XtRiskEngine.cpp
    gateway/XtRiskGateway.cpp
    engine/XtRiskMonitor.cpp
    backtest/XtBacktestRiskGuardContext.cpp
    live/XtLiveRiskGuardContext.cpp
    live/XtLiveRejectSink.cpp
    live/SelfMatchDetector.cpp
    state/SafetyStateStore.cpp
)

target_include_directories(xt_risk PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_account
)

target_link_libraries(xt_risk PUBLIC
    xt_core
    xt_cost            # XtRiskEngine.cpp uses xt_fill_cost::
    xt_engine_common
    xt_runtime
    xt_logger
    xt_util
)

# risk close path 取 max(local_outstanding, broker_frozen)
# 双源取严; 当两源 disagree 时 emit xt_risk_close_quota_disagreement_total。
# xt_metrics 可选(CLOUDTRADER_BUILD_METRICS_LAYER=OFF 时退化 no-op via #ifdef)。
if(TARGET xt_metrics)
    target_link_libraries(xt_risk PUBLIC xt_metrics)
    target_compile_definitions(xt_risk PUBLIC XT_HAVE_METRICS=1)
endif()
