cmake_minimum_required(VERSION 3.5)
project(xt_live_execution LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_live_execution STATIC
    XtLiveOrderGateway.cpp
    LogicalOrderTracker.cpp
)

target_include_directories(xt_live_execution PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_runtime
    ${CMAKE_SOURCE_DIR}/xt_risk
    ${CMAKE_SOURCE_DIR}/xt_risk/live
    ${CMAKE_SOURCE_DIR}/xt_account
    ${CMAKE_SOURCE_DIR}/xt_account/core
    ${CMAKE_SOURCE_DIR}/xt_engine
    ${CMAKE_SOURCE_DIR}/xt_live/telemetry
    ${CMAKE_SOURCE_DIR}/xt_live/gateway
    ${CMAKE_SOURCE_DIR}/xt_live/common
    ${CMAKE_SOURCE_DIR}/xt_live/control
    ${CMAKE_SOURCE_DIR}/xt_live/config
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine/core
)

target_link_libraries(xt_live_execution PUBLIC
    xt_core
    xt_runtime
    xt_risk
    xt_account
    xt_engine_common
    xt_paramdomain   # XtLiveParamDomain 下沉为 leaf lib; PUBLIC → HFT runner 经此 transitive 拿到
    xt_live_telemetry
)
