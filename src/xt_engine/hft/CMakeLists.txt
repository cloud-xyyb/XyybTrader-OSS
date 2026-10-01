cmake_minimum_required(VERSION 3.5)
project(xt_engine_hft LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_engine_hft STATIC
    XtHftEngine.cpp
    HftLifecycleTelemetry.cpp
)

target_include_directories(xt_engine_hft PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_runtime
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_util
)

target_link_libraries(xt_engine_hft PUBLIC
    xt_core
    xt_runtime
    xt_util
    xt_stra
    xt_engine_sim
)
