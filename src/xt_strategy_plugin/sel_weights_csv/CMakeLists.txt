cmake_minimum_required(VERSION 3.5)
project(SelWeightsCsvStrategy LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# SEL 最简策略 — 读 weights.csv 推 target_weights。
# 与 cta_sma_xover 同 plugin 模式（独立编译，dlopen 加载）。

if(NOT XT_SRC_ROOT)
    message(FATAL_ERROR "XT_SRC_ROOT not set (e.g. -DXT_SRC_ROOT=$(pwd)/src)")
endif()

add_library(sel_weights_csv_strategy SHARED
    SelWeightsCsvStrategy.cpp
)

target_include_directories(sel_weights_csv_strategy PRIVATE
    ${XT_SRC_ROOT}
    ${XT_SRC_ROOT}/xt_strategy_engine
    ${XT_SRC_ROOT}/xt_core
    ${XT_SRC_ROOT}/api
)

set_target_properties(sel_weights_csv_strategy PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "sel_weights_csv_strategy"
)
