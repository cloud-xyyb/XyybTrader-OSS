# market_order_support Stage 4 — 一次性市价单实盘验收插件
# 与 hft_tick_revert CMakeLists 同模板（XtStrategyBase 同 ABI，override on_tick/on_fill）。
# 独立 out-of-tree 编译：cmake -S . -B <build> -DXT_SRC_ROOT=<repo>/src
cmake_minimum_required(VERSION 3.5)
project(HftMarketVerifyStrategy LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

if(NOT XT_SRC_ROOT)
  message(FATAL_ERROR "XT_SRC_ROOT not set")
endif()

add_subdirectory(${XT_SRC_ROOT}/third_party/yaml-cpp
                 ${CMAKE_BINARY_DIR}/third_party/yaml-cpp)

add_library(hft_market_verify_strategy SHARED
    HftMarketVerifyStrategy.cpp
)

target_include_directories(hft_market_verify_strategy PRIVATE
    ${XT_SRC_ROOT}
    ${XT_SRC_ROOT}/xt_strategy_engine
    ${XT_SRC_ROOT}/xt_core
    ${XT_SRC_ROOT}/xt_util
    ${XT_SRC_ROOT}/third_party/yaml-cpp/include
)

target_link_libraries(hft_market_verify_strategy PRIVATE yaml-cpp)

set_target_properties(hft_market_verify_strategy PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "hft_market_verify_strategy"
)
