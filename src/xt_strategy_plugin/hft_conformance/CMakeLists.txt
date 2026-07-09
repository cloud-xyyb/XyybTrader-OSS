# HFT 全功能高压自检策略插件（conformance / stress oracle）。
# 与 hft_tick_revert CMakeLists 同模板，HFT 走 XtStrategyBase 同 ABI（on_tick override）。
cmake_minimum_required(VERSION 3.5)
project(HftConformanceStrategy LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

if(NOT XT_SRC_ROOT)
  message(FATAL_ERROR "XT_SRC_ROOT not set")
endif()

add_subdirectory(${XT_SRC_ROOT}/third_party/yaml-cpp
                 ${CMAKE_BINARY_DIR}/third_party/yaml-cpp)

add_library(hft_conformance_strategy SHARED
    HftConformanceStrategy.cpp
)

target_include_directories(hft_conformance_strategy PRIVATE
    ${XT_SRC_ROOT}
    ${XT_SRC_ROOT}/xt_strategy_engine
    ${XT_SRC_ROOT}/xt_core
    ${XT_SRC_ROOT}/xt_util
    ${XT_SRC_ROOT}/third_party/yaml-cpp/include
)

target_link_libraries(hft_conformance_strategy PRIVATE yaml-cpp)

set_target_properties(hft_conformance_strategy PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "hft_conformance_strategy"
)
