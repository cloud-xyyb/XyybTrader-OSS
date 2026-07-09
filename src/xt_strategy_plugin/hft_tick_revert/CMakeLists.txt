# [HFT-EXPERIMENTAL] wt-recon Step 9 prep / 9.0b — 最简 HFT 反转策略插件
# 与 cta_sma_xover CMakeLists 同模板，HFT 走 XtStrategyBase 同 ABI（on_tick override）。
cmake_minimum_required(VERSION 3.5)
project(HftTickRevertStrategy LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

if(NOT XT_SRC_ROOT)
  message(FATAL_ERROR "XT_SRC_ROOT not set")
endif()

add_subdirectory(${XT_SRC_ROOT}/third_party/yaml-cpp
                 ${CMAKE_BINARY_DIR}/third_party/yaml-cpp)

add_library(hft_tick_revert_strategy SHARED
    HftTickRevertStrategy.cpp
)

target_include_directories(hft_tick_revert_strategy PRIVATE
    ${XT_SRC_ROOT}
    ${XT_SRC_ROOT}/xt_strategy_engine
    ${XT_SRC_ROOT}/xt_core
    ${XT_SRC_ROOT}/xt_util
    ${XT_SRC_ROOT}/third_party/yaml-cpp/include
)

target_link_libraries(hft_tick_revert_strategy PRIVATE yaml-cpp)

set_target_properties(hft_tick_revert_strategy PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "hft_tick_revert_strategy"
)
