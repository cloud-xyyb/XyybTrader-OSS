# 经典 HFT 做市策略插件 — 与 hft_tick_revert 共享相同的 ABI 模板。
# 编译产物 libhft_market_making_strategy.so 装载于 var/plugins/strategies/。
cmake_minimum_required(VERSION 3.5)
project(HftMarketMakingStrategy LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

if(NOT XT_SRC_ROOT)
  message(FATAL_ERROR "XT_SRC_ROOT not set")
endif()

add_subdirectory(${XT_SRC_ROOT}/third_party/yaml-cpp
                 ${CMAKE_BINARY_DIR}/third_party/yaml-cpp)

add_library(hft_market_making_strategy SHARED
    HftMarketMakingStrategy.cpp
)

target_include_directories(hft_market_making_strategy PRIVATE
    ${XT_SRC_ROOT}
    ${XT_SRC_ROOT}/xt_strategy_engine
    ${XT_SRC_ROOT}/xt_core
    ${XT_SRC_ROOT}/xt_util
    ${XT_SRC_ROOT}/third_party/yaml-cpp/include
)

target_link_libraries(hft_market_making_strategy PRIVATE yaml-cpp)

set_target_properties(hft_market_making_strategy PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "hft_market_making_strategy"
)
