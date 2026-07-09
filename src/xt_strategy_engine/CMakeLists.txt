cmake_minimum_required(VERSION 3.5)

project(XtStrategyEngine LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_stra
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine/XtStopOrderBook.cpp
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine/XtStrategyContext.cpp
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine/loader/XtStrategyLoader.cpp
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine/loader/XtStrategyFactory.cpp
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine/view/XtRuntimeView.cpp
)
target_include_directories(xt_stra PUBLIC
        ${CMAKE_SOURCE_DIR}  # 支持跨模块完整 include，如 "xt_metrics/XtStrategyMetricsAggregator.h"
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine/loader
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine/util
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine/view
        ${CMAKE_SOURCE_DIR}/xt_config
        ${CMAKE_SOURCE_DIR}/xt_runtime
)
# SDK 纯下层：telemetry 经 IStrategyTelemetrySink 由 runner 注入，不 link xt_live_*。
target_link_libraries(xt_stra PUBLIC xt_core xt_logger xt_runtime xt_util)
target_link_libraries(xt_stra PUBLIC dl)

# plugin loader sha256 manifest verify 需要 OpenSSL EVP + CRYPTO_memcmp。
# OpenSSL::Crypto 已经在 src/CMakeLists.txt 顶层 find_package(OpenSSL REQUIRED) 满足。
# Windows 路径不调用这些符号，但保留 link 可避免 toolchain 差异；非 Windows 强制 link。
if(NOT WIN32)
    target_link_libraries(xt_stra PRIVATE OpenSSL::Crypto)
endif()
