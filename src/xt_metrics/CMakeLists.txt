cmake_minimum_required(VERSION 3.22)
project(ctMetrics LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# ============================================================================
# xt_metrics — runner 进程内 Prometheus metrics
# ============================================================================
#
# 本模块负责注册 runner 侧 Prometheus families，并提供 HTTP /metrics
# exposer 给 scrape stack 拉取。调用方在 emit callsite 传入 strategy /
# book / venue / symbol 等 label，不依赖进程级全局 strategy 常量。
#
# prometheus-cpp 是本模块的 exposition format 和 histogram bucket 实现；
# 更完整的选型理由记录在 xt_metrics module context。
# ----------------------------------------------------------------------------

# prometheus-cpp 只启用 core + pull，避免引入 push/compression/test 依赖。
set(BUILD_SHARED_LIBS              OFF CACHE BOOL "" FORCE)
set(ENABLE_PUSH                    OFF CACHE BOOL "" FORCE)
set(ENABLE_PULL                    ON  CACHE BOOL "" FORCE)
set(ENABLE_COMPRESSION             OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTING                 OFF CACHE BOOL "" FORCE)
set(USE_THIRDPARTY_LIBRARIES       ON  CACHE BOOL "" FORCE)
set(GENERATE_PKGCONFIG             OFF CACHE BOOL "" FORCE)
set(OVERRIDE_CXX_STANDARD_FLAGS    OFF CACHE BOOL "" FORCE)

# prometheus-cpp vendored 入仓；构建阶段不依赖外网，也不走 FetchContent。
add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/prometheus_cpp
                 ${CMAKE_BINARY_DIR}/_prometheus_cpp_build
                 EXCLUDE_FROM_ALL)

# ----------------------------------------------------------------------------
# xt_metrics — runner-side Prometheus metric registry + HTTP exposer
# ----------------------------------------------------------------------------
add_library(xt_metrics STATIC
    XtMetricRegistry.cpp
    XtStrategyPnlAggregator.cpp
    XtStrategyPnlBaseline.cpp
    XtMdConnectorExporter.cpp
)
target_include_directories(xt_metrics PUBLIC
    ${CMAKE_SOURCE_DIR}                                       # repo-root include, e.g. "xt_metrics/XtMetricRegistry.h"
    ${CMAKE_CURRENT_SOURCE_DIR}                               # 模块内自引用
)
target_link_libraries(xt_metrics PUBLIC
    xt_util
    prometheus-cpp::core
    prometheus-cpp::pull
)
# XtLogger 仅在 .cpp 内使用（PnL state quarantine 的 error 日志）；PRIVATE 不外泄给 consumer。
target_link_libraries(xt_metrics PRIVATE
    xt_logger
)
