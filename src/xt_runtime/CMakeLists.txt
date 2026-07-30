cmake_minimum_required(VERSION 3.5)

project(ctRuntime LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_runtime
    market/XtRuntimeMarketCache.cpp
    order/XtOrderRuntimeBook.cpp
)

# xt_account 是 include-not-link：本模块 header 依赖 AccountStateStore/CloseableView，
# 但不 link xt_account（防 xt_account ↔ xt_runtime link 环）；符号由最终二进制的
# consumer link xt_account 提供。
target_include_directories(xt_runtime PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_account
)

target_link_libraries(xt_runtime PUBLIC
    xt_core
    xt_util_sync_hazard
    # OrderAttributionStore.h（xt_identity header-only）的 checked journal append 在失败/
    # 降级时经 XtLogger 限频告警；XtOrderRuntimeBook.cpp ODR-use 该 append → 需 xt_logger。
    xt_logger
)

# xt_metrics 是可选观测层；关闭 metrics target 时，runtime book drop 计数保留
# process-local atomic，Prometheus 上报路径由 XT_HAVE_METRICS 条件编译隔离。
if(TARGET xt_metrics)
    target_link_libraries(xt_runtime PUBLIC xt_metrics)
    target_compile_definitions(xt_runtime PUBLIC XT_HAVE_METRICS=1)
endif()
