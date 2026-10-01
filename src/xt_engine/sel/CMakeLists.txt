cmake_minimum_required(VERSION 3.5)
project(XtEngineSel LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# SEL Selector Engine — 与 xt_engine/cta + xt_engine/hft 平行。
# 设计要点（基于 2026-05-09 三 agent 调研）：
#   - 不 mirror XtCtaContext wrapper 反模式（C3）— 直接持 vector<unique_ptr<XtStrategyContext>>
#   - handle_pos_change 由装配方提供，直接链接实际依赖
#   - SelTaskScheduler 抽象（live + backtest 共用 cron + holiday rollover 算法 wt parity）

add_library(xt_engine_sel STATIC
    SelTaskScheduler.cpp
    XtSelEngine.cpp
)

target_include_directories(xt_engine_sel PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/api
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_runtime
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_logger
    ${CMAKE_SOURCE_DIR}/xt_config
)

target_link_libraries(xt_engine_sel PUBLIC
    xt_util
    xt_core
    xt_runtime
    xt_logger
    xt_stra
)
