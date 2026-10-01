cmake_minimum_required(VERSION 3.5)
project(ctBtSel LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# SEL backtest runner — 与 xt_backtest/cta + xt_backtest/hft 平行。
# 回测组件：
#   - XtSelRunnerConfig: yaml 解析（datalake / selector / account / report）
#   - XtSelDataLoader:    trading dates 加载（csv fallback / parquet via 13c XtCalendarReader）
#   - XtSelBacktestRunner: 主循环（trading_dates → on_minute_tick → on_target_weights → trades.csv）
# XtBacktestOrderGateway 提供 sim 撮合、风控和 fee。

add_library(xt_bt_sel STATIC
    XtSelRunnerConfig.cpp
    XtSelDataLoader.cpp
    XtSelTradability.cpp
    XtSelCorporateAction.cpp
    XtSelTraceSink.cpp
    XtSelBacktestRunner.cpp
)

target_include_directories(xt_bt_sel PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/api
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine/loader
    ${CMAKE_SOURCE_DIR}/xt_engine/sel
    ${CMAKE_SOURCE_DIR}/xt_engine/sim
    ${CMAKE_SOURCE_DIR}/xt_backtest/cta
    ${CMAKE_SOURCE_DIR}/xt_runtime
    ${CMAKE_SOURCE_DIR}/xt_account
    ${CMAKE_SOURCE_DIR}/xt_risk
    ${CMAKE_SOURCE_DIR}/xt_data/datalake
)

target_link_libraries(xt_bt_sel PUBLIC
    xt_bt_common       # BarTradeRow / XtBacktestTradeCapture header
    xt_engine_sel
    xt_data_datalake
    xt_stra
    xt_bt_sel_gateway  # 回测订单网关与共享装配头
    xt_engine_sim
    xt_runtime
    xt_account
    xt_risk
    xt_config
    xt_core
    xt_cost            # Plan v1.4 A1c: explicit xt_cost (XtSelRunnerConfig includes XtTradeCostConfig.h)
    xt_logger
    yaml-cpp
)
