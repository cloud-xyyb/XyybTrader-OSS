cmake_minimum_required(VERSION 3.5)
project(ctBtHft LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_bt_hft
    XtHftBacktestConfig.cpp
    XtHftBacktestOrderGateway.cpp
    XtHftBacktestDriver.cpp
    XtLatencyModelFactory.cpp
    XtTradeExcursionTracker.cpp
)

target_include_directories(xt_bt_hft PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_backtest/core
    ${CMAKE_SOURCE_DIR}/xt_backtest/reporter
    ${CMAKE_SOURCE_DIR}/xt_report
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_risk
    ${CMAKE_SOURCE_DIR}/xt_backtest/infra
    ${CMAKE_SOURCE_DIR}/xt_backtest/data
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine/loader
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine/core
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine/backtest
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
    ${CMAKE_SOURCE_DIR}/xt_util
)

target_link_libraries(xt_bt_hft PUBLIC
    xt_bt_common    # T1.60: XtBacktestReport.h / collector headers
    xt_bt_infra
    xt_bt_data
    xt_bt_reporter  # Plan v5  XtHftBacktestConfig 调 parse_report_config
    xt_engine_sim
    xt_risk
    xt_runtime
    xt_account

    xt_stra
    xt_logger
    xt_config
    xt_util
    xt_core
    xt_cost          # Plan v1.4 A1c: explicit xt_cost (XtFillCostApplier / XtTradeCostConfig)
    xt_report        # XtReportCfg / XtReportTypes (header-only consumers)
)
