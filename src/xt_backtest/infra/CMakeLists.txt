cmake_minimum_required(VERSION 3.5)
project(ctBtInfra LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_bt_infra
    XtReplayer.cpp
    XtSessionFilterConfig.cpp
    ReplayFilterCalendar.cpp
)

target_include_directories(xt_bt_infra PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
    ${CMAKE_SOURCE_DIR}/xt_backtest/data
    ${CMAKE_SOURCE_DIR}/xt_backtest/util
)

target_link_libraries(xt_bt_infra PUBLIC
    xt_bt_data
    xt_core
    xt_config
    xt_logger
)
