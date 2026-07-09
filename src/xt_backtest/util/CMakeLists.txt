cmake_minimum_required(VERSION 3.5)
project(ctBtUtil LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_bt_util
    OrderFlowCsvUtil.cpp
    TickCsvUtil.cpp
    TickFileUtil.cpp
)

target_include_directories(xt_bt_util PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_logger
)

target_link_libraries(xt_bt_util PUBLIC
    xt_core
    xt_util  # explicit (XtParseUtil.h includes XtTimeParse.h)
    xt_logger
)
