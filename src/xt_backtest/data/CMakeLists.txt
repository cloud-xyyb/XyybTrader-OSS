cmake_minimum_required(VERSION 3.5)
project(ctBtData LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

set(XT_BT_DATA_SOURCES
    # Legacy bar 模式回放（bar mode replay），日常回测以 tick 模式为主
    event_stream/XtBookReplayer.cpp           # L2 order book replay state machine
    event_stream/ReplayEventBuilder.cpp
    legacy/XtBarModeDriver.cpp
    legacy/XtBarSimTickGen.cpp
    reader/XtCsvTickReader.cpp
    reader/XtBinTickReader.cpp
    reader/XtFbsTickReader.cpp          # fbs event-store reader
    reader/XtEventParquetReader.cpp     # canonical events parquet reader
    reader/XtTickCollector.cpp
    source/XtMarketDataSource.cpp
    source/XtMarketDataSource_prepare.cpp
    source/XtMarketDataSource_memory.cpp
    source/XtMarketDataSourceConfig.cpp
)

add_library(xt_bt_data ${XT_BT_DATA_SOURCES})

target_include_directories(xt_bt_data PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/event_stream
    ${CMAKE_CURRENT_SOURCE_DIR}/legacy
    ${CMAKE_CURRENT_SOURCE_DIR}/reader
    ${CMAKE_CURRENT_SOURCE_DIR}/source
    ${CMAKE_SOURCE_DIR}/xt_backtest/util
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
)

target_link_libraries(xt_bt_data PUBLIC
    xt_bt_util
    xt_core
    xt_util           # direct utility dependency used by data readers
    xt_data           # XtFbsTickReader event-store dependency
    xt_arrow_parquet
    xt_config
    xt_logger
)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_libraries(xt_bt_data PUBLIC stdc++fs)
endif()
