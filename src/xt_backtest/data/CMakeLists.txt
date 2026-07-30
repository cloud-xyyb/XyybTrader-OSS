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
    reader/XtRawShardTickReader.cpp     # RawShardV3 分片 reader
    reader/XtTickShardBuilder.cpp       # CSV -> RawShardV3 分片建库（单 pass、驻留有界）
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

# 分片计划落定注入点（见 source/detail/XtShardPlanHook.h）只装进需要它的测试可执行文件。
#
# seam 要生效必须让 ensure_tick_shard_cache 那个 TU 带着 XT_BT_TEST_SEAMS 编译，而
# xt_bt_data 同时被 XtBackTestRunner 消费——定义挂在库上就必然跟着进生产二进制。故单独
# 把该 TU 再编一份放进 object library：链接时 object 先于 archive 落位，用它的符号顶掉
# 归档里那份，xt_bt_data / XtBackTestRunner 自身一个 hook 符号都不带（含 tests=ON 的同
# 一构建树）。EXCLUDE_FROM_ALL：只有显式链它的测试目标才会编。
if(CLOUDTRADER_BUILD_TESTS)
    add_library(xt_bt_data_test_seams OBJECT EXCLUDE_FROM_ALL
        source/XtMarketDataSource_prepare.cpp
    )
    target_compile_definitions(xt_bt_data_test_seams PUBLIC XT_BT_TEST_SEAMS)
    target_link_libraries(xt_bt_data_test_seams PUBLIC xt_bt_data)
endif()
