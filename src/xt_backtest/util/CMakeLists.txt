cmake_minimum_required(VERSION 3.5)
project(ctBtUtil LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_bt_util
    OrderFlowCsvUtil.cpp
    TickCsvUtil.cpp
    TickFileUtil.cpp
    XtTickClosureIndex.cpp   # RawShardV3: 交易日 → 有序源文件闭包索引
    XtTickCacheStoreV3.cpp   # RawShardV3: family / generation / CURRENT / flock / GC 计划
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

# closure index 的 JSON 序列化只在 .cpp 内使用，公开 header 不暴露 nlohmann 类型。
target_link_libraries(xt_bt_util PRIVATE nlohmann_json::nlohmann_json)
