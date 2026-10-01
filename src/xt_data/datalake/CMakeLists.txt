cmake_minimum_required(VERSION 3.22)
project(ctDataDatalake LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# 数据湖读端核心库：root 解析、schema_version.json 校验与 parquet readers。

add_library(xt_data_datalake STATIC
    XtDatalakeRoot.cpp
    XtCalendarReader.cpp
)

target_include_directories(xt_data_datalake PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_logger
)

# nlohmann_json 由顶层 FetchContent 提供。
# xt_arrow_parquet 携带 Arrow/Parquet link 与共享 parquet 读取 helper include path。
target_link_libraries(xt_data_datalake PUBLIC
    xt_logger
    nlohmann_json::nlohmann_json
    xt_arrow_parquet
)
