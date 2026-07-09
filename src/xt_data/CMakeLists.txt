cmake_minimum_required(VERSION 3.22)  # 锁 jammy 3.22，保证 EXCLUDE_FROM_ALL 行为一致。
project(ctData LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# FlatBuffers 使用仓内 third_party 源码，构建机无需联网拉取依赖。
# 这里只暴露 flatc + runtime；生成头落在 build dir，不进入源码树。

include(FetchContent)

# FlatBuffers 构建选项：只保留 schema codegen 与 runtime lib。
set(FLATBUFFERS_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
set(FLATBUFFERS_INSTALL        OFF CACHE BOOL "" FORCE)
set(FLATBUFFERS_BUILD_FLATC    ON  CACHE BOOL "" FORCE)
set(FLATBUFFERS_BUILD_FLATLIB  ON  CACHE BOOL "" FORCE)
set(FLATBUFFERS_BUILD_FLATHASH OFF CACHE BOOL "" FORCE)

# flatbuffers 已 vendored 到 third_party/flatbuffers；add_subdirectory 保持离线构建。
add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/flatbuffers
                 ${CMAKE_BINARY_DIR}/_flatbuffers_build
                 EXCLUDE_FROM_ALL)

# ---------------------------------------------------------------------------
# FlatBuffers schema codegen
# ---------------------------------------------------------------------------
set(XT_DATA_FBS        ${CMAKE_CURRENT_SOURCE_DIR}/schema/event.fbs)
set(XT_DATA_FBS_EXT    ${CMAKE_CURRENT_SOURCE_DIR}/schema/event_ext.fbs)
# control-plane schema 使用 control.proto；FlatBuffers 只生成 event/event_ext。
set(XT_DATA_GEN_DIR    ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(XT_DATA_GEN_HEADER ${XT_DATA_GEN_DIR}/event_generated.h)
set(XT_DATA_GEN_EXT    ${XT_DATA_GEN_DIR}/event_ext_generated.h)

add_custom_command(
    OUTPUT  ${XT_DATA_GEN_HEADER}
    COMMAND $<TARGET_FILE:flatc>
            --cpp
            --scoped-enums
            -o ${XT_DATA_GEN_DIR}
            ${XT_DATA_FBS}
    DEPENDS flatc ${XT_DATA_FBS}
    COMMENT "flatc: generating xt_data/schema/event_generated.h"
    VERBATIM
)

# EventExt schema 依赖 event.fbs 中的 SourceKind。
add_custom_command(
    OUTPUT  ${XT_DATA_GEN_EXT}
    COMMAND $<TARGET_FILE:flatc>
            --cpp
            --scoped-enums
            -I ${CMAKE_CURRENT_SOURCE_DIR}/schema
            -o ${XT_DATA_GEN_DIR}
            ${XT_DATA_FBS_EXT}
    DEPENDS flatc ${XT_DATA_FBS_EXT} ${XT_DATA_FBS}
    COMMENT "flatc: generating xt_data/schema/event_ext_generated.h"
    VERBATIM
)


add_custom_target(xt_data_schema_gen DEPENDS ${XT_DATA_GEN_HEADER} ${XT_DATA_GEN_EXT})

# ---------------------------------------------------------------------------
# xt_data_schema — INTERFACE target，下游 link 即可用 FlatBuffers
# ---------------------------------------------------------------------------
#
# 典型用法：
#   target_link_libraries(my_tgt PRIVATE xt_data_schema)
#   #include "event_generated.h"
#
add_library(xt_data_schema INTERFACE)
target_include_directories(xt_data_schema INTERFACE ${XT_DATA_GEN_DIR})
target_link_libraries(xt_data_schema INTERFACE flatbuffers)
add_dependencies(xt_data_schema xt_data_schema_gen)

# ---------------------------------------------------------------------------
# xt_data — 运行时核心库（IEventStore / LocalFsEventStore）
# ---------------------------------------------------------------------------
#
# 下游用法：
#   target_link_libraries(my_tgt PRIVATE xt_data)
#   #include "xt_data/IEventStore.h"
#
add_library(xt_data STATIC
    LocalFsEventStore.cpp
)
target_include_directories(xt_data PUBLIC
    ${CMAKE_SOURCE_DIR}                 # 允许 "xt_data/IEventStore.h" 形式 include
    ${CMAKE_CURRENT_SOURCE_DIR}         # 模块内自引用
)
target_link_libraries(xt_data PUBLIC
    xt_data_schema
)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_libraries(xt_data PUBLIC stdc++fs)
endif()


