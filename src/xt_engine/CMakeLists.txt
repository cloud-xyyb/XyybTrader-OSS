cmake_minimum_required(VERSION 3.5)

project(xt_engine LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_engine_common INTERFACE)

target_link_libraries(xt_engine_common INTERFACE
    xt_core
    xt_config
)

add_library(xt_engine_sim
        sim/HFTMatchEngine.cpp
        sim/XtOrderBook.cpp
        sim/XtOrderManager.cpp
        sim/InterpolatedLatencyModel.cpp
        sim/CdfLatencyModel.cpp
)

target_include_directories(xt_engine_sim PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/sim
    ${CMAKE_SOURCE_DIR}/xt_core
)

target_link_libraries(xt_engine_sim PUBLIC
    xt_core
    xt_engine_common
)

# Backward-compat aggregator; prefer linking xt_engine_sim/xt_engine_common directly.
add_library(xt_engine INTERFACE)
target_link_libraries(xt_engine INTERFACE
    xt_engine_sim
    xt_engine_common
)

add_subdirectory(hft)
add_subdirectory(sel)  # SEL Selector Engine 与 cta/hft 平行
