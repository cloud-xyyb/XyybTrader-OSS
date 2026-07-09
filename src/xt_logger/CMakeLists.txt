cmake_minimum_required(VERSION 3.5)

project(xt_logger LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_logger
    XtLogger.cpp
)

target_include_directories(xt_logger PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/spdlog
                 ${CMAKE_BINARY_DIR}/third_party/spdlog)
target_link_libraries(xt_logger PUBLIC spdlog::spdlog_header_only)
target_link_libraries(xt_logger PUBLIC yaml-cpp)
