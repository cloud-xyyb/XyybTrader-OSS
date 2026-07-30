cmake_minimum_required(VERSION 3.5)
project(HftMicropriceMarketMakingStrategy LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT XT_SRC_ROOT)
  message(FATAL_ERROR "XT_SRC_ROOT not set")
endif()

add_subdirectory(${XT_SRC_ROOT}/third_party/yaml-cpp
                 ${CMAKE_BINARY_DIR}/third_party/yaml-cpp)

add_library(hft_microprice_mm_strategy SHARED
    HftMicropriceMarketMakingStrategy.cpp
)

target_include_directories(hft_microprice_mm_strategy PRIVATE
    ${XT_SRC_ROOT}
    ${XT_SRC_ROOT}/xt_strategy_engine
    ${XT_SRC_ROOT}/xt_core
    ${XT_SRC_ROOT}/xt_util
    ${XT_SRC_ROOT}/third_party/yaml-cpp/include
)

target_link_libraries(hft_microprice_mm_strategy PRIVATE yaml-cpp)

set_target_properties(hft_microprice_mm_strategy PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "hft_microprice_mm_strategy"
)
