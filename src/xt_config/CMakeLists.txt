cmake_minimum_required(VERSION 3.5)
project(xt_config LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/yaml-cpp
                 ${CMAKE_BINARY_DIR}/third_party/yaml-cpp)

add_library(xt_config
    ConfigLocator.cpp
    ConfigSchemaValidator.cpp
    catalog/XtCommodityCatalog.cpp
    catalog/XtFeeRuleBook.cpp
    yaml/YAMLMergeUtil.cpp
    yaml/YAMLPathUtil.cpp
)

target_include_directories(xt_config PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/catalog
    ${CMAKE_CURRENT_SOURCE_DIR}/yaml
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
)

target_link_libraries(xt_config PUBLIC yaml-cpp xt_util)  # explicit xt_util (ConfigYamlUtil.h includes XtStringUtil.h)
