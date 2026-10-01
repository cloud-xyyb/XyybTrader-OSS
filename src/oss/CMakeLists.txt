# 两个回归插件独立编译，并放在 loader 默认查找目录。
foreach(name IN ITEMS hft_tick_revert sel_weights_csv)
    if(name STREQUAL "hft_tick_revert")
        set(stem HftTickRevertStrategy)
    else()
        set(stem SelWeightsCsvStrategy)
    endif()
    add_library(${name}_strategy SHARED
        ${CMAKE_SOURCE_DIR}/xt_strategy_plugin/${name}/${stem}.cpp)
    target_include_directories(${name}_strategy PRIVATE
        ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/api
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine ${CMAKE_SOURCE_DIR}/xt_core
        ${CMAKE_SOURCE_DIR}/xt_util)
    target_link_libraries(${name}_strategy PRIVATE yaml-cpp)
    set_target_properties(${name}_strategy PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/../var/plugins/strategies")
endforeach()
