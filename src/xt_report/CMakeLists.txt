cmake_minimum_required(VERSION 3.5)
project(xtReport LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# 跨引擎 live 报告库；本 target 不依赖 xt_bt_*。
add_library(xt_report
    core/XtCsvReportSink.cpp
    util/XtCsvReportUtil.cpp
    util/XtReportEventBuilder.cpp
    live/XtLiveReporter.cpp
    live/XtLiveReportAdapter.cpp    # Live → report event 转换 mapping（HftRunnerControl 读侧）
)

target_include_directories(xt_report PUBLIC
    ${CMAKE_SOURCE_DIR}  # repo-root 解析 "xt_report/..." 全路径 include。
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/core
    ${CMAKE_CURRENT_SOURCE_DIR}/util
    ${CMAKE_CURRENT_SOURCE_DIR}/live
)

target_link_libraries(xt_report PUBLIC
    xt_core
    xt_util  # xt_time::to_ns / packed ts helpers (live fills 时间换算)
    xt_logger
)
