cmake_minimum_required(VERSION 3.5)
project(ctBtCore LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# 回测 runner 共享接口/工具（IReplayReporter.h / BtRunnerUtil.h）保留在此目录；
# 实际编译单元在 xt_backtest/hft/（xt_bt_hft 库）。
add_library(xt_bt_core_iface INTERFACE)
target_include_directories(xt_bt_core_iface INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}
)
