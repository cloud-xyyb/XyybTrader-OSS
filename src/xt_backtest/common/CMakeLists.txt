cmake_minimum_required(VERSION 3.5)
project(ctBtCommon LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# xt_bt_common — backtest 公共 header-only helpers (T1.60).
#
# 装载：
#   - XtBacktestReport.h: packed_ts_from_trade / packed_ts_from_tick / BarTradeRow /
#     to_bar_trade_row / write_trades_csv 等 backtest-wide shared types + helpers
#   - XtBacktestTradeCapture.h: 统一 trade capture observer，支持 single-bucket
#     显式 sink + multi-bucket strict strategy_id routing 两模式)
#
# 设计：INTERFACE lib (0 .cpp source) — 所有 helper 都是 inline / template,
# consumer 通过 link 自动获得 include path + transitive deps。
#
# 注意 XtBacktestReporterObserver.{h,cpp} 不在此 lib 内 — 该 observer 直接依赖
# concrete XtBacktestReporter,跟 xt_bt_reporter 同 lib 才不破坏单向依赖。

add_library(xt_bt_common INTERFACE)

target_include_directories(xt_bt_common INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_runtime
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_util
)

# 去掉 xt_account include + link：trade capture 保持 pure read-only observer,
# 不再调 account.apply / account state。
target_link_libraries(xt_bt_common INTERFACE
    xt_core
    xt_runtime
    xt_config
    xt_util
)
