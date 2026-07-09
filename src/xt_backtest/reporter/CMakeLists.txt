cmake_minimum_required(VERSION 3.5)
project(xtBtReporter LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# Plan v5  backtest-specific reporter 从 xt_report/backtest 搬到
# xt_backtest/reporter, 让 xt_report 只剩 cross-engine 通用代码 (core / util / live).
# 解决原 xt_report/backtest 反向依赖 xt_bt_* 的链接环.
#
# T1.60: XtBacktestReporterObserver.{h,cpp} 从 xt_backtest/common 搬到 reporter/
# (该 observer 直接依赖 concrete XtBacktestReporter,跟 reporter 同 lib 才闭合)。
# common header-only helpers (BarTradeRow / packed_ts_from_*) 通过 xt_bt_common
# INTERFACE lib 提供。
add_library(xt_bt_reporter
    XtBacktestReporter.cpp
    XtBacktestDailyBreakdown.cpp   # Phase 2 (2026-05-17): per-trading_date counter SRP 拆
    XtReportConfigParser.cpp
    XtBacktestReporterObserver.cpp
    XtEventStreamSink.cpp          # BT full event tape (events_full.csv); BT-only 归位本 lib
)

target_include_directories(xt_bt_reporter PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
    ${CMAKE_SOURCE_DIR}/xt_runtime
    ${CMAKE_SOURCE_DIR}/xt_backtest/core
    ${CMAKE_SOURCE_DIR}/xt_backtest/infra
)

target_link_libraries(xt_bt_reporter PUBLIC
    xt_bt_common     # T1.60: BarTradeRow / packed_ts_from_* / collector headers
    xt_bt_infra      # XtReplayer.h forward decl 来源
    xt_report        # XtReportTypes / IReportSink / XtReportEventBuilder
    xt_config
    xt_logger
    xt_core
    # Phase 6.b (2026-05-17): 去掉 xt_risk link — reporter 只需 api/XtRiskTypes.h POD,
    # caller (risk_action::dispatch_reject) 已保证 decision.reason_text 非空,
    # 不再 fallback 到 risk_reason_text。
)
