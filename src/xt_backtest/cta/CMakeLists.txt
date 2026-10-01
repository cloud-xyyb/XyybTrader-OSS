# SEL 回测复用订单网关和撮合器，不构建 CTA 引擎或 runner。
add_library(xt_bt_sel_gateway STATIC
    XtBacktestOrderGateway.cpp
    ${CMAKE_SOURCE_DIR}/xt_engine/sim/XtCtaMatchEngine.cpp
)
target_include_directories(xt_bt_sel_gateway PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_backtest/util ${CMAKE_SOURCE_DIR}/xt_util
    ${CMAKE_SOURCE_DIR}/xt_engine/sim
)
target_link_libraries(xt_bt_sel_gateway PUBLIC
    xt_bt_common xt_core xt_cost xt_config xt_risk xt_account xt_report
    xt_stra xt_bt_infra xt_runtime xt_engine_sim xt_logger yaml-cpp
)
