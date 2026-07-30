# xt_gateway_common — venue-中立 gateway 基础设施。
#   - XtWsConnection      ：Boost.Beast TLS WebSocket session（重连/退避/stale）
#   - XtClockSkewEstimator ：server event_time vs 本地 monotonic 偏移估计（metric-gated）
#   - XtMdFreshness        ：MD stale watchdog 纯函数（header-only）
#   - IoContextThread      ：单 io_context + 独占线程 lifecycle RAII（header-only）
#   - XtExecDeliveryLedger ：per-order 交付投影（fill 记账/终态 payload/撤单证据）
#   - WriteSerializer / TdReadinessGate / ReverseSyncHelper / ... （header-only helper）
add_library(xt_gateway_common STATIC
    XtWsConnection.cpp
    XtExecDeliveryLedger.cpp
)

target_include_directories(xt_gateway_common PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(xt_gateway_common
    PUBLIC
        xt_core            # Plan v1.4 Z6
        xt_util            # XtSteadyClock
        xt_logger          # XtLogger / XtCallbackGuard
        Boost::boost       # Beast (header-only)
        Boost::system      # Asio / Beast 运行时
        OpenSSL::SSL       # Beast WSS TLS
        OpenSSL::Crypto    # ERR_get_error / SSL_set_tlsext_host_name
)

# clock-skew estimator 依赖 metric registry；仅在 xt_metrics 存在时编译并发布指标。
if(TARGET xt_metrics)
    target_sources(xt_gateway_common PRIVATE XtClockSkewEstimator.cpp)
    target_link_libraries(xt_gateway_common PUBLIC xt_metrics)
    target_compile_definitions(xt_gateway_common PUBLIC XT_HAVE_METRICS=1)
endif()
