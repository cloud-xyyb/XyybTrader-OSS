cmake_minimum_required(VERSION 3.5)

project(ctLiveTelemetry LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_live_telemetry STATIC
    latency/XtLiveLatencyTracker.cpp
    recording/XtLiveRecorder.cpp
    # FillsCsvTail.cpp → xt_panel/query lib
)
target_include_directories(xt_live_telemetry PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(xt_live_telemetry PUBLIC xt_engine_common xt_core xt_data xt_util xt_logger)  # +xt_logger: FillsCsvTail 归位
# drain thread auto-triggers Prometheus metric inc
# alongside JSONL / fbs sink. xt_metrics is optional (if CLOUDTRADER_BUILD_
# METRICS_LAYER=OFF, recorder skips metric calls via #ifdef XT_HAVE_METRICS).
if(TARGET xt_metrics)
    target_link_libraries(xt_live_telemetry PUBLIC xt_metrics)
    target_compile_definitions(xt_live_telemetry PUBLIC XT_HAVE_METRICS=1)
endif()

if(CLOUDTRADER_BUILD_BENCHMARKS)
    add_executable(xt_latency_bench
        bench/LatencyBenchMain.cpp
    )
    target_link_libraries(xt_latency_bench PRIVATE xt_live_telemetry)

    add_executable(xt_recorder_enqueue_bench
        bench/RecorderEnqueueBenchMain.cpp
    )
    target_link_libraries(xt_recorder_enqueue_bench PRIVATE xt_live_telemetry)

    # gateway 两 TU 改 link xt_live_execution（TU 单一所有权）；该 target 在
    # xt_live/execution 子目录（晚于本目录处理）定义，CMake link 解析在 generate
    # 期，引用后定义 target 合法。
    add_executable(xt_inner_latency_bench
        bench/InnerLatencyBenchMain.cpp
    )
    target_include_directories(xt_inner_latency_bench PRIVATE
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine
        ${CMAKE_SOURCE_DIR}/xt_strategy_engine/core
        ${CMAKE_SOURCE_DIR}/xt_core
        ${CMAKE_SOURCE_DIR}/xt_live/execution
        ${CMAKE_SOURCE_DIR}/xt_live/common
        ${CMAKE_SOURCE_DIR}/xt_live/gateway
        ${CMAKE_SOURCE_DIR}/xt_live/gateway/ctp
        ${CMAKE_SOURCE_DIR}/xt_live/control
        ${CMAKE_SOURCE_DIR}/xt_live/telemetry
        ${CMAKE_SOURCE_DIR}/xt_config
        ${CMAKE_SOURCE_DIR}/xt_account
        ${CMAKE_SOURCE_DIR}/xt_account/core
        ${CMAKE_SOURCE_DIR}/xt_risk
        ${CMAKE_SOURCE_DIR}/xt_risk/live
        ${CMAKE_SOURCE_DIR}/xt_runtime
        ${CMAKE_SOURCE_DIR}/xt_engine
    )
    target_link_libraries(xt_inner_latency_bench PRIVATE
        xt_stra xt_core xt_logger
        xt_risk xt_account xt_runtime xt_engine_common xt_config
        xt_live_telemetry xt_paramdomain xt_live_execution
    )
endif()
