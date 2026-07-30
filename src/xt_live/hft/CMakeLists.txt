cmake_minimum_required(VERSION 3.5)
project(XtRunner LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_live_hft_components STATIC
    runtime/XtHftEventLoop.cpp
    telemetry/XtHftTelemetryWiring.cpp
    control/HftRunnerControl.cpp
    control/HftControlFrameSample.cpp
    control/HftControlPolicyChain.cpp
    control/HftControlForceClose.cpp
    control/HftControlSafetyState.cpp
    control/HftCloseoutGate.cpp
    control/PanelForceExitLatch.cpp
    control/PerSidLiquidator.cpp
    reconcile/XtHftReconcileLoop.cpp
    reconcile/XtHftAccountReconcile.cpp
    telemetry/XtHftControlReportEmit.cpp
    telemetry/HftRunnerControlStrategyPnlEmit.cpp
    runtime/XtHftChannels.cpp
)

target_include_directories(xt_live_hft_components PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/control
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile
    ${CMAKE_CURRENT_SOURCE_DIR}/runtime
    ${CMAKE_CURRENT_SOURCE_DIR}/telemetry
    ${CMAKE_SOURCE_DIR}/xt_live/common
    ${CMAKE_SOURCE_DIR}/xt_live/config
    ${CMAKE_SOURCE_DIR}/xt_live/control
    ${CMAKE_SOURCE_DIR}/xt_live/execution
    ${CMAKE_SOURCE_DIR}/xt_live/gateway
    ${CMAKE_SOURCE_DIR}/xt_live/gateway/ctp
    ${CMAKE_SOURCE_DIR}/xt_live/gateway/xtp
    ${CMAKE_SOURCE_DIR}/xt_report
    ${CMAKE_SOURCE_DIR}/xt_metrics
)

target_link_libraries(xt_live_hft_components PUBLIC
    xt_engine_hft
    xt_engine_common
    xt_stra
    xt_core
    xt_cost
    xt_report
    xt_account
    xt_runtime
    xt_util
    xt_logger
    xt_config
    xt_gateway_binance
    xt_gateway_common
    xt_risk
    xt_live_telemetry
    xt_live_execution
    xt_live_config
    xt_live_common
    xt_live_control
    xt_paramdomain
    $<$<TARGET_EXISTS:xt_metrics>:xt_metrics>
)

add_executable(XtRunner
    # ── runner 装配 / 事件循环 ──
    runtime/XtHftRunnerMain.cpp
    runtime/XtHftRunner.cpp
    # common/Bootstrap + config 四 TU → xt_live_common / xt_live_config lib（见下方 link）
    # ControlGrpcCommon / HftControlGrpcBinder / PanelHelpers / XtKillSwitchState
    # （PanelHelpers/XtKillSwitchState → xt_live_control lib；gRPC 绑定件 → xt_panel）
    # [Design] GatewayFactory.cpp 蓄意 per-target 直编（TU 单一所有权的登记例外）：
    # XT_NO_CTP / XT_HAS_XTP per-venue 宏按 runner/host 分叉（binance-only host
    # 必须把 CTP 编出去，见下方 OpenSSL 冲突注释），单 lib 无法承载多套 venue 组合。
    ../gateway/GatewayFactory.cpp
)

target_include_directories(XtRunner PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/control
    ${CMAKE_CURRENT_SOURCE_DIR}/reconcile
    ${CMAKE_CURRENT_SOURCE_DIR}/runtime
    ${CMAKE_CURRENT_SOURCE_DIR}/telemetry
    ${CMAKE_SOURCE_DIR}/xt_live/common
    ${CMAKE_SOURCE_DIR}/xt_live/config
    ${CMAKE_SOURCE_DIR}/xt_live/control
    ${CMAKE_SOURCE_DIR}/xt_live/execution
    ${CMAKE_SOURCE_DIR}/xt_live/gateway
    ${CMAKE_SOURCE_DIR}/xt_live/gateway/ctp
    ${CMAKE_SOURCE_DIR}/xt_live/gateway/xtp
    ${CMAKE_SOURCE_DIR}/xt_report
    ${CMAKE_SOURCE_DIR}/xt_metrics
)

# CTP6.3.15 静态嵌入旧 OpenSSL；若其先加载，SSL/CRYPTO 符号会绑到旧版本。
# 强制系统 libssl/libcrypto 进入 NEEDED 并放在 CTP 之前，避免 Boost.Beast SSL crash。
find_package(OpenSSL REQUIRED)
target_link_options(XtRunner BEFORE PRIVATE
    "-Wl,--push-state,--no-as-needed"
    "-l:libssl.so.3"
    "-l:libcrypto.so.3"
    "-Wl,--pop-state"
)

target_link_libraries(XtRunner PRIVATE xt_engine_hft xt_engine_common xt_stra xt_core xt_cost xt_report xt_account xt_runtime xt_util)
target_link_libraries(XtRunner PRIVATE xt_logger xt_config xt_gateway_binance)
target_link_libraries(XtRunner PRIVATE xt_live_hft_components)
# multibook: md.transport=shm 时 create_md_gateway 返 XtShmMdGateway（从 connector ring 读行情）。
target_link_libraries(XtRunner PRIVATE xt_ipc_md_gateway)

# ── CTP gateway (optional; default ON) ─────────────────────────────────
# CTP's thost*.so statically embeds a 2nd OpenSSL and exports BN_free /
# RSA_free / SSL_CTX_free etc. as GLOBAL dynamic symbols. Linked alongside the
# system OpenSSL that libcurl + Boost.Beast use for binance REST/WSS, the two
# OpenSSL implementations collide under concurrent TLS → heap corruption →
# SIGABRT crash-loop (2026-06-05 binance outage; RC in memory notes). A
# binance-only runner must therefore NOT link CTP → exactly one OpenSSL.
#   ON  (default)      : link xt_md/xt_td (thost); "ctp" venue available.
#   OFF (binance host) : compile CTP out via XT_NO_CTP (GatewayFactory.cpp),
#                        no thost in NEEDED, single OpenSSL, no collision.
# Option declared in xt_live/gateway/CMakeLists.txt (shared by all 3 runners).
if(XT_BUILD_GATEWAY_CTP)
    target_link_libraries(XtRunner PRIVATE xt_md xt_td)
else()
    target_compile_definitions(XtRunner PRIVATE XT_NO_CTP)
endif()

target_link_libraries(XtRunner PRIVATE xt_live_telemetry xt_live_execution xt_live_config xt_live_common xt_live_control)
target_link_libraries(XtRunner PRIVATE xt_metrics)

set_target_properties(XtRunner PROPERTIES
    BUILD_RPATH "${CMAKE_SOURCE_DIR}/third_party/CTP6.3.15/linux;\$ORIGIN/../../third_party/CTP6.3.15/linux"
)

# Split DWARF debug symbols into a sibling `.debug` file (555 MB → 16 MB prod
# binary). Split rather than plain `strip` so crash dumps still symbolicate:
# gdb / addr2line auto-load XtRunner.debug via the `.gnu_debuglink` section.
# Enabled for Release + RelWithDebInfo; Debug keeps symbols inline.
# 迭代提速: -DXT_SKIP_DEBUG_SPLIT=ON 跳过整段 (部署默认 OFF, 照常拆)。debug-split 是
# post-link objcopy, 只动调试段不改 .text/.data → 拆/不拆可执行码逐字节相同, 跳过对
# 测试/regression 行为零影响; 仅迭代时省一次几百 MB objcopy。详见 docs/development/build_speed.md。
option(XT_SKIP_DEBUG_SPLIT "Skip XtRunner POST_BUILD debug-split (迭代提速; 部署保持 OFF)" OFF)
if((CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo") AND NOT XT_SKIP_DEBUG_SPLIT)
    add_custom_command(TARGET XtRunner POST_BUILD
        COMMAND ${CMAKE_OBJCOPY} --only-keep-debug
                $<TARGET_FILE:XtRunner> $<TARGET_FILE:XtRunner>.debug
        COMMAND ${CMAKE_OBJCOPY} --strip-debug
                $<TARGET_FILE:XtRunner>
        COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:XtRunner>.debug
                $<TARGET_FILE:XtRunner>
        COMMENT "XtRunner: extract debug → XtRunner.debug; strip; debuglink"
        VERBATIM)
    # POST_BUILD doesn't register $<TARGET_FILE:XtRunner>.debug as a known
    # output, so `cmake --build --target clean` (and ninja clean) won't remove
    # the ~540 MB sibling file. Register it explicitly so clean reclaims space.
    set_property(TARGET XtRunner APPEND PROPERTY
        ADDITIONAL_CLEAN_FILES $<TARGET_FILE:XtRunner>.debug)
endif()
