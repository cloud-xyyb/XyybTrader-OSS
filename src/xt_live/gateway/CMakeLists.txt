cmake_minimum_required(VERSION 3.5)
project(XtLiveGateway LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_subdirectory(common)


# ── CTP gateway link flag ────────────────────────────────────────────
# Declared here (gateway subdir is processed before all runner subdirs in
# src/CMakeLists.txt: gateway → cta → hft → sel) so all 3 runners share one
# order-independent option.
#   ON  (default)      : runner links xt_md/xt_td; "ctp" venue available.
#   OFF (binance host) : runner defines XT_NO_CTP → GatewayFactory compiles CTP
#                        out → no thost in NEEDED → single OpenSSL (avoids the
#                        2026-06-05 binance OpenSSL symbol-collision crash).
# xt_md/xt_td targets are always built (SDK headers vendored); the per-runner
# link guard lives in xt_live/{hft,cta,sel}/CMakeLists.txt.
option(XT_BUILD_GATEWAY_CTP "Link CTP gateway (Chinese futures via thost SDK) into runners" ON)

# ── CTP SDK imported targets ─────────────────────────────────────────
set(XT_CTP_SDK_DIR "${CMAKE_SOURCE_DIR}/third_party/CTP6.3.15")
set(XT_CTP_SDK_LINUX_DIR "${XT_CTP_SDK_DIR}/linux")

# CTP 运行时 .so 是仓内 vendored 的厂商二进制（sha256/字节数见 SDK_MANIFEST.yaml，
# 正常 checkout 随 clone 就位；.gitignore 的 *.so 模式不影响已 tracked 文件）。
# 若因误删/剥离式导出等异常缺失，IMPORTED_LOCATION 会在 link 阶段崩成天书
# 「cannot find .../thost*.so」——这里 configure 期 fail-fast 给可操作指引。
# 仅 CTP link 打开时校验；OFF 时 runner 不 link CTP 消费方，缺 .so 不影响
# pinned runner target（只做 binance 用 -DXT_BUILD_GATEWAY_CTP=OFF 或 binance preset）。
# 注意 xt_ipc 的 xt_md_connector_ctp 不读本 option、恒 link CTP::Md——OFF 下裸
# ninja all 在 .so 缺失时仍会在该 target 撞 link 错，不在豁免面。
if(XT_BUILD_GATEWAY_CTP)
    foreach(_ctp_so thostmduserapi_se.so thosttraderapi_se.so)
        if(NOT EXISTS "${XT_CTP_SDK_LINUX_DIR}/${_ctp_so}")
            message(FATAL_ERROR
                "CTP SDK 运行时缺失: ${XT_CTP_SDK_LINUX_DIR}/${_ctp_so}\n"
                "  该 .so 随仓库分发（repo 内 vendored），缺失属异常（误删/剥离式 checkout）。\n"
                "  → 恢复：git checkout -- 该文件；或从 SimNow 官方分发重新获取\n"
                "    CTP 6.3.15 linux .so 放入该目录（sha256 对照 "
                "${XT_CTP_SDK_DIR}/SDK_MANIFEST.yaml），或\n"
                "  → 只做 binance/回测时用 -DXT_BUILD_GATEWAY_CTP=OFF 编译。")
        endif()
    endforeach()
endif()

add_library(CTP::Md SHARED IMPORTED GLOBAL)
set_target_properties(CTP::Md PROPERTIES
    IMPORTED_LOCATION "${XT_CTP_SDK_LINUX_DIR}/thostmduserapi_se.so"
    INTERFACE_INCLUDE_DIRECTORIES "${XT_CTP_SDK_DIR}"
)

add_library(CTP::Trader SHARED IMPORTED GLOBAL)
set_target_properties(CTP::Trader PROPERTIES
    IMPORTED_LOCATION "${XT_CTP_SDK_LINUX_DIR}/thosttraderapi_se.so"
    INTERFACE_INCLUDE_DIRECTORIES "${XT_CTP_SDK_DIR}"
)

# ── xt_md: 行情网关 ──────────────────────────────────────────────
add_library(xt_md
    ctp/md/MdCTPGateway.cpp
    ctp/md/ParserCTP.cpp
)
target_include_directories(xt_md PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/ctp
    ${CMAKE_CURRENT_SOURCE_DIR}/ctp/common
    ${CMAKE_CURRENT_SOURCE_DIR}/ctp/md
    ${CMAKE_SOURCE_DIR}/xt_live/telemetry
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
)
target_link_libraries(xt_md PUBLIC xt_core xt_config xt_logger xt_live_telemetry xt_util CTP::Md)  # Plan v1.4 Z6

# ── xt_td: 交易网关 ──────────────────────────────────────────────
add_library(xt_td
    ctp/td/TraderCTP.cpp
    ctp/td/TraderCTPSession.cpp      # TD login retry 与 settlement confirm gate
    ctp/td/TraderCTPQuery.cpp        # query 发送、流控、dirty 合并与 retry worker
    ctp/td/CtpOrderRefStore.cpp      # OrderRef seed 恢复、预留与 crash-safe 持久化
    ctp/td/TraderCTPReconnect.cpp    # front reconnect 与 ready 后周期刷新
    ctp/td/TraderCTPCashTransfer.cpp # 银期转账推送归一化
    ctp/td/TraderCTPRecovery.cpp     # post-login recovery timeout 与 reverse-sync
    ctp/td/TraderCTPOrderOps.cpp     # submit/cancel 与订单/成交 SPI 回报
    ctp/recovery/CTPStateTracker.cpp
    ctp/recovery/CTPRecovery.cpp
    # (2026-05-15) ctp/CtpVenueSession.cpp 删: IVenueSession Phase B facade 0 prod caller, xt_oms 退役
)
target_include_directories(xt_td PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/ctp
    ${CMAKE_CURRENT_SOURCE_DIR}/ctp/common
    ${CMAKE_CURRENT_SOURCE_DIR}/ctp/recovery
    ${CMAKE_CURRENT_SOURCE_DIR}/ctp/td
    ${CMAKE_SOURCE_DIR}/xt_live/telemetry
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
    ${CMAKE_SOURCE_DIR}/xt_account
)
target_link_libraries(xt_td PUBLIC xt_core xt_config xt_logger xt_engine_common xt_live_telemetry xt_util xt_gateway_common CTP::Trader)  # Plan v1.4 Z6; xt_gateway_common: XtExecDeliveryLedger
