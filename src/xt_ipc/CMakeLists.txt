# ── xt_ipc — 跨进程 IPC 原语 ───────────────────────────────────────────────
# 提供 MD fan-out 共享内存 ring、wire contract，以及 Linux-only connector/reader
# 诊断二进制。公共库保持 header-only，consumer 走完整 `xt_ipc/...` include 路径。
#
# xt_ipc transport 原语不反向引入回测或 runner 目标，避免 IPC 诊断目标影响离线回归面。

add_library(xt_ipc INTERFACE)
target_include_directories(xt_ipc INTERFACE ${CMAKE_SOURCE_DIR})
target_link_libraries(xt_ipc INTERFACE xt_core)   # XtRawTick（wire element）

# ── xt_ipc_md_gateway — shm transport 的 IMdGateway 实现（STATIC，Linux-only）─────
# XtShmMdGateway（IMdGateway impl，内部 ring-reader 线程）由 create_md_gateway 在
# md.transport=shm 时返回。GatewayFactory.cpp 在 3 runner + connector 各自 per-target
# 直编，故本实现拆 STATIC lib 供其 link（vs 编进每个 target）。只依赖 IMdGateway +
# xt_ipc header，不引入 venue SDK。
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_library(xt_ipc_md_gateway STATIC XtShmMdGateway.cpp)
    target_include_directories(xt_ipc_md_gateway PUBLIC ${CMAKE_SOURCE_DIR})
    target_link_libraries(xt_ipc_md_gateway PUBLIC xt_ipc xt_core pthread)
endif()

# connector / reader binary 走 POSIX shm（shm_open/mmap），仅 Linux 编。
# shm_open/mmap 在 jammy GLIBC 2.35 已在 libc（xt_core→xt_util PUBLIC rt 兜底旧 glibc）。
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
# connector 的 bin replay 源依赖 xt_bt_data；live 源通过 GatewayFactory.cpp 进入
# per-venue target。generic target 编出所有 venue（XT_NO_CTP/XT_NO_BINANCE/XT_MD_ONLY），
# 保持 SDK-free；--source live 在 validation 阶段 fail-closed。
    add_executable(xt_md_connector
        xt_md_connector.cpp
        ${CMAKE_SOURCE_DIR}/xt_live/gateway/GatewayFactory.cpp
    )
    target_compile_definitions(xt_md_connector PRIVATE XT_NO_CTP XT_NO_BINANCE XT_MD_ONLY)
    target_include_directories(xt_md_connector PRIVATE ${CMAKE_SOURCE_DIR}/xt_live/gateway)
    target_link_libraries(xt_md_connector PRIVATE xt_ipc xt_ipc_md_gateway xt_bt_data xt_metrics pthread)

    add_executable(xt_md_reader xt_md_reader.cpp)
    target_link_libraries(xt_md_reader PRIVATE xt_ipc pthread)

    # per-venue connector 的 venue 宏由 target 名固定，不读取 runner 的
    # XT_BUILD_GATEWAY_* host option；connector 是每 venue 一个二进制。

    # ── xt_md_connector_ctp — per-venue live MD connector（CTP only）─────────────
    # 复用 gateway_factory::create_md_gateway("ctp", ...) 接 CTP 行情前置并写入
    # XtShmRing。GatewayFactory.cpp per-target 直编以承载固定 venue 宏：
    #   XT_NO_BINANCE → binance gateway + Boost.Beast/OpenSSL 不进入二进制。
    #   XT_MD_ONLY    → 所有 TD (trade gateway) factory 编出去 → 不链 xt_td /
    #                   TraderCTP / xt_engine_common，二进制不含 order/account 路径。
    #   不定义 XT_NO_CTP → CTP MD venue 保留。
    # link xt_md（MdCTPGateway/ParserCTP + CTP MD SDK）+ xt_ipc + xt_bt_data
    # （--source bin dev 路径）；不 link xt_gateway_binance / xt_gateway_xtp / xt_td。
    add_executable(xt_md_connector_ctp
        xt_md_connector.cpp
        ${CMAKE_SOURCE_DIR}/xt_live/gateway/GatewayFactory.cpp
    )
    target_compile_definitions(xt_md_connector_ctp PRIVATE XT_NO_BINANCE XT_MD_ONLY)
    target_include_directories(xt_md_connector_ctp PRIVATE
        ${CMAKE_SOURCE_DIR}/xt_live/gateway
        ${CMAKE_SOURCE_DIR}/xt_live/gateway/ctp
    )
    # GatewayFactory.cpp 引用 XtShmMdGateway（shm transport 分支）→ link 其实现 lib。
    # XtShmMdGateway 不依赖 venue SDK，不改变 CTP connector 的 venue 符号边界。
    target_link_libraries(xt_md_connector_ctp PRIVATE xt_ipc xt_ipc_md_gateway xt_md xt_bt_data xt_metrics pthread)

    # CTP6.3.15 静态嵌入旧 OpenSSL；强制系统 libssl/libcrypto 先进 NEEDED（与 runner
    # 同口径），即便本 binary 无 binance/beast 也保 TLS 符号绑定到系统 OpenSSL。
    find_package(OpenSSL REQUIRED)
    target_link_options(xt_md_connector_ctp BEFORE PRIVATE
        "-Wl,--push-state,--no-as-needed"
        "-l:libssl.so.3"
        "-l:libcrypto.so.3"
        "-Wl,--pop-state"
    )
    set_target_properties(xt_md_connector_ctp PROPERTIES
        BUILD_RPATH "${CMAKE_SOURCE_DIR}/third_party/CTP6.3.15/linux;\$ORIGIN/../../third_party/CTP6.3.15/linux"
    )

    # ── xt_md_connector_binance — per-venue live MD connector（binance U only）─────
    # 镜像 xt_md_connector_ctp，venue 翻转：
    #   XT_NO_CTP  → CTP gateway + thost（静态嵌旧 OpenSSL）整段编出 → 二进制只链系统
    #                OpenSSL（binance Boost.Beast/CURL 用）。因此 CTP connector 需要的
    #                libssl.so.3 强制 NEEDED 处理在这里不需要（无 thost）。
    #   XT_MD_ONLY → 所有 TD factory 编出 → 不引 order/account 下单路径。
    #   不定义 XT_NO_BINANCE → binance MD venue 保留。
    # binance public MD 无鉴权（BinanceMdCfg 仅 WS/REST URL）。link xt_gateway_binance
    # （含 MdBinanceUGateway + Boost.Beast/OpenSSL/CURL，PUBLIC 传递 include/链接）。
    # static lib 语义：XT_MD_ONLY 下 GatewayFactory 不引用 TraderBinance*，其 .o 不被拉入。
    # xt_gateway_binance target 在 generation 期解析，允许本子目录先于 gateway/binance parse。
    add_executable(xt_md_connector_binance
        xt_md_connector.cpp
        ${CMAKE_SOURCE_DIR}/xt_live/gateway/GatewayFactory.cpp
    )
    target_compile_definitions(xt_md_connector_binance PRIVATE XT_NO_CTP XT_MD_ONLY)
    target_include_directories(xt_md_connector_binance PRIVATE
        ${CMAKE_SOURCE_DIR}/xt_live/gateway
        ${CMAKE_SOURCE_DIR}/xt_live/gateway/binance
    )
    target_link_libraries(xt_md_connector_binance PRIVATE
        xt_ipc xt_ipc_md_gateway xt_gateway_binance xt_bt_data xt_metrics pthread)
endif()
