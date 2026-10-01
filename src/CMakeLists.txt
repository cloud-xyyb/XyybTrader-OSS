cmake_minimum_required(VERSION 3.5)
project(xyybtrader LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# +1.4 (2026-05-11): xtpy_native (SHARED module) links xt_bt_cta + transitive
# STATIC libs (spdlog mdc TLS variable 需要 -fPIC)。全 project STATIC libs 走 PIC,
# 对 binary 行为无影响（只多 ~1-2% relative addressing 成本）.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT UNIX)
    message(FATAL_ERROR "xyybtrader is Linux-only and requires a UNIX toolchain")
endif()

# Default CMAKE_BUILD_TYPE to RelWithDebInfo when unset: the XtRunner POST_BUILD
# strip-split step (src/xt_live/hft/CMakeLists.txt) is guarded by build type ==
# Release|RelWithDebInfo. An empty build type would make it silently no-op →
# deploy uploads a 555 MB unstripped binary. RelWithDebInfo keeps -O2 (prod
# parity) + -g (so the .debug split has content); explicit Debug stays untouched.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING
        "Build type (Debug / Release / RelWithDebInfo / MinSizeRel). Defaults to RelWithDebInfo so the XtRunner POST_BUILD strip-split fires; see src/xt_live/hft/CMakeLists.txt."
        FORCE)
    message(STATUS "CMAKE_BUILD_TYPE not set; defaulting to RelWithDebInfo (POST_BUILD strip-split fires)")
endif()

enable_testing()
option(CLOUDTRADER_BUILD_TESTS "Build unit tests (requires bundled googletest)" OFF)
option(CLOUDTRADER_BUILD_BENCHMARKS "Build local benchmark/perf binaries" OFF)
option(CLOUDTRADER_ENABLE_COVERAGE "Enable gcov/llvm coverage flags" OFF)

# xt_data (event store / FlatBuffers / control_proto codegen) 构建开关。上移到此
# options 簇是因为下方 gRPC provider 块的 gating 需要它先定义。
option(CLOUDTRADER_BUILD_DATA_LAYER "Build xt_data (event store, FlatBuffers)" ON)

# Sanitizer 构建选项（audit P1.9 — closes 15 D8 needs-runtime findings）。
# 用法:
#   cmake -B build_jammy_tsan  -DCLOUDTRADER_ENABLE_TSAN=ON  -S src
#   cmake -B build_jammy_asan  -DCLOUDTRADER_ENABLE_ASAN=ON  -S src
#   cmake -B build_jammy_ubsan -DCLOUDTRADER_ENABLE_UBSAN=ON -S src
# 通过 scripts/run_sanitizer_regression.sh tsan|asan|ubsan 一键 build + run。
# TSan 与 ASan 互斥（运行时 race 互相干扰），同时 ON 触发 FATAL_ERROR。
# UBSan 可与 ASan 共存（共享 cleanup runtime），暂不组合启用以保信号简单。
option(CLOUDTRADER_ENABLE_TSAN  "Enable ThreadSanitizer for concurrency race detection"   OFF)
option(CLOUDTRADER_ENABLE_ASAN  "Enable AddressSanitizer for memory safety detection"      OFF)
option(CLOUDTRADER_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer for UB detection"       OFF)

if(CLOUDTRADER_ENABLE_TSAN AND CLOUDTRADER_ENABLE_ASAN)
    message(FATAL_ERROR
        "TSan and ASan are mutually exclusive — their runtimes conflict. "
        "Choose one: -DCLOUDTRADER_ENABLE_TSAN=ON OR -DCLOUDTRADER_ENABLE_ASAN=ON.")
endif()

if(CLOUDTRADER_ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(-O0 -g --coverage)
        add_link_options(--coverage)
        message(STATUS "Coverage instrumentation enabled")
    else()
        message(WARNING "CLOUDTRADER_ENABLE_COVERAGE is ON but compiler is unsupported")
    endif()
endif()

if(CLOUDTRADER_ENABLE_TSAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -O1 -g)
        add_link_options(-fsanitize=thread)
        message(STATUS "TSAN (ThreadSanitizer) enabled — concurrency race detection")
    else()
        message(WARNING "CLOUDTRADER_ENABLE_TSAN ON but compiler not GCC/Clang")
    endif()
endif()

if(CLOUDTRADER_ENABLE_ASAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(-fsanitize=address -fno-omit-frame-pointer -O1 -g)
        add_link_options(-fsanitize=address)
        message(STATUS "ASAN (AddressSanitizer) enabled — memory safety detection")
    else()
        message(WARNING "CLOUDTRADER_ENABLE_ASAN ON but compiler not GCC/Clang")
    endif()
endif()

if(CLOUDTRADER_ENABLE_UBSAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        # -fno-sanitize-recover=undefined 让 UB 触发即 abort，便于 CI 失败定位。
        # alignment + bool + bounds + enum + integer-divide-by-zero + null + ...
        # 默认 group `undefined` 涵盖大部分常见 UB；vptr 在 jammy GCC 11 默认 off。
        add_compile_options(-fsanitize=undefined -fno-sanitize-recover=undefined
                            -fno-omit-frame-pointer -O1 -g)
        add_link_options(-fsanitize=undefined)
        message(STATUS "UBSAN (UndefinedBehaviorSanitizer) enabled — UB detection (abort-on-trap)")
    else()
        message(WARNING "CLOUDTRADER_ENABLE_UBSAN ON but compiler not GCC/Clang")
    endif()
endif()

# ── 网络/序列化第三方库 ────────────────────────────────────
# Boost.Beast = WS/HTTP（header-only）；libcurl = REST；nlohmann/json = JSON；
# OpenSSL = TLS。
include(FetchContent)

# Boost.Beast 及其依赖：系统包（Ubuntu 24.04 libboost-dev 1.83 自带 Beast）。
# 仅显式 link Boost::boost (header meta) + Boost::system（Asio/Beast 运行时依赖）。
find_package(Boost 1.74 REQUIRED COMPONENTS system)

# nlohmann/json：vendored 到 third_party/（与 spdlog / yaml-cpp 同模式）。
# WSL 内 github HTTPS 不通，Windows 端 git clone v3.11.3 一次性入仓。
set(JSON_BuildTests OFF CACHE INTERNAL "")
set(JSON_Install OFF CACHE INTERNAL "")
add_subdirectory(third_party/nlohmann_json)


# OpenSSL（Beast TLS + HMAC）：系统包。
find_package(OpenSSL REQUIRED)

# libcurl（binance REST client）：系统包。
find_package(CURL REQUIRED)


# Apache Arrow / Parquet：event parquet PoC reader。优先使用系统包；
# 本地开发机若只有 pyarrow wheel，则回退到 pyarrow bundled C++ headers/libs。
find_package(Arrow QUIET)
find_package(Parquet QUIET)

if(NOT Arrow_FOUND OR NOT Parquet_FOUND)
    find_program(XT_PYTHON_EXECUTABLE NAMES python3 python)
    if(XT_PYTHON_EXECUTABLE)
        execute_process(
            COMMAND ${XT_PYTHON_EXECUTABLE} -c
                "import glob, pathlib, pyarrow; d=pathlib.Path(pyarrow.__file__).parent; inc=pyarrow.get_include(); arrow=sorted(glob.glob(str(d/'libarrow.*')))[:1]; parquet=sorted(glob.glob(str(d/'libparquet.*')))[:1]; print(';'.join([inc, str(d), arrow[0] if arrow else '', parquet[0] if parquet else '']))"
            OUTPUT_VARIABLE XT_PYARROW_INFO
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )
        if(XT_PYARROW_INFO)
            string(REPLACE ";" "\n" XT_PYARROW_LINES "${XT_PYARROW_INFO}")
            string(REGEX REPLACE "\n.*" "" XT_PYARROW_INCLUDE "${XT_PYARROW_LINES}")
            string(REGEX REPLACE "^[^\n]*\n([^\n]*).*" "\\1" XT_PYARROW_LIBDIR "${XT_PYARROW_LINES}")
            string(REGEX REPLACE "^[^\n]*\n[^\n]*\n([^\n]*).*" "\\1" XT_PYARROW_ARROW_LIB "${XT_PYARROW_LINES}")
            string(REGEX REPLACE "^[^\n]*\n[^\n]*\n[^\n]*\n([^\n]*).*" "\\1" XT_PYARROW_PARQUET_LIB "${XT_PYARROW_LINES}")
        endif()
    endif()
    if(XT_PYARROW_INCLUDE AND XT_PYARROW_ARROW_LIB AND XT_PYARROW_PARQUET_LIB)
        add_library(XtPyArrow::arrow SHARED IMPORTED)
        set_target_properties(XtPyArrow::arrow PROPERTIES
            IMPORTED_LOCATION "${XT_PYARROW_ARROW_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${XT_PYARROW_INCLUDE}")
        add_library(XtPyArrow::parquet SHARED IMPORTED)
        set_target_properties(XtPyArrow::parquet PROPERTIES
            IMPORTED_LOCATION "${XT_PYARROW_PARQUET_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${XT_PYARROW_INCLUDE}")
        set(Arrow_FOUND TRUE)
        set(Parquet_FOUND TRUE)
        set(XT_ARROW_TARGET XtPyArrow::arrow)
        set(XT_PARQUET_TARGET XtPyArrow::parquet)
        message(STATUS "Using pyarrow bundled Arrow/Parquet libraries from ${XT_PYARROW_LIBDIR}")
    else()
        message(FATAL_ERROR "Apache Arrow/Parquet C++ libraries not found. Install libarrow-dev/libparquet-dev on Linux or apache-arrow via Homebrew.")
    endif()
endif()

if(NOT XT_ARROW_TARGET)
    if(TARGET Arrow::arrow_shared)
        set(XT_ARROW_TARGET Arrow::arrow_shared)
    elseif(TARGET Arrow::arrow_static)
        set(XT_ARROW_TARGET Arrow::arrow_static)
    elseif(Arrow_LIBRARIES)
        set(XT_ARROW_TARGET ${Arrow_LIBRARIES})
    else()
        set(XT_ARROW_TARGET Arrow::arrow)
    endif()
endif()
if(NOT XT_PARQUET_TARGET)
    if(TARGET Parquet::parquet_shared)
        set(XT_PARQUET_TARGET Parquet::parquet_shared)
    elseif(TARGET Parquet::parquet_static)
        set(XT_PARQUET_TARGET Parquet::parquet_static)
    elseif(Parquet_LIBRARIES)
        set(XT_PARQUET_TARGET ${Parquet_LIBRARIES})
    else()
        set(XT_PARQUET_TARGET Parquet::parquet)
    endif()
endif()

add_library(xt_arrow_parquet INTERFACE)
target_link_libraries(xt_arrow_parquet INTERFACE ${XT_ARROW_TARGET} ${XT_PARQUET_TARGET})
# 共享 parquet 读取 helper（read_parquet_table / col_array / typed accessor），
# 下游 link xt_arrow_parquet 即获得（datalake 与 backtest 读路径共用同一份实现）。
# 该 helper 不放 xt_util：它 include Arrow / Parquet headers，必须随本 target 的
# 外部依赖边界传播，避免通用工具库被 Arrow 污染。
target_include_directories(xt_arrow_parquet INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/xt_arrow)

# 通用工具库：time/string/path/runtime/market/stats 等跨模块轻量工具。
add_library(xt_util_sync_hazard STATIC
    ${CMAKE_SOURCE_DIR}/xt_util/sync/XtHazardDomain.cpp
)
target_include_directories(xt_util_sync_hazard PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_util
)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(xt_util_sync_hazard PUBLIC pthread)
endif()

add_library(xt_util STATIC
    ${CMAKE_SOURCE_DIR}/xt_util/path/PathUtil.cpp
    ${CMAKE_SOURCE_DIR}/xt_util/runtime/XtProcessUtil.cpp
    ${CMAKE_SOURCE_DIR}/xt_util/time/XtTimeParse.cpp
    ${CMAKE_SOURCE_DIR}/xt_util/string/XtStringUtil.cpp
    ${CMAKE_SOURCE_DIR}/xt_util/runtime/ThreadUtil.cpp
    ${CMAKE_SOURCE_DIR}/xt_util/time/XtTime.cpp
    ${CMAKE_SOURCE_DIR}/xt_util/runtime/SdNotify.cpp  # T1.81 systemd Type=notify protocol
)
target_include_directories(xt_util PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_util
)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(xt_util PUBLIC rt)
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_libraries(xt_util PUBLIC stdc++fs)
endif()
# xt_util 收敛为 stdlib-only generic leaf：原唯一反向依赖 XtBarAggregator.h 已迁
# xt_runtime/market、XtSessionFilter.h 已迁 xt_core/calendar，xt_util 不再 link xt_core。
target_link_libraries(xt_util PUBLIC xt_util_sync_hazard)

# Plan v1.4 E2: xt_core 自带 src/xt_core/CMakeLists.txt (was inline declared here).
# 跟项目 house style (xt_account / xt_backtest / xt_config / xt_data 等子目录都有
# own CMakeLists) 对齐. xt_core CMakeLists 包含 Plan v1.4 history 文档头.
add_subdirectory(xt_core)

# Plan v1.4 A1: 业务逻辑 trio + parser 抽出到独立 xt_cost lib.
add_subdirectory(xt_cost)

# MD fan-out connector (shm) Phase 1: 跨进程 IPC 原语（header-only ring + wire 契约）.
add_subdirectory(xt_ipc)

if(CLOUDTRADER_BUILD_DATA_LAYER)
    add_subdirectory(xt_data)
endif()

# in-process Prometheus metrics — runner direct emit
# 替代 retired sidecar 扫 JSONL 重算路径。xt_metrics 库依赖 prometheus-cpp
# (FetchContent + civetweb HTTP server)。
option(CLOUDTRADER_BUILD_METRICS_LAYER
       "Build xt_metrics (in-process Prometheus metrics, prometheus-cpp)" ON)
if(CLOUDTRADER_BUILD_METRICS_LAYER)
    add_subdirectory(xt_metrics)
endif()

add_subdirectory(xt_config)
add_subdirectory(xt_logger)
add_subdirectory(xt_live/gateway)
add_subdirectory(xt_live/gateway/binance)
add_subdirectory(xt_runtime)
add_subdirectory(xt_risk)
add_subdirectory(xt_account)

# concurrency_substrate_bench 在 src/xt_tests/CMakeLists.txt（与 HazardStressTest
# 同目录，同源 hazard pointer），不跨 src 边界引 tools/bench/。

add_subdirectory(xt_engine)
add_subdirectory(xt_live/telemetry)
# 参数底座小 lib (零业务依赖 leaf): frame-loop 读 + panel 写共用,
# 必须先于 execution/cta/hft/sel/panel（这些目录的 target 都 link 它）。
add_subdirectory(xt_live/paramdomain)
# live config 四 TU + 公共装配基础设施 lib（TU 单一所有权）。
# config 先于 common（common link config）。
add_subdirectory(xt_live/config)
add_subdirectory(xt_live/common)
add_subdirectory(xt_live/execution)
# 跨引擎 runner 控制操作 lib（PanelHelpers + XtKillSwitchState）。
add_subdirectory(xt_live/control)
# panel/operator gRPC 控制平面绑定层 (command wrapper/binder/query)。单向依赖
# runner 域 lib。目录可整体移除（闭源拆分）：EXISTS gate 让物理删除后照常
# configure，消费 target 以 if(TARGET xt_panel) 退化。
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/xt_panel/CMakeLists.txt)
    add_subdirectory(xt_panel)
endif()
add_subdirectory(xt_backtest/util)
add_subdirectory(xt_backtest/common)  # T1.60: header-only shared helpers (BarTradeRow / packed_ts_from_*)
add_subdirectory(xt_backtest/data)
add_subdirectory(xt_backtest/infra)
add_subdirectory(xt_backtest/core)
add_subdirectory(xt_report)
add_subdirectory(xt_backtest/reporter)  # Plan v5 从 xt_report/backtest 搬来
add_subdirectory(xt_backtest/hft)
add_subdirectory(xt_backtest/cta)
add_subdirectory(xt_backtest/sel)  # SEL backtest runner
add_subdirectory(xt_live/hft)
add_subdirectory(xt_strategy_engine)
if(CLOUDTRADER_BUILD_TESTS)
    add_subdirectory(xt_tests)
endif()

# XtBackTestRunner — HFT 回测可执行（统一走 XtHftEngine）。
add_executable(XtBackTestRunner ${CMAKE_SOURCE_DIR}/xt_backtest/hft/XtHftBacktestRunner.cpp)
target_include_directories(XtBackTestRunner PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine/loader
    ${CMAKE_SOURCE_DIR}/xt_backtest/hft
    ${CMAKE_SOURCE_DIR}/xt_backtest/core
    ${CMAKE_SOURCE_DIR}/xt_backtest/infra
    ${CMAKE_SOURCE_DIR}/xt_engine/hft
    ${CMAKE_SOURCE_DIR}/xt_engine/sim
    ${CMAKE_SOURCE_DIR}/xt_runtime/market
    ${CMAKE_SOURCE_DIR}/xt_account/state
    ${CMAKE_SOURCE_DIR}/xt_account/ingest
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
    ${CMAKE_SOURCE_DIR}/xt_risk
    ${CMAKE_SOURCE_DIR}/xt_report
    ${CMAKE_SOURCE_DIR}/xt_backtest/reporter
)
target_link_libraries(XtBackTestRunner PRIVATE
    xt_bt_hft xt_bt_reporter xt_engine_hft xt_report xt_logger xt_core xt_stra
    dl
)

add_executable(XtSelBacktestRunner
    ${CMAKE_SOURCE_DIR}/xt_backtest/sel/XtSelBacktestRunnerMain.cpp
)
target_include_directories(XtSelBacktestRunner PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/api
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine
    ${CMAKE_SOURCE_DIR}/xt_strategy_engine/loader
    ${CMAKE_SOURCE_DIR}/xt_engine/sel
    ${CMAKE_SOURCE_DIR}/xt_backtest/sel
    ${CMAKE_SOURCE_DIR}/xt_data/datalake
)
target_link_libraries(XtSelBacktestRunner PRIVATE
    xt_bt_sel
    xt_engine_sel
    xt_data_datalake
    xt_stra
    xt_runtime         # XtOrderRuntimeBook
    xt_account         # AccountStateStore symbols (position_*_qty / position_mark_price)
    xt_config
    xt_core
    xt_logger
    yaml-cpp
    dl
)

# 公开回归使用的两个独立策略插件。
add_subdirectory(oss)
