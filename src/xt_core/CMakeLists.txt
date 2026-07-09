# xt_core 是底层 leaf library，只持有跨模块共享的 POD types 与基础 primitives。
# 本目标不反向依赖配置、运行时缓存、网关协议或 report/backtest 后处理组件。

add_library(xt_core STATIC
    # 仅编译有非 inline 实现的核心转换/状态机；大部分 POD helper 保持 header-only。
    order/XtOrderLifecycle.cpp
    market/XtTickConvert.cpp
    market/XtTickBinCodec.cpp
    market/XtRollEngine.cpp   # futures continuous-contract roll
    XtLiveTapContext.cpp      # live-tap TLS context (low shared layer)
)

# PUBLIC include path 只暴露 xt_core 的 no-prefix 兼容入口和队列 header 依赖。
# `${CMAKE_SOURCE_DIR}` 仅给 xt_core 自身解析 `#include "xt_core/..."`，避免 consumer
# 通过 xt_core 间接看到整棵 src/ 树。需要其它模块 header 的目标必须显式 link 对应模块。
target_include_directories(xt_core
    PRIVATE
        ${CMAKE_SOURCE_DIR}                # xt_core 内部 #include "xt_core/Foo.h" 路径解析
    PUBLIC
        ${CMAKE_SOURCE_DIR}/xt_core        # consumer 用 #include "XtOrder.h" no-prefix (本 project 少用, 保 backward compat)
)

# xt_core PUBLIC link xt_util：xt_core/session/ 等 header 依赖 xt_util/time generic
# helper（xt_util 是 stdlib-only leaf，依赖方向 xt_core → xt_util）。
target_link_libraries(xt_core PUBLIC xt_util)
