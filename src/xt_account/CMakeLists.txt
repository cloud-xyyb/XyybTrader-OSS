cmake_minimum_required(VERSION 3.5)

project(ctAccount LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

add_library(xt_account
    state/AccountState.cpp
    state/AccountStateStore.cpp
    state/MarkPriceTable.cpp
    state/AccountReducer.cpp
    state/detail/AccountSymbolTable.cpp
    venue/AccountTruthStoreProvider.cpp
    venue/binance/BinanceAccountTruthSource.cpp
    venue/ctp/CtpAccountTruthSource.cpp
    venue/generic/StoreBackedAccountTruthSource.cpp
)

# venue/ 子目录故意不加进 PUBLIC include，consumer 必须走完整路径
# xt_account/venue/<venue>/... 而不是裸名 #include "binance/...h"。防 transitive
# include leak + namespace 污染，跟 xt_live/gateway/<venue>/ include 风格对称。
target_include_directories(xt_account
    PRIVATE
        ${CMAKE_SOURCE_DIR}
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/api
        ${CMAKE_CURRENT_SOURCE_DIR}/state
        ${CMAKE_CURRENT_SOURCE_DIR}/ingest
        ${CMAKE_SOURCE_DIR}/api
        ${CMAKE_SOURCE_DIR}/xt_config
        ${CMAKE_SOURCE_DIR}/xt_core
        ${CMAKE_SOURCE_DIR}/xt_util
)

target_link_libraries(xt_account PUBLIC
    xt_config
    xt_core
    xt_util
    xt_util_sync_hazard
    xt_logger
)

# 删 PUBLIC xt_runtime 反向 link
# - 实测 xt_account 仅引用 1 个 xt_runtime symbol: xt_runtime::IAccountView
# - 该 symbol 在 src/api/IAccountView.h (pure header, namespace xt_runtime)
# - API namespace xt_runtime 是 plugin ABI 边界保留（§6 strong constraint），
#   不动 namespace。但 xt_account → xt_runtime library binary link 是 zombie:
#   api/ 是 header-only，xt_runtime library binary 不含 IAccountView 任何 .cpp。
# - 删 PUBLIC xt_runtime 后，xt_account 通过 ${CMAKE_SOURCE_DIR}/api include
#   dir 仍能 include IAccountView.h，但不再传递 link xt_runtime binary.
# - 加 xt_core PUBLIC（前是通过 xt_runtime transitive link，删 xt_runtime 后需直接 link）
