cmake_minimum_required(VERSION 3.5)
project(XtGatewayBinance LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# ── xt_gateway_binance: Binance U 本位永续网关库 ────────────────────────────
# 依赖：
#   - Boost.Beast (header-only via Boost::boost) + Boost::system (Asio 运行时)
#   - OpenSSL::SSL (Beast TLS) + OpenSSL::Crypto (HMAC-SHA256)
#   - CURL::libcurl (REST client)
#   - nlohmann_json::nlohmann_json (JSON 解析)
#   - xt_core (IMdGateway / ITradeGateway 接口 + XtRawTick 等)
#   - xt_account (BinanceAccountTruthSource for account/position truth snapshots)
#   - xt_config + xt_logger (配置 + 日志)
#
add_library(xt_gateway_binance STATIC
    # md/：WS 行情 demux、本地全量 book 与 snapshot 抓取。
    md/MdBinanceUGateway.cpp
    md/BinanceOrderBook.cpp
    md/BinanceSnapshotFetcher.cpp
    md/BinanceAggTradesFetcher.cpp

    # rest/：HMAC 签名、libcurl REST client、无签名 HTTP GET 与资金流水查询。
    rest/BinanceSign.cpp
    rest/BinanceRestClient.cpp
    rest/BinancePublicHttp.cpp
    rest/BinanceIncomeProvider.cpp

    # refdata/：exchangeInfo 解析 + PRICE_FILTER/LOT_SIZE 进程级缓存（tick/step 真值源）。
    refdata/BinanceExchangeInfo.cpp

    # transport/：codec、URL 解析、WS/REST trade transport 与 UDS 私有数据流。
    transport/BinanceTdWireUtil.cpp
    transport/BinanceWsUrl.cpp
    transport/WsTradeCodec.cpp
    transport/WsTradeTransport.cpp
    transport/RestTradeTransport.cpp
    transport/BinanceUserDataStream.cpp  # UDS：listenKey + ORDER_TRADE_UPDATE / ACCOUNT_UPDATE

    # td/：PImpl 转发、生命周期、回报回调与 recovery/reconcile。
    td/BinanceOrderLedger.cpp              # 订单身份/状态唯一权威账本（单表 + 单锁）
    td/BinanceTdParse.cpp                  # REST body 纯函数解析集（account / marks / gap-fill）
    td/BinanceReconcilerSync.cpp           # reconciler：bootstrap/resync + snapshot-vs-stream 栅栏 + account truth
    td/BinanceReconcilerSweep.cpp          # reconciler：ghost/leak sweep + gap-fill + reverse-sync + blank-sys-id
    td/TraderBinanceUGateway.cpp           # PImpl 转发 + 单测入口包装
    td/TraderBinanceUGatewayInit.cpp       # init / start / stop / check_health / cold worker
    td/TraderBinanceUGatewayCallbacks.cpp  # submit / cancel / UDS+ack 回调路径
)

if(TARGET xt_metrics)
    target_link_libraries(xt_gateway_binance PUBLIC xt_metrics)
    target_compile_definitions(xt_gateway_binance PUBLIC XT_HAVE_METRICS=1)
endif()

add_custom_target(xt_binance_gateway DEPENDS xt_gateway_binance)

target_include_directories(xt_gateway_binance PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/xt_core
    ${CMAKE_SOURCE_DIR}/xt_config
    ${CMAKE_SOURCE_DIR}/xt_logger
)

target_link_libraries(xt_gateway_binance
    PUBLIC
        xt_core             # IMdGateway / ITradeGateway / XtRawTick
        xt_config
        xt_logger
        xt_account          # BinanceAccountTruthSource
        xt_util             # XtTradingDay
        xt_gateway_common   # XtWsConnection / XtClockSkewEstimator / XtMdFreshness
        xt_live_telemetry   # XtLiveRecorder (recorder tap for MD ticks)
        Boost::boost        # Beast (header-only)
        Boost::system       # Asio / Beast 运行时依赖
        OpenSSL::SSL        # Beast WSS TLS
        OpenSSL::Crypto     # HMAC-SHA256
        CURL::libcurl       # REST client
        nlohmann_json::nlohmann_json
)

