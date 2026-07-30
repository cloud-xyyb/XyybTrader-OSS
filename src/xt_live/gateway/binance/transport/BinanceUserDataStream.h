#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "api/ITradeGateway.h"
#include "api/XtPositionSnapshot.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"

namespace binance_u {

class BinanceRestClient;

/// @brief Binance 交易网关里的 User Data Stream 接收器。
///
/// - Binance UDS 需要先用 REST 申请 listenKey，再用该 listenKey 建立 WS 连接。
/// - BinanceUserDataStream 把 Binance JSON 转成 ITradeGateway 统一 DTO
class BinanceUserDataStream {
public:
    struct Config {
        std::string host;
        std::string port = "443";
        uint32_t keepalive_ms = 30 * 60 * 1000;
    };

    using OnOrderUpdate = std::function<void(const XtOrderExecUpdate&)>;

    using OnTrade       = std::function<void(const XtTradeRecord&, XtLatencyEligibility)>;
    using OnAccount     = std::function<void(const ITradeGateway::AccountSnapshot&)>;
    using OnPosition    = std::function<void(const std::vector<XtPositionSnapshot>&)>;

    using ParseLocalId  = std::function<uint64_t(std::string_view client_order_id)>;

    using OnUnattributedFill = std::function<void()>;

    using FillDedup = std::function<bool(uint64_t order_id, std::string_view symbol,
                                         int64_t trade_id)>;

    BinanceUserDataStream(Config cfg, BinanceRestClient& rest);
    ~BinanceUserDataStream();

    BinanceUserDataStream(const BinanceUserDataStream&)            = delete;
    BinanceUserDataStream& operator=(const BinanceUserDataStream&) = delete;

    void bind_callbacks(OnOrderUpdate on_order,
                        OnTrade on_trade,
                        OnAccount on_account,
                        OnPosition on_position,
                        ParseLocalId parse_local_id);

    void set_unattributed_fill_handler(OnUnattributedFill handler);

    void set_fill_dedup(FillDedup fn);

    bool start();

    void stop();

    bool check_health(uint32_t now_ms);

    void request_reconnect();

    int64_t last_seen_trade_id(const std::string& symbol) const;

    std::vector<std::string> tracked_trade_symbols() const;

    bool inject_gap_fill_trade(const XtTradeRecord& tr);

#ifdef XT_BUILD_TESTS

    void inject_test_message(std::string_view payload);

    void force_keepalive_for_test();
#endif

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
