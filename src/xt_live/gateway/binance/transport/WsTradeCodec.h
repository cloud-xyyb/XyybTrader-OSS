#pragma once

#include <string>
#include <string_view>

#include "ITradeTransport.h"
#include "BinanceOrderParams.h"

namespace binance_u {

/// @brief WS 交易请求签名所需凭据。
struct WsTradeCredentials {
    std::string api_key;
    std::string api_secret;
    int recv_window_ms = 5000;

};

	std::string build_submit_request(const TransportSubmitReq& req,
	                                 std::string_view req_id,
	                                 int64_t now_ms,
	                                 const WsTradeCredentials& cred,
	                                 BinanceSubmitWireValues* wire_out = nullptr);

std::string build_cancel_request(const TransportCancelReq& req,
                                 std::string_view req_id,
                                 int64_t now_ms,
                                 const WsTradeCredentials& cred);

std::string parse_response(std::string_view payload, TransportAck& out_ack);

}
