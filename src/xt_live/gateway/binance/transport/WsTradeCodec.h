#pragma once

#include <string>
#include <string_view>

#include "ITradeTransport.h"
#include "BinanceOrderParams.h"

namespace binance_u {

/// @brief WS 交易请求签名所需凭据。
struct WsTradeCredentials {
    std::string api_key;       ///< Binance API key，写入 WS params.apiKey。
    std::string api_secret;    ///< Binance API secret，仅用于本地计算 signature。
    int recv_window_ms = 5000; ///< Binance recvWindow，单位 ms。
    /// @note server_time_offset_ms 不存放在 credentials 内。transport 在实际发送前
    ///       从 BinanceRestClient 原子读取最新 offset，再把校准后的 now_ms 传入 codec。
};

/// @brief 构造 `order.place` WS 请求 JSON。
/// @param req transport 层 submit 请求。
/// @param req_id 写入 JSON `id`，调用方负责保证 inflight 内唯一。
/// @param now_ms 写入 Binance `timestamp` 的 wall-clock ms；必须是发送时刻而非入队时刻，
///        且已叠加 serverTime offset，避免 Binance `-1021` 时间戳拒绝。
/// @param cred API key、secret 与 recvWindow。
	std::string build_submit_request(const TransportSubmitReq& req,
	                                 std::string_view req_id,
	                                 int64_t now_ms,
	                                 const WsTradeCredentials& cred,
	                                 BinanceSubmitWireValues* wire_out = nullptr);

/// @brief 构造 `order.cancel` WS 请求 JSON。
/// @param req transport 层 cancel 请求。
/// @param req_id 写入 JSON `id`，调用方负责保证 inflight 内唯一。
/// @param now_ms 写入 Binance `timestamp` 的 wall-clock ms，必须使用发送时刻。
/// @param cred API key、secret 与 recvWindow。
std::string build_cancel_request(const TransportCancelReq& req,
                                 std::string_view req_id,
                                 int64_t now_ms,
                                 const WsTradeCredentials& cred);

/// @brief 解析 WS 交易响应 payload。
/// @param out_ack 输出统一 ack；codec 只填 kind/sys_id/filled_fp/raw_status/err_code/err_msg，
///        local_id 保持 0，由 transport 根据 req_id 查 inflight 后补齐。
/// @return JSON `id` 字段解析出的 req_id；返回空串表示 payload 无法用于归因，调用方应丢弃。
std::string parse_response(std::string_view payload, TransportAck& out_ack);

}  // namespace binance_u
