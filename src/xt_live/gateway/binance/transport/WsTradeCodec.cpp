#include "WsTradeCodec.h"

#include "BinanceTdWireUtil.h"
#include "BinanceOrderParams.h"
#include "RestTradeTransport.h"
#include "rest/BinanceSign.h"
#include "refdata/BinanceExchangeInfo.h"

#include <cmath>
#include <map>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

namespace binance_u {
namespace {

thread_local nlohmann::json g_build_json;
thread_local nlohmann::json g_parse_json;

std::string id_to_string(const nlohmann::json& j) {

    auto it = j.find("id");
    if (it == j.end() || it->is_null()) return {};
    if (it->is_string()) return it->get_ref<const std::string&>();
    if (it->is_number_unsigned()) return std::to_string(it->get<uint64_t>());
    if (it->is_number_integer()) return std::to_string(it->get<int64_t>());
    return {};
}

void fill_json_params(nlohmann::json& params,
                      const std::map<std::string, std::string>& signed_params) {
    for (const auto& [k, v] : signed_params) {
        if (k == "recvWindow" || k == "timestamp") {
            params[k] = std::stoll(v);
        } else {
            params[k] = v;
        }
    }
}

std::string build_request(std::string_view req_id, std::string_view method,
                          const std::map<std::string, std::string>& signed_params) {

    g_build_json = nlohmann::json::object();
    g_build_json["id"] = std::string(req_id);
    g_build_json["method"] = std::string(method);
    auto& params = g_build_json["params"];
    params = nlohmann::json::object();
    fill_json_params(params, signed_params);
    return g_build_json.dump();
}

int json_int_or(const nlohmann::json& j, const char* key, int dflt) {
    try { return j.value(key, dflt); } catch (...) { return dflt; }
}

TransportAck::Kind classify_failure_kind(int status, int err_code) {
    return classify_rest_submit_failure(status, err_code) ==
                   RestSubmitFailureClass::kExecutionUnknown
               ? TransportAck::Kind::kTransportError_TimeoutAfterSend
               : TransportAck::Kind::kRejected;
}

std::string json_string_or_empty(const nlohmann::json& j, const char* key) {
    auto it = j.find(key);
    if (it == j.end()) return {};
    try {
        if (it->is_string()) return it->get_ref<const std::string&>();
        if (it->is_number_unsigned()) return std::to_string(it->get<uint64_t>());
        if (it->is_number_integer()) return std::to_string(it->get<int64_t>());
    } catch (...) {}
    return {};
}

void parse_success_result(const nlohmann::json& result, TransportAck& out_ack) {
    out_ack.sys_id = json_string_or_empty(result, "orderId");

    auto status_it = result.find("status");
    if (status_it != result.end() && status_it->is_string()) {
        out_ack.raw_status = status_it->get_ref<const std::string&>();
    }
    auto eq_it = result.find("executedQty");
    if (eq_it != result.end() && eq_it->is_string()) {
        try {
            const double eq = std::stod(eq_it->get_ref<const std::string&>());

            out_ack.filled_fp = std::isfinite(eq) ? order_exec_to_fp(eq) : 0;
        } catch (...) {
            out_ack.filled_fp = 0;
        }
    }

    const XtOrderStatus status = parse_binance_status(out_ack.raw_status);
    if (status == XtOrderStatus::Canceled) {
        out_ack.kind = TransportAck::Kind::kCanceled;
    } else if (status == XtOrderStatus::Rejected) {
        out_ack.kind = TransportAck::Kind::kRejected;
    } else {
        out_ack.kind = TransportAck::Kind::kAccepted;
    }
}

}

	std::string build_submit_request(const TransportSubmitReq& req,
	                                 std::string_view req_id,
	                                 int64_t now_ms,
	                                 const WsTradeCredentials& cred,
	                                 BinanceSubmitWireValues* wire_out) {
	    auto built = build_submit_order_params(req);
	    if (wire_out) *wire_out = built.wire;
	    std::map<std::string, std::string> params;
	    params["apiKey"] = cred.api_key;
	    params.insert(built.params.begin(), built.params.end());
	    params["recvWindow"] = std::to_string(cred.recv_window_ms);
	    params["timestamp"] = std::to_string(now_ms);
	    params["signature"] = sign::sign_params(cred.api_secret, params);
    return build_request(req_id, "order.place", params);
}

std::string build_cancel_request(const TransportCancelReq& req,
                                 std::string_view req_id,
                                 int64_t now_ms,
                                 const WsTradeCredentials& cred) {
    std::map<std::string, std::string> params;
    params["apiKey"] = cred.api_key;
    params["symbol"] = req.symbol;
    params["origClientOrderId"] = req.client_order_id;
    params["recvWindow"] = std::to_string(cred.recv_window_ms);
    params["timestamp"] = std::to_string(now_ms);
    params["signature"] = sign::sign_params(cred.api_secret, params);
    return build_request(req_id, "order.cancel", params);
}

std::string parse_response(std::string_view payload, TransportAck& out_ack) {
    out_ack = TransportAck{};

    out_ack.timing_origin = TransportAck::TimingOrigin::kVenueResponse;

    g_parse_json = nlohmann::json::parse(payload, nullptr, false);
    if (g_parse_json.is_discarded() || !g_parse_json.is_object()) return {};
    std::string req_id = id_to_string(g_parse_json);
    if (req_id.empty()) return {};

    const int status = json_int_or(g_parse_json, "status", 0);
    auto err_it = g_parse_json.find("error");
    if (err_it != g_parse_json.end() && err_it->is_object()) {
        out_ack.err_code = json_int_or(*err_it, "code", status);

        auto msg_it = err_it->find("msg");
        if (msg_it != err_it->end() && msg_it->is_string()) {
            out_ack.err_msg = msg_it->get_ref<const std::string&>();
        }
        out_ack.kind = (out_ack.err_code == -2011)
                           ? TransportAck::Kind::kCancelNoop
                           : classify_failure_kind(status, out_ack.err_code);
        return req_id;
    }

    if (status != 200) {

        out_ack.kind = classify_failure_kind(status,  0);
        out_ack.err_code = status;
        return req_id;
    }
    auto result_it = g_parse_json.find("result");
    if (result_it == g_parse_json.end() || !result_it->is_object()) return {};
    parse_success_result(*result_it, out_ack);
    return req_id;
}

}
