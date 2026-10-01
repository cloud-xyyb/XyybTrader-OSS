#include "WsTradeCodec.h"

#include "BinanceTdWireUtil.h"
#include "BinanceOrderParams.h"
#include "RestTradeTransport.h"  // classify_rest_submit_failure：BIN-D14 分类 SSOT
#include "rest/BinanceSign.h"
#include "refdata/BinanceExchangeInfo.h"

#include <cmath>
#include <map>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

namespace binance_u {
namespace {

// WS TD codec 只在 transport IO 线程调用；thread_local 复用 JSON 节点，避免热路径反复构造。
thread_local nlohmann::json g_build_json;
thread_local nlohmann::json g_parse_json;

std::string id_to_string(const nlohmann::json& j) {
    // Binance WS API 允许 id 为 string 或 number；transport inflight key 统一用 string。
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
    // 复用 thread_local root 与 params；dump() 仍分配返回 string，但避免根节点和
    // nested object 在每笔请求上重复构造。
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

/// @brief WS 失败回包 → TransportAck::Kind。
/// @note Binance WS-API 的 `status` 字段镜像等价 REST 端点的 HTTP 状态码，
///       `error.code` 与 REST 同一码表 → 复用 BIN-D14 的
///       classify_rest_submit_failure 作分类 SSOT：
///       - execution-unknown（5xx / 3xx / status 缺失(0) / -1007 / -1006 / -1001）
///         → kTransportError_TimeoutAfterSend：委托可能已挂上/成交，不得合成终态
///         销账；ledger 对 submit 保持 Submitted 交 reconcile，对 cancel 走
///         KeepInflight 早返回短路。
///       - 其余（4xx 业务拒 / 429/418/-1003/-1008 限流忙 = venue 明确未执行）
///         → kRejected 终态化，保持现行拒绝语义。
///       - -2011 kCancelNoop 在 caller 侧优先短路，不进本函数。
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
    // 用 find()+get_ref 避免 value(key, std::string()) 临时构造
    auto status_it = result.find("status");
    if (status_it != result.end() && status_it->is_string()) {
        out_ack.raw_status = status_it->get_ref<const std::string&>();
    }
    auto eq_it = result.find("executedQty");
    if (eq_it != result.end() && eq_it->is_string()) {
        try {
            const double eq = std::stod(eq_it->get_ref<const std::string&>());
            // P15：stod("nan") 不抛；非有限不进 fp（同字段缺失语义 filled_fp=0）。
            out_ack.filled_fp = std::isfinite(eq) ? order_exec_to_fp(eq) : 0;
        } catch (...) {
            out_ack.filled_fp = 0;
        }
    }

    // raw_status 已 materialize 到 out_ack 字段，复用为 string_view 走 zero-copy
    // 比较（parse_binance_status 接 string_view）。
    const XtOrderStatus status = parse_binance_status(out_ack.raw_status);
    if (status == XtOrderStatus::Canceled) {
        out_ack.kind = TransportAck::Kind::kCanceled;
    } else if (status == XtOrderStatus::Rejected) {
        out_ack.kind = TransportAck::Kind::kRejected;
    } else {
        out_ack.kind = TransportAck::Kind::kAccepted;
    }
}

}  // namespace

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
    // 收到真实 WS 帧 = venue 响应（含 5xx/-1007 execution-unknown），到达时刻可采端点。
    out_ack.timing_origin = TransportAck::TimingOrigin::kVenueResponse;
    // 复用 thread_local g_parse_json — parse 重置 value 等价
    // clear+parse；inline reuse 对每次入口的 ack 都生效。
    g_parse_json = nlohmann::json::parse(payload, nullptr, false);
    if (g_parse_json.is_discarded() || !g_parse_json.is_object()) return {};
    std::string req_id = id_to_string(g_parse_json);
    if (req_id.empty()) return {};

    const int status = json_int_or(g_parse_json, "status", 0);
    auto err_it = g_parse_json.find("error");
    if (err_it != g_parse_json.end() && err_it->is_object()) {
        out_ack.err_code = json_int_or(*err_it, "code", status);
        // err_msg 会随 TransportAck 跨调用栈派发，不能保留 JSON string_view。
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
        // 无 error 对象的非 200 帧（含 status 缺失取 0 的畸形帧）：同码表分类
        // （无业务码传 0），err_code 沿用 ws status 供日志归因。
        out_ack.kind = classify_failure_kind(status, /*err_code=*/0);
        out_ack.err_code = status;
        return req_id;
    }
    auto result_it = g_parse_json.find("result");
    if (result_it == g_parse_json.end() || !result_it->is_object()) return {};
    parse_success_result(*result_it, out_ack);
    return req_id;
}

}  // namespace binance_u
