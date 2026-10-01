#include "BinanceRestClient.h"

#include "BinanceCurlDetail.h"  // curl_write_cb
#include "BinanceSign.h"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <cstdint>
#include <cstring>
#include <utility>

namespace binance_u {

namespace {

/// @brief curl_slist RAII 守卫，确保 header list 在 do_request 退出时释放。
struct SlistGuard {
    curl_slist* list = nullptr;
    ~SlistGuard() { if (list) curl_slist_free_all(list); }
    void append(const std::string& hdr) { list = curl_slist_append(list, hdr.c_str()); }
};

}  // anonymous namespace

// 每次 do_request reset 后显式开启 TCP keepalive。部分公网链路会在长 idle 后
// 静默丢弃半开连接，OS 探测能提前发现失效连接，避免下一笔 REST 请求才付重连成本。

namespace {
/// @brief 进程级 curl_global_init 守卫。
/// @note 必须在任意线程使用 libcurl/OpenSSL 前完成一次。链接顺序需保证系统 OpenSSL
///       先于 CTP `.so` 内嵌符号解析，否则 Beast/libcurl SSL 初始化可能被污染。
struct CurlGlobalInit {
    CurlGlobalInit() { curl_global_init(CURL_GLOBAL_ALL); }
    ~CurlGlobalInit() { curl_global_cleanup(); }
};
const CurlGlobalInit g_curl_init;
}  // namespace

BinanceRestClient::BinanceRestClient(Config cfg) : cfg_(std::move(cfg)) {
    // curl_global_init 已由 g_curl_init 静态守卫完成；实例只创建自己的 easy handle。
    curl_handle_ = curl_easy_init();
}

BinanceRestClient::~BinanceRestClient() {
    if (curl_handle_) {
        curl_easy_cleanup(static_cast<CURL*>(curl_handle_));
        curl_handle_ = nullptr;
    }
}

void BinanceRestClient::inject_timestamp(std::map<std::string, std::string>& params) const {
    const int64_t ts = static_cast<int64_t>(sign::now_ms()) + offset_ms_.load(std::memory_order_relaxed);
    params["timestamp"] = std::to_string(ts);
    params["recvWindow"] = std::to_string(cfg_.recv_window_ms);
}

BinanceRestClient::Response BinanceRestClient::do_request(
    const char* method,
    const std::string& full_url,
    const std::string& body,
    const std::map<std::string, std::string>& extra_headers,
    long timeout_ms_override) {

    Response resp;
    std::lock_guard<std::mutex> lk(curl_mu_);
    if (!curl_handle_) {
        resp.curl_err = "curl_easy_init failed earlier";
        return resp;
    }
    CURL* curl = static_cast<CURL*>(curl_handle_);
    // 注意：curl_easy_reset 重置 *option*，但保留 connection cache / DNS cache
    //       / TLS session cache（per libcurl docs）。所以单 CURL 实例 + reset
    //       仍能 keep-alive 复用连接，每次只重设 option 不付 reconnect 成本。
    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, full_url.c_str());
    if (timeout_ms_override > 0) {
        // shutdown path 用毫秒级 timeout 上限（e.g. 3000ms）替代
        // cfg_.timeout_sec(10s)，把 close_listen_key 阻塞控制在 ~3s 内。
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms_override);
    } else {
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, cfg_.timeout_sec);
    }
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);  // binance 不走 redirect
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "XyybTrader/binance_u/1.0");
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp.body);
    // ── HFT keep-alive 三件套 ────────────────────────────────────────────
    // 单 CURL handle + reset 默认已保留连接池；以下三项让 OS 在长 idle
    // (binance demo 实测 30-60s 无单空闲) 时主动探测，杜绝半开连接导致
    // 下一笔订单付 TLS 握手 ~70ms。FORBID_REUSE=0 是默认；显式写出避免被
    // 后续未显式置 0 的 setopt 误改。
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 30L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 10L);
    curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
    curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0L);
    curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);

    if (!body.empty()) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
    }

    SlistGuard headers;
    for (const auto& [k, v] : extra_headers) {
        headers.append(k + ": " + v);
    }
    if (!body.empty() && extra_headers.count("Content-Type") == 0) {
        headers.append("Content-Type: application/x-www-form-urlencoded");
    }
    if (headers.list) {
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers.list);
    }

    const CURLcode rc = curl_easy_perform(curl);
    if (rc != CURLE_OK) {
        resp.curl_err = curl_easy_strerror(rc);
        return resp;
    }
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    resp.http_status = http_code;
    // 429/418 限频响应携带 Retry-After；CURLINFO_RETRY_AFTER 统一解析
    // delta-seconds / HTTP-date 两种写法，无 header 时保持 0。
    curl_off_t retry_after = 0;
    if (curl_easy_getinfo(curl, CURLINFO_RETRY_AFTER, &retry_after) == CURLE_OK) {
        resp.retry_after_sec = static_cast<long>(retry_after);
    }
    return resp;
}

BinanceRestClient::Response BinanceRestClient::post_signed(
    const std::string& path, std::map<std::string, std::string> params) {
    inject_timestamp(params);
    const std::string signed_body = sign::sign_query(cfg_.api_secret, params);
    std::map<std::string, std::string> headers;
    headers["X-MBX-APIKEY"] = cfg_.api_key;
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    return do_request("POST", cfg_.base_url + path, signed_body, headers);
}

BinanceRestClient::Response BinanceRestClient::delete_signed(
    const std::string& path, std::map<std::string, std::string> params) {
    inject_timestamp(params);
    const std::string signed_query = sign::sign_query(cfg_.api_secret, params);
    std::map<std::string, std::string> headers;
    headers["X-MBX-APIKEY"] = cfg_.api_key;
    const std::string url = cfg_.base_url + path + "?" + signed_query;
    return do_request("DELETE", url, "", headers);
}

BinanceRestClient::Response BinanceRestClient::get_signed(
    const std::string& path, std::map<std::string, std::string> params) {
    inject_timestamp(params);
    const std::string signed_query = sign::sign_query(cfg_.api_secret, params);
    std::map<std::string, std::string> headers;
    headers["X-MBX-APIKEY"] = cfg_.api_key;
    const std::string url = cfg_.base_url + path + "?" + signed_query;
    return do_request("GET", url, "", headers);
}

BinanceRestClient::Response BinanceRestClient::get_public(
    const std::string& path, const std::map<std::string, std::string>& params) {
    std::string url = cfg_.base_url + path;
    if (!params.empty()) url += "?" + sign::build_query(params);
    return do_request("GET", url, "", {});
}

bool BinanceRestClient::sync_server_time() {
    auto r = get_public("/fapi/v1/time");
    if (!r.ok()) {
        last_sync_err_ = r.curl_err.empty() ? ("HTTP " + std::to_string(r.http_status)) : r.curl_err;
        return false;
    }
    auto j = nlohmann::json::parse(r.body, nullptr, false);
    if (j.is_discarded() || !j.contains("serverTime")) {
        last_sync_err_ = "invalid server time response: " + r.body;
        return false;
    }
    const int64_t server_ms = j["serverTime"].get<int64_t>();
    const int64_t local_ms = static_cast<int64_t>(sign::now_ms());
    offset_ms_.store(server_ms - local_ms, std::memory_order_relaxed);
    return true;
}

// ── listenKey lifecycle（binance 特例：有 X-MBX-APIKEY header 但无 HMAC 签名）──

std::string BinanceRestClient::acquire_listen_key(std::string* err) {
    // listenKey 端点是 Binance 特例：只需要 X-MBX-APIKEY header，不需要 HMAC signature。
    std::map<std::string, std::string> headers;
    headers["X-MBX-APIKEY"] = cfg_.api_key;
    // acquire/keepalive 在 UDS io_context 线程同步执行；这里用 3s 上限，避免
    // testnet HTTPS 卡住时阻塞成交/持仓处理到默认 10s。
    auto r = do_request("POST", cfg_.base_url + "/fapi/v1/listenKey", "", headers,
                        /*timeout_ms_override=*/3000);
    if (!r.ok()) {
        if (err) *err = r.curl_err.empty() ? ("HTTP " + std::to_string(r.http_status) + ": " + r.body)
                                           : r.curl_err;
        return {};
    }
    auto j = nlohmann::json::parse(r.body, nullptr, false);
    if (j.is_discarded() || !j.contains("listenKey")) {
        if (err) *err = "invalid listenKey response: " + r.body;
        return {};
    }
    return j["listenKey"].get<std::string>();
}

bool BinanceRestClient::keepalive_listen_key(std::string* err) {
    std::map<std::string, std::string> headers;
    headers["X-MBX-APIKEY"] = cfg_.api_key;
    // 与 acquire 一样限制在 3s 内，失败后由 UDS 重连流程重新申请 listenKey。
    auto r = do_request("PUT", cfg_.base_url + "/fapi/v1/listenKey", "", headers,
                        /*timeout_ms_override=*/3000);
    if (!r.ok()) {
        if (err) *err = r.curl_err.empty() ? ("HTTP " + std::to_string(r.http_status) + ": " + r.body)
                                           : r.curl_err;
        return false;
    }
    return true;
}

bool BinanceRestClient::close_listen_key(std::string* err, long timeout_ms) {
    std::map<std::string, std::string> headers;
    headers["X-MBX-APIKEY"] = cfg_.api_key;
    auto r = do_request("DELETE", cfg_.base_url + "/fapi/v1/listenKey", "", headers,
                        timeout_ms);
    if (!r.ok()) {
        if (err) *err = r.curl_err.empty() ? ("HTTP " + std::to_string(r.http_status) + ": " + r.body)
                                           : r.curl_err;
        return false;
    }
    return true;
}

}  // namespace binance_u
