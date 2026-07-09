#include "BinanceRestClient.h"

#include "BinanceCurlDetail.h"
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

}

namespace {
/// @brief 进程级 curl_global_init 守卫。
/// @note 必须在任意线程使用 libcurl/OpenSSL 前完成一次。链接顺序需保证系统 OpenSSL
///       先于 CTP `.so` 内嵌符号解析，否则 Beast/libcurl SSL 初始化可能被污染。
struct CurlGlobalInit {
    CurlGlobalInit() { curl_global_init(CURL_GLOBAL_ALL); }
    ~CurlGlobalInit() { curl_global_cleanup(); }
};
const CurlGlobalInit g_curl_init;
}

BinanceRestClient::BinanceRestClient(Config cfg) : cfg_(std::move(cfg)) {

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

    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, full_url.c_str());
    if (timeout_ms_override > 0) {

        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms_override);
    } else {
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, cfg_.timeout_sec);
    }
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "XyybTrader/binance_u/1.0");
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp.body);

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

std::string BinanceRestClient::acquire_listen_key(std::string* err) {

    std::map<std::string, std::string> headers;
    headers["X-MBX-APIKEY"] = cfg_.api_key;

    auto r = do_request("POST", cfg_.base_url + "/fapi/v1/listenKey", "", headers,
                         3000);
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

    auto r = do_request("PUT", cfg_.base_url + "/fapi/v1/listenKey", "", headers,
                         3000);
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

}
