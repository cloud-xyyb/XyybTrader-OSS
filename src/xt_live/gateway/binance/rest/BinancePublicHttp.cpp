#include "BinancePublicHttp.h"

#include "BinanceCurlDetail.h"

#include <sstream>

#include <curl/curl.h>

namespace binance_u {

namespace {

int curl_xferinfo_cancel(void* clientp,
                         curl_off_t  , curl_off_t  ,
                         curl_off_t  , curl_off_t  ) {
    auto* cancel = static_cast<std::atomic<bool>*>(clientp);
    if (cancel && cancel->load(std::memory_order_acquire)) return 1;
    return 0;
}

}

bool binance_public_http_get(const std::string& host,
                             const std::string& port,
                             const std::string& path_query,
                             long timeout_sec,
                             std::atomic<bool>* cancel,
                             std::string* out_body,
                             long* out_http,
                             std::string* err) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        if (err) *err = "curl_easy_init failed";
        return false;
    }
    std::string body;
    std::ostringstream url;
    url << "https://" << host;
    if (port != "443") url << ":" << port;
    url << path_query;

    curl_easy_setopt(curl, CURLOPT_URL, url.str().c_str());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_sec);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "XyybTrader/binance_u/1.0");
    curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "");
    if (cancel) {
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, &curl_xferinfo_cancel);
        curl_easy_setopt(curl, CURLOPT_XFERINFODATA, cancel);
    }

    const CURLcode rc = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);
    if (out_http) *out_http = http_code;

    if (rc != CURLE_OK) {
        if (err) *err = std::string("curl: ") + curl_easy_strerror(rc);
        return false;
    }
    if (http_code < 200 || http_code >= 300) {
        if (err) {
            std::ostringstream oss;
            oss << "binance public GET HTTP " << http_code << ": " << body;
            *err = oss.str();
        }
        return false;
    }
    if (out_body) *out_body = std::move(body);
    return true;
}

}
