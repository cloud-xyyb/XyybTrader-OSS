#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace binance_u {

/// @brief Binance REST 的同步 HTTP 客户端，负责请求签名、listenKey 生命周期和 server 时间校准。
///
/// 每个实例用一把 mutex 串行发请求，所有方法线程安全；
/// endpoint 的业务语义由上层 gateway 封装。
class BinanceRestClient {
public:
    struct Config {
        std::string base_url;
        std::string api_key;
        std::string api_secret;
        int recv_window_ms = 5000;
        long timeout_sec = 10;
    };

    struct Response {
        long http_status = 0;
        std::string body;
        std::string curl_err;
        long retry_after_sec = 0;

        bool ok() const { return http_status >= 200 && http_status < 300; }
    };

    explicit BinanceRestClient(Config cfg);
    virtual ~BinanceRestClient();

    BinanceRestClient(const BinanceRestClient&)            = delete;
    BinanceRestClient& operator=(const BinanceRestClient&) = delete;

    virtual Response post_signed(const std::string& path,
                                 std::map<std::string, std::string> params);

    virtual Response delete_signed(const std::string& path,
                                   std::map<std::string, std::string> params);

    virtual Response get_signed(const std::string& path,
                                std::map<std::string, std::string> params);

    virtual Response get_public(const std::string& path,
                                const std::map<std::string, std::string>& params = {});

    bool sync_server_time();

    int64_t server_time_offset_ms() const { return offset_ms_.load(std::memory_order_relaxed); }

    virtual std::string acquire_listen_key(std::string* err = nullptr);

    virtual bool keepalive_listen_key(std::string* err = nullptr);

    virtual bool close_listen_key(std::string* err = nullptr, long timeout_ms = 0);

private:

    Response do_request(const char* method,
                        const std::string& full_url,
                        const std::string& body,
                        const std::map<std::string, std::string>& extra_headers,
                        long timeout_ms_override = 0);

    void inject_timestamp(std::map<std::string, std::string>& params) const;

    Config                cfg_;
    std::atomic<int64_t>  offset_ms_{0};
    std::mutex            curl_mu_;
    void*                 curl_handle_ = nullptr;
    std::string           last_sync_err_;
};

}
