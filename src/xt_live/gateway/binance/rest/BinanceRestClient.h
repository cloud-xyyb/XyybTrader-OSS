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
        std::string base_url;        ///< REST base URL，如 `https://fapi.binance.com`。
        std::string api_key;         ///< 写入 `X-MBX-APIKEY` header。
        std::string api_secret;      ///< HMAC-SHA256 签名密钥。
        int recv_window_ms = 5000;   ///< Binance `recvWindow` 字段，单位 ms。
        long timeout_sec = 10;       ///< CURL 请求总超时，覆盖 connect + transfer。
    };

    struct Response {
        long http_status = 0;        ///< HTTP 状态码（200 / 400 / 418 等；0 = transport error）
        std::string body;            ///< 响应 body（JSON 原文；调用方自行解析）
        std::string curl_err;        ///< transport 层错误（curl_easy_perform 失败时填）
        long retry_after_sec = 0;    ///< Retry-After header 秒数（429/418 限频响应携带；0 = 无）
        /// @brief HTTP 2xx 视为成功；调用方仍需解析 body 判断业务字段。
        bool ok() const { return http_status >= 200 && http_status < 300; }
    };

    /// @brief 创建同步 REST client；构造后可立即发请求。
    explicit BinanceRestClient(Config cfg);
    virtual ~BinanceRestClient();

    // 独占底层 CURL easy handle；复制会导致句柄共享和析构期重复释放风险。
    BinanceRestClient(const BinanceRestClient&)            = delete;
    BinanceRestClient& operator=(const BinanceRestClient&) = delete;

    // ── Signed endpoints（HMAC-SHA256）────────────────────────────────
    /// @brief 有签名的 POST：params 自动补 timestamp/recvWindow + signature；
    ///        request body 是 `<signed_query>`，Content-Type 为
    ///        `application/x-www-form-urlencoded`（binance 官方推荐）。
    virtual Response post_signed(const std::string& path,
                                 std::map<std::string, std::string> params);

    /// @brief 有签名的 DELETE（用于 cancel_order）；签名后的 query 放 URL 中。
    virtual Response delete_signed(const std::string& path,
                                   std::map<std::string, std::string> params);

    /// @brief 有签名的 GET（用于 account/position 查询）；签名后的 query 放 URL 中。
    virtual Response get_signed(const std::string& path,
                                std::map<std::string, std::string> params);

    // ── Unsigned endpoints（公开数据）──────────────────────────────────────
    /// @brief 无签名 GET（`/fapi/v1/time`, `/fapi/v1/depth` 等）。
    /// @note virtual：测试可脚本化 serverTime 采样，覆盖 resync fence。
    virtual Response get_public(const std::string& path,
                                const std::map<std::string, std::string>& params = {});

    // ── Server time drift ─────────────────────────────────────────────────
    /// @brief 调 `GET /fapi/v1/time`，更新 `server_time_offset_ms_`。
    ///        offset = serverTime - local_now_ms。同步阻塞。
    /// @return true 成功；false 失败（err 详情写入 last_sync_err_）
    bool sync_server_time();

    /// @brief 返回最近一次成功 sync 得到的 server-local 时间偏移，单位 ms。
    int64_t server_time_offset_ms() const { return offset_ms_.load(std::memory_order_relaxed); }

    // ── Helper：listenKey lifecycle（User Data Stream 用）──────────────────
    /// @brief POST /fapi/v1/listenKey（有 api_key header 但**无签名**；binance 特例）。
    /// @return listenKey 字符串（成功）；空串（失败，错误见 Response::curl_err）
    virtual std::string acquire_listen_key(std::string* err = nullptr);
    /// @brief PUT /fapi/v1/listenKey 续期；key 有 60min 过期，建议 30min 续一次。
    virtual bool keepalive_listen_key(std::string* err = nullptr);
    /// @brief DELETE /fapi/v1/listenKey（关闭 user data stream）。
    /// @param err          可选错误输出
    /// @param timeout_ms   可选请求级超时上限（毫秒，> 0 时覆盖
    ///                     `cfg_.timeout_sec`）。shutdown path 用这个把
    ///                     close 限制在 ~3s 内，避免 SIGTERM 期间网络故障
    ///                     拖到 10s curl timeout。
    virtual bool close_listen_key(std::string* err = nullptr, long timeout_ms = 0);

private:
    /// @brief 底层 request 执行（libcurl easy）。
    /// @param method   "GET" / "POST" / "PUT" / "DELETE"
    /// @param full_url 完整 URL（含 query string）
    /// @param body     请求 body（POST/PUT 用）；可为空
    /// @param extra_headers  额外 header（e.g. X-MBX-APIKEY）
    /// @param timeout_ms_override  > 0 时 CURLOPT_TIMEOUT_MS 覆盖
    ///        `cfg_.timeout_sec * 1000`；0 表示用默认。shutdown path 传 3000
    ///        把 close_listen_key 限到 3s。
    Response do_request(const char* method,
                        const std::string& full_url,
                        const std::string& body,
                        const std::map<std::string, std::string>& extra_headers,
                        long timeout_ms_override = 0);

    /// @brief 在 params 上添加 timestamp（now_ms + offset_ms_）+ recvWindow。
    void inject_timestamp(std::map<std::string, std::string>& params) const;

    Config                cfg_;                 ///< 构造期配置，后续只读。
    std::atomic<int64_t>  offset_ms_{0};        ///< serverTime - local_now_ms。
    std::mutex            curl_mu_;             ///< 序列化 CURL easy 调用（非线程安全）。
    void*                 curl_handle_ = nullptr; ///< 实为 CURL*；避免在头文件暴露 libcurl.h。
    std::string           last_sync_err_;       ///< 最近一次 sync_server_time 失败原因。
};

}  // namespace binance_u
