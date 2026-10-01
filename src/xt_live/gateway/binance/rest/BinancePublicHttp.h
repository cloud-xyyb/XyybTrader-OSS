#pragma once

#include <atomic>
#include <string>

namespace binance_u {

/// @brief Binance public（无签名）REST GET —— 短生命周期 CURL easy handle。
///
/// 供 depth snapshot 与 aggTrades 回补共用。不持 API key、不签名、不维护 server time offset。
///
/// @param host         REST host，如 `fapi.binance.com` / testnet host。
/// @param port         HTTPS 端口（"443" 时不拼端口）。
/// @param path_query   path + query，如 `/fapi/v1/depth?symbol=BTCUSDT&limit=1000`。
/// @param timeout_sec  CURL 总超时（connect + TLS + read）。
/// @param cancel       可选 shutdown 标志；非 nullptr 且 load()==true 时 curl progress
///                     callback 返回非零让 curl_easy_perform 立即 CURLE_ABORTED_BY_CALLBACK
///                     退出（避免阻塞到 timeout_sec）。
/// @param out_body     成功时填响应 body（JSON 原文，调用方自行解析）。
/// @param out_http     HTTP 状态码（0 = transport error）。
/// @param err          失败原因输出（可为 nullptr）。
/// @return true 表示 HTTP 2xx 且 body 已填；false 表示 transport error 或非 2xx。
/// @warning 同步阻塞调用，必须放到独立线程，勿阻塞 WS io_context。
bool binance_public_http_get(const std::string& host,
                             const std::string& port,
                             const std::string& path_query,
                             long timeout_sec,
                             std::atomic<bool>* cancel,
                             std::string* out_body,
                             long* out_http,
                             std::string* err);

}  // namespace binance_u
