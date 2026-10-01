#pragma once

#include <cstddef>
#include <string>

namespace binance_u {

/// @brief libcurl CURLOPT_WRITEFUNCTION 回调：把响应体追加进 userdata 指向的 std::string。
inline std::size_t curl_write_cb(char* ptr, std::size_t size, std::size_t nmemb, void* userdata) {
    const std::size_t total = size * nmemb;
    auto* body = static_cast<std::string*>(userdata);
    body->append(ptr, total);
    return total;
}

}  // namespace binance_u
