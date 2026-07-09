#pragma once

#include <cstddef>
#include <string>

namespace binance_u {

inline std::size_t curl_write_cb(char* ptr, std::size_t size, std::size_t nmemb, void* userdata) {
    const std::size_t total = size * nmemb;
    auto* body = static_cast<std::string*>(userdata);
    body->append(ptr, total);
    return total;
}

}
