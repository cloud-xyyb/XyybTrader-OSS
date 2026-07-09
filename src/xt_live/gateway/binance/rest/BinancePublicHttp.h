#pragma once

#include <atomic>
#include <string>

namespace binance_u {

bool binance_public_http_get(const std::string& host,
                             const std::string& port,
                             const std::string& path_query,
                             long timeout_sec,
                             std::atomic<bool>* cancel,
                             std::string* out_body,
                             long* out_http,
                             std::string* err);

}
