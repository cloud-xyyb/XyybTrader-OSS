#pragma once

#include <arpa/inet.h>

#include <string>

/// @file XtLoopbackHost.h
/// @brief 判断 listen 地址是不是本机 loopback 的 helper
///
/// 控制面如果开 dev_insecure 明文模式，只能监听本机地址，不能暴露到外网。XtLoopbackHost 负责统一判断

namespace xt_net {

inline std::string extract_host(const std::string& addr) {
    if (!addr.empty() && addr.front() == '[') {
        const auto rb = addr.find(']');
        if (rb == std::string::npos) return std::string();
        return addr.substr(1, rb - 1);
    }
    const auto first = addr.find(':');
    const auto last = addr.rfind(':');
    if (first != last) return addr;
    return (last == std::string::npos) ? addr : addr.substr(0, last);
}

inline bool host_is_loopback_literal(const std::string& host) {
    if (host == "localhost" || host == "::1") return true;
    in_addr v4{};
    if (::inet_pton(AF_INET, host.c_str(), &v4) == 1) {

        const uint32_t h = ntohl(v4.s_addr);
        return (h >> 24) == 127u;
    }
    return false;
}

inline bool listen_addr_is_loopback_literal(const std::string& addr) {
    return host_is_loopback_literal(extract_host(addr));
}

}
