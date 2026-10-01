#pragma once

#include <arpa/inet.h>

#include <string>

/// @file XtLoopbackHost.h
/// @brief 判断 listen 地址是不是本机 loopback 的 helper
///
/// 控制面如果开 dev_insecure 明文模式，只能监听本机地址，不能暴露到外网。XtLoopbackHost 负责统一判断

namespace xt_net {

/// @brief 从 listen_addr 提取 host 部分 (去掉 :port，剥 IPv6 方括号)。
/// @note  接受形态：`host`、`host:port`、`[ipv6]`、`[ipv6]:port`、裸 `::1`。
///        端口拆分仅在单冒号 (IPv4/hostname `host:port`) 时进行；含 >=2 冒号且无方括号
///        的串视为裸 IPv6 字面量 (无 port 可拆)，整串当 host。bracketed IPv6 走方括号分支。
inline std::string extract_host(const std::string& addr) {
    if (!addr.empty() && addr.front() == '[') {          // [ipv6] 或 [ipv6]:port
        const auto rb = addr.find(']');
        if (rb == std::string::npos) return std::string();  // 畸形 → 空 host (非 loopback)
        return addr.substr(1, rb - 1);
    }
    const auto first = addr.find(':');
    const auto last = addr.rfind(':');
    if (first != last) return addr;                      // >=2 冒号且无 [] → 裸 IPv6，不拆 port
    return (last == std::string::npos) ? addr : addr.substr(0, last);  // host:port 或裸 host
}

/// @brief host 是否 loopback **字面量** (127.0.0.0/8 数字 IPv4 / ::1 / 精确 "localhost")。
/// @note  只认数字 IP 字面量与精确字符串 "localhost"；任意其它 hostname 一律拒绝。
///        "localhost" 是唯一放行的名字 (gRPC/glibc 恒解析到 loopback 字面量)。
///        通过 inet_pton 把 host 当 IPv4 字面量解析，成功才看首字节是否 127 ——
///        这能 kill 掉 `127.evil.example` (inet_pton 失败) 与 `127.0.0.1.evil.com`。
inline bool host_is_loopback_literal(const std::string& host) {
    if (host == "localhost" || host == "::1") return true;
    in_addr v4{};
    if (::inet_pton(AF_INET, host.c_str(), &v4) == 1) {
        // ntohl → 主机序，取最高字节 (首个点分八位组) 判 127.0.0.0/8。
        const uint32_t h = ntohl(v4.s_addr);
        return (h >> 24) == 127u;
    }
    return false;  // 非数字 IPv4 字面量、非精确 localhost/::1 → 拒绝 (含 hostname 旁路)
}

/// @brief listen_addr (host[:port] / [ipv6][:port]) 的 host 部分是否 loopback 字面量。
inline bool listen_addr_is_loopback_literal(const std::string& addr) {
    return host_is_loopback_literal(extract_host(addr));
}

}  // namespace xt_net
