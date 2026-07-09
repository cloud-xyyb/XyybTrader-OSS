#include "BinanceSign.h"

#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <stdexcept>

#include <openssl/hmac.h>
#include <openssl/evp.h>

namespace binance_u {
namespace sign {

std::string hmac_sha256_hex(std::string_view secret, std::string_view payload) {
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int md_len = 0;
    const unsigned char* key = reinterpret_cast<const unsigned char*>(secret.data());
    const unsigned char* data = reinterpret_cast<const unsigned char*>(payload.data());
    unsigned char* result = HMAC(EVP_sha256(),
                                 key, static_cast<int>(secret.size()),
                                 data, payload.size(),
                                 md, &md_len);
    if (!result || md_len == 0) {
        throw std::runtime_error("Binance HMAC-SHA256 signing failed");
    }
    std::string hex;
    hex.reserve(static_cast<std::size_t>(md_len) * 2);
    static constexpr char kHexChars[] = "0123456789abcdef";
    for (unsigned int i = 0; i < md_len; ++i) {
        hex.push_back(kHexChars[(md[i] >> 4) & 0x0F]);
        hex.push_back(kHexChars[md[i] & 0x0F]);
    }
    return hex;
}

std::string url_encode(std::string_view s) {
    std::string out;

    out.reserve(s.size() * 3);
    static constexpr char kHexChars[] = "0123456789ABCDEF";
    for (unsigned char c : s) {
        const bool unreserved =
            (c >= 'A' && c <= 'Z') ||
            (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') ||
            c == '-' || c == '_' || c == '.' || c == '~';
        if (unreserved) {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(kHexChars[(c >> 4) & 0x0F]);
            out.push_back(kHexChars[c & 0x0F]);
        }
    }
    return out;
}

std::string build_query(const std::map<std::string, std::string>& params) {
    std::string q;

    std::size_t approx = 0;
    for (const auto& [k, v] : params) approx += k.size() + v.size() + 2;
    q.reserve(approx);
    bool first = true;
    for (const auto& [k, v] : params) {
        if (!first) q.push_back('&');
        first = false;
        q.append(k);
        q.push_back('=');
        q.append(url_encode(v));
    }
    return q;
}

std::string sign_query(std::string_view secret,
                       const std::map<std::string, std::string>& params) {
    const std::string query = build_query(params);
    const std::string sig = hmac_sha256_hex(secret, query);
    std::string out;
    out.reserve(query.size() + 11 + sig.size());
    out.append(query);
    out.append("&signature=");
    out.append(sig);
    return out;
}

std::string sign_params(std::string_view secret,
                        const std::map<std::string, std::string>& params) {
    return hmac_sha256_hex(secret, build_query(params));
}

std::string api_key_sha8(std::string_view api_key) {
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int md_len = 0;

    if (EVP_Digest(api_key.data(), api_key.size(), md, &md_len,
                   EVP_sha256(), nullptr) != 1 || md_len < 4) {
        throw std::runtime_error("api_key_sha8: SHA256 digest failed");
    }
    std::string hex;
    hex.reserve(8);
    static constexpr char kHexChars[] = "0123456789abcdef";
    for (unsigned int i = 0; i < 4; ++i) {
        hex.push_back(kHexChars[(md[i] >> 4) & 0x0F]);
        hex.push_back(kHexChars[md[i] & 0x0F]);
    }
    return hex;
}

uint64_t now_ms() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

}
}
