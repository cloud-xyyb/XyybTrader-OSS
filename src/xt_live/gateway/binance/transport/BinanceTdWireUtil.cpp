#include "BinanceTdWireUtil.h"

#include <algorithm>
#include <cstdio>
#include <limits>

namespace binance_u {
namespace {

constexpr char kBase36Alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyz";

}  // namespace

std::string base36_encode(uint64_t value) {
    if (value == 0) return "0";
    std::string out;
    while (value > 0) {
        out.push_back(kBase36Alphabet[value % 36]);
        value /= 36;
    }
    std::reverse(out.begin(), out.end());
    return out;
}

std::optional<uint64_t> base36_decode(std::string_view s) {
    if (s.empty()) return std::nullopt;
    uint64_t value = 0;
    for (char c : s) {
        uint64_t digit = 0;
        if (c >= '0' && c <= '9') {
            digit = static_cast<uint64_t>(c - '0');
        } else if (c >= 'a' && c <= 'z') {
            digit = static_cast<uint64_t>(c - 'a' + 10);
        } else {
            return std::nullopt;
        }
        // 在乘 36 前做上界检查，避免非法 clientOrderId 触发 uint64 wrap。
        if (value > (std::numeric_limits<uint64_t>::max() - digit) / 36) {
            return std::nullopt;
        }
        value = value * 36 + digit;
    }
    return value;
}

std::string make_client_order_id(uint64_t local_id, uint64_t nonce) {
    return "ct_" + base36_encode(local_id) + "_" + base36_encode(nonce);
}

uint64_t parse_local_id(std::string_view client_order_id) {
    // 只接受 make_client_order_id() 生成的 `ct_<local_id>_<nonce>` 格式。
    if (client_order_id.size() < 5) return 0;
    if (client_order_id.substr(0, 3) != "ct_") return 0;
    const auto p1 = client_order_id.find('_', 3);
    if (p1 == std::string_view::npos) return 0;
    if (client_order_id.find('_', p1 + 1) != std::string_view::npos) return 0;
    const std::string_view local_part = client_order_id.substr(3, p1 - 3);
    const std::string_view nonce_part = client_order_id.substr(p1 + 1);
    if (local_part.empty() || nonce_part.empty()) return 0;

    const auto local36 = base36_decode(local_part);
    const auto nonce36 = base36_decode(nonce_part);
    if (local36 && nonce36) return *local36;
    return 0;
}

std::string fp_to_str(int64_t fp) {
    char buf[48];
    std::snprintf(buf, sizeof(buf), "%.6f",
                  static_cast<double>(fp) / static_cast<double>(kOrderExecFpScale));
    std::string s(buf);
    if (s.find('.') != std::string::npos) {
        while (!s.empty() && s.back() == '0') s.pop_back();
        if (!s.empty() && s.back() == '.') s.pop_back();
    }
    return s;
}

std::optional<XtOrderStatus> parse_binance_status_opt(std::string_view s) {
    if (s == "NEW") return XtOrderStatus::Accepted;
    if (s == "PARTIALLY_FILLED") return XtOrderStatus::PartiallyFilled;
    if (s == "FILLED") return XtOrderStatus::Filled;
    if (s == "CANCELED") return XtOrderStatus::Canceled;
    if (s == "EXPIRED") return XtOrderStatus::Canceled;           // IOC/FOK 未成交在本地撤单语义下收敛。
    if (s == "EXPIRED_IN_MATCH") return XtOrderStatus::Canceled;  // STP 自成交防护过期，终态按撤单口径。
    if (s == "REJECTED") return XtOrderStatus::Rejected;
    return std::nullopt;
}

XtOrderStatus parse_binance_status(std::string_view s) {
    // ack 路径：未知状态串保守按业务拒绝处理。
    return parse_binance_status_opt(s).value_or(XtOrderStatus::Rejected);
}

}  // namespace binance_u
