#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

/// @brief `XtLiveOrderGateway::risk_stats_snapshot()` 返回的冷路径统计快照。
///
/// `cancel_attempts/success` 统计主动 cancel API；`canceled_terminal_total`
/// 统计订单终态为 Canceled 的总数，两者不能互相推导。
struct RiskStatsSnapshot {
    uint64_t submit_attempts = 0;
    uint64_t submit_success = 0;
    uint64_t cancel_attempts = 0;
    uint64_t cancel_success = 0;
    uint64_t canceled_terminal_total = 0;

    std::vector<std::pair<std::string, uint64_t>> reject_by_reason;

    [[nodiscard]] uint64_t total_rejects() const {
        uint64_t sum = 0;
        for (const auto& kv : reject_by_reason) {

            if (kv.first.compare(0, 7, "CANCEL_") == 0) continue;
            sum += kv.second;
        }
        return sum;
    }
};
