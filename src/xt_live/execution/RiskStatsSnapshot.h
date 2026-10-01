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
    uint64_t submit_success = 0;    ///< 报单成功总数（TD 已受理）
    uint64_t cancel_attempts = 0;
    uint64_t cancel_success = 0;    ///< 主动撤单 API 成功总数（TD 已受理）
    uint64_t canceled_terminal_total = 0;  ///< 订单终态为 Canceled 的总数
                                           ///  (含 IOC 超时 / 主动撤 / 交易所撤)
    std::vector<std::pair<std::string, uint64_t>> reject_by_reason; ///< 按拒绝原因分类计数

    /// @brief submit-side pre-trade risk reject 累计总数（排除 cancel 域撤单摩擦）。
    /// @note 口径 = submit-side pre-trade rejects only。cancel 域拒因（CANCEL_COOLDOWN /
    ///       CANCEL_RATE_LIMITED / CANCEL_TRADING_NOT_READY / CANCEL_TD_NOT_INITIALIZED /
    ///       CANCEL_SEND_FAILED，均以 `CANCEL_` 前缀）是撤单摩擦，**不计入**——否则撤单
    ///       风暴会虚高 `xt_risk_block_count` / HighRiskBlockRate。
    /// @note 由 `record_reject` 单调递增；进程内 cumulative（重启清零）。是三 runner
    ///       `xt_risk_block_count` gauge / `xt_risk_rejects_total` counter 的同源真值口径，
    ///       取代旧 `submit_attempts − submit_success` 跨计数域近似（后者按逻辑 submit 计
    ///       attempts、按物理 slice 计 success，multi-slice 会把 reject 吞成 0）。
    /// @warning 排除依赖 `live_reject_reason_str`（XtLiveRejectReason.h）对 Cancel* 域的
    ///          `CANCEL_` 前缀命名契约；新增 cancel 域拒因时须保持该前缀，否则会漏排。
    [[nodiscard]] uint64_t total_rejects() const {
        uint64_t sum = 0;
        for (const auto& kv : reject_by_reason) {
            // cancel 域（`CANCEL_` 前缀）= 撤单摩擦，排除出 submit-side reject 口径。
            if (kv.first.compare(0, 7, "CANCEL_") == 0) continue;
            sum += kv.second;
        }
        return sum;
    }
};
