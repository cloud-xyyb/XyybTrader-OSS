#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

/// @brief 回测报告里的按交易日诊断摘要。
///
/// `XtBacktestReporter` 在 replay 线程把行情、下单、成交、拒单和日终账户快照交给本类。
/// 本类按交易日累加这些输入；`write_json()` 在回测结束时写出日级摘要，
/// 让调用方按天判断回放是否收到行情、策略是否下单、订单是否成交、是否被风控拒绝，
/// 并查看日终账户状态。
class XtBacktestDailyBreakdown {
public:

    void on_tick(uint32_t trading_date) noexcept;

    void on_submit(uint32_t trading_date) noexcept;

    void on_fill(uint32_t trading_date) noexcept;

    void on_risk_reject(uint32_t trading_date, std::string_view reason);

    void on_day_account(uint32_t trading_date, double equity, double cash, double frozen);

    [[nodiscard]] bool write_json(const std::string& out_path,
                                  uint32_t cfg_start_date,
                                  uint32_t cfg_end_date) const;

    [[nodiscard]] bool empty() const noexcept { return daily_stats_.empty(); }

    /// @brief test seam: 读 per-date 计数。
    struct DailyStats {
        uint64_t tick_count = 0;
        uint64_t submit_count = 0;
        uint64_t fill_count = 0;
        uint64_t risk_reject_count = 0;
        std::map<std::string, uint64_t> rejects_by_reason;
        bool has_account = false;
        double eod_equity = 0.0;
        double eod_cash = 0.0;
        double eod_frozen = 0.0;
    };
    [[nodiscard]] const std::map<uint32_t, DailyStats>& stats() const noexcept { return daily_stats_; }

    static constexpr size_t kMaxReasonBuckets = 32;

private:

    std::map<uint32_t, DailyStats> daily_stats_;
};
