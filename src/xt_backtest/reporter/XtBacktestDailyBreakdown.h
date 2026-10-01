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
    /// @brief tick 计数 (trading_date != 0 时累加)。on_tick_pre 入口调。
    void on_tick(uint32_t trading_date) noexcept;
    /// @brief order submit 计数（含被 reject 的 submit）。
    void on_submit(uint32_t trading_date) noexcept;
    /// @brief fill 计数 (用 XtFillRow.trading_day fallback 链派生 trading_date)。
    void on_fill(uint32_t trading_date) noexcept;
    /// @brief risk reject 计数 + 按 reason 分桶（reason 空则只计总数）。
    /// @note 分桶按去重后 reason 字符串，封顶 kMaxReasonBuckets 防基数爆炸
    ///       （超出并入 "(other)"）。
    void on_risk_reject(uint32_t trading_date, std::string_view reason);
    /// @brief 记录某交易日的日终账户快照（重复调用覆盖，幂等）。
    void on_day_account(uint32_t trading_date, double equity, double cash, double frozen);

    /// @brief 写 daily_breakdown.json 到指定路径;empty 时静默不写 (返回 true)。
    /// @param cfg_start_date / cfg_end_date 回测 config 窗口（YYYYMMDD，0=未配），
    ///        原样落顶层字段——消费者据此区分「策略静默」与「窗口本来就短」。
    /// @param shared_execution_counters true 时落顶层 `"shared_execution_counters": true`
    ///        标志——CTA bar 模式多策略共享 submit/reject 累加器（per-strategy 副本共用同一
    ///        计数），消费端据此把「submits>0 且 fills==0 → 执行死链」判据降级为 amber（避免
    ///        某策略 0 成交却因共享 submits 被误判硬红）。默认 false → 字段缺席（单策略 / HFT /
    ///        计数非共享路径逐字节不变）。
    /// @return false 当 open 失败 (caller 须 propagate fsync error)。
    [[nodiscard]] bool write_json(const std::string& out_path,
                                  uint32_t cfg_start_date,
                                  uint32_t cfg_end_date,
                                  bool shared_execution_counters = false) const;

    /// @brief 是否无任何数据 (write_json 静默 skip 用)。
    [[nodiscard]] bool empty() const noexcept { return daily_stats_.empty(); }

    /// @brief test seam: 读 per-date 计数。
    struct DailyStats {
        uint64_t tick_count = 0;
        uint64_t submit_count = 0;
        uint64_t fill_count = 0;
        uint64_t risk_reject_count = 0;
        std::map<std::string, uint64_t> rejects_by_reason;  ///< 拒因 → 计数（有序输出）。
        bool has_account = false;   ///< eod_* 三字段是否已采样。
        double eod_equity = 0.0;    ///< 日终权益。
        double eod_cash = 0.0;      ///< 日终可用现金。
        double eod_frozen = 0.0;    ///< 日终冻结。
    };
    [[nodiscard]] const std::map<uint32_t, DailyStats>& stats() const noexcept { return daily_stats_; }

    /// @brief 拒因分桶上限；超出的 reason 并入 "(other)"。
    static constexpr size_t kMaxReasonBuckets = 32;

private:
    /// @brief trading_date(YYYYMMDD) → DailyStats;std::map 自动按日期排序输出。
    std::map<uint32_t, DailyStats> daily_stats_;
};
