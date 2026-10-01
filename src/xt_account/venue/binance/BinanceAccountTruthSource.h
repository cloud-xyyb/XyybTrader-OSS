#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "xt_account/api/IAccountTruthSink.h"
#include "xt_account/api/IAccountTruthSource.h"

/// @brief Binance account truth 的 source（读）+ sink（写）实现。
///
/// 面向 live control、report 和 metrics 输出 Binance 口径的 AccountTruth。
/// 账本字段来自注入的 SnapshotProvider（AccountStateStore 聚合）；Source 自身只维护
/// Binance broker overlay、成交手续费和 income cashflow 的 day-scoped 缓存，
/// fetch_snapshot() 把两者合并后按值返回，不写本地 reducer 状态。
/// 内部可变状态由 mutex 保护。
class BinanceAccountTruthSource final : public IAccountTruthSource, public IAccountTruthSink {
public:
    /// @brief 返回聚合 AccountTruth 的回调；通常由 AccountStateStore adapter 提供。
    using SnapshotProvider = std::function<AccountTruth()>;

    /// @brief Binance income row；用于识别当日外部现金流。
    struct IncomeRecord {
        std::string income_type;  ///< Binance incomeType。
        std::string asset;        ///< 资产代码；空或 USDT 才计入外部现金流。
        int64_t income_fp = 0;    ///< income 金额（fixed-point）。
        uint64_t time_ms = 0;     ///< Binance income time（epoch ms）。
    };

    /// @brief 拉取指定 UTC 自然日窗口内的 income rows。
    /// @note 可抛异常；poll() 吞掉异常并保留上一次 cashflow 缓存。
    using IncomeProvider = std::function<std::vector<IncomeRecord>(
        int64_t trading_day_id,
        uint64_t start_ms,
        uint64_t end_ms)>;

    /// @brief Binance income provider 轮询节流配置。
    struct IncomePollConfig {
        uint64_t poll_interval_ms = 60'000;  ///< income provider 最小轮询间隔（ms）。
    };

    /// @brief 创建 source；fetch 时合并 provider 账本、broker overlay 和 day-scoped 缓存。
    /// @param provider 聚合真值回调；空回调时账本字段全 0（只剩 overlay 与 day-scoped 缓存）。
    explicit BinanceAccountTruthSource(SnapshotProvider provider = SnapshotProvider{});
    /// @brief 创建 provider-backed source，并启用 Binance income cashflow 轮询。
    BinanceAccountTruthSource(SnapshotProvider provider, IncomeProvider income_provider);
    /// @brief 创建 provider-backed source，并指定 income 轮询节流。
    BinanceAccountTruthSource(SnapshotProvider provider,
                              IncomeProvider income_provider,
                              IncomePollConfig income_poll);

    /// @brief 替换 income provider，并清空节流窗口让下一次 poll() 立即重新轮询。
    void set_income_provider(IncomeProvider provider);

    /// @brief 按节流窗口重新拉取当日 income，刷新 external cashflow 缓存（REST I/O）。
    /// @note income 窗口的交易日在此唯一推导；刷新出的缓存即 fetch_snapshot() 报告的
    ///       cashflow base，两者不各自推导 day key。
    void poll() override;
    /// @brief 读取 Binance 账户真值：provider 聚合 + REST overlay + day-scoped 缓存。
    /// @note 纯内存合并；external cashflow 直接复用 poll() 建立的 base，本函数不做 I/O。
    AccountTruth fetch_snapshot() override;
    /// @brief 切换交易日时清空 day-scoped fees/cashflow 缓存。
    void on_trading_day_rollover(int64_t prev_day, int64_t new_day) override;
    /// @brief 覆盖写入 REST overlay（available/maint/total + per_symbol）。
    void update_broker_overlay(const BrokerVenueTruth& bt) override;
    /// @brief 增量累加一笔 venue commission 到对应 UTC 自然日；runner 从 on_trade 转发。
    /// @note 非幂等；调用方必须保证同一 fill 只转发一次。
    void note_fill_fee_fp(int64_t fee_fp, uint64_t ts_ns) override;

    /// @brief 判断 Binance incomeType 是否属于外部现金流。
    static bool is_external_cashflow_income_type(const std::string& income_type);

private:
    static int64_t derive_trading_day_id(uint64_t ts_ns);
    static uint64_t trading_day_start_ms(int64_t trading_day_id);
    static uint64_t now_ms();

    void maybe_refresh_external_cashflow(int64_t trading_day_id, uint64_t ts_ns);

    mutable std::mutex mu_;             ///< 保护 overlay_、income/fee 缓存和 income provider 切换。
    BrokerVenueTruth overlay_;          ///< venue REST broker overlay；update_broker_overlay 覆盖写。
    SnapshotProvider provider_;         ///< 聚合真值回调；构造后只读，fetch_snapshot() 在锁外调用。
    IncomeProvider income_provider_;    ///< Binance income 拉取回调；mu_ 保护，可运行期替换。
    IncomePollConfig income_poll_;      ///< income 轮询节流参数；mu_ 保护。
    int64_t fees_today_fp_ = 0;         ///< fees_trading_day_id_ 对应日期的 venue commission 累计。
    int64_t fees_trading_day_id_ = 0;   ///< fees_today_fp_ 所属 UTC 自然日；0 表示未初始化。
    int64_t external_cashflow_today_fp_ = 0;      ///< external_cashflow_trading_day_id_ 当日缓存。
    int64_t external_cashflow_trading_day_id_ = 0; ///< external_cashflow_today_fp_ 所属 UTC 自然日。
    uint64_t last_income_poll_ms_ = 0;  ///< 上次 income provider 轮询时间；0 表示下次强制轮询。
};
