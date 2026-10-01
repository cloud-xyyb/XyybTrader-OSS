#pragma once

/// @file
/// @brief 账户真值（account truth）读侧接口和跨 venue 通用 DTO。
///
/// Source 负责把 venue、AccountStateStore provider 或测试数据转换为 AccountTruth；
/// 调用方只消费快照字段，不通过该接口修改 reducer 状态。
/// venue 回报灌入（broker overlay / 成交手续费）走写侧 IAccountTruthSink。

#include <cstdint>
#include <string>
#include <vector>

#include "api/BrokerVenueTruth.h"  // PerSymbolTruth + BrokerVenueTruth (gateway-layer wire POD)

/// @brief 跨 venue 的账户真值快照。
///
/// 由 IAccountTruthSource 按值返回，供 live control、report 和 metrics 消费。
/// 金额字段均为 fixed-point(1e6)，ts_ns 为 epoch nanoseconds，
/// trading_day_id 为 YYYYMMDD。venue 无法提供的字段保持 0；调用方不能仅凭
/// 0 区分真实零值和 unknown/unavailable。
struct AccountTruth {
    int64_t  wallet_balance_fp = 0;             ///< 钱包/账户余额，不含 unrealized PnL。
    int64_t  position_value_fp = 0;             ///< 聚合持仓市值；venue 不提供时可由 source 估算。
    int64_t  unrealized_pnl_fp = 0;             ///< 聚合未实现盈亏。
    int64_t  fees_today_fp = 0;                 ///< 当前 trading day 已知手续费。
    int64_t  external_cashflow_today_fp = 0;    ///< 当前 trading day 出入金/划转累计。
    uint64_t ts_ns = 0;                         ///< 快照时间戳；0 表示未知。
    int64_t  trading_day_id = 0;                ///< 交易日 YYYYMMDD；0 表示未知。
    int64_t  available_balance_fp = 0;          ///< venue 可用余额；0 也可能表示 venue 未提供。
    int64_t  maint_margin_fp = 0;               ///< venue 维持保证金；0 也可能表示 venue 未提供。
    int64_t  total_margin_balance_fp = 0;       ///< venue 总保证金余额；0 表示不可用于 raw equity cross-check。
    std::vector<PerSymbolTruth> per_symbol;     ///< broker/venue per-symbol 明细；可为空。
    bool per_symbol_is_broker_truth = false;    ///< true=per_symbol 来自 venue/broker 真值(可做 store-vs-broker 背离比对); false=account_store 镜像(仅供展示,不可自比)。

    /// account_store 内部 mark cache 诊断快照，独立于 broker/venue per_symbol truth。
    /// 用于对比 runtime mark 与 venue mark，定位 mark path 断裂或 mark=0 覆盖问题。
    std::vector<std::pair<std::string, int64_t>> runtime_per_symbol_marks_fp;
};

/// @brief account truth 的读侧 source 接口。
///
/// 实现把 venue、AccountStateStore provider 或测试数据转换成 AccountTruth。
/// 调用方通过 fetch_snapshot() 按值读取，并用 rollover 入口同步 source 自己维护的
/// day-scoped 状态；本接口不修改本地 reducer 状态，也不接受 venue 回报灌入
/// （broker overlay / 成交手续费在写侧 IAccountTruthSink）。
class IAccountTruthSource {
public:
    virtual ~IAccountTruthSource() = default;
    /// @brief 刷新 source 自维护的外部数据缓存（可能做网络 I/O）。
    /// @note 与 fetch_snapshot() 分离，让「读快照」不隐含 I/O。调用方应在每次
    ///       fetch_snapshot() 前调用；实现自带节流，重复调用不会放大 I/O。
    ///       无外部数据源的 source 默认 no-op。
    virtual void poll() {}
    /// @brief 获取当前账户真值快照。
    /// @return 按值返回的 AccountTruth；可推导的 trading_day_id 由 source 补齐，其他缺失字段保持 0。
    /// @note 纯内存合并，不做 I/O；外部数据（如 income cashflow）读的是 poll() 刷新的缓存。
    [[nodiscard]] virtual AccountTruth fetch_snapshot() = 0;
    /// @brief 通知 source 交易日切换，重置 day-scoped 累加字段。
    /// @param prev_day 上一个交易日 YYYYMMDD；实现可按需忽略。
    /// @param new_day 新交易日 YYYYMMDD。
    virtual void on_trading_day_rollover(int64_t prev_day, int64_t new_day) = 0;
};

/// @brief 账户权益公式：wallet_balance + unrealized_pnl。
inline int64_t account_truth_equity_fp(const AccountTruth& truth) noexcept {
    return truth.wallet_balance_fp + truth.unrealized_pnl_fp;
}

/// @brief 计算当日净 PnL，扣除交易日内外部现金流增量。
/// @param current 当前 AccountTruth。
/// @param open_equity_fp 交易日开盘/基准权益（fixed-point）。
/// @param open_external_cashflow_fp 基准点已累计的外部现金流；重启恢复时用于差分。
inline int64_t account_truth_today_pnl_fp(const AccountTruth& current,
                                          int64_t open_equity_fp,
                                          int64_t open_external_cashflow_fp = 0) noexcept {
    return account_truth_equity_fp(current) - open_equity_fp -
           (current.external_cashflow_today_fp - open_external_cashflow_fp);
}
