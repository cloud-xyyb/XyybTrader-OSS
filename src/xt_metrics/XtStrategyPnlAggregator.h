#pragma once

#include <atomic>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ct::metrics {

/// @brief 策略每个品种的已实现盈亏与手续费累加器。
///
/// 同一进程跑多个策略时，panel 要按策略分别显示赚了多少、付了多少手续费；
/// engine 每笔平仓/成交把 realized PnL 和 fee 打进来，control/report 线程
/// 低频取快照读走。
/// realized 内部用 ×1e6 fixed-point 存储；空 strategy_id 归一化为 "_default"。
class XtStrategyPnlAggregator {
public:
    /// @brief load_state() 三态结果，区分冷启动缺失与损坏拒绝。
    ///
    /// 旧接口用单一 bool 无法区分「文件不存在」（正常冷启动）与「文件损坏」
    /// （风控输入不可信），caller 无从 fail-closed；本枚举把两者分开。
    enum class LoadResult {
        kLoaded,   ///< 全部校验通过，rows_ 已覆盖为文件内容。
        kMissing,  ///< 文件不存在，caller 走冷启动。
        kInvalid,  ///< 文件存在但校验失败：rows_ 未改，原文件已 quarantine。
    };

    /// @brief 累计快照行。
    ///
    /// realized 是由内部 fixed-point 反算后的金额；fees/fills 保持累计口径。
    /// 调用方若需要 today 指标，应再减去 XtStrategyPnlBaseline 的开盘累计值。
    struct Row {
        std::string sid;
        std::string code;
        double      realized = 0.0;
        double      fees     = 0.0;
        uint64_t    fills    = 0;
    };

    /// @brief 累加平仓 realized PnL。
    /// @param sid strategy_id；空值归一化为 "_default"。
    /// @param code instrument code；空值直接丢弃。
    /// @param realized_pnl_fp 增量 realized PnL，单位 ×1e6 fixed-point；0 值直接丢弃。
    void on_close(std::string_view sid, std::string_view code,
                  int64_t realized_pnl_fp);

    /// @brief 累加单笔成交 fee，并将 fills 计数加一。
    /// @param sid strategy_id；空值归一化为 "_default"。
    /// @param code instrument code；空值直接丢弃。
    /// @param fee 单笔 fee，maker rebate 可为负；0 fee 仍计入 fills。
    void on_fill(std::string_view sid, std::string_view code, double fee);

    /// @brief 返回累计快照副本，不 reset 内部状态。
    std::vector<Row> snapshot() const;

    /// @brief 单策略跨品种累计 realized PnL，单位 fp ×1e6。
    /// @note 只包含 realized，不含 mark-to-market unrealized。
    /// @param sid strategy_id；空值按 on_close/on_fill 相同规则归一化。
    int64_t realized_fp_for_sid(std::string_view sid) const;

    /// @brief 将累计 state 原子持久化为 JSON v1。
    /// @return true 表示文件写入和 AtomicOfstream commit 成功。
    bool save_state(std::string_view path) const;

    /// @brief 从 versioned JSON v1 严格加载累计 state。
    ///
    /// 任一校验失败即整文件拒绝（whole-file fail-closed），不做逐行 fail-soft
    /// 吸收——半损坏文件会以「成功 reload」姿态进入 per-strategy daily-loss 风控。
    /// @return kLoaded  全部校验通过，rows_ 已覆盖（空 rows 数组也算 kLoaded）；
    ///         kMissing 确认文件不存在，caller 冷启动；
    ///         kInvalid 校验失败 / 无法 stat，rows_ 保持调用前内容。
    /// @note kInvalid 且文件存在时把原文件 rename 到 `<path>.quarantine`（同名已存在
    ///       追加 `.<epoch_ms>` 后缀）、error 日志一次、递增 invalid_load_count()。
    ///       stat 失败（无法确认存在）走 fail-closed kInvalid：不 rename，仅 error
    ///       日志 + 递增 invalid_load_count()（与 SafetyStateStore::load_checked 对齐）。
    LoadResult load_state(std::string_view path);

    /// @brief load_state() 判定整文件损坏并 quarantine 的累计次数（进程内观测）。
    uint64_t invalid_load_count() const noexcept {
        return invalid_load_count_.load(std::memory_order_relaxed);
    }

private:
    /// @brief rows_ 的 map key；first 为归一化 sid，second 为非空 code。
    using Key = std::pair<std::string, std::string>;

    /// @brief Key 在 unordered_map 中使用的组合 hash。
    struct PairHash {
        size_t operator()(const Key& k) const noexcept {
            // boost::hash_combine 风格；只影响进程内分桶，不进入持久化格式。
            size_t h1 = std::hash<std::string>{}(k.first);
            size_t h2 = std::hash<std::string>{}(k.second);
            return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
        }
    };

    /// @brief 单个 (sid, code) 的累计状态。
    struct State {
        int64_t  realized_fp = 0;    ///< 累计 realized PnL，单位 ×1e6 fixed-point。
        double   fees        = 0.0;  ///< 累计 fee；maker rebate 可为负。
        uint64_t fills       = 0;    ///< 累计成交笔数；0 fee 成交也计入。
    };

    /// @brief load_state() 判定整文件损坏时的收口：递增计数、quarantine 原文件、
    ///        error 日志一次；不触碰 rows_。
    /// @param path   损坏文件路径（rename 源）。
    /// @param reason 供日志的失败原因短语。
    /// @return 恒为 LoadResult::kInvalid，供 load_state() 直接 return。
    LoadResult reject_and_quarantine_(const std::string& path, std::string_view reason);

    /// @brief 保护 rows_ 的共享锁；读路径共享持有，写入和 load_state() 独占持有。
    mutable std::shared_mutex                          mu_;
    /// @brief 归一化后的 per-(sid, code) 累计表；save_state() 直接按该表持久化。
    std::unordered_map<Key, State, PairHash>           rows_;
    /// @brief load_state() quarantine 计数；进程内观测，不进 Prometheus registry。
    std::atomic<uint64_t>                              invalid_load_count_{0};
};

}  // namespace ct::metrics
