#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace ct::metrics {

/// @brief 交易日开盘时刻的 PnL 累计值快照，today 指标的基线。
///
/// 要计算当日盈亏，需要保存交易日开盘时的 PnL
/// XtStrategyPnlBaseline 把这份基线按 instance 和交易日持久化到文件，
/// 重启后 today 口径不断裂。
/// 内部用 mutex 串行化低频 load/save/rotate；不面向成交热路径。
class XtStrategyPnlBaseline {
public:
    /// @brief 最近一次 load() 的结果。
    ///
    /// Missing/Corrupt/UnsupportedVersion/IoError 都表示 baseline 不可用于 today
    /// 派生指标；调用方应同时检查 ready()，不要只看返回值历史状态。
    enum class LoadStatus {
        NotLoaded,
        Loaded,
        Missing,
        Corrupt,
        UnsupportedVersion,
        IoError,
    };

    /// @brief 单个 strategy/instrument 的交易日开盘累计快照。
    ///
    /// 空 sid 统一归一化为 "_default"；空 code 行在 load/rotate 时视为无效。
    /// realized/fees/fills 都是 aggregator 的累计值，不是日内增量。
    struct BaselineRow {
        std::string sid;
        std::string code;
        double      realized = 0.0;
        double      fees     = 0.0;
        uint64_t    fills    = 0;
    };

    /// @brief 按 runner instance_id 构造 baseline state。
    /// @param instance_id 用于状态文件子目录；空值归一化为 "_default"。
    explicit XtStrategyPnlBaseline(std::string instance_id);

    /// @brief 从 `base_dir/<instance_id>/pnl_baseline_<trading_day>.json` 加载 baseline。
    /// @param base_dir runner 状态根目录。
    /// @param trading_day_id 当前 venue 边界下的 YYYYMMDD 交易日。
    /// @return true 表示文件存在且版本/交易日/行数据校验通过。
    /// @note 失败 load 采用 fail-closed：不清空已有内存 baseline，但会令 ready() 为 false。
    bool load(std::string_view base_dir, int trading_day_id);

    /// @brief 跨日轮转：当前 aggregator 累计快照成为新交易日 baseline。
    /// @param rows 来自 aggregator snapshot 的累计行；空 code 行会被丢弃。
    /// @note 成功轮转后 ready() 为 true；调用方仍需显式 save() 落盘。
    void rotate_day(const std::vector<BaselineRow>& rows);

    /// @brief 将当前 baseline 原子写入状态文件。
    /// @return true 表示目录创建、文件写入和 AtomicOfstream commit 全部成功。
    bool save(std::string_view base_dir, int trading_day_id) const;

    /// @brief 读取指定 key 的开盘 baseline。
    ///
    /// 缺失时返回 realized/fees/fills 为 0 的同 key 行，并通过 is_new 标记告警路径。
    /// 调用方应在 ready() 为 true 后再用返回值派生 today PnL。
    /// @param[out] is_new 可选输出；true 表示该 key 不存在 baseline。
    BaselineRow get(std::string_view sid, std::string_view code, bool* is_new = nullptr) const;

    /// @brief 记录某缺失 key 首次触发 baseline-new 告警计数；返回 true 表示本 epoch 首次。
    ///
    /// emit 路径用它把 `xt_strategy_baseline_new_total` 从「缺 baseline 的 row 每帧重复
    /// 递增」（get() 为 const 从不写入 baseline_，缺失 key 每帧 is_new=true）收敛为
    /// 「每 (sid,code) 每 baseline epoch 一次」。rotate_day()/load() 建立新 epoch 时清空
    /// 去重集（新 baseline 已含既有行 → is_new=false，不会重复触发）。
    bool mark_new_reported(std::string_view sid, std::string_view code);

    /// @brief 返回某 sid 跨所有 code 的开盘 realized 累计，单位 ×1e6 fixed-point。
    ///
    /// 风控 per-strategy daily-loss gate 用「aggregator 当前累计 − 本基准」得到 today
    /// realized；与 emit 路径 today_realized_fp 同口径。空 sid 归一化为 "_default"；
    /// 该 sid 无任何 baseline 行时返回 0（gate 退化为 lifetime，与未注入 baseline 一致）。
    int64_t realized_fp_for_sid(std::string_view sid) const;

    /// @brief 从 system_clock 和 venue timezone offset 推导 YYYYMMDD 交易日。
    /// @param tz_offset_h 小时偏移；binance_u 为 0，CTP/XTP 中国交易日边界通常为 8。
    /// @return YYYYMMDD；系统时间转换失败时返回 0。
    static int compute_trading_day_id(int tz_offset_h);

    size_t size() const;

    /// @brief true 表示最近一次有效 load() 或 rotate_day() 已建立可用 baseline。
    bool ready() const;

    LoadStatus last_load_status() const;
    static const char* load_status_name(LoadStatus status) noexcept;

    /// @brief 返回 baseline 副本，供 report/control 路径和测试检查。
    std::vector<BaselineRow> snapshot() const;

private:
    using Key = std::pair<std::string, std::string>;

    struct PairHash {
        size_t operator()(const Key& k) const noexcept {
            size_t h1 = std::hash<std::string>{}(k.first);
            size_t h2 = std::hash<std::string>{}(k.second);
            return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
        }
    };

    std::string path_for(std::string_view base_dir, int trading_day_id) const;

    std::string                                          instance_id_;
    mutable std::mutex                                   mu_;
    std::unordered_map<Key, BaselineRow, PairHash>       baseline_;
    /// 本 baseline epoch 内已计过 baseline-new 的缺失 key（去重 counter，rotate/load 清空）。
    std::unordered_set<Key, PairHash>                    new_reported_keys_;
    LoadStatus                                           last_load_status_ = LoadStatus::NotLoaded;
    bool                                                 ready_ = false;
};

}  // namespace ct::metrics
