#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
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

    explicit XtStrategyPnlBaseline(std::string instance_id);

    bool load(std::string_view base_dir, int trading_day_id);

    void rotate_day(const std::vector<BaselineRow>& rows);

    bool save(std::string_view base_dir, int trading_day_id) const;

    BaselineRow get(std::string_view sid, std::string_view code, bool* is_new = nullptr) const;

    int64_t realized_fp_for_sid(std::string_view sid) const;

    static int compute_trading_day_id(int tz_offset_h);

    size_t size() const;

    bool ready() const;

    LoadStatus last_load_status() const;
    static const char* load_status_name(LoadStatus status) noexcept;

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
    LoadStatus                                           last_load_status_ = LoadStatus::NotLoaded;
    bool                                                 ready_ = false;
};

}
