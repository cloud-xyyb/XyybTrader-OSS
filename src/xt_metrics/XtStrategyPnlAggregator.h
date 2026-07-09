#pragma once

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

    void on_close(std::string_view sid, std::string_view code,
                  int64_t realized_pnl_fp);

    void on_fill(std::string_view sid, std::string_view code, double fee);

    std::vector<Row> snapshot() const;

    int64_t realized_fp_for_sid(std::string_view sid) const;

    bool save_state(std::string_view path) const;

    bool load_state(std::string_view path);

private:

    using Key = std::pair<std::string, std::string>;

    /// @brief Key 在 unordered_map 中使用的组合 hash。
    struct PairHash {
        size_t operator()(const Key& k) const noexcept {

            size_t h1 = std::hash<std::string>{}(k.first);
            size_t h2 = std::hash<std::string>{}(k.second);
            return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
        }
    };

    /// @brief 单个 (sid, code) 的累计状态。
    struct State {
        int64_t  realized_fp = 0;
        double   fees        = 0.0;
        uint64_t fills       = 0;
    };

    mutable std::shared_mutex                          mu_;

    std::unordered_map<Key, State, PairHash>           rows_;
};

}
