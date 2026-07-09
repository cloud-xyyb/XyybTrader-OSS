#pragma once

#include <cstdint>
#include <string>
#include <vector>

/// @brief 回测换月解析器（roll engine）。
///
/// 根据交易日解析主力合约、换月并行窗口内可接受合约和逻辑标的映射。
/// XtRollEngine 缓存最近一次 `resolve()` 结果，不提供并发读写保证。
class XtRollEngine {
public:
    /** @brief 换月切换点（roll switch point）。 */
    struct SwitchPoint {
        uint32_t    date          = 0;
        std::string symbol;
        uint32_t    known_on_date = 0;
        double      old_close     = 0.0;
        double      new_close     = 0.0;
    };

    /** @brief 换月引擎配置（roll engine config）。 */
    struct Config {
        bool        active                 = false;
        std::string logical_symbol;
        int         window_before_days     = 0;
        int         window_after_days      = 0;
        bool        dispatch_parallel_ticks = false;
        std::vector<SwitchPoint> switches;

        void validate(std::vector<std::string>& errors) const;
    };

    /** @brief resolve() 的查询结果（当日主力 + 并行窗口合约）。 */
    struct ResolveResult {
        bool        active          = false;
        bool        in_roll_window  = false;
        std::string primary_symbol;
        std::vector<std::string> accepted_symbols;
        double      price_adj      = 0.0;
    };

    XtRollEngine() = default;
    explicit XtRollEngine(const Config& cfg) { set_config(cfg); }

    void set_config(const Config& cfg);

    bool active() const { return active_; }
    bool has_logical_symbol() const { return !logical_symbol_.empty(); }
    const std::string& logical_symbol() const { return logical_symbol_; }
    const std::vector<SwitchPoint>& switches() const { return switches_; }
    const std::vector<std::string>& all_symbols() const { return all_symbols_; }

    ResolveResult resolve(uint32_t date) const;

    bool accepts_tick(uint32_t date, const char* code) const;

    bool should_dispatch_tick(uint32_t date, const char* code) const;

    bool is_parallel_time_valid(uint32_t date, uint32_t action_time,
                                const char* code, uint32_t primary_action_time) const;

    bool is_logical_symbol(const char* code) const;

    std::vector<std::string> symbols_for_logical(const char* code) const;

    std::vector<std::string> resolve_symbols(const char* code) const;

    bool to_logical(const char* code, std::string& out) const;

    bool to_physical(uint32_t date, const char* logical_code, std::string& out) const;

    // 离线日历生成：不参与运行时 resolve/accept/dispatch 路径。
    /** @brief 持仓量快照（open interest snapshot），供离线推断换月日历。 */
    struct DetectRecord {
        uint32_t    date          = 0;
        std::string code;
        double      open_interest = 0.0;
    };

    static std::vector<SwitchPoint> detect_switches(
        const std::vector<DetectRecord>& records, int confirm_days = 3,
        int lag_days = 2);

private:

    int active_index(uint32_t date) const;

    bool can_pre_roll_to(size_t switch_idx, uint32_t date) const;

    bool        active_                  = false;
    std::string logical_symbol_;
    std::string logical_product_;
    int         window_before_days_      = 0;
    int         window_after_days_       = 0;
    bool        dispatch_parallel_ticks_ = false;
    std::vector<SwitchPoint> switches_;
    std::vector<std::string> all_symbols_;

    mutable uint32_t      cached_date_ = 0;
    mutable ResolveResult cached_result_;
};
