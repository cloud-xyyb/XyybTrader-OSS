#pragma once

#include <cstdint>
#include <string>
#include <vector>

/// @brief 主力合约换月解析器。
///
/// 期货合约都有到期日，一条长周期回测跟踪的「品种」（例如 RB@MAIN）实际上要在一串
/// 会陆续到期的具体合约（rb2401、rb2405…）之间切换。XtRollEngine 就是这个切换的解析器：
/// 给定交易日，回答现在的主力是哪个合约、是否处在换月并行窗口、该窗口内哪些次主力合约
/// 的 tick 可以进 runtime，以及主力从旧合约跳到新合约时价格如何前复权衔接。
///
/// 它只做解析与缓存（记住最近一次 resolve 结果），不碰订单、账户或撮合。只被串行回放
/// 线程读写，不提供并发保证。
class XtRollEngine {
public:
    /** @brief 换月切换点（roll switch point）。 */
    struct SwitchPoint {
        uint32_t    date          = 0;   ///< 切换生效日（trading date，YYYYMMDD）
        std::string symbol;              ///< 切换后主力合约代码
        uint32_t    known_on_date = 0;   ///< 换月信息公开日（look-ahead bias 防护；0 = 当日已知）
        double      old_close     = 0.0; ///< 旧合约结算价（前复权用；0 = 不复权）
        double      new_close     = 0.0; ///< 新合约结算价（前复权用；0 = 不复权）
    };

    /** @brief 换月引擎配置（roll engine config）。 */
    struct Config {
        bool        active                 = false; ///< 是否启用换月引擎
        std::string logical_symbol;                 ///< 逻辑标的别名（如 "RB@MAIN"）
        int         window_before_days     = 0;     ///< 预换月窗口宽度（pre-roll window，自然日）
        int         window_after_days      = 0;     ///< 切换后窗口宽度（post-roll window，自然日）
        bool        dispatch_parallel_ticks = false; ///< 是否将并行窗口内次主力 tick 派发给策略
        std::vector<SwitchPoint> switches;           ///< 换月切换点序列（按 date 递增）

        /** @brief 校验配置合法性，错误信息追加到 errors。 */
        void validate(std::vector<std::string>& errors) const;
    };

    /** @brief resolve() 的查询结果（当日主力 + 并行窗口合约）。 */
    struct ResolveResult {
        bool        active          = false; ///< 当日是否有有效主力（date >= 首个 switch）
        bool        in_roll_window  = false; ///< 当日是否处于换月并行窗口内
        std::string primary_symbol;          ///< 当日主力合约代码
        std::vector<std::string> accepted_symbols; ///< 可接受合约列表（含主力 + 并行窗口合约）
        double      price_adj      = 0.0;   ///< 累计前复权偏移（sum of new_close - old_close）
    };

    XtRollEngine() = default;
    explicit XtRollEngine(const Config& cfg) { set_config(cfg); }

    /** @brief 设置换月配置（排序 switches、提取 all_symbols、解析逻辑标的）。 */
    void set_config(const Config& cfg);

    bool active() const { return active_; }
    bool has_logical_symbol() const { return !logical_symbol_.empty(); }
    const std::string& logical_symbol() const { return logical_symbol_; }
    const std::vector<SwitchPoint>& switches() const { return switches_; }
    const std::vector<std::string>& all_symbols() const { return all_symbols_; }

    /** @brief 解析给定交易日的主力合约与并行窗口合约集合。 */
    ResolveResult resolve(uint32_t date) const;
    /**
     * @brief 判断 tick 是否进入 runtime。
     *
     * 接受条件：code 在当日 `resolve()` 的 accepted_symbols 中。接受后的 tick 可进入
     * 行情缓存、session 过滤和撮合；不接受的 tick 直接丢弃。
     */
    bool accepts_tick(uint32_t date, const char* code) const;
    /**
     * @brief 判断已接受 tick 是否派发给策略。
     *
     * 默认只派发 primary_symbol；并行窗口内的次主力 tick 只更新 runtime state，
     * 不触发策略回调。`dispatch_parallel_ticks=true` 时才派发并行窗口 tick。
     */
    bool should_dispatch_tick(uint32_t date, const char* code) const;
    /**
     * @brief 校验并行窗口内非主力 tick 的时间是否合法（无 look-ahead bias）。
     *
     * 若非主力合约的 action_time 超过当前主力的最新 action_time，返回 false。
     * 用于应对 event_stream 排序不严格的场景，避免策略提前看到新主力的信息。
     *
     * 前置条件：调用方已确认 roll engine active。
     * @param primary_action_time  当前主力合约的上一帧 action_time。
     *                             调用方须在更新行情快照之前传入，否则比较失效。
     */
    bool is_parallel_time_valid(uint32_t date, uint32_t action_time,
                                const char* code, uint32_t primary_action_time) const;
    /** @brief 判断 code 是否为已配置的逻辑标的（支持 "RB@MAIN" 大小写不敏感匹配）。 */
    bool is_logical_symbol(const char* code) const;
    /** @brief 将逻辑标的展开为历史全部合约列表；非逻辑标的返回空。 */
    std::vector<std::string> symbols_for_logical(const char* code) const;

    /** @brief 将逻辑标的展开为全部合约列表；非逻辑标的返回单元素向量。 */
    std::vector<std::string> resolve_symbols(const char* code) const;
    /** @brief 将真实合约反查逻辑标的；不命中时返回 false。 */
    bool to_logical(const char* code, std::string& out) const;
    /** @brief 将逻辑标的解析为当日主力物理合约；非逻辑标的或未激活时返回 false。 */
    bool to_physical(uint32_t date, const char* logical_code, std::string& out) const;

    // 离线日历生成：不参与运行时 resolve/accept/dispatch 路径。
    /** @brief 持仓量快照（open interest snapshot），供离线推断换月日历。 */
    struct DetectRecord {
        uint32_t    date          = 0;   ///< 交易日（YYYYMMDD）
        std::string code;                ///< 合约代码
        double      open_interest = 0.0; ///< 当日持仓量（open interest）
    };

    /**
     * @brief 根据持仓量记录离线推断换月切换点。
     *
     * @details
     * - Runtime resolver 只消费 Config::switches；本 helper 不参与运行时 tick 过滤。
     * - 算法按交易日取持仓量最高的合约作为候选主力，连续 confirm_days 天领先后确认切换。
     * - lag_days 只用于回推 known_on_date；调用方需完成持久化和配置校验。
     *
     * @param records       各合约各日持仓量记录（无需预排序）。
     * @param confirm_days  连续 N 天持仓量领先才确认切换；小于 1 时返回空列表。
     * @param lag_days      known_on_date 相对确认日回溯的交易日数量；负数按 0 处理。
     * @return              推断出的 SwitchPoint 列表，供离线生成或检查换月日历。
     */
    static std::vector<SwitchPoint> detect_switches(
        const std::vector<DetectRecord>& records, int confirm_days = 3,
        int lag_days = 2);

private:
    /// @brief 查找指定交易日已生效的切换点索引。
    /// @return `switches_` 中最后一个 `date <= 参数 date` 的下标；未激活或早于首个切换点时返回 -1。
    int active_index(uint32_t date) const;

    /// @brief 判断目标切换点是否已到 pre-roll 可见日。
    /// @param switch_idx `switches_` 中目标新主力切换点下标。
    /// @return true 表示 pre-roll window 可以接受该新主力；越界或 `known_on_date` 未到返回 false。
    bool can_pre_roll_to(size_t switch_idx, uint32_t date) const;

    // 配置状态：set_config 后只读，查询路径不写这些字段。
    bool        active_                  = false; ///< set_config 后的有效配置开关。
    std::string logical_symbol_;                  ///< 配置中的逻辑标的别名，如 "RB@MAIN"。
    std::string logical_product_;                 ///< 从 logical_symbol_ 解析出的品种前缀，如 "RB"。
    int         window_before_days_      = 0;     ///< pre-roll window 宽度，单位为自然日（Gregorian calendar days）。
    int         window_after_days_       = 0;     ///< post-roll window 宽度，单位为自然日。
    bool        dispatch_parallel_ticks_ = false; ///< true 表示并行窗口 tick 也派发给策略。
    std::vector<SwitchPoint> switches_;           ///< 按 date 递增排序的切换点序列。
    std::vector<std::string> all_symbols_;        ///< 历史主力合约集合，去重且保持出现顺序。

    // resolve 缓存：仅串行回放线程读写；本类不加锁。
    mutable uint32_t      cached_date_ = 0; ///< cached_result_ 对应交易日；0 表示无可复用缓存。
    mutable ResolveResult cached_result_;   ///< cached_date_ 对应的 resolve() 结果。
};
