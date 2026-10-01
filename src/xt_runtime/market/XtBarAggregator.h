#pragma once
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <unordered_map>

#include "xt_core/market/XtBarStruct.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_core/market/XtBarSlot.h"
#include "xt_util/time/XtTimeParse.h"
#include "xt_util/string/XtCStringUtil.h"
#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_runtime/market/detail/XtTransparentMapSlot.h"

/// @brief CTA tick 流的 bar 聚合器。
///
/// 调用方先注册要聚合的 code+period，再把 tick 串行交给 on_tick()。
/// on_tick() 在新周期首笔 tick 到达时，通过回调输出上一根完整 bar。
///
/// @note session 结束前必须调用 flush() 输出尾 bar。
/// @note 本类不做内部同步；subscribe()、on_tick()、flush() 必须串行调用。
class XtBarAggregator {
public:
    /// @brief 完整 bar 输出回调；参数为 code、period 和 bar。
    using BarCallback = std::function<void(const char* code,
                                           const char* period,
                                           const XtBar& bar)>;

    /// @brief 新 bar 打开回调；open_px 是当前 bar 的开盘价。
    using BarOpenCallback = std::function<void(const char* code,
                                               const char* period,
                                               double      open_px)>;

    /// @brief 注册一个 code+period 的聚合目标。
    /// @note 对同一 code+period 重复调用不会新增状态。
    void subscribe(const char* code, const char* period) {
        const std::string key = slot_key(code, period);
        auto [it, inserted] = slots_.try_emplace(key);
        if (inserted) {
            it->second.code       = code;
            it->second.period     = period;
            it->second.period_min = parse_period_minutes(period);
            code_index_[code].push_back(key);
        }
    }

    /// @brief 用一笔 tick 推进所有已订阅的对应周期。
    ///
    /// tick.code 未订阅时直接返回 0。新周期出现时先通过 cb 输出上一根完整 bar，
    /// 再通过 open_cb 通知新 bar 的开盘价。
    ///
    /// @return 本次输出的完整 bar 数量。
    int on_tick(const XtTick& tick, const BarCallback& cb,
                const BarOpenCallback& open_cb = nullptr) {
        auto idx_it = code_index_.find(tick.code);
        if (idx_it == code_index_.end()) return 0;
        int fired = 0;
        const double price = xt_tick_convert::price_from_fp(tick.price_fp);
        double vol = xt_tick_convert::qty_from_fp(tick.volume_fp);
        const double total_vol = xt_tick_convert::qty_from_fp(tick.total_volume_fp);
        if (total_vol > 0.0) {
            double& last = xt_runtime::market::detail::slot_for(last_total_volume_, tick.code);
            if (vol <= 0.0) {
                if (last > 0.0 && total_vol >= last) vol = total_vol - last;
            }
            last = total_vol;
        }
        for (const auto& key : idx_it->second) {
            auto sit = slots_.find(key);
            if (sit == slots_.end()) continue;
            fired += update_slot(sit->second, tick.action_date, tick.action_time,
                                 tick.trading_date, price, vol, cb, open_cb);
        }
        return fired;
    }

    /// @brief 输出所有仍在构建中的 bar，并把这些 bar 标记为 inactive。
    /// @return 输出的 bar 数量。
    int flush(const BarCallback& cb) {
        int fired = 0;
        for (auto& [key, s] : slots_) {
            if (!s.active) continue;
            cb(s.code.c_str(), s.period.c_str(), s.bar);
            s.active = false;
            ++fired;
        }
        return fired;
    }

    /// @brief 解析周期字符串，返回分钟数；解析失败退回 1 分钟并 warn-once。
    /// @note 支持 m/h/d 后缀，例如 "5m"、"1h"、"1d"。
    static int parse_period_minutes(const char* period) {
        if (!period || period[0] == '\0') return warn_fallback_1m_(period);
        char* end = nullptr;
        const long n = std::strtol(period, &end, 10);
        if (!end || n <= 0) return warn_fallback_1m_(period);
        if (*end == 'm' || *end == 'M') return static_cast<int>(n);
        if (*end == 'h' || *end == 'H') return static_cast<int>(n) * 60;
        if (*end == 'd' || *end == 'D') return static_cast<int>(n) * 1440;
        return warn_fallback_1m_(period);
    }

private:
    /// @brief 单个 code+period 正在构建的 bar 状态。
    struct BarSlot {
        std::string code;               ///< 合约代码。
        std::string period;             ///< 周期字符串，例如 "5m"。
        int         period_min = 1;     ///< 周期分钟数。
        bool        active     = false; ///< 是否已有正在构建的 bar。
        uint64_t    slot_id    = 0;     ///< 当前 bar 所属的时间桶编号。
        XtBar       bar{};              ///< 正在累积 OHLCV 的 bar。
    };

    /// @brief 计算 tick 所属的时间桶编号。
    /// @note 委托 xt_bar_slot，保证 CTA live/backtest 使用同一套周期边界。
    static uint64_t compute_slot_id(uint32_t action_date, uint32_t action_time,
                                    uint32_t trading_date, int period_min) {
        return xt_bar_slot::compute_slot_id(action_date, action_time, trading_date, period_min);
    }

    /// @brief 周期解析失败时退回 1m，并只打印一次告警。
    static int warn_fallback_1m_(const char* period) {
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true)) {
            std::fprintf(stderr,
                         "[XtBarAggregator] WARN: unparsable period '%s', fallback to 1m\n",
                         period ? period : "(null)");
        }
        return 1;
    }

    /// @brief 用当前 tick 更新一个 code+period 的 bar 状态。
    /// @return 0 表示未输出完整 bar，1 表示本次切周期并输出上一根 bar。
    int update_slot(BarSlot& s, uint32_t action_date, uint32_t action_time,
                    uint32_t trading_date, double price, double vol,
                    const BarCallback& cb, const BarOpenCallback& open_cb) {
        const uint64_t sid = compute_slot_id(action_date, action_time,
                                             trading_date, s.period_min);
        int fired = 0;

        if (!s.active) {
            open_bar(s, action_date, action_time, trading_date, price, vol, sid);
            if (open_cb)
                open_cb(s.code.c_str(), s.period.c_str(), price);
        } else if (sid != s.slot_id) {
            cb(s.code.c_str(), s.period.c_str(), s.bar);
            ++fired;
            open_bar(s, action_date, action_time, trading_date, price, vol, sid);
            if (open_cb)
                open_cb(s.code.c_str(), s.period.c_str(), price);
        } else {
            if (price > s.bar.high)  s.bar.high   = price;
            if (price < s.bar.low)   s.bar.low    = price;
            s.bar.close   = price;
            s.bar.volume += vol;
            s.bar.trading_date = trading_date;  // 夜盘可能跨自然日，持续取最新交易日。
        }
        return fired;
    }

    /// @brief 用当前 tick 打开一根新 bar。
    static void open_bar(BarSlot& s, uint32_t action_date, uint32_t action_time,
                         uint32_t trading_date, double price, double vol, uint64_t sid) {
        s.active      = true;
        s.slot_id     = sid;
        std::memset(&s.bar, 0, sizeof(s.bar));
        XtCStringUtil::copy(s.bar.code, s.code.c_str());
        s.bar.action_date  = action_date;
        s.bar.action_time  = action_time;
        s.bar.trading_date = trading_date;
        s.bar.open  = price;
        s.bar.high  = price;
        s.bar.low   = price;
        s.bar.close = price;
        s.bar.volume = vol;
    }

    /// @brief 构造内部 map key，格式为 code@period。
    static std::string slot_key(const char* code, const char* period) {
        return std::string(code) + "@" + period;
    }

    std::unordered_map<std::string, BarSlot> slots_;  ///< code@period -> 聚合状态。
    /// code -> 该 code 订阅的 code@period key 列表。
    xt_core::sync::TransparentStringMap<std::vector<std::string>> code_index_;
    /// code -> 上一次 total_volume；tick.volume_fp 缺失时用于推导增量成交量。
    xt_core::sync::TransparentStringMap<double> last_total_volume_;
};
