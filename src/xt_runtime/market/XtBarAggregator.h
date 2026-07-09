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

    using BarCallback = std::function<void(const char* code,
                                           const char* period,
                                           const XtBar& bar)>;

    using BarOpenCallback = std::function<void(const char* code,
                                               const char* period,
                                               double      open_px)>;

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
        std::string code;
        std::string period;
        int         period_min = 1;
        bool        active     = false;
        uint64_t    slot_id    = 0;
        XtBar       bar{};
    };

    static uint64_t compute_slot_id(uint32_t action_date, uint32_t action_time,
                                    uint32_t trading_date, int period_min) {
        return xt_bar_slot::compute_slot_id(action_date, action_time, trading_date, period_min);
    }

    static int warn_fallback_1m_(const char* period) {
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true)) {
            std::fprintf(stderr,
                         "[XtBarAggregator] WARN: unparsable period '%s', fallback to 1m\n",
                         period ? period : "(null)");
        }
        return 1;
    }

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
            s.bar.trading_date = trading_date;
        }
        return fired;
    }

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

    static std::string slot_key(const char* code, const char* period) {
        return std::string(code) + "@" + period;
    }

    std::unordered_map<std::string, BarSlot> slots_;

    xt_core::sync::TransparentStringMap<std::vector<std::string>> code_index_;

    xt_core::sync::TransparentStringMap<double> last_total_volume_;
};
