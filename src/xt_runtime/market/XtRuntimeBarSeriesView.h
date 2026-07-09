#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "xt_core/market/XtBarStruct.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_runtime/market/detail/XtTransparentMapSlot.h"

namespace xt_runtime::market {

/// @brief 固定容量 bar 环形缓冲（circular buffer）。
///
/// 调用方先通过 init() 固定容量；之后 push_back 满时覆盖最旧 bar。
/// operator[] 按逻辑时间顺序访问，i=0 为最旧、i=size()-1 为最新。
/// init() 之后的追加和查询路径不再主动分配堆内存。
struct CircularBarBuffer {
    std::vector<XtBar> data;
    size_t head = 0;
    size_t sz = 0;

    void init(size_t capacity) {
        if (capacity == 0) capacity = 1;
        data.assign(capacity, XtBar{});
        head = 0;
        sz = 0;
    }

    size_t capacity() const noexcept { return data.size(); }
    size_t size()     const noexcept { return sz; }
    bool   empty()    const noexcept { return sz == 0; }

    void push_back(const XtBar& bar) {
        if (data.empty()) return;
        if (sz < data.size()) {
            data[(head + sz) % data.size()] = bar;
            ++sz;
        } else {
            data[head] = bar;
            head = (head + 1) % data.size();
        }
    }

    const XtBar& operator[](size_t i) const noexcept {
        return data[(head + i) % data.size()];
    }
};

/// @brief Runtime 内部 bar 序列视图。
///
/// 策略侧通过 get_bars() 拷贝到调用方缓冲区读取。
/// 对象不返回内部容器引用。
/// 默认按 tick 增量聚合 bar，CTA 外部 aggregator 可直接注入完成 bar。
class XtRuntimeBarSeriesView {
public:

    void set_history_cap(size_t cap) {
        bar_history_cap_ = cap > 0 ? cap : 512;
    }

    void on_tick(const XtRawTick& tick) {
        on_tick(xt_tick_convert::to_core(tick));
    }

    void on_tick(const XtTick& tick) {
        if (!tick.code[0]) return;
        detail::slot_for(last_core_ticks_, tick.code) = tick;
        update_bar_series(tick);
    }

    void inject_completed_bar(const char* code, const char* period, const XtBar& bar) {
        if (!code || code[0] == '\0' || !period || period[0] == '\0') return;
        const std::string key = bar_key(code, period);
        auto it = bar_series_.find(key);
        if (it == bar_series_.end()) {
            BarSeries series;
            if (!parse_period_ms(period, series.period_ms)) return;
            series.bars.init(bar_history_cap_);
            bar_keys_by_code_[code].push_back(key);
            it = bar_series_.emplace(key, std::move(series)).first;
        }
        BarSeries& s = it->second;
        if (s.has_bar) {
            s.bars.push_back(s.cur);
            s.has_bar = false;
        }
        s.bars.push_back(bar);
    }

    void set_series_capacity(const char* code, const char* period, size_t cap) {
        if (!code || code[0] == '\0' || !period || period[0] == '\0' || cap == 0) return;
        const std::string key = bar_key(code, period);
        auto it = bar_series_.find(key);
        if (it == bar_series_.end()) {
            BarSeries series;
            if (!parse_period_ms(period, series.period_ms)) return;
            series.bars.init(cap);
            bar_keys_by_code_[code].push_back(key);
            bar_series_.emplace(key, std::move(series));
        }
    }

    size_t get_bars(const char* code, const char* period, XtBar* out, size_t max) {
        if (!code || code[0] == '\0' || !period || period[0] == '\0' || !out || max == 0) return 0;
        const std::string key = bar_key(code, period);
        auto it = bar_series_.find(key);
        if (it == bar_series_.end()) {
            BarSeries series;
            if (!parse_period_ms(period, series.period_ms)) return 0;
            series.bars.init(bar_history_cap_);
            bar_keys_by_code_[code].push_back(key);
            it = bar_series_.emplace(key, std::move(series)).first;
            auto lt = last_core_ticks_.find(code);
            if (lt != last_core_ticks_.end()) update_bar_series(lt->second);
        }
        return copy_bars(it->second, out, max);
    }

private:
    struct BarSeries {
        uint32_t period_ms = 0;
        uint32_t last_date = 0;
        uint64_t last_bucket = 0;
        bool has_bar = false;
        XtBar cur{};
        CircularBarBuffer bars;
    };

    static bool parse_period_ms(const char* period, uint32_t& out_ms) {
        out_ms = 0;
        if (!period || period[0] == '\0') return false;
        std::string s(period);

        size_t i = 0;
        while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) ++i;

        int val = 0;
        std::string unit;
        if (i > 0) {
            try { val = std::stoi(s.substr(0, i)); } catch (...) { return false; }
            unit = s.substr(i);
        } else {
            size_t j = 0;
            while (j < s.size() && !std::isdigit(static_cast<unsigned char>(s[j]))) ++j;
            if (j == 0 || j >= s.size()) return false;
            unit = s.substr(0, j);
            try { val = std::stoi(s.substr(j)); } catch (...) { return false; }
        }
        if (val <= 0) return false;

        if (unit == "ms") {
            out_ms = static_cast<uint32_t>(val);
            return out_ms > 0;
        }
        const char u = unit.empty() ? '\0' : unit[0];
        uint64_t ms = 0;
        switch (u) {
        case 's': ms = static_cast<uint64_t>(val) * 1000ULL; break;
        case 'm': ms = static_cast<uint64_t>(val) * 60ULL * 1000ULL; break;
        case 'h': ms = static_cast<uint64_t>(val) * 60ULL * 60ULL * 1000ULL; break;
        case 'd': ms = static_cast<uint64_t>(val) * 24ULL * 60ULL * 60ULL * 1000ULL; break;
        default: return false;
        }
        if (ms == 0 || ms > std::numeric_limits<uint32_t>::max()) return false;
        out_ms = static_cast<uint32_t>(ms);
        return true;
    }

    static uint32_t time_ms_of_day(uint32_t action_time) {
        const uint32_t hh = action_time / 10000000;
        const uint32_t mm = (action_time / 100000) % 100;
        const uint32_t ss = (action_time / 1000) % 100;
        const uint32_t ms = action_time % 1000;
        return ((hh * 60 + mm) * 60 + ss) * 1000 + ms;
    }

    void update_bar_series(const XtTick& tick) {
        auto it_keys = bar_keys_by_code_.find(tick.code);
        if (it_keys == bar_keys_by_code_.end()) return;
        const uint32_t date = xt_tick_convert::trading_date(tick);
        const uint32_t ms_day = time_ms_of_day(tick.action_time);
        const double price = xt_tick_convert::price_from_fp(tick.price_fp);
        double vol = xt_tick_convert::qty_from_fp(tick.volume_fp);
        const double total_vol = xt_tick_convert::qty_from_fp(tick.total_volume_fp);
        if (vol <= 0.0 && total_vol > 0.0) {
            double& last = detail::slot_for(last_total_volume_, tick.code);
            if (last > 0.0 && total_vol >= last) vol = total_vol - last;
            last = total_vol;
        }
        double turnover = xt_tick_convert::turnover_from_fp(tick.turnover_fp);
        const double total_turnover = xt_tick_convert::turnover_from_fp(tick.total_turnover_fp);
        if (turnover <= 0.0 && total_turnover > 0.0) {
            double& last = detail::slot_for(last_total_turnover_, tick.code);
            if (last > 0.0 && total_turnover >= last) turnover = total_turnover - last;
            last = total_turnover;
        }

        for (const auto& key : it_keys->second) {
            auto it = bar_series_.find(key);
            if (it == bar_series_.end()) continue;
            BarSeries& series = it->second;
            if (series.period_ms == 0) continue;
            const uint64_t bucket = static_cast<uint64_t>(ms_day) / static_cast<uint64_t>(series.period_ms);
            const bool date_changed = (date != 0 && series.last_date != date);
            const bool new_bucket = (!series.has_bar || date_changed || series.last_bucket != bucket);
            if (new_bucket) {
                if (series.has_bar) {
                    series.bars.push_back(series.cur);
                }
                series.has_bar = true;
                series.last_date = date;
                series.last_bucket = bucket;
                std::memset(&series.cur, 0, sizeof(series.cur));
                std::snprintf(series.cur.code, sizeof(series.cur.code), "%s", tick.code);
                series.cur.action_date = date;
                series.cur.action_time = tick.action_time;
                series.cur.open = price;
                series.cur.high = price;
                series.cur.low = price;
                series.cur.close = price;
                series.cur.volume = vol > 0.0 ? vol : 0.0;
                series.cur.turnover = turnover > 0.0 ? turnover : 0.0;
                series.cur.open_interest = xt_tick_convert::interest_from_fp(tick.open_interest_fp);
            } else {
                series.cur.high = std::max(series.cur.high, price);
                series.cur.low = (series.cur.low <= 0.0) ? price : std::min(series.cur.low, price);
                series.cur.close = price;
                if (vol > 0.0) series.cur.volume += vol;
                if (turnover > 0.0) series.cur.turnover += turnover;
                series.cur.open_interest = xt_tick_convert::interest_from_fp(tick.open_interest_fp);
                series.cur.action_time = tick.action_time;
            }
        }
    }

    static size_t copy_bars(const BarSeries& series, XtBar* out, size_t max) {
        if (!out || max == 0) return 0;
        const size_t hist_count = series.bars.size();
        const size_t total = hist_count + (series.has_bar ? 1 : 0);
        if (total == 0) return 0;
        const size_t n = total < max ? total : max;
        const size_t start = total > n ? (total - n) : 0;
        size_t idx = 0;
        if (start < hist_count) {
            for (size_t i = start; i < hist_count && idx < n; ++i) {
                out[idx++] = series.bars[i];
            }
            if (idx < n && series.has_bar) out[idx++] = series.cur;
        } else if (series.has_bar && idx < n) {
            out[idx++] = series.cur;
        }
        return idx;
    }

    static std::string bar_key(const char* code, const char* period) {
        std::string k;
        k.reserve(32);
        if (code) k.append(code);
        k.push_back('@');
        if (period) k.append(period);
        return k;
    }

    size_t bar_history_cap_ = 512;

    xt_core::sync::TransparentStringMap<BarSeries> bar_series_;
    xt_core::sync::TransparentStringMap<std::vector<std::string>> bar_keys_by_code_;
    xt_core::sync::TransparentStringMap<double> last_total_volume_;
    xt_core::sync::TransparentStringMap<double> last_total_turnover_;
    xt_core::sync::TransparentStringMap<XtTick> last_core_ticks_;
};

}
