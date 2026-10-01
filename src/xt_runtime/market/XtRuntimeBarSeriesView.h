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
    std::vector<XtBar> data;  ///< init() 一次性分配的固定容量存储。
    size_t head = 0;          ///< 最旧元素的物理索引。
    size_t sz = 0;            ///< 当前有效元素数量。

    /// @brief 按 capacity 初始化存储；capacity=0 按 1 处理。
    void init(size_t capacity) {
        if (capacity == 0) capacity = 1;
        data.assign(capacity, XtBar{});
        head = 0;
        sz = 0;
    }

    size_t capacity() const noexcept { return data.size(); }
    size_t size()     const noexcept { return sz; }
    bool   empty()    const noexcept { return sz == 0; }

    /// @brief 追加 bar；容量已满时覆盖最旧元素。
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
    /// @brief 设置默认 bar 历史容量；cap=0 时恢复为 512。
    ///
    /// 已创建的 series 不受影响，调用方应在首次订阅/查询前配置。
    void set_history_cap(size_t cap) {
        bar_history_cap_ = cap > 0 ? cap : 512;
    }

    /// @brief 写入 raw tick；内部先转换为 core tick。
    void on_tick(const XtRawTick& tick) {
        on_tick(xt_tick_convert::to_core(tick));
    }

    /// @brief 写入一笔 tick，并推进已订阅周期的 in-progress bar。
    void on_tick(const XtTick& tick) {
        if (!tick.code[0]) return;
        detail::slot_for(last_core_ticks_, tick.code) = tick;
        update_bar_series(tick);
    }

    /// @brief 直接注入一根完成 bar，不走 tick 聚合。
    ///
    /// 若该 series 已有 tick 聚合中的 in-progress bar，先写入历史再注入。
    /// 这是 CTA tick/bar 模式切换时的防御性处理，正常路径不应混用同一 series。
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

    /// @brief 设置指定品种+周期的 bar 历史容量。
    ///
    /// 仅在 series 尚未创建时生效；已存在 series 不重新分配。
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

    /// @brief 拷贝最近 bar 到调用方缓冲区。
    ///
    /// 首次查询一个合法 `(code, period)` 会创建对应 series；若已有最新 tick，
    /// 会立即生成当前 in-progress bar，使查询端不必等待下一笔 tick。
    /// @return 实际写入 out 的 bar 数量，最多 max。
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
        uint32_t period_ms = 0;   ///< 聚合周期，单位 ms。
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

        // 兼容 WonderTrader 风格周期：既接受 "5m"，也接受 "m5"。
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

    // 内部 tick 聚合按 (trading_date, ms_of_day / period_ms) 分桶，并在
    // trading_date 翻转时强制切 bar；这样可覆盖 ms/s 周期。CTA/SEL 的完成
    // bar 由外部聚合器经 inject_completed_bar 注入，不走本路径。
    // @note >6h 周期在夜盘 21:00 处会按 trading_date 切分；启用大周期前须
    //       先与 xt_bar_slot 的 action_date 分桶语义统一。
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

    /// @brief 返回 `<code>@<period>` 拼接 key，by-value 让 caller 持 ownership。
    static std::string bar_key(const char* code, const char* period) {
        std::string k;
        k.reserve(32);  // 覆盖典型 `<code>@<period>` 长度。
        if (code) k.append(code);
        k.push_back('@');
        if (period) k.append(period);
        return k;
    }

    size_t bar_history_cap_ = 512;
    // 透明 hash 避免查询路径为 const char* key 构造临时 string。
    xt_core::sync::TransparentStringMap<BarSeries> bar_series_;
    xt_core::sync::TransparentStringMap<std::vector<std::string>> bar_keys_by_code_;
    xt_core::sync::TransparentStringMap<double> last_total_volume_;
    xt_core::sync::TransparentStringMap<double> last_total_turnover_;
    xt_core::sync::TransparentStringMap<XtTick> last_core_ticks_;
};

}  // namespace xt_runtime::market
