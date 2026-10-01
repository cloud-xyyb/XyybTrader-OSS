#include "TickCsvUtil.h"

#include "XtParseUtil.h"
#include "xt_util/string/XtStringUtil.h"
#include "xt_util/time/XtTimeParse.h"
#include "xt_core/session/XtTradingDateInfer.h"

#include <cstring>
#include <cmath>
#include <string>
#include <vector>

using XtParseUtil::split_csv_trim_sv;

namespace {

// "bidprice7" → prefix "bidprice" + 纯数字档位 7；非纯数字后缀或超出
// MAX_DEPTH 返回 0（L1 由调用方显式分支处理，本函数只服务 L2+）。
int depth_alias_level(const std::string& norm, const char* prefix) {
    const std::size_t plen = std::strlen(prefix);
    if (norm.size() <= plen || norm.compare(0, plen, prefix) != 0) return 0;
    int level = 0;
    for (std::size_t i = plen; i < norm.size(); ++i) {
        const char c = norm[i];
        if (c < '0' || c > '9') return 0;
        level = level * 10 + (c - '0');
        if (level > XtTickCsvSpec::MAX_DEPTH) return 0;
    }
    return level;
}

}  // namespace

XtTickVolumeMode parse_tick_volume_mode(const std::string& mode) {
    const std::string v = XtStringUtil::lower(XtStringUtil::trim(mode));
    if (v == "incremental" || v == "inc") return XtTickVolumeMode::Incremental;
    return XtTickVolumeMode::Cumulative;
}

bool build_tick_spec_from_header(const std::string& line, XtTickCsvSpec& spec) {
    // 复用 split buffer；字段视图只在本次调用期间有效。
    thread_local std::vector<std::string_view> cols;
    split_csv_trim_sv(line, cols);

    for (int idx = 0; idx < static_cast<int>(cols.size()); ++idx) {
        const std::string norm = XtParseUtil::normalize_col(cols[idx]);

        // 表头别名映射到统一列索引。
        if      (norm == "instrumentid")   spec.idx_instrument     = idx;
        else if (norm == "code")           spec.idx_instrument     = idx;
        else if (norm == "tradingday")     spec.idx_trading_day    = idx;
        else if (norm == "updatetime")     spec.idx_update_time    = idx;
        else if (norm == "updatemillisec") spec.idx_update_ms      = idx;
        else if (norm == "updatetimems")   spec.idx_update_time_ms = idx;
        else if (norm == "epochus")        spec.idx_epoch_us       = idx;
        else if (norm == "lastprice")      spec.idx_last_price     = idx;
        else if (norm == "price")          spec.idx_last_price     = idx;
        else if (norm == "volume")         spec.idx_volume         = idx;
        else if (norm == "highprice")      spec.idx_high           = idx;
        else if (norm == "lowprice")       spec.idx_low            = idx;
        else if (norm == "turnover")       spec.idx_turnover       = idx;
        else if (norm == "openinterest")   spec.idx_open_interest  = idx;
        else if (norm == "upperlimit")      spec.idx_upper_limit    = idx;
        else if (norm == "upperlimitprice") spec.idx_upper_limit    = idx;
        else if (norm == "lowerlimit")      spec.idx_lower_limit    = idx;
        else if (norm == "lowerlimitprice") spec.idx_lower_limit    = idx;
        else if (norm == "bidprice1")  { spec.idx_bid_price1 = idx; spec.idx_bid_price_n[0] = idx; }
        else if (norm == "bidvolume1") { spec.idx_bid_qty1   = idx; spec.idx_bid_qty_n[0]   = idx; }
        else if (norm == "askprice1")  { spec.idx_ask_price1 = idx; spec.idx_ask_price_n[0] = idx; }
        else if (norm == "askvolume1") { spec.idx_ask_qty1   = idx; spec.idx_ask_qty_n[0]   = idx; }
        // L2..L20 别名走统一档位解析（"bidprice2".."askvolume20"）。
        else if (int lv = depth_alias_level(norm, "bidprice"); lv >= 2)
            spec.idx_bid_price_n[lv - 1] = idx;
        else if (int lv = depth_alias_level(norm, "askprice"); lv >= 2)
            spec.idx_ask_price_n[lv - 1] = idx;
        else if (int lv = depth_alias_level(norm, "bidvolume"); lv >= 2)
            spec.idx_bid_qty_n[lv - 1] = idx;
        else if (int lv = depth_alias_level(norm, "askvolume"); lv >= 2)
            spec.idx_ask_qty_n[lv - 1] = idx;
    }
    // header 判定：必须命中至少一个已知列别名。旧判据「任意字母即 header」
    // 会把 headerless 首行（"ag2606,..."）吞成空 spec，整文件静默解析为空。
    bool alias_hit =
        spec.idx_instrument >= 0 || spec.idx_trading_day >= 0 ||
        spec.idx_update_time >= 0 || spec.idx_update_ms >= 0 ||
        spec.idx_update_time_ms >= 0 || spec.idx_epoch_us >= 0 ||
        spec.idx_last_price >= 0 || spec.idx_volume >= 0 ||
        spec.idx_high >= 0 || spec.idx_low >= 0 || spec.idx_turnover >= 0 ||
        spec.idx_open_interest >= 0 || spec.idx_upper_limit >= 0 ||
        spec.idx_lower_limit >= 0;
    for (int i = 0; !alias_hit && i < XtTickCsvSpec::MAX_DEPTH; ++i) {
        alias_hit = spec.idx_bid_price_n[i] >= 0 || spec.idx_ask_price_n[i] >= 0 ||
                    spec.idx_bid_qty_n[i] >= 0 || spec.idx_ask_qty_n[i] >= 0;
    }
    if (!alias_hit) return false;
    spec.has_header = true;

    if (spec.idx_update_time < 0 && spec.idx_update_time_ms >= 0)
        spec.idx_update_time = spec.idx_update_time_ms;

    spec.detected_depth = 1;
    for (int i = 1; i < XtTickCsvSpec::MAX_DEPTH; ++i) {
        if (spec.idx_bid_price_n[i] >= 0 || spec.idx_ask_price_n[i] >= 0)
            spec.detected_depth = i + 1;
    }

    // simple 格式不提供时间；调用方会得到 key=0。
    if (spec.idx_instrument >= 0 && spec.idx_last_price >= 0 &&
        spec.idx_trading_day < 0 && spec.idx_update_time < 0) {
        spec.simple = true;
    }
    return true;
}

bool parse_tick_csv_line(const std::string& line,
                         const XtTickCsvSpec& spec,
                         XtRawTick& out,
                         uint64_t& key,
                         const std::unordered_set<std::string>& symbol_filter,
                         double qty_base,
                         XtTickVolumeMode vol_mode,
                         TickParseDrop* out_drop) {
    // out_drop 观测参数：默认 None，各 false 分支覆写为具体原因；成功保持 None。
    if (out_drop) *out_drop = TickParseDrop::None;
    const auto drop = [&](TickParseDrop reason) {
        if (out_drop) *out_drop = reason;
        return false;
    };
    // 复用 split buffer；字段视图引用 line，调用返回后立即失效。
    thread_local std::vector<std::string_view> cols;
    split_csv_trim_sv(line, cols);
    if (cols.empty()) return drop(TickParseDrop::BadRow);

    auto col = [&](int idx) -> std::string_view {
        if (idx >= 0 && idx < static_cast<int>(cols.size())) return cols[idx];
        return {};
    };

    if (spec.simple) {
        const std::string_view code  = col(0);
        const std::string_view price = col(1);
        if (code.empty() || price.empty()) return drop(TickParseDrop::BadRow);
        // C++17 unordered_set 无异构查找；典型合约代码走 SSO 临时 string。
        if (!symbol_filter.empty() && !symbol_filter.count(std::string(code)))
            return drop(TickParseDrop::SymbolMiss);

        double px = 0.0;
        if (!XtParseUtil::parse_double(price, px)) return drop(TickParseDrop::BadRow);

        std::memset(&out, 0, sizeof(out));
        const std::size_t nc = std::min(code.size(), sizeof(out.code) - 1);
        std::memcpy(out.code, code.data(), nc);
        out.price = px;
        for (int i = 0; i < XtRawTick::DEPTH; ++i) {
            out.bid_prices[i] = px;
            out.ask_prices[i] = px;
            out.bid_qty[i]    = qty_base;
            out.ask_qty[i]    = qty_base;
        }
        key = make_tick_sort_key(out.trading_date, out.action_time);
        return true;
    }

    const std::string_view code = col(spec.idx_instrument);
    if (code.empty()) return drop(TickParseDrop::BadRow);
    if (!symbol_filter.empty() && !symbol_filter.count(std::string(code)))
        return drop(TickParseDrop::SymbolMiss);

    // last_price 是标准格式唯一强制字段；提交点前失败不修改 out。
    double last_price = 0.0;
    if (!XtParseUtil::parse_double(col(spec.idx_last_price), last_price))
        return drop(TickParseDrop::BadRow);

    // 提交点：此后不再 early return，out 会被完整重建。
    std::memset(&out, 0, sizeof(out));
    const std::size_t nc = std::min(code.size(), sizeof(out.code) - 1);
    std::memcpy(out.code, code.data(), nc);
    out.price = last_price;

    uint32_t trading_date = 0;
    XtParseUtil::parse_date(col(spec.idx_trading_day), trading_date);
    out.trading_date = trading_date;
    out.action_date  = trading_date;

    uint32_t action_time = 0;
    const std::string_view update_time_str = col(spec.idx_update_time);
    if (!update_time_str.empty())
        XtParseUtil::parse_time_with_ms(update_time_str, action_time);
    const std::string_view update_ms_str = col(spec.idx_update_ms);
    if (!update_ms_str.empty()) {
        int ms = 0;
        if (XtParseUtil::parse_i32(update_ms_str, ms))
            action_time = XtTimeParse::add_ms(action_time, ms);
    }
    out.action_time = action_time;

    // CTP TradingDay 夜盘前段归属下一交易日；action_date 反推为实际自然日。
    if (trading_date != 0) {
        const uint32_t hhmm = action_time / 100000;
        if (hhmm >= 1900) {
            out.action_date = XtTradingDateInfer::prev_weekday(trading_date);
        }
    }

    double vol = 0.0;
    XtParseUtil::parse_double(col(spec.idx_volume), vol);
    if (vol_mode == XtTickVolumeMode::Incremental)
        out.volume       = vol;
    else
        out.total_volume = vol;

    double turnover = 0.0, oi = 0.0;
    XtParseUtil::parse_double(col(spec.idx_turnover),      turnover);
    XtParseUtil::parse_double(col(spec.idx_open_interest), oi);
    out.total_turnover = turnover;
    out.open_interest  = oi;

    // 涨跌停价：列缺失、解析失败或哨兵值保持 0（消费侧 0 = 无限制 fail-open）。
    // 清洗口径对齐 live CTP（ctp_util::normalize_price：非有限/非正/>1e20 归 0），
    // 本地实现不引 xt_live 头；DBL_MAX 类哨兵直通会在 to_fp 越界（UB）。
    const auto clean_limit = [](double v) {
        return (std::isfinite(v) && v > 0.0 && v <= 1e20) ? v : 0.0;
    };
    double upper = 0.0, lower = 0.0;
    XtParseUtil::parse_double(col(spec.idx_upper_limit), upper);
    XtParseUtil::parse_double(col(spec.idx_lower_limit), lower);
    out.upper_limit = clean_limit(upper);
    out.lower_limit = clean_limit(lower);

    // L1 缺价时回退 last_price，缺量时回退 qty_base。
    {
        double bp = 0.0, ap = 0.0, bq = 0.0, aq = 0.0;
        if (spec.idx_bid_price_n[0] >= 0) XtParseUtil::parse_double(col(spec.idx_bid_price_n[0]), bp);
        if (spec.idx_ask_price_n[0] >= 0) XtParseUtil::parse_double(col(spec.idx_ask_price_n[0]), ap);
        if (spec.idx_bid_qty_n[0]   >= 0) XtParseUtil::parse_double(col(spec.idx_bid_qty_n[0]),   bq);
        if (spec.idx_ask_qty_n[0]   >= 0) XtParseUtil::parse_double(col(spec.idx_ask_qty_n[0]),   aq);
        out.bid_prices[0] = (bp > 0.0) ? bp : last_price;
        out.ask_prices[0] = (ap > 0.0) ? ap : last_price;
        out.bid_qty[0]    = (bq > 0.0) ? bq : qty_base;
        out.ask_qty[0]    = (aq > 0.0) ? aq : qty_base;
    }

    // L2-L20 缺价时继承上一档价格；缺量保持 0。
    for (int i = 1; i < XtTickCsvSpec::MAX_DEPTH; ++i) {
        double bp = 0.0, ap = 0.0, bq = 0.0, aq = 0.0;
        if (spec.idx_bid_price_n[i] >= 0) XtParseUtil::parse_double(col(spec.idx_bid_price_n[i]), bp);
        if (spec.idx_ask_price_n[i] >= 0) XtParseUtil::parse_double(col(spec.idx_ask_price_n[i]), ap);
        if (spec.idx_bid_qty_n[i]   >= 0) XtParseUtil::parse_double(col(spec.idx_bid_qty_n[i]),   bq);
        if (spec.idx_ask_qty_n[i]   >= 0) XtParseUtil::parse_double(col(spec.idx_ask_qty_n[i]),   aq);
        out.bid_prices[i] = (bp > 0.0) ? bp : out.bid_prices[i - 1];
        out.ask_prices[i] = (ap > 0.0) ? ap : out.ask_prices[i - 1];
        out.bid_qty[i]    = bq;
        out.ask_qty[i]    = aq;
    }

    // XtRawTick 深度超过 CSV 可识别档数时，延续末档价格并清零数量
    //（当前 MAX_DEPTH == XtRawTick::DEPTH，本循环为空转守卫）。
    for (int i = XtTickCsvSpec::MAX_DEPTH; i < XtRawTick::DEPTH; ++i) {
        out.bid_prices[i] = out.bid_prices[XtTickCsvSpec::MAX_DEPTH - 1];
        out.ask_prices[i] = out.ask_prices[XtTickCsvSpec::MAX_DEPTH - 1];
        out.bid_qty[i]    = 0.0;
        out.ask_qty[i]    = 0.0;
    }

    key = make_tick_sort_key(out.trading_date, out.action_time);
    return true;
}
