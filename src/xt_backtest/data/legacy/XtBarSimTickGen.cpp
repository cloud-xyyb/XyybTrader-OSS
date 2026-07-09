#include "xt_backtest/data/legacy/XtBarSimTickGen.h"

#include "xt_util/string/XtStringUtil.h"
#include "xt_core/market/XtTickBinCodec.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_util/time/XtTimeParse.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>
#include <sstream>

static constexpr int    kMinTicksPerBar      = 4;
static constexpr int    kMaxExtraTicksPerBar = 16;
static constexpr double kVolumePerExtraTick  = 500.0;

static constexpr int kOhlcPathSegments = 3;

static constexpr double kSkewDampRatio       = 0.8;
static constexpr double kSkewJitterAmplitude = 0.04;
static constexpr int    kSkewJitterPeriod    = 5;
static constexpr double kSkewClampAbs        = 0.6;
static constexpr double kDepthLevelSpread    = 0.5;
static constexpr double kDepthQtyScalePerLevel = 1.0;
static constexpr double kMinQty              = 1.0;
static constexpr double kMinPriceRange       = 1e-6;

static int calc_ticks_per_bar(double volume) {
    if (!(volume > 0.0)) return kMinTicksPerBar;
    const double extra_d = volume / kVolumePerExtraTick;
    const int    extra   = (extra_d >= kMaxExtraTicksPerBar)
                           ? kMaxExtraTicksPerBar
                           : static_cast<int>(extra_d);
    return kMinTicksPerBar + extra;
}

static void build_ohlc_anchors(const ParsedBarRow& row, double anchors[4]) {
    anchors[0] = row.open;
    if (row.close >= row.open) {
        anchors[1] = row.low;
        anchors[2] = row.high;
    } else {
        anchors[1] = row.high;
        anchors[2] = row.low;
    }
    anchors[3] = row.close;
}

static double interpolate_ohlc_path(const double anchors[4], double bar_frac) {
    const double seg_pos  = bar_frac * kOhlcPathSegments;
    const int    seg      = std::min(static_cast<int>(seg_pos), kOhlcPathSegments - 1);
    const double seg_frac = seg_pos - seg;
    return anchors[seg] + (anchors[seg + 1] - anchors[seg]) * seg_frac;
}

static double calc_book_skew(double price, double open_price,
                             double price_range, int tick_idx) {
    const double price_skew = (price - open_price) / price_range;
    const double jitter     = (static_cast<double>(tick_idx % kSkewJitterPeriod)
                                - kSkewJitterPeriod / 2) * kSkewJitterAmplitude;
    return std::clamp(price_skew * kSkewDampRatio + jitter, -kSkewClampAbs, kSkewClampAbs);
}

bool parse_bar_row_line(const std::string& line, ParsedBarRow& out) {

    std::stringstream ss(line);
    std::string col1, col2, o, h, l, c, v;
    if (!std::getline(ss, col1, ',')) return false;
    if (!std::getline(ss, col2, ',')) return false;
    if (!std::getline(ss, o,    ',')) return false;
    if (!std::getline(ss, h,    ',')) return false;
    if (!std::getline(ss, l,    ',')) return false;
    if (!std::getline(ss, c,    ',')) return false;
    if (!std::getline(ss, v,    ',')) return false;

    col1 = XtStringUtil::trim(col1);
    col2 = XtStringUtil::trim(col2);
    o    = XtStringUtil::trim(o);
    h    = XtStringUtil::trim(h);
    l    = XtStringUtil::trim(l);
    c    = XtStringUtil::trim(c);
    v    = XtStringUtil::trim(v);
    if (col1.empty()) return false;

    const bool has_separate_time = !col2.empty()
                                   && col2.find(':') != std::string::npos
                                   && col1.find('/') != std::string::npos;
    if (has_separate_time) {
        if (!XtTimeParse::parse_datetime(col1, out.trading_date, out.action_time)) return false;
        if (!XtTimeParse::parse_time_hms(col2, out.action_time)) return false;
    } else {
        if (!XtTimeParse::parse_datetime(col1, out.trading_date, out.action_time)) return false;
        o = col2;
    }

    if (!XtStringUtil::parse_double(o, out.open))   return false;
    if (!XtStringUtil::parse_double(h, out.high))   return false;
    if (!XtStringUtil::parse_double(l, out.low))    return false;
    if (!XtStringUtil::parse_double(c, out.close))  return false;
    if (!XtStringUtil::parse_double(v, out.volume)) out.volume = 0.0;
    return true;
}

void generate_ticks_from_bar_row(const ParsedBarRow&     row,
                                 const std::string&      code,
                                 int                     interval_sec,
                                 double                  spread,
                                 double                  qty_base,
                                 double&                 running_total_volume,
                                 std::vector<XtRawTick>& out_ticks) {
    const int      ticks_n      = calc_ticks_per_bar(row.volume);
    const int      bar_ms       = interval_sec * 1000;
    const int      step_ms      = bar_ms / ticks_n;
    const uint32_t bar_start    = XtTimeParse::add_ms(row.action_time, -bar_ms);
    const double   vol_per_tick = row.volume / ticks_n;

    double anchors[4];
    build_ohlc_anchors(row, anchors);
    const double price_range = std::max(row.high - row.low, kMinPriceRange);

    out_ticks.clear();
    out_ticks.reserve(static_cast<size_t>(ticks_n));

    for (int i = 0; i < ticks_n; ++i) {

        const double bar_frac = static_cast<double>(i) / (ticks_n - 1);
        const double price = interpolate_ohlc_path(anchors, bar_frac);
        const double skew  = calc_book_skew(price, row.open, price_range, i);

        running_total_volume += vol_per_tick;

        XtRawTick t{};
        std::strncpy(t.code, code.c_str(), sizeof(t.code) - 1);
        t.open         = row.open;
        t.high         = row.high;
        t.low          = row.low;
        t.price        = price;
        t.trading_date = row.trading_date;

        t.action_date  = row.trading_date;
        t.action_time  = XtTimeParse::add_ms(bar_start, step_ms * (i + 1));
        t.volume       = vol_per_tick;
        t.total_volume = running_total_volume;

        for (int k = 0; k < XtRawTick::DEPTH; ++k) {
            const double shift      = spread * kDepthLevelSpread * (1 + k);
            const double depth_mult = 1.0 + k * kDepthQtyScalePerLevel;
            t.bid_prices[k] = price - shift;
            t.ask_prices[k] = price + shift;
            t.bid_qty[k]    = std::max(qty_base * depth_mult * (1.0 + skew), kMinQty);
            t.ask_qty[k]    = std::max(qty_base * depth_mult * (1.0 - skew), kMinQty);
        }

        out_ticks.push_back(t);
    }
}

void write_ticks_csv(std::ofstream& out, const std::vector<XtRawTick>& ticks) {
    for (const auto& t : ticks) {
        out << t.action_date   << ","
            << t.action_time   << ","
            << t.code          << ","
            << t.price         << ","
            << t.bid_prices[0] << ","
            << t.ask_prices[0] << ","
            << t.bid_qty[0]    << ","
            << t.ask_qty[0]    << "\n";
    }
}

void write_ticks_bin(std::ofstream& out, XtTickBinFormat fmt, const std::vector<XtRawTick>& ticks) {
    for (const auto& t : ticks) {
        const XtTick core = xt_tick_convert::to_core(t);
        (void)xt_tick_bin::write_tick(out, fmt, t, core);
    }
}

std::string extract_yymm_from_bar_path(const std::string& path) {
    const size_t sep       = path.find_last_of("/\\");
    const std::string name = (sep == std::string::npos) ? path : path.substr(sep + 1);

    for (size_t i = 0; i + 3 < name.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(name[i]))     ||
            !std::isdigit(static_cast<unsigned char>(name[i + 1])) ||
            !std::isdigit(static_cast<unsigned char>(name[i + 2])) ||
            !std::isdigit(static_cast<unsigned char>(name[i + 3]))) {
            continue;
        }
        const int mm = (name[i + 2] - '0') * 10 + (name[i + 3] - '0');
        if (mm < 1 || mm > 12) continue;

        const bool left_ok  = (i == 0)               || !std::isdigit(static_cast<unsigned char>(name[i - 1]));
        const bool right_ok = (i + 4 >= name.size()) || !std::isdigit(static_cast<unsigned char>(name[i + 4]));
        if (!left_ok || !right_ok) continue;

        return name.substr(i, 4);
    }
    return {};
}
