#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "xt_backtest/data/reader/XtTickCollector.h"
#include "XtLogger.h"
#include "xt_core/market/XtTickBinCodec.h"
#include "xt_util/path/PathUtil.h"
#include "xt_backtest/data/event_stream/ReplayEventBuilder.h"
#include "xt_backtest/data/source/detail/XtTickAdmission.h"
#include "TickFileUtil.h"

#include <fstream>
#include <string>
#include <vector>

namespace {

void filter_ticks_by_trading_day_range(std::vector<XtRawTick>& ticks,
                                       uint32_t start_day,
                                       uint32_t end_day) {
    if (ticks.empty()) return;
    if (start_day == 0 && end_day == 0) return;

    size_t dst = 0;
    for (size_t i = 0; i < ticks.size(); ++i) {
        if (!xt_bt_admission::in_day_range(ticks[i], start_day, end_day)) {
            continue;
        }
        if (dst != i) ticks[dst] = ticks[i];
        ++dst;
    }
    ticks.resize(dst);
}

void apply_session_filter(std::vector<XtRawTick>& ticks,
                          const xt_core::Config& session) {
    if (!xt_core::is_enabled(session) || ticks.empty()) return;

    size_t dst = 0;
    size_t dropped = 0;
    size_t preopen_count = 0;
    for (size_t i = 0; i < ticks.size(); ++i) {
        const auto adm = xt_bt_admission::classify_session(ticks[i], session);
        if (adm == xt_bt_admission::TickAdmit::Drop) {
            ++dropped;
            continue;
        }
        if (adm == xt_bt_admission::TickAdmit::Preopen) {
            ticks[i].kind = static_cast<uint32_t>(TickKind::Preopen);
            ++preopen_count;
        } else {
            ticks[i].kind = static_cast<uint32_t>(TickKind::Normal);
        }
        if (dst != i) ticks[dst] = ticks[i];
        ++dst;
    }
    ticks.resize(dst);
    if (dropped > 0 || preopen_count > 0) {
        XtLogger::info_m("MD", "session filter: kept={} preopen={} dropped={}",
                         ticks.size(), preopen_count, dropped);
    }
}

}

void XtMarketDataSource::load_memory_source() {
    mem_.ticks.clear();

    if (tick_.source == TickSource::Csv) {
        load_csvs_to_memory(tick_.files);
        return;
    }
    if (bar_driver_.has_memory_source()) {
        if (!bar_driver_.load_to_memory(mem_.ticks))
            XtLogger::error_m("MD", "bar driver loaded no ticks");
        return;
    }

    const char* mode_str = (mode_ == XtReplayMode::Bar)  ? "bar"
                         : (mode_ == XtReplayMode::Tick) ? "tick"
                                                         : "event_stream";
    XtLogger::error_m("MD", "no data source configured for memory load (mode={})", mode_str);
}

bool XtMarketDataSource::load_csvs_to_memory(const std::vector<std::string>& paths) {
    std::vector<XtTickWithKey> all;
    if (!collect_ticks_from_paths(paths, all, tick_.symbol_set, tick_.qty_base,
                                  parse_tick_volume_mode(tick_.volume_mode))) {
        XtLogger::error_m("MD", "no ticks collected from CSV files");
        return false;
    }
    mem_.ticks.clear();
    mem_.ticks.reserve(all.size());
    for (const auto& t : all) mem_.ticks.push_back(t.tick);

    filter_ticks_by_trading_day_range(mem_.ticks, range_.start, range_.end);
    apply_session_filter(mem_.ticks, session_);
    if (skip_last_tick_per_day_ && mem_.ticks.size() > 1) {

        const size_t before = mem_.ticks.size();
        xt_bt_admission::drop_last_tick_per_symbol_day(mem_.ticks);
        const size_t removed = before - mem_.ticks.size();
        XtLogger::info_m("MD", "skip_last_tick_per_day: removed {} ticks", removed);
    }
    mem_.cursor = 0;
    return !mem_.ticks.empty();
}

bool XtMarketDataSource::load_tick_bin_into_memory() {
    auto load_one = [&](const std::string& bin) {
        std::ifstream in(bin, std::ios::binary);
        if (!in.is_open()) {
            XtLogger::warn_m("MD", "cannot open tick bin file, skipped: {}", bin);
            return;
        }
        in.seekg(0, std::ios::end);
        const auto file_bytes = static_cast<size_t>(in.tellg());
        in.seekg(0, std::ios::beg);
        XtTickBinFormat fmt = XtTickBinFormat::DualV2;
        if (!xt_tick_bin::detect_stream_format(in, fmt)) return;

        constexpr size_t est_per_record = 256;
        mem_.ticks.reserve(mem_.ticks.size() + file_bytes / est_per_record + 1);
        XtRawTick tmp;
        while (xt_tick_bin::read_next_tick(in, fmt, tmp, nullptr))
            mem_.ticks.push_back(tmp);
    };

    mem_.ticks.clear();
    if (tick_.bin_paths.empty()) return false;
    for (const auto& bin : tick_.bin_paths) load_one(bin);
    filter_ticks_by_trading_day_range(mem_.ticks, range_.start, range_.end);
    apply_session_filter(mem_.ticks, session_);
    if (skip_last_tick_per_day_ && mem_.ticks.size() > 1) {
        xt_bt_admission::drop_last_tick_per_symbol_day(mem_.ticks);
    }
    mem_.cursor = 0;
    return !mem_.ticks.empty();
}

bool XtMarketDataSource::build_event_stream_cache() {

    events_.clear();
    event_cursor_ = 0;
    return build_replay_event_stream(mem_.ticks,
                                     orderflow_.order_queue_files,
                                     orderflow_.order_detail_files,
                                     orderflow_.transaction_files,
                                     events_);
}
