#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "xt_backtest/data/reader/XtRawShardTickReader.h"
#include "xt_backtest/data/reader/XtTickCollector.h"
#include "XtLogger.h"
#include "xt_core/market/XtTickBinCodec.h"
#include "xt_util/path/PathUtil.h"
#include "xt_backtest/data/event_stream/ReplayEventBuilder.h"
#include "xt_backtest/data/source/detail/XtTickAdmission.h"
#include "TickFileUtil.h"
#include "xt_util/time/XtTime.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

constexpr size_t kBinStreamBufBytes = 1u << 20;

std::vector<std::string> prefilter_paths_by_trading_day_range(
    const std::vector<std::string>& paths, uint32_t start_day, uint32_t end_day) {
    if (start_day == 0 && end_day == 0) return paths;
    const uint32_t band_start =
        (start_day != 0)
            ? xt_time::yyyymmdd_add_days(
                  start_day, -static_cast<int64_t>(derive_night_carry_guard_days_for_paths(paths)))
            : 0;
    std::vector<std::string> kept;
    kept.reserve(paths.size());
    for (const auto& p : paths) {
        uint32_t fdate = 0;
        const std::string name = std::filesystem::path(p).filename().string();
        if (extract_date_from_filename(name, fdate)) {
            if (band_start != 0 && fdate < band_start) continue;
            if (end_day != 0 && fdate > end_day) continue;
        }
        kept.push_back(p);
    }
    return kept;
}

size_t filter_ticks_by_trading_day_range(std::vector<XtRawTick>& ticks,
                                         uint32_t start_day,
                                         uint32_t end_day) {
    if (ticks.empty()) return 0;
    if (start_day == 0 && end_day == 0) return 0;

    const size_t before = ticks.size();
    size_t dst = 0;
    for (size_t i = 0; i < ticks.size(); ++i) {
        if (!xt_bt_admission::in_day_range(ticks[i], start_day, end_day)) {
            continue;
        }
        if (dst != i) ticks[dst] = ticks[i];
        ++dst;
    }
    ticks.resize(dst);
    return before - dst;
}

size_t apply_session_filter(std::vector<XtRawTick>& ticks,
                            const xt_core::Config& session) {
    if (!xt_core::is_enabled(session) || ticks.empty()) return 0;

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
    return dropped;
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

    const std::vector<std::string> scoped =
        prefilter_paths_by_trading_day_range(paths, range_.start, range_.end);

    const auto keep = [this](const XtRawTick& t) {
        return xt_bt_admission::in_day_range(t, range_.start, range_.end);
    };
    std::vector<XtTickWithKey> all;
    CollectStats cs;
    if (!collect_ticks_from_paths(scoped, all, tick_.symbol_set, tick_.qty_base,
                                  parse_tick_volume_mode(tick_.volume_mode), keep, &cs)) {
        XtLogger::error_m("MD", "no ticks collected from CSV files");
        return false;
    }

    drop_stats_.rows_loaded      = cs.lines_read;
    drop_stats_.symbol           = cs.symbol;
    drop_stats_.bad_row          = cs.bad_row;
    drop_stats_.window           = cs.window;
    drop_stats_.loaded_trackable = true;
    mem_.ticks.clear();
    mem_.ticks.reserve(all.size());
    for (const auto& t : all) mem_.ticks.push_back(t.tick);
    std::vector<XtTickWithKey>().swap(all);

    drop_stats_.window += static_cast<int64_t>(
        filter_ticks_by_trading_day_range(mem_.ticks, range_.start, range_.end));
    drop_stats_.session += static_cast<int64_t>(apply_session_filter(mem_.ticks, session_));
    if (skip_last_tick_per_day_ && mem_.ticks.size() > 1) {

        const size_t before = mem_.ticks.size();
        xt_bt_admission::drop_last_tick_per_symbol_day(mem_.ticks);
        const size_t removed = before - mem_.ticks.size();
        drop_stats_.skip_last += static_cast<int64_t>(removed);
        XtLogger::info_m("MD", "skip_last_tick_per_day: removed {} ticks", removed);
    }
    mem_.cursor = 0;
    return !mem_.ticks.empty();
}

bool XtMarketDataSource::load_tick_bin_into_memory() {

    const bool windowed = (range_.start != 0 || range_.end != 0);
    int64_t records_read = 0;
    int64_t window_drops = 0;
    auto load_one = [&](const std::string& bin) {

        std::vector<char> stream_buf(kBinStreamBufBytes);
        std::ifstream in;
        in.rdbuf()->pubsetbuf(stream_buf.data(),
                              static_cast<std::streamsize>(stream_buf.size()));
        in.open(bin, std::ios::binary);
        if (!in.is_open()) {
            XtLogger::warn_m("MD", "cannot open tick bin file, skipped: {}", bin);
            return;
        }
        in.seekg(0, std::ios::end);
        const auto file_bytes = static_cast<size_t>(in.tellg());
        in.seekg(0, std::ios::beg);
        XtTickBinFormat fmt = XtTickBinFormat::DualV2;
        if (!xt_tick_bin::detect_stream_format(in, fmt)) return;

        constexpr size_t kRecordBytes   = sizeof(XtRawTick) + sizeof(XtTick);
        constexpr size_t kProgressBytes = 256ull * 1024 * 1024;

        const auto header_pos = static_cast<std::streamoff>(in.tellg());
        const size_t header_bytes = header_pos > 0 ? static_cast<size_t>(header_pos) : 0;

        const size_t est_records =
            file_bytes > header_bytes ? (file_bytes - header_bytes) / kRecordBytes : 0;
        mem_.ticks.reserve(mem_.ticks.size() + est_records);
        const size_t total_mb   = file_bytes / (1024 * 1024);
        size_t read_bytes       = 0;
        size_t next_progress    = kProgressBytes;
        XtRawTick tmp;
        while (xt_tick_bin::read_next_tick(in, fmt, tmp, nullptr)) {
            ++records_read;
            read_bytes += kRecordBytes;
            if (read_bytes >= next_progress) {
                std::fprintf(stdout, "[tick-cache] load progress: %zu MB / %zu MB\n",
                             read_bytes / (1024 * 1024), total_mb);
                std::fflush(stdout);
                next_progress = read_bytes + kProgressBytes;
            }
            if (windowed && !xt_bt_admission::in_day_range(tmp, range_.start, range_.end)) {
                ++window_drops;
                continue;
            }
            mem_.ticks.push_back(tmp);
        }
    };

    mem_.ticks.clear();
    if (tick_.bin_paths.empty()) return false;
    for (const auto& bin : tick_.bin_paths) load_one(bin);

    drop_stats_.rows_loaded      = records_read;
    drop_stats_.window           = window_drops;
    drop_stats_.loaded_trackable = true;

    drop_stats_.window += static_cast<int64_t>(
        filter_ticks_by_trading_day_range(mem_.ticks, range_.start, range_.end));
    drop_stats_.session += static_cast<int64_t>(apply_session_filter(mem_.ticks, session_));
    if (skip_last_tick_per_day_ && mem_.ticks.size() > 1) {
        const size_t before = mem_.ticks.size();
        xt_bt_admission::drop_last_tick_per_symbol_day(mem_.ticks);
        drop_stats_.skip_last += static_cast<int64_t>(before - mem_.ticks.size());
    }
    mem_.cursor = 0;
    return !mem_.ticks.empty();
}

bool XtMarketDataSource::load_tick_shards_into_memory() {
    mem_.ticks.clear();
    if (tick_.shard_paths.empty()) return false;

    const bool windowed = (range_.start != 0 || range_.end != 0);
    int64_t records_read = 0;
    int64_t window_drops = 0;

    XtRawShardTickReader reader(tick_.shard_paths);
    if (!reader.init()) return false;
    XtRawTick tmp{};
    while (reader.next_tick(tmp)) {
        ++records_read;
        if (windowed && !xt_bt_admission::in_day_range(tmp, range_.start, range_.end)) {
            ++window_drops;
            continue;
        }
        mem_.ticks.push_back(tmp);
    }
    if (reader.has_io_error()) {

        XtLogger::error_m("MD", "tick shard unusable ({}): {}",
                          xt_raw_shard::to_string(reader.failure_status()),
                          reader.failed_shard());
        mem_.ticks.clear();
        return false;
    }

    drop_stats_.rows_loaded      = records_read;
    drop_stats_.window           = window_drops;
    drop_stats_.loaded_trackable = true;
    drop_stats_.window += static_cast<int64_t>(
        filter_ticks_by_trading_day_range(mem_.ticks, range_.start, range_.end));
    drop_stats_.session += static_cast<int64_t>(apply_session_filter(mem_.ticks, session_));
    if (skip_last_tick_per_day_ && mem_.ticks.size() > 1) {
        const size_t before = mem_.ticks.size();
        xt_bt_admission::drop_last_tick_per_symbol_day(mem_.ticks);
        drop_stats_.skip_last += static_cast<int64_t>(before - mem_.ticks.size());
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
