#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "xt_backtest/data/reader/XtTickCollector.h"
#include "xt_backtest/data/reader/XtBinTickReader.h"
#include "xt_backtest/data/reader/XtCsvTickReader.h"
#include "xt_backtest/data/reader/XtEventParquetReader.h"
#include "xt_backtest/data/reader/XtFbsTickReader.h"
#include "XtLogger.h"
#include "xt_core/market/XtTickBinCodec.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_util/path/PathUtil.h"
#include "xt_backtest/data/source/detail/XtTickAdmission.h"
#include "TickFileUtil.h"

#include <string>

void XtMarketDataSource::apply_common_config(const XtMarketDataSourceConfig& d) {
    use_stream_ = (d.data_mode != "memory");
    if (d.stream_override_set) use_stream_ = d.stream_override;
    skip_last_tick_per_day_ = d.skip_last_tick_per_day;
    session_                = d.session;

    if      (d.replay_mode == "event_stream") mode_ = XtReplayMode::EventStream;
    else if (d.mode == "tick")                mode_ = XtReplayMode::Tick;
    else                                      mode_ = XtReplayMode::Bar;

    tick_.qty_base    = d.qty_base;
    tick_.volume_mode = d.tick_volume_mode;
    tick_.root        = d.tick_root;
    tick_.files       = d.tick_files;
    if (!d.tick_main_symbol.empty()) {

        tick_.symbols    = { d.tick_main_symbol };
        tick_.symbol_set = { d.tick_main_symbol };
    } else {
        tick_.symbols    = d.tick_symbols;
        tick_.symbol_set = std::unordered_set<std::string>(tick_.symbols.begin(), tick_.symbols.end());
    }
    tick_.cache_dir      = d.tick_cache_dir;
    tick_.use_bin_cache  = d.tick_use_bin_cache;
    tick_.fbs_root       = d.tick_fbs_root;
    tick_.events_parquet_root = d.tick_events_parquet_root;

    tick_.source = TickSource::None;
    tick_.bin_paths.clear();

    bar_driver_.apply_config(d);

    orderflow_.order_queue_files  = d.order_queue_files;
    orderflow_.order_detail_files = d.order_detail_files;
    orderflow_.transaction_files  = d.transaction_files;
    events_.clear();
    event_cursor_ = 0;

    range_.start = d.start_date;
    range_.end   = d.end_date;
}

bool XtMarketDataSource::collect_existing_tick_files(bool fail_on_missing) {
    std::vector<std::string> existing;
    existing.reserve(tick_.files.size());
    size_t missing = 0;
    for (const auto& f : tick_.files) {
        if (PathUtil::file_exists(f)) {
            existing.push_back(f);
        } else {
            ++missing;
            XtLogger::warn_m("MD", "tick file missing: {}", f);
        }
    }
    if (existing.empty()) {
        XtLogger::error_m("MD", "tick mode has no readable files");
        return false;
    }
    if (missing > 0 && fail_on_missing) {
        XtLogger::error_m("MD", "explicit tick file list has {} missing file(s)", missing);
        return false;
    }
    if (missing > 0)
        XtLogger::warn_m("MD", "tick files missing: {} (using remaining {})", missing, existing.size());
    tick_.files.swap(existing);
    return true;
}

bool XtMarketDataSource::prepare_tick_mode(const XtMarketDataSourceConfig& d) {
    if (d.has_bar_path || !d.bar_path.empty() || d.has_bar_section) {
        XtLogger::error_m("MD", "tick mode forbids bar datasource fields");
        return false;
    }

    if (!tick_.fbs_root.empty()) {
        tick_.source = TickSource::Fbs;

        use_stream_ = true;
        return true;
    }

    if (!tick_.events_parquet_root.empty()) {
        tick_.source = TickSource::EventsParquet;
        use_stream_ = true;
        return true;
    }

    const bool explicit_tick_files = !tick_.files.empty();
    if (tick_.files.empty() && !tick_.root.empty()) {
        append_tick_files_from_root(tick_.root, range_.start, range_.end, tick_.files);
    }
    if (tick_.files.empty()) {
        XtLogger::error_m("MD", "tick mode requires datasource.tick.files or datasource.tick.root");
        return false;
    }
    if (!collect_existing_tick_files(explicit_tick_files)) return false;

    if (tick_.symbols.empty() && d.tick_auto_main) {
        std::string main_sym;
        if (find_first_symbol_in_files(tick_.files, main_sym, tick_.qty_base,
                                       parse_tick_volume_mode(tick_.volume_mode))) {
            tick_.symbols    = { main_sym };
            tick_.symbol_set = { main_sym };
            XtLogger::info_m("MD", "auto main symbol: {}", main_sym);
        }
    }

    if (tick_.use_bin_cache && ensure_tick_bin_cache(tick_.files)) {
        tick_.source = TickSource::BinStream;
        return true;
    }
    tick_.source = TickSource::Csv;
    return true;
}

void XtMarketDataSource::init_stream_reader() {

    if (tick_.source == TickSource::Fbs) {
        XtFbsTickReader::Options opt;
        opt.fbs_root          = tick_.fbs_root;
        opt.start_trading_day = range_.start;
        opt.end_trading_day   = range_.end;
        opt.symbols.assign(tick_.symbols.begin(), tick_.symbols.end());
        tick_.reader = std::make_unique<XtFbsTickReader>(std::move(opt));
        if (!tick_.reader->init()) {
            XtLogger::error_m("MD", "failed to init fbs tick stream reader (root={})",
                              tick_.fbs_root);
            tick_.reader.reset();
        }
        return;
    }

    if (tick_.source == TickSource::EventsParquet) {
        XtEventParquetReader::Options opt;
        opt.events_root = tick_.events_parquet_root;
        opt.symbols.assign(tick_.symbols.begin(), tick_.symbols.end());
        opt.start_trading_day = range_.start;
        opt.end_trading_day = range_.end;
        opt.volume_mode = parse_tick_volume_mode(tick_.volume_mode);
        tick_.reader = std::make_unique<XtEventParquetReader>(std::move(opt));
        if (!tick_.reader->init()) {
            XtLogger::error_m("MD", "failed to init events parquet tick stream reader (root={})",
                              tick_.events_parquet_root);
            tick_.reader.reset();
        }
        return;
    }

    auto open_bin_reader = [&](std::vector<std::string> paths,
                               const std::string& fallback,
                               const char* label) {
        if (paths.empty() && !fallback.empty()) paths.push_back(fallback);
        tick_.reader = std::make_unique<XtBinTickReader>(std::move(paths));
        if (!tick_.reader->init()) {
            XtLogger::error_m("MD", "failed to init {} stream reader", label);
            tick_.reader.reset();
        }
    };

    if (tick_.source == TickSource::BinStream) {
        open_bin_reader(tick_.bin_paths, "", "tick bin");
        return;
    }
    std::vector<std::string> simtick_paths;
    std::string simtick_fallback;
    if (bar_driver_.has_simtick_bin_stream(simtick_paths, simtick_fallback)) {
        open_bin_reader(std::move(simtick_paths), simtick_fallback, "simtick bin");
        tick_.reader_is_bar_simtick = (tick_.reader != nullptr);
        return;
    }
    if (tick_.source == TickSource::Csv) {
        XtCsvTickReader::Options opts;
        opts.paths         = tick_.files;
        opts.symbol_filter = tick_.symbol_set;
        opts.qty_base      = tick_.qty_base;
        opts.volume_mode   = parse_tick_volume_mode(tick_.volume_mode);
        tick_.reader = std::make_unique<XtCsvTickReader>(std::move(opts));
        if (!tick_.reader->init()) {
            XtLogger::error_m("MD", "failed to init tick csv stream reader");
            tick_.reader.reset();
        }
    }
}

bool XtMarketDataSource::build_tick_bin_from_csvs(const std::vector<std::string>& csv_paths,
                                                   const std::string& bin_path) {
    std::vector<XtTickWithKey> all;
    if (!collect_ticks_from_paths(csv_paths, all, tick_.symbol_set, tick_.qty_base,
                                  parse_tick_volume_mode(tick_.volume_mode))) {
        XtLogger::error_m("MD", "failed to collect ticks for bin {}", bin_path);
        return false;
    }

    if (range_.start != 0 || range_.end != 0) {
        const size_t before = all.size();
        size_t dst = 0;
        for (size_t i = 0; i < all.size(); ++i) {
            if (!xt_bt_admission::in_day_range(all[i].tick, range_.start, range_.end)) continue;
            if (dst != i) all[dst] = std::move(all[i]);
            ++dst;
        }
        all.resize(dst);
        if (all.empty()) {
            XtLogger::error_m("MD",
                "no ticks in trading-day range [{}, {}] for bin {} (sources have {} ticks)",
                range_.start, range_.end, bin_path, before);
            return false;
        }
    }

    PathUtil::AtomicOfstream out(bin_path);
    if (!out.is_open()) {
        XtLogger::error_m("MD", "failed to open tick bin tmp for write: {}", bin_path);
        return false;
    }
    if (!xt_tick_bin::write_v2_header(out.stream())) {
        XtLogger::error_m("MD", "failed to write tick v2 header: {}", bin_path);
        return false;
    }
    for (const auto& tw : all) {

        const XtTick core = xt_tick_convert::to_core(tw.tick);
        if (!xt_tick_bin::write_tick(out.stream(), XtTickBinFormat::DualV2, tw.tick, core)) {
            XtLogger::error_m("MD", "failed to write tick record: {}", bin_path);
            return false;
        }
    }
    if (!out.commit()) {
        XtLogger::error_m("MD", "failed to commit tick bin: {}", bin_path);
        return false;
    }
    return true;
}

bool XtMarketDataSource::ensure_tick_bin_cache(const std::vector<std::string>& sources) {
    if (sources.empty()) return false;

    if (!tick_.bin_paths.empty()) return true;

    const std::string bin = build_tick_cache_path(tick_.cache_dir, range_.start, range_.end, tick_.symbols);
    if (bin.empty()) return false;

    const double qty_base = tick_.qty_base;
    const XtTickVolumeMode vol_mode = parse_tick_volume_mode(tick_.volume_mode);
    const bool cache_exists = PathUtil::file_exists(bin);

    XtTickBinFormat detected_fmt{};
    const bool cache_valid =
        cache_exists
        && verify_cache_manifest(bin, sources, qty_base, vol_mode, range_.start, range_.end)
        && xt_tick_bin::detect_file_format(bin, detected_fmt);
    if (!cache_valid) {
        if (cache_exists) {
            XtLogger::info_m("MD",
                "tick bin cache stale (manifest mismatch or missing), rebuilding: {}",
                bin);
        }
        if (!build_tick_bin_from_csvs(sources, bin)) return false;

        const uint64_t sig =
            compute_source_signature(sources, qty_base, vol_mode, range_.start, range_.end);
        if (sig != 0 && !write_cache_manifest(bin, sig)) {
            XtLogger::warn_m("MD",
                "tick bin cache built but manifest write failed: {}.manifest", bin);
        }
    }
    tick_.bin_paths.push_back(bin);
    return true;
}
