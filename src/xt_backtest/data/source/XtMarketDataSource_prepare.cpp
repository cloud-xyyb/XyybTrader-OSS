#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "xt_backtest/data/reader/XtTickCollector.h"
#include "xt_backtest/data/reader/XtBinTickReader.h"
#include "xt_backtest/data/reader/XtCsvTickReader.h"
#include "xt_backtest/data/reader/XtRawShardTickReader.h"
#include "xt_backtest/data/reader/XtTickShardBuilder.h"
#include "XtTickCacheStoreV3.h"
#include "XtTickClosureIndex.h"
#include "xt_core/market/XtRawShardCodecV3.h"
#include "xt_backtest/data/reader/XtEventParquetReader.h"
#include "xt_backtest/data/reader/XtFbsTickReader.h"
#include "XtLogger.h"
#include "xt_core/market/XtTickBinCodec.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_util/path/PathUtil.h"
#include "xt_backtest/data/source/detail/XtShardPlanHook.h"
#include "xt_backtest/data/source/detail/XtTickAdmission.h"
#include "TickFileUtil.h"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

void XtMarketDataSource::apply_common_config(const XtMarketDataSourceConfig& d) {

    at_start_ = false;
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
    tick_.cache_format   = d.tick_cache_format;
    tick_.fbs_root       = d.tick_fbs_root;
    tick_.events_parquet_root = d.tick_events_parquet_root;

    tick_.source = TickSource::None;
    tick_.bin_cache_hit = false;
    tick_.bin_paths.clear();
    tick_.shard_cache_hit = false;
    tick_.shard_paths.clear();
    tick_.shard_stats = ShardCacheStats{};

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

void XtMarketDataSource::note_stream_forced_(const char* source_tag) {
    if (use_stream_) return;
    if (data_mode_ignored_count_++ == 0) {
        XtLogger::warn_m("MD",
            "data_mode: memory 被忽略（tick 源 {} 仅支持 stream，按 trading_day lazy open）; "
            "后续同类只计数不重复告警", source_tag);
    }
}

bool XtMarketDataSource::prepare_tick_mode(const XtMarketDataSourceConfig& d) {
    if (d.has_bar_path || !d.bar_path.empty() || d.has_bar_section) {
        XtLogger::error_m("MD", "tick mode forbids bar datasource fields");
        return false;
    }

    if (!tick_.fbs_root.empty()) {
        tick_.source = TickSource::Fbs;

        note_stream_forced_("fbs");
        use_stream_ = true;
        return true;
    }

    if (!tick_.events_parquet_root.empty()) {
        tick_.source = TickSource::EventsParquet;
        note_stream_forced_("events_parquet");
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
        const std::vector<std::string> scan =
            filter_files_for_auto_main(tick_.files, range_.start);
        if (find_first_symbol_in_files(scan, main_sym, tick_.qty_base,
                                       parse_tick_volume_mode(tick_.volume_mode))) {
            tick_.symbols    = { main_sym };
            tick_.symbol_set = { main_sym };
            XtLogger::info_m("MD", "auto main symbol: {}", main_sym);
        }
    }

    if (tick_.use_bin_cache && tick_.cache_format == "v3" &&
        mode_ != XtReplayMode::EventStream) {

        std::vector<std::string> all_sources;
        bool allow_undated = true;
        if (explicit_tick_files || tick_.root.empty()) {
            all_sources = tick_.files;
        } else if (range_.start == 0 && range_.end == 0) {
            all_sources = tick_.files;
        } else {
            append_all_tick_files_from_root(tick_.root, all_sources);

            allow_undated = false;
        }
        if (ensure_tick_shard_cache(all_sources, allow_undated)) {
            tick_.source = TickSource::ShardStream;
            return true;
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

    if (tick_.source == TickSource::ShardStream) {
        tick_.reader = std::make_unique<XtRawShardTickReader>(tick_.shard_paths);
        if (!tick_.reader->init()) {
            XtLogger::error_m("MD", "failed to init tick shard stream reader");
            tick_.reader.reset();
        }
        return;
    }
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

    const bool windowed = (range_.start != 0 || range_.end != 0);
    size_t source_ticks = 0;
    std::function<bool(const XtRawTick&)> keep;
    if (windowed) {
        keep = [this, &source_ticks](const XtRawTick& t) {
            ++source_ticks;
            return xt_bt_admission::in_day_range(t, range_.start, range_.end);
        };
    }
    std::vector<XtTickWithKey> all;
    if (!collect_ticks_from_paths(csv_paths, all, tick_.symbol_set, tick_.qty_base,
                                  parse_tick_volume_mode(tick_.volume_mode), keep)) {

        if (windowed && source_ticks > 0) {
            XtLogger::error_m("MD",
                "no ticks in trading-day range [{}, {}] for bin {} (sources have {} ticks)",
                range_.start, range_.end, bin_path, source_ticks);
        } else {
            XtLogger::error_m("MD", "failed to collect ticks for bin {}", bin_path);
        }
        return false;
    }

    if (windowed) {
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

    constexpr size_t kRecordBytes    = sizeof(XtRawTick) + sizeof(XtTick);
    constexpr size_t kProgressBytes  = 256ull * 1024 * 1024;
    size_t written_bytes   = 0;
    size_t written_records = 0;
    size_t next_progress   = kProgressBytes;
    for (const auto& tw : all) {

        const XtTick core = xt_tick_convert::to_core(tw.tick);
        if (!xt_tick_bin::write_tick(out.stream(), XtTickBinFormat::DualV2, tw.tick, core)) {
            XtLogger::error_m("MD", "failed to write tick record: {}", bin_path);
            return false;
        }
        ++written_records;
        written_bytes += kRecordBytes;
        if (written_bytes >= next_progress) {
            std::fprintf(stdout, "[tick-cache] write progress: %zu MB (%zu records)\n",
                         written_bytes / (1024 * 1024), written_records);
            std::fflush(stdout);
            next_progress = written_bytes + kProgressBytes;
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

    const double qty_base = tick_.qty_base;
    const XtTickVolumeMode vol_mode = parse_tick_volume_mode(tick_.volume_mode);

    const std::string bin = build_tick_cache_path(
        tick_.cache_dir, range_.start, range_.end, tick_.symbols,
        qty_base, vol_mode, sources);
    if (bin.empty()) return false;

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

        std::fprintf(stdout, "[tick-cache] build start: %zu source files -> %s\n",
                     sources.size(), bin.c_str());
        std::fflush(stdout);
        if (!build_tick_bin_from_csvs(sources, bin)) return false;
        std::fprintf(stdout, "[tick-cache] build done: %s\n", bin.c_str());
        std::fflush(stdout);

        const uint64_t sig =
            compute_source_signature(sources, qty_base, vol_mode, range_.start, range_.end);
        if (sig != 0 && !write_cache_manifest(bin, sig)) {
            XtLogger::warn_m("MD",
                "tick bin cache built but manifest write failed: {}.manifest", bin);
        }
    } else {
        tick_.bin_cache_hit = true;
        std::fprintf(stdout, "[tick-cache] hit %s\n", bin.c_str());
        std::fflush(stdout);
    }
    tick_.bin_paths.push_back(bin);
    return true;
}

namespace {

constexpr uint32_t kShardBuilderRev = 5;

bool shard_header_ok(const std::string& path, uint32_t expect_day,
                     uint64_t* out_records = nullptr) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return false;
    xt_raw_shard::XtRawShardInfo info{};
    if (!xt_raw_shard::read_header(in, info)) return false;
    if (info.trading_day != expect_day) return false;
    if (out_records != nullptr) *out_records = info.record_count;
    return true;
}

bool has_duplicate_paths(const std::vector<std::string>& paths) {
    std::unordered_set<std::string> seen;
    seen.reserve(paths.size());
    for (const auto& p : paths) {
        if (!seen.insert(p).second) return true;
    }
    return false;
}

uint64_t probe_current_generation_artifacts(const std::string& family_dir) {
    std::string generation;
    if (!read_current(family_dir, generation)) return 0;
    const std::string gen_dir = family_dir + "/" + generation;

    uint64_t malformed = 0;
    XtTickGenerationManifest manifest;
    if (!read_generation_manifest(generation_manifest_path(gen_dir), manifest)) ++malformed;
    XtTickClosureIndex closure;
    if (!read_closure_index(closure_index_path(gen_dir), closure)) ++malformed;
    return malformed;
}

XtTickClosureIndex read_current_closure_index(const std::string& family_dir) {
    XtTickClosureIndex closure;
    std::string generation;
    if (!read_current(family_dir, generation)) return closure;
    if (!read_closure_index(closure_index_path(family_dir + "/" + generation), closure)) {
        return XtTickClosureIndex{};
    }
    return closure;
}

bool claim_first_warning(std::atomic<bool>& flag) {
    return !flag.exchange(true, std::memory_order_relaxed);
}

#ifdef XT_BT_TEST_SEAMS

xt_bt_shard_detail::PlanSettledHook& plan_settled_hook() {
    static xt_bt_shard_detail::PlanSettledHook hook;
    return hook;
}
#endif

}

#ifdef XT_BT_TEST_SEAMS
namespace xt_bt_shard_detail {

void set_plan_settled_hook(PlanSettledHook hook) { plan_settled_hook() = std::move(hook); }

void invoke_plan_settled_hook() {
    const auto& hook = plan_settled_hook();
    if (hook) hook();
}

}
#endif

bool XtMarketDataSource::ensure_tick_shard_cache(const std::vector<std::string>& all_sources,
                                                 bool allow_undated_sources) {
    if (all_sources.empty()) return false;

    if (!tick_.shard_paths.empty()) return true;
    if (tick_.cache_dir.empty()) return false;

    if (has_duplicate_paths(all_sources)) {
        ++tick_.shard_stats.duplicate_sources;
        static std::atomic<bool> warned{false};
        if (claim_first_warning(warned)) {
            XtLogger::warn_m("MD",
                "tick shard disabled: source list has duplicate paths ({} entries), "
                "falling back to default cache path; later occurrences only counted",
                all_sources.size());
        }
        return false;
    }

    const XtTickVolumeMode vol_mode = parse_tick_volume_mode(tick_.volume_mode);

    XtTickCacheFamilyKey key;
    key.ordered_sources = all_sources;
    key.symbols = tick_.symbols;
    key.qty_base = tick_.qty_base;
    key.volume_mode = static_cast<int32_t>(vol_mode);
    key.codec_version = xt_raw_shard::kRawShardCodecVersion;
    key.builder_rev = kShardBuilderRev;
    const uint64_t fingerprint = compute_family_fingerprint(key);
    const std::string family_dir = family_dir_path(tick_.cache_dir, fingerprint);
    if (family_dir.empty() || !ensure_family_dir(family_dir)) {
        XtLogger::warn_m("MD", "tick shard family dir unavailable, falling back to CSV: {}",
                         family_dir);
        return false;
    }

    tick_.shard_stats.manifest_malformed = probe_current_generation_artifacts(family_dir);
    if (tick_.shard_stats.manifest_malformed != 0) {
        static std::atomic<bool> warned{false};
        if (claim_first_warning(warned)) {
            XtLogger::warn_m("MD",
                "tick shard family has {} malformed forensic artifact(s) under CURRENT "
                "(manifest / closure index); shard selection is unaffected: {}",
                tick_.shard_stats.manifest_malformed, family_dir);
        }
    }

    XtTickClosureIndex prior_closure = read_current_closure_index(family_dir);

    std::unordered_map<std::string, XtTickSourceStamp> stamps;
    XtTickClosureIndex index;
    bool full_rebuild = false;
    auto refresh_index = [&]() {
        stamps.clear();
        stamps.reserve(all_sources.size());
        std::vector<XtClosureStamp> ordered_stamps;
        ordered_stamps.reserve(all_sources.size());
        for (const auto& p : all_sources) {
            const XtTickSourceStamp st = stat_tick_source(p);
            ordered_stamps.push_back({st.mtime_ns, st.size});
            stamps.emplace(p, st);
        }
        index = build_closure_index(tick_.root, all_sources, ordered_stamps);
        carry_over_registrations(prior_closure, index);

        full_rebuild = has_unregistered_undated_source(index);
    };
    refresh_index();

    if (!allow_undated_sources) {
        const std::vector<std::string> undated = undated_sources(index);
        if (!undated.empty()) {
            XtLogger::warn_m("MD",
                "tick shard needs dated source filenames under root, falling back: "
                "{} undated file(s)", undated.size());
            return false;
        }
    }

    auto fingerprint_of = [&](uint32_t day, const std::vector<std::string>& closure) {
        XtTickDayShardKey dk;
        dk.trading_day = day;
        dk.ordered_sources.reserve(closure.size());
        for (const auto& f : closure) {
            const auto it = stamps.find(f);
            dk.ordered_sources.push_back(it != stamps.end() ? it->second : stat_tick_source(f));
        }
        dk.symbols = tick_.symbols;
        dk.qty_base = tick_.qty_base;
        dk.volume_mode = static_cast<int32_t>(vol_mode);
        dk.codec_version = xt_raw_shard::kRawShardCodecVersion;
        dk.builder_rev = kShardBuilderRev;
        return compute_day_shard_fingerprint(dk);
    };

    /// 窗内某一天的取用计划。
    struct DayPlan {
        uint32_t day = 0;
        uint64_t shard_fp = 0;
        std::vector<std::string> read_set;
        bool resolved = false;
        bool in_pool = false;
    };

    std::vector<uint32_t> universe;
    std::vector<DayPlan> plan;
    std::vector<uint32_t> missing;

    auto make_plan = [&]() {
        const XtClosureDayLookup lookup(index);
        plan.clear();
        missing.clear();
        for (uint32_t day : universe) {
            if (range_.start != 0 && day < range_.start) continue;
            if (range_.end != 0 && day > range_.end) continue;
            DayPlan p;
            p.day = day;
            p.resolved = lookup.candidates_for_day(day).empty();
            p.read_set = lookup.read_set_for_day(day);
            if (p.resolved) {
                p.shard_fp = fingerprint_of(day, p.read_set);
                p.in_pool = shard_header_ok(pooled_shard_path(family_dir, day, p.shard_fp), day);
            }
            if (!p.in_pool) missing.push_back(day);
            plan.push_back(std::move(p));
        }
        tick_.shard_stats.days_required = plan.size();
        tick_.shard_stats.days_reused = plan.size() - missing.size();
        xt_bt_shard_detail::invoke_plan_settled_hook();
    };
    universe = closure_candidate_days(index);
    make_plan();

    if (plan.empty() && !full_rebuild) {

        return false;
    }

    auto publish = [&]() {
        std::vector<std::string> paths;
        paths.reserve(plan.size());
        for (const auto& p : plan) {
            const std::string path = pooled_shard_path(family_dir, p.day, p.shard_fp);
            if (!shard_header_ok(path, p.day)) return false;
            paths.push_back(path);
        }
        if (paths.empty()) return false;
        tick_.shard_paths.swap(paths);
        return true;
    };

    auto publish_or_fall_back = [&]() {
        if (publish()) return true;
        tick_.shard_cache_hit = false;
        ++tick_.shard_stats.publish_failed;
        static std::atomic<bool> warned{false};
        if (claim_first_warning(warned)) {
            XtLogger::warn_m("MD",
                "tick shard vanished between planning and publish ({} day(s) planned), "
                "falling back to default cache path: {}", plan.size(), family_dir);
        }
        return false;
    };

    if (missing.empty() && !full_rebuild) {
        tick_.shard_cache_hit = true;
        return publish_or_fall_back();
    }

    XtTickCacheFamilyLock lock;
    if (!lock.try_lock(family_dir)) {
        XtLogger::warn_m("MD",
            "tick shard family is being built elsewhere, falling back to CSV: {}", family_dir);
        return false;
    }

    prior_closure = read_current_closure_index(family_dir);
    refresh_index();
    universe = closure_candidate_days(index);
    make_plan();
    if (missing.empty() && !full_rebuild) {
        tick_.shard_cache_hit = true;
        return publish_or_fall_back();
    }
    const bool did_full_rebuild = full_rebuild;

    std::vector<std::string> read_set;
    if (did_full_rebuild) {
        read_set = all_sources;
    } else {
        std::unordered_set<std::string> needed;
        for (const auto& p : plan) {
            if (p.in_pool) continue;
            needed.insert(p.read_set.begin(), p.read_set.end());
        }
        for (const auto& src : all_sources) {
            if (needed.erase(src) != 0) read_set.push_back(src);
        }
    }
    if (read_set.empty()) return false;

    static std::atomic<uint64_t> gen_seq{0};
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    const std::string new_gen = make_generation_name(now_ms, static_cast<int>(::getpid()),
                                                     gen_seq.fetch_add(1));
    std::string gen_dir;
    if (!create_generation_dir(family_dir, new_gen, gen_dir)) {
        XtLogger::warn_m("MD", "cannot create tick shard generation, falling back to CSV: {}",
                         new_gen);
        return false;
    }

    std::fprintf(stdout, "[tick-shard] build start: %zu day(s) from %zu source file(s)%s\n",
                 missing.size(), read_set.size(), did_full_rebuild ? " [full rebuild]" : "");
    std::fflush(stdout);

    XtTickShardBuildOptions bopts;
    bopts.ordered_sources = read_set;
    bopts.symbol_filter = tick_.symbol_set;
    bopts.qty_base = tick_.qty_base;
    bopts.volume_mode = vol_mode;
    if (!did_full_rebuild) {
        bopts.day_filter = missing;
        bopts.known_days = universe;
    }
    const XtTickShardBuildReport report = build_tick_shards(bopts, gen_dir);

    auto abandon = [&](const char* why) {

        std::error_code ec;
        std::filesystem::remove_all(gen_dir, ec);
        XtLogger::warn_m("MD", "tick shard {}, falling back to CSV", why);
        return false;
    };

    if (report.status != XtTickShardBuildStatus::Ok) {
        std::error_code ec;
        std::filesystem::remove_all(gen_dir, ec);
        XtLogger::warn_m("MD",
            "tick shard build failed ({}), falling back to CSV: {} ticks read",
            to_string(report.status), report.ticks_read);
        return false;
    }
    std::fprintf(stdout,
                 "[tick-shard] build done: %llu ticks / %zu days (skipped %llu, peak day %llu, "
                 "spilled %llu MB)\n",
                 static_cast<unsigned long long>(report.ticks_written), report.days.size(),
                 static_cast<unsigned long long>(report.ticks_skipped),
                 static_cast<unsigned long long>(report.peak_day_records),
                 static_cast<unsigned long long>(report.spilled_bytes / (1024 * 1024)));
    std::fflush(stdout);

    for (size_t i = 0; i < read_set.size() && i < report.source_days.size(); ++i) {
        register_source_days(index, read_set[i], report.source_days[i]);
    }

    if (did_full_rebuild) {

        universe = closure_days(index);
        make_plan();
    } else {

        const XtClosureDayLookup lookup(index);
        for (auto& p : plan) {
            const uint64_t fp = fingerprint_of(p.day, lookup.files_for_day(p.day));
            if (p.in_pool && fp != p.shard_fp) {
                return abandon("day closure contradicts a reused shard");
            }
            p.shard_fp = fp;
            p.resolved = true;
        }
    }

    std::unordered_map<uint32_t, uint64_t> built;
    std::vector<XtTickGenerationDay> manifest_days;
    {
        const XtClosureDayLookup lookup(index);
        for (size_t i = 0; i < report.days.size(); ++i) {
            const uint32_t day = report.days[i];
            const uint64_t fp = fingerprint_of(day, lookup.files_for_day(day));
            if (!adopt_shard_into_pool(shard_path_for_day(gen_dir, day), family_dir, day, fp)) {
                return abandon("shard could not be adopted into the pool");
            }
            built.emplace(day, fp);
            manifest_days.push_back({day, fp, report.day_records[i]});
        }
    }
    tick_.shard_stats.days_built = report.days.size();
    tick_.shard_stats.full_rebuild = did_full_rebuild;

    if (plan.empty()) return abandon("generation has no shard inside the replay window");

    for (const auto& p : plan) {
        if (built.count(p.day) != 0) continue;
        uint64_t records = 0;
        const std::string path = pooled_shard_path(family_dir, p.day, p.shard_fp);
        if (!shard_header_ok(path, p.day, &records) &&
            !write_empty_pooled_shard(family_dir, p.day, p.shard_fp)) {
            return abandon("empty-day shard could not be written");
        }
        manifest_days.push_back({p.day, p.shard_fp, records});
    }
    std::sort(manifest_days.begin(), manifest_days.end(),
              [](const XtTickGenerationDay& a, const XtTickGenerationDay& b) {
                  return a.trading_day < b.trading_day;
              });

    XtTickGenerationManifest manifest;
    manifest.family_fingerprint = fingerprint;
    manifest.full_coverage = did_full_rebuild;
    manifest.days = std::move(manifest_days);
    if (!write_generation_manifest(generation_manifest_path(gen_dir), manifest)) {
        return abandon("generation manifest could not be written");
    }

    if (!write_closure_index(closure_index_path(gen_dir), index)) {
        return abandon("closure index could not be written");
    }
    if (!commit_current(family_dir, new_gen)) {
        return abandon("generation could not be committed");
    }
    return publish_or_fall_back();
}
