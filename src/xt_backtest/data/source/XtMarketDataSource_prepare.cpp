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

// ---------------------------------------------------------------------------
// 通用配置分发
// ---------------------------------------------------------------------------

void XtMarketDataSource::apply_common_config(const XtMarketDataSourceConfig& d) {
    // 新一轮 load() 重新解析配置：上一轮的物化结果按新配置作废，reset() 幂等闸复位，
    // 保证本轮 load() 尾部的 reset() 必定重建（否则会拿旧 config 的数据继续回放）。
    at_start_ = false;
    use_stream_ = (d.data_mode != "memory");
    if (d.stream_override_set) use_stream_ = d.stream_override;
    skip_last_tick_per_day_ = d.skip_last_tick_per_day;
    session_                = d.session;

    // replay_mode 优先级高于 mode；event_stream 必须由 validation/load 保证基于 tick。
    if      (d.replay_mode == "event_stream") mode_ = XtReplayMode::EventStream;
    else if (d.mode == "tick")                mode_ = XtReplayMode::Tick;
    else                                      mode_ = XtReplayMode::Bar;

    // tick 数据源状态由 prepare 阶段写入，reset/reader 阶段只读。
    tick_.qty_base    = d.qty_base;
    tick_.volume_mode = d.tick_volume_mode;
    tick_.root        = d.tick_root;
    tick_.files       = d.tick_files;
    if (!d.tick_main_symbol.empty()) {
        // tick_main_symbol 覆盖 tick_symbols：用于单品种回测只关注主力合约。
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
    // 分支状态每次 load() 重新归零，避免上一轮 tick/bar 模式残留。
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

// ---------------------------------------------------------------------------
// Tick 模式准备
// ---------------------------------------------------------------------------

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

    // fbs event store 分支依赖 validate 保证与 CSV/bin input 互斥且 date range 有效。
    if (!tick_.fbs_root.empty()) {
        tick_.source = TickSource::Fbs;
        // fbs reader 仅支持 stream 模式；即使配置 memory，也保持按 trading_day lazy open。
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

    // 文件列表推导优先级：显式 tick.files > tick.root 目录扫描。
    const bool explicit_tick_files = !tick_.files.empty();
    if (tick_.files.empty() && !tick_.root.empty()) {
        append_tick_files_from_root(tick_.root, range_.start, range_.end, tick_.files);
    }
    if (tick_.files.empty()) {
        XtLogger::error_m("MD", "tick mode requires datasource.tick.files or datasource.tick.root");
        return false;
    }
    if (!collect_existing_tick_files(explicit_tick_files)) return false;

    // 自动识别主力合约：扫描文件中首个 symbol，用于多合约文件的单品种筛选。
    // 推断只看严格窗口内文件：root-scan guard band 内的文件可能字典序领跑，
    // 多合约语料下会把窗口之前那些文件的 symbol 误推成主力。
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

    // cache 分支优先级：RawShardV3 分片库（opt-in）> DualV2 单文件 > CSV 直读。
    // v3 不可用（建库失败/脏日期行/缺片）时逐级回落，三条路交付同一条 tick 序列。
    // event_stream 读端只消费 memory 物化的 tick 缓存，其物化分支不含分片库，
    // 故该模式不选 v3（沿用既有 bin/CSV 路径）。
    if (tick_.use_bin_cache && tick_.cache_format == "v3" &&
        mode_ != XtReplayMode::EventStream) {
        // 分片库的 family 身份必须与窗口无关，喂给它的是**源全集**而不是 tick_.files
        // （后者按窗口裁过）：否则「1.21-2.17」与「1.21-2.18」是两个 family，差一天也
        // 得从头全量建库——正是分片库要消除的浪费。
        //
        // 三种取法各自与窗口无关：显式 tick.files 本就不随窗口变；root 无窗时
        // tick_.files 已是整根递归扫描；root 有窗时按同一套 `YYYY/MM` 布局全月扫。
        std::vector<std::string> all_sources;
        bool allow_undated = true;
        if (explicit_tick_files || tick_.root.empty()) {
            all_sources = tick_.files;
        } else if (range_.start == 0 && range_.end == 0) {
            all_sources = tick_.files;
        } else {
            append_all_tick_files_from_root(tick_.root, all_sources);
            // 有窗 root 扫描会把无日期文件按**月目录跨度**纳入，取用随窗口变；分片给不出
            // 一份与窗口无关的内容，只能整批让位给 v2。
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

// ---------------------------------------------------------------------------
// 流式 reader 初始化
// ---------------------------------------------------------------------------

void XtMarketDataSource::init_stream_reader() {
    // reader 优先级：fbs event store > events parquet > tick bin > simtick bin > tick CSV。

    // fbs event store reader。
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

// ---------------------------------------------------------------------------
// Tick bin 缓存构建
// ---------------------------------------------------------------------------

bool XtMarketDataSource::build_tick_bin_from_csvs(const std::vector<std::string>& csv_paths,
                                                   const std::string& bin_path) {
    // 建库同样把交易日窗过滤下沉到收集循环（同 load_csvs_to_memory）：窗外 tick 不
    // 进内存，首建峰值驻留从「全源数据集」收敛到「窗内规模」。谓词逐 tick 独立判定、
    // 施加于排序之前，不改变留存 tick 的相对顺序，故与「整批排序后再过滤」逐字节等价。
    const bool windowed = (range_.start != 0 || range_.end != 0);
    size_t source_ticks = 0;  // 谓词见到的源 tick 数 == 旧实现里过滤前的 all.size()
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
        // 谓词把窗内清空时 collect 直接返回 false；沿用旧实现的窗口专属诊断，
        // 让「源里有数据但窗内为空」与「源里根本没数据」保持可区分。
        if (windowed && source_ticks > 0) {
            XtLogger::error_m("MD",
                "no ticks in trading-day range [{}, {}] for bin {} (sources have {} ticks)",
                range_.start, range_.end, bin_path, source_ticks);
        } else {
            XtLogger::error_m("MD", "failed to collect ticks for bin {}", bin_path);
        }
        return false;
    }
    // 建库按交易日窗过滤：cache 文件名携带 range，内容必须与之一致——否则
    // 子窗口 cache（如 walk-forward fold）会以全量数据落盘（体积虚胖 + 语义错标）。
    // 收集期谓词已完成过滤，此处是权威过滤点兜底（预筛后空转）。
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
    // tick bin 缓存必须通过 AtomicOfstream 落盘：
    //   - 直接 ofstream(bin_path, trunc) crash 时留 0 字节 / 半截 bin
    //     -> file_exists() 返回 true 但 reader 解析失败，触发 "tick cache 损坏" 静默禁用路径
    //   - dtor 自动 abort (未 commit) 保证只有完整写完才能 rename 到 target
    PathUtil::AtomicOfstream out(bin_path);
    if (!out.is_open()) {
        XtLogger::error_m("MD", "failed to open tick bin tmp for write: {}", bin_path);
        return false;
    }
    if (!xt_tick_bin::write_v2_header(out.stream())) {
        XtLogger::error_m("MD", "failed to write tick v2 header: {}", bin_path);
        return false;  // dtor abort: tmp 文件 unlink, target 保持原状
    }
    // 数 GB 级 bin 写盘可持续数分钟，期间既有打点全部沉默（上层只能看到 merge
    // progress 停在最后一行），心跳让调用方能区分「在写」与「卡死」。按记录数推算
    // 字节数（DualV2 定长 record，见 XtTickBinCodec），不额外查询流位置。
    constexpr size_t kRecordBytes    = sizeof(XtRawTick) + sizeof(XtTick);
    constexpr size_t kProgressBytes  = 256ull * 1024 * 1024;
    size_t written_bytes   = 0;
    size_t written_records = 0;
    size_t next_progress   = kProgressBytes;
    for (const auto& tw : all) {
        // DualV2 同时落盘 XtRawTick (double) 和 XtTick (fixed-point)，
        // 使 bin reader 可直接向不同调用层提供对应精度而无需重新转换。
        const XtTick core = xt_tick_convert::to_core(tw.tick);
        if (!xt_tick_bin::write_tick(out.stream(), XtTickBinFormat::DualV2, tw.tick, core)) {
            XtLogger::error_m("MD", "failed to write tick record: {}", bin_path);
            return false;  // dtor abort: tmp 文件 unlink, target 保持原状
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
    // 同一 load() 周期内已构建则直接复用；apply_common_config 会在下一轮清空 bin_paths。
    if (!tick_.bin_paths.empty()) return true;

    const double qty_base = tick_.qty_base;
    const XtTickVolumeMode vol_mode = parse_tick_volume_mode(tick_.volume_mode);
    // 当前缓存协议把所有 CSV 合并为单一 bin 文件；文件名含参数指纹，
    // 不同 qty_base/volume_mode/source 集的并行建库各写各的原子对。
    const std::string bin = build_tick_cache_path(
        tick_.cache_dir, range_.start, range_.end, tick_.symbols,
        qty_base, vol_mode, sources);
    if (bin.empty()) return false;

    // Cache invalidation 协议：
    //   - file_exists(bin) && verify_cache_manifest(bin, sources, qty_base, vol_mode)
    //     -> manifest signature (mtime+size+解析参数) 一致才用，否则重建
    //     （qty_base/volume_mode 变更同样失效，避免读到旧口径解析的 bin）
    //   - 新建后 write_cache_manifest 落盘 sidecar (.manifest)
    //   - 失败路径 (write 失败 / 旧 cache 无 manifest / 旧版 signature 算法) -> 强制重建
    const bool cache_exists = PathUtil::file_exists(bin);
    // detect 兑底：manifest 对但 bin header 版本/损坏不符（外部替换、旧版残留）时
    // 同样重建，否则 reader 侧 detect fail 会静默跳文件造成 0-tick 空跑。
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
        // stdout 进度行与 XT_PROGRESS 同流进 run.log（viewer live tail 可见；stderr 会被
        // run_engine.sh 引到 time.log），独立于 log_cfg；管道下全缓冲需逐行 fflush。
        std::fprintf(stdout, "[tick-cache] build start: %zu source files -> %s\n",
                     sources.size(), bin.c_str());
        std::fflush(stdout);
        if (!build_tick_bin_from_csvs(sources, bin)) return false;
        std::fprintf(stdout, "[tick-cache] build done: %s\n", bin.c_str());
        std::fflush(stdout);
        // sidecar manifest 失败仅 warn；下轮可重建，不阻塞本轮 backtest。
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

// ---------------------------------------------------------------------------
// RawShardV3 分片库准备（cache_format: v3）
// ---------------------------------------------------------------------------

namespace {

/// @brief 建库语义修订号；分片切日/排序口径变更（非字节格式变更）时 bump。
///
/// 混进 family 与单日指纹：bump 后旧目录自然失效，读者不会拿旧口径的分片当新的用。
/// rev 5：冷启候选上界的夜盘 guard 由固定 15 自然日改为按语料推导（见
/// `derive_night_carry_guard_days`）；超长休市语料下候选集变宽，那些天的分片内容随之改变。
constexpr uint32_t kShardBuilderRev = 5;

/// @brief 只读 header 判分片是否可用（不算 payload CRC）。
///
/// 建库刚跑完就整批复算 CRC 会让「命中即开跑」退化成再读一遍全量数据；payload 校验
/// 因此下沉到 reader 的按日载入（失败即 sticky I/O 错误 → run 失败，不静默截断）。
/// 这里只拦住启动期就能看出的坏片：缺文件、magic/版本不符、header 自身损坏。
/// @param out_records 非空时写入 header 声明的记录数。
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

/// @brief 源列表里是否存在重复路径。
bool has_duplicate_paths(const std::vector<std::string>& paths) {
    std::unordered_set<std::string> seen;
    seen.reserve(paths.size());
    for (const auto& p : paths) {
        if (!seen.insert(p).second) return true;
    }
    return false;
}

/// @brief 校验 CURRENT generation 的 forensic 产物是否可解析。
///
/// 清单（manifest.json）只做留痕：取片按池内指纹直接命中，不读它，generation 半途崩掉
/// 时留下的孤儿分片正是靠这条被下一轮重新采纳（**刻意的崩溃恢复语义**）。闭包索引
/// （closure_index.json）另当别论——它带着各源文件的交易日清单，读不到只是让本轮重扫
/// 一遍源、重新入册，产出不变、指纹不变，孤儿分片照样被采纳。
/// @return 畸形产物个数；无 CURRENT（从未提交过 generation）返回 0。
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

/// @brief 读 CURRENT generation 的闭包索引（各源文件的交易日清单来源）。
/// @return 空索引表示无 CURRENT 或解析失败——此时本轮全部源当作未入册，重扫后重新入册。
XtTickClosureIndex read_current_closure_index(const std::string& family_dir) {
    XtTickClosureIndex closure;
    std::string generation;
    if (!read_current(family_dir, generation)) return closure;
    if (!read_closure_index(closure_index_path(family_dir + "/" + generation), closure)) {
        return XtTickClosureIndex{};
    }
    return closure;
}

/// @brief 进程级 warn-once：同一类回落在一次运行里只吼一嗓子，后续只进计数。
///
/// 分片库的回落是 per-load 的，长跑（walk-forward 多 fold）下同一原因会命中几十次；
/// 逐次打印会把真正的首次诊断淹掉。
bool claim_first_warning(std::atomic<bool>& flag) {
    return !flag.exchange(true, std::memory_order_relaxed);
}

#ifdef XT_BT_TEST_SEAMS
/// 计划落定注入点的当前钩子（见 detail/XtShardPlanHook.h）。
xt_bt_shard_detail::PlanSettledHook& plan_settled_hook() {
    static xt_bt_shard_detail::PlanSettledHook hook;
    return hook;
}
#endif

}  // namespace

#ifdef XT_BT_TEST_SEAMS
namespace xt_bt_shard_detail {

void set_plan_settled_hook(PlanSettledHook hook) { plan_settled_hook() = std::move(hook); }

void invoke_plan_settled_hook() {
    const auto& hook = plan_settled_hook();
    if (hook) hook();
}

}  // namespace xt_bt_shard_detail
#endif

bool XtMarketDataSource::ensure_tick_shard_cache(const std::vector<std::string>& all_sources,
                                                 bool allow_undated_sources) {
    if (all_sources.empty()) return false;
    // 同一 load() 周期内已选好分片则复用；apply_common_config 会在下一轮清空。
    if (!tick_.shard_paths.empty()) return true;
    if (tick_.cache_dir.empty()) return false;

    // 重复源路径：默认 cache 路径会把同一个文件读两遍（同一条 tick 交付两次），而闭包
    // 索引与建库读集都按路径去重、只读一遍。两条路的交付条数因此不同，分片给不出默认
    // 路径的口径——整批让位，不在这里悄悄「修正」用户写重的配置。
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

    // family 身份 = 有序源全集 + 解析参数 + 格式/建库修订号。刻意不含交易日窗：
    // 分片内容只由这些输入决定，窗口是读端的挑片条件，同一批分片可服务多个窗口。
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

    // 坏产物留痕：取片按池内指纹直接命中，清单读不读都不影响；闭包索引读不到只是让本轮
    // 重扫一遍源重新入册（产出与指纹不变）。故解析失败**不改变交付的任何一个字节**，只记一笔。
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

    // 闭包索引：源文件 → 它实际含有的交易日（清单），倒过来就是「建某一天要读哪些源」。
    // 清单由建库读该文件时记下、由 stat 签名背书，跨 generation 沿用（见
    // `XtTickClosureIndex`）；上一代的那份在这里取回来。
    XtTickClosureIndex prior_closure = read_current_closure_index(family_dir);

    // 源签名一次采齐：每一天的指纹都要用，逐日 stat 会把启动期变成 syscall 风暴。
    // 取锁后必须连索引一起重建——见 refresh_index 的第二个调用点。
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
        // 无日期又无清单的源：它里面藏着哪些交易日既圈不出也查不到，交易日全集因此
        // 失去依据，只能整读一遍让数据自己说话。入册之后清单顶上，不再需要全量。
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

    // 单日指纹 = 该日闭包（有序）+ 各源 mtime/size + 解析参数。失效粒度因此是「日」：
    // 改一个源文件只让它参与的那几天换指纹，其余天原样复用。闭包必须是入册后的精确集
    // ——按候选（含尚未入册的源）算出来的指纹会随「这轮碰巧读了谁」漂移。
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
        uint64_t shard_fp = 0;             ///< 只在 `resolved` 时有意义
        std::vector<std::string> read_set; ///< 建这天要读的有序源（精确闭包 ∪ 待入册候选）
        bool resolved = false;             ///< 闭包已精确（没有待入册的候选源）
        bool in_pool = false;              ///< 池里已有同指纹分片
    };

    std::vector<uint32_t> universe;
    std::vector<DayPlan> plan;
    std::vector<uint32_t> missing;

    // 窗内必需日 → 逐日查池。命中与否只看文件是否存在且头部自洽，不读 payload。
    // 还有待入册候选的天算不出稳定指纹（闭包未定），一律当缺片：候选要么真含这天的行
    // （那就本该重建），要么读完入册后指纹与旧片一致（下一轮直接复用）。
    // 逐日查询走视图：窗内每天各查一次，线性扫会让启动期退化成 O(days × 源数)。
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
                p.shard_fp = fingerprint_of(day, p.read_set);  // 无候选时 read_set 即精确闭包
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
        // 有日期源却圈不出窗内任何交易日：这个窗本就没有数据，让位给默认 cache 路径。
        return false;
    }

    // 收尾：把计划里的池内路径交给 reader（按交易日升序）。
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

    // publish 失败 = 计划落定之后分片又没了（并发 GC、外部删除、写盘半途）。此时本轮
    // 走的是回落，不是命中：hit 标志必须撤回，否则统计面把一次回落记成一次 cache 命中。
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

    // 建库串行化：同一 family 同时只允许一个建库者；抢不到锁的进程直接回落 CSV，
    // 而不是排队等一个可能跑几十分钟的建库。
    XtTickCacheFamilyLock lock;
    if (!lock.try_lock(family_dir)) {
        XtLogger::warn_m("MD",
            "tick shard family is being built elsewhere, falling back to CSV: {}", family_dir);
        return false;
    }
    // 锁外采的签名只够用来判「要不要抢锁」。真正决定分片名字的那份必须在锁内重采：
    // 锁外到取锁之间源文件可以被改（长跑里增量落盘的语料就是这么变的），拿旧签名算
    // 指纹会产出「名字说的是旧内容、里面装的是新内容」的分片——池是按名字复用的，
    // 这份错配会一直被后续所有窗口引用。取锁期间对手也可能刚建完并提交了新一代清单，
    // 故连闭包索引一并重取、重排计划。
    prior_closure = read_current_closure_index(family_dir);
    refresh_index();
    universe = closure_candidate_days(index);
    make_plan();
    if (missing.empty() && !full_rebuild) {
        tick_.shard_cache_hit = true;
        return publish_or_fall_back();
    }
    const bool did_full_rebuild = full_rebuild;

    // 读哪些源：全量重建读全集；增量只读缺失日的读取集并集，且按源全集的顺序读——
    // 同 key 记录的先后由读取顺序决定，乱了顺序平局次序就与整批读不一致。
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

    // 序号消同毫秒重名：增量建库常常快到毫秒时钟分不开前后两次，而 generation 目录
    // 重名即拒建（不可复用），撞上就等于整条 v3 路径无声退回 v2。
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
        // 半份 generation 从未被 CURRENT 指向，删掉即彻底消失；已进池的分片按签名寻址，
        // 留着也只会被同指纹的下一次建库复用，不会被错认成别的内容。
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

    // 入册：读过的源从此带着内容派生的交易日清单，闭包不再有「按文件名猜」的成分。
    // 清单随本代索引落盘，下一轮 carry_over_registrations 直接沿用。
    for (size_t i = 0; i < read_set.size() && i < report.source_days.size(); ++i) {
        register_source_days(index, read_set[i], report.source_days[i]);
    }

    if (did_full_rebuild) {
        // 全量重建读了全集，交易日全集到此才真正已知（数据自己说的），据此重排计划。
        universe = closure_days(index);
        make_plan();
    } else {
        // 入册后闭包精确 → 重算窗内每一天的指纹（建之前尚有候选的那些天此刻才定名）。
        // 复用日的指纹不得因入册而改变：变了就意味着某个源在闭包判定之外贡献了那天的
        // 行，池里那份同名分片名不副实，只能整批放弃。
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

    // 分片进池：文件名带该日指纹，同名即视作同内容（stat 寻址，见 XtTickCacheStoreV3.h），
    // 可被任意 generation 引用而无需复制。
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

    // 清单只记引用（day → 池内分片指纹），不复制数据。本轮没建的天分两种：池里已有
    // 的直接引用；仍缺片的说明其全部源都读过却一条数据都没有，补一个空分片——
    // 「这天没数据」不落成实体，下次装载又会把它当缺片，把同一批源再读一遍。
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
    // 闭包索引随清单落盘：事后要复盘「这一代认为某天该由哪些源建」，只能靠它。
    if (!write_closure_index(closure_index_path(gen_dir), index)) {
        return abandon("closure index could not be written");
    }
    if (!commit_current(family_dir, new_gen)) {
        return abandon("generation could not be committed");
    }
    return publish_or_fall_back();
}
