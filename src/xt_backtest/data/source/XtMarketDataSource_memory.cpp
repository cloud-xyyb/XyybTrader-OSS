#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "xt_backtest/data/reader/XtRawShardTickReader.h"
#include "xt_backtest/data/reader/XtTickCollector.h"
#include "XtLogger.h"
#include "xt_core/market/XtTickBinCodec.h"
#include "xt_util/path/PathUtil.h"
#include "xt_backtest/data/event_stream/ReplayEventBuilder.h"
#include "xt_backtest/data/source/detail/XtTickAdmission.h"
#include "TickFileUtil.h"
#include "xt_util/time/XtTime.h"  // yyyymmdd_add_days（起点 guard band）

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

/// @brief tick bin 整文件装载的流缓冲大小（同 XtBinTickReader::kStreamBufBytes）。
constexpr size_t kBinStreamBufBytes = 1u << 20;

// 文件级窗口预筛：文件名能解析出日期且完全落在（带 guard band 的）窗口之外时跳过
// 读取，让窗外文件不进内存。语义照抄 root-scan（append_tick_files_from_root）——
// 用文件名日期与交易日窗直接比较，解析不出日期的文件保守保留、交由行级过滤兜底。
// 命名契约：文件名日期视为文件内容 trading_day，起点另向前放宽一段 guard（widen-only）：
// 窗口首日的夜盘段落在上一个 session 的文件里，而上一个 session 可能隔着周末或长假。
// guard 由这批路径自身的文件名日集合推导（derive_night_carry_guard_days，零额外 I/O），
// 与 root-scan 同一套口径。放宽后该类文件被纳入读取，多读的窗外行由行级过滤
// （in_day_range）精确裁掉。band 之外的命名不在兜底范围。
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

// grouping day / range / session / skip-last 判定统一走 detail/XtTickAdmission.h——
// stream 读端逐条准入与本 TU 的整批 compaction 必须同判定，否则两种
// data_mode 对同一 config 产生不同 tick 序列（TickCsvUtil / ParserCTP 负责
// calendar-day 与 exchange-day TD 的归一化）。
// @return 被丢弃（窗外）的 tick 数。
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

// ---------------------------------------------------------------------------
// Session 过滤 + pre-open 标记
// ---------------------------------------------------------------------------
//
// 规则：
//   - 在任意 section 内 -> keep，kind=Normal
//   - 不在任何 section 内但落在"cross_midnight section 开始前一分钟" -> keep,
//     kind=Preopen（模拟集合竞价阶段真实盘口）
//   - 其他（纯 out-of-session，如收盘后 idle 心跳） -> drop
//
// 注意：本函数修改 ticks[i].kind 原地标记，只对 Preopen 写值；Normal 维持 0（默认）。
// @return 被 session 过滤丢弃的 tick 数。
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

} // namespace

// ---------------------------------------------------------------------------
// 内存模式分发
// ---------------------------------------------------------------------------

void XtMarketDataSource::load_memory_source() {
    mem_.ticks.clear();
    // tick_.source==Csv 与 bar_driver_.has_memory_source() 在同一 load() 周期内互斥。
    if (tick_.source == TickSource::Csv) {
        load_csvs_to_memory(tick_.files);
        return;
    }
    if (bar_driver_.has_memory_source()) {
        if (!bar_driver_.load_to_memory(mem_.ticks))
            XtLogger::error_m("MD", "bar driver loaded no ticks");
        return;
    }
    // 防御性分支：mode 与数据源标志不一致时不尝试猜测 fallback。
    const char* mode_str = (mode_ == XtReplayMode::Bar)  ? "bar"
                         : (mode_ == XtReplayMode::Tick) ? "tick"
                                                         : "event_stream";
    XtLogger::error_m("MD", "no data source configured for memory load (mode={})", mode_str);
}

// ---------------------------------------------------------------------------
// CSV tick 加载
// ---------------------------------------------------------------------------

bool XtMarketDataSource::load_csvs_to_memory(const std::vector<std::string>& paths) {
    // 文件级窗口预筛（显式 files 列表）：窗外文件不读进内存；无日期文件保守保留。
    const std::vector<std::string> scoped =
        prefilter_paths_by_trading_day_range(paths, range_.start, range_.end);
    // 行级窗口过滤下沉到收集循环：只有窗内 tick 进内存，峰值驻留 ≈ 窗内行数而非全
    // 文件集；下方 filter_ticks_by_trading_day_range 仍是权威过滤点（预筛后空转）。
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
    // consumed manifest 归因：CSV 路可精确归因（reader lines_read = symbol + bad_row
    // + 交付；keep 谓词 = window）。session/skip_last 在下方补齐。
    drop_stats_.rows_loaded      = cs.lines_read;
    drop_stats_.symbol           = cs.symbol;
    drop_stats_.bad_row          = cs.bad_row;
    drop_stats_.window           = cs.window;
    drop_stats_.loaded_trackable = true;
    mem_.ticks.clear();
    mem_.ticks.reserve(all.size());
    for (const auto& t : all) mem_.ticks.push_back(t.tick);
    std::vector<XtTickWithKey>().swap(all);  // 及时释放收集缓冲，收窄后续步骤峰值驻留。
    // filter 使用已归一化的 trading_date，和 skip_last_tick_per_day 的 grouping 一致。
    // keep 谓词已在收集时窗过滤，此处权威过滤点预筛后空转（drop 计入 window 兜底）。
    drop_stats_.window += static_cast<int64_t>(
        filter_ticks_by_trading_day_range(mem_.ticks, range_.start, range_.end));
    drop_stats_.session += static_cast<int64_t>(apply_session_filter(mem_.ticks, session_));
    if (skip_last_tick_per_day_ && mem_.ticks.size() > 1) {
        // 对齐 WT per-contract store_ticks：每个合约、每个 trading_day 的最后 1 条 tick 不入库。
        const size_t before = mem_.ticks.size();
        xt_bt_admission::drop_last_tick_per_symbol_day(mem_.ticks);
        const size_t removed = before - mem_.ticks.size();
        drop_stats_.skip_last += static_cast<int64_t>(removed);
        XtLogger::info_m("MD", "skip_last_tick_per_day: removed {} ticks", removed);
    }
    mem_.cursor = 0;
    return !mem_.ticks.empty();
}

// ---------------------------------------------------------------------------
// Tick bin 加载
// ---------------------------------------------------------------------------

bool XtMarketDataSource::load_tick_bin_into_memory() {
    // 行级窗口过滤下沉到读取循环：只有窗内 tick 进内存，峰值驻留 ≈ 窗内行数。
    // 不对 bin_paths 做文件名日期预筛——bin cache 名携带的是「建库交易日窗」起点
    // （ticks_<start>_<end>_...），并非单条 tick 的交易日，按它预筛会误删窗内 tick。
    const bool windowed = (range_.start != 0 || range_.end != 0);
    int64_t records_read = 0;
    int64_t window_drops = 0;
    auto load_one = [&](const std::string& bin) {
        // 三步 open：libstdc++ 的 filebuf setbuf 只在 open 之前生效，构造即 open 的写法
        // 会让 pubsetbuf 静默无效（同 XtBinTickReader::open_next_file）。放大到 1 MiB 后
        // 1664B/record 的逐条读从 ~5 record/syscall 提到 ~630 record/syscall。
        // buf 必须比 stream 先声明：析构逆序，保证 filebuf 先于其外部缓冲释放。
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
        // cache 命中后的整文件装载同样是长静默段（数 GB 的 bin 需数分钟），落心跳区分
        // 「在读」与「卡死」。总大小已知，带分母；字节数按定长 DualV2 record 推算。
        constexpr size_t kRecordBytes   = sizeof(XtRawTick) + sizeof(XtTick);
        constexpr size_t kProgressBytes = 256ull * 1024 * 1024;
        // detect 成功后流停在 header 之后 → tellg() 即 header 字节数（不在此硬编码
        // header 尺寸，codec 版本演进时自动跟随）。
        const auto header_pos = static_cast<std::streamoff>(in.tellg());
        const size_t header_bytes = header_pos > 0 ? static_cast<size_t>(header_pos) : 0;
        // DualV2 是定长 record，(file_bytes - header) / kRecordBytes 就是本文件的记录数，
        // 也是窗口激活时窗内记录数的上界。窗口场景同样按上界预留：bin cache 文件名携带
        // 的是「建库交易日窗」（ticks_<start>_<end>_...），命中的 cache 内容本就基本落在
        // 窗内，上界与实际规模同量级；而 push_back 几何增长既要在重分配瞬间同时持有新旧
        // 缓冲，又会留下容量过冲，峰值比按上界一次预留更高。
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
    // consumed manifest 归因：bin 无内部丢弃 → records_read = 交付前总量；
    // 行级窗口过滤在 load_one 计入 window。session/skip_last 在下方补齐。
    drop_stats_.rows_loaded      = records_read;
    drop_stats_.window           = window_drops;
    drop_stats_.loaded_trackable = true;
    // 权威过滤点保留（行级预筛后空转）：与旧实现同一 grouping 口径。
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

// ---------------------------------------------------------------------------
// RawShardV3 分片装载
// ---------------------------------------------------------------------------

bool XtMarketDataSource::load_tick_shards_into_memory() {
    mem_.ticks.clear();
    if (tick_.shard_paths.empty()) return false;

    // 分片按交易日切，窗口裁剪已在 prepare 期按日完成；此处仍逐行判窗，因为窗口边界
    // 那两天的分片是整片选入的（夜盘行的交易日与文件名日不同，只能行级裁）。
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
        // 分片损坏必须 fail-closed：静默截断会把半段行情当成完整回放出结果。
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

// ---------------------------------------------------------------------------
// 事件流缓存构建
// ---------------------------------------------------------------------------

bool XtMarketDataSource::build_event_stream_cache() {
    // mem_.ticks 须在此之前由 load_memory_source 填充；builder 只负责多源事件归并。
    events_.clear();
    event_cursor_ = 0;
    return build_replay_event_stream(mem_.ticks,
                                     orderflow_.order_queue_files,
                                     orderflow_.order_detail_files,
                                     orderflow_.transaction_files,
                                     events_);
}
