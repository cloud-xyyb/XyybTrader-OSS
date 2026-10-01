#include "XtTickShardBuilder.h"

#include "xt_backtest/data/reader/XtCsvTickReader.h"
#include "xt_core/market/XtRawShardCodecV3.h"
#include "xt_core/market/XtTickSortKey.h"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

/// 分桶驻留默认预算：832B/tick 下约 32 万条，足够摊薄 spill 的 open/close 开销，
/// 又远低于 D7 的 1.5GiB builder 上界。
constexpr uint64_t kDefaultBufferBudgetBytes = 256ull * 1024 * 1024;

/// spill 与分片写盘共用的流缓冲；默认 ~8KB 下 832B/record 逐条写 syscall 密集。
constexpr size_t kStreamBufBytes = 1u << 20;

/// generation 声明的交易日清单文件名（见 `read_shard_day_list`）。
constexpr const char* kShardListFile = "shards.list";

std::string join(const std::string& dir, const std::string& leaf) {
    if (dir.empty()) return leaf;
    if (dir.back() == '/' || dir.back() == '\\') return dir + leaf;
    return dir + "/" + leaf;
}

std::string spill_path_for_day(const std::string& generation_dir, uint32_t day) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "spill_%08u.tmp", day);
    return join(generation_dir, buf);
}

/// @brief 追加一批记录到当日 spill；spill 内顺序始终等于源读取顺序。
bool append_records(const std::string& path, const std::vector<XtRawTick>& recs) {
    if (recs.empty()) return true;
    std::vector<char> buf(kStreamBufBytes);
    std::ofstream out;
    out.rdbuf()->pubsetbuf(buf.data(), static_cast<std::streamsize>(buf.size()));
    out.open(path, std::ios::binary | std::ios::app);
    if (!out.is_open()) return false;
    out.write(reinterpret_cast<const char*>(recs.data()),
              static_cast<std::streamsize>(recs.size() * sizeof(XtRawTick)));
    out.flush();
    return out.good();
}

/// @brief 读回整份 spill 追加到 `out`。
///
/// 调用方只在该日确有落盘记录时才调；因此「文件读不到 / 长度不是记录尺寸整数倍 /
/// 条数与账本对不上」一律是错，不能当空文件放过——放过就是无声少一段行情。
/// @param expect_n 账本记录的该日落盘条数。
bool read_records(const std::string& path, size_t expect_n, std::vector<XtRawTick>& out) {
    std::error_code ec;
    const auto bytes = std::filesystem::file_size(path, ec);
    if (ec) return false;
    if (bytes % sizeof(XtRawTick) != 0) return false;
    const size_t n = static_cast<size_t>(bytes / sizeof(XtRawTick));
    if (n != expect_n) return false;

    std::vector<char> buf(kStreamBufBytes);
    std::ifstream in;
    in.rdbuf()->pubsetbuf(buf.data(), static_cast<std::streamsize>(buf.size()));
    in.open(path, std::ios::binary);
    if (!in.is_open()) return false;
    const size_t base = out.size();
    out.resize(base + n);
    in.read(reinterpret_cast<char*>(out.data() + base),
            static_cast<std::streamsize>(n * sizeof(XtRawTick)));
    return in.good() || in.gcount() == static_cast<std::streamsize>(n * sizeof(XtRawTick));
}

}  // namespace

const char* to_string(XtTickShardBuildStatus s) {
    switch (s) {
        case XtTickShardBuildStatus::Ok:                   return "ok";
        case XtTickShardBuildStatus::NoSources:            return "no_sources";
        case XtTickShardBuildStatus::ReaderInitFailed:     return "reader_init_failed";
        case XtTickShardBuildStatus::DirtyTradingDay:      return "dirty_trading_day";
        case XtTickShardBuildStatus::NoTicks:              return "no_ticks";
        case XtTickShardBuildStatus::UnexpectedTradingDay: return "unexpected_trading_day";
        case XtTickShardBuildStatus::WriteFailed:          return "write_failed";
    }
    return "unknown";
}

std::string shard_path_for_day(const std::string& out_dir, uint32_t trading_day) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "shard_%08u.bin", trading_day);
    return join(out_dir, buf);
}

bool read_shard_day_list(const std::string& out_dir, std::vector<uint32_t>& out_days) {
    std::ifstream in(join(out_dir, kShardListFile), std::ios::binary);
    if (!in.is_open()) return false;
    std::vector<uint32_t> days;
    std::string line;
    uint32_t prev = 0;
    while (std::getline(in, line)) {
        while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) line.pop_back();
        if (line.empty()) continue;
        if (line.size() != 8) return false;
        uint32_t day = 0;
        for (char c : line) {
            if (c < '0' || c > '9') return false;
            day = day * 10 + static_cast<uint32_t>(c - '0');
        }
        // 严格升序：清单与分片一一对应，乱序/重复说明这份 generation 不是本 builder 产物。
        if (day == 0 || day <= prev) return false;
        prev = day;
        days.push_back(day);
    }
    if (days.empty()) return false;
    out_days.swap(days);
    return true;
}

XtTickShardBuildReport build_tick_shards(const XtTickShardBuildOptions& opts,
                                         const std::string& out_dir) {
    XtTickShardBuildReport report;
    if (opts.ordered_sources.empty() || out_dir.empty()) {
        report.status = XtTickShardBuildStatus::NoSources;
        return report;
    }
    const std::string& generation_dir = out_dir;

    const std::unordered_set<uint32_t> wanted(opts.day_filter.begin(), opts.day_filter.end());
    const std::unordered_set<uint32_t> known(opts.known_days.begin(), opts.known_days.end());

    // reader 一次性接管全部源：多文件 heap merge 的发牌顺序（同 key 按源列表先后）
    // 与 DualV2 建库走的是同一条 reader 路径，分片内的平局次序因此与之逐条一致。
    XtCsvTickReader::Options ropts;
    ropts.paths = opts.ordered_sources;
    ropts.symbol_filter = opts.symbol_filter;
    ropts.qty_base = opts.qty_base;
    ropts.volume_mode = opts.volume_mode;
    XtCsvTickReader reader(std::move(ropts));
    if (!reader.init()) {
        report.status = XtTickShardBuildStatus::ReaderInitFailed;
        return report;
    }

    const uint64_t budget =
        opts.buffer_budget_bytes != 0 ? opts.buffer_budget_bytes : kDefaultBufferBudgetBytes;

    std::map<uint32_t, std::vector<XtRawTick>> buckets;  // 交易日 → 该日尚在内存的记录
    std::map<uint32_t, uint64_t> spilled;                // 交易日 → 已落盘记录数
    uint64_t buffered_bytes = 0;

    // 预算超限时整桶落盘：spill 只追加，读回时天然保持源读取顺序。
    auto spill_largest = [&]() -> bool {
        auto victim = buckets.end();
        for (auto it = buckets.begin(); it != buckets.end(); ++it) {
            if (victim == buckets.end() || it->second.size() > victim->second.size()) victim = it;
        }
        if (victim == buckets.end() || victim->second.empty()) return true;
        const std::string path = spill_path_for_day(generation_dir, victim->first);
        if (!append_records(path, victim->second)) return false;
        const uint64_t bytes = victim->second.size() * sizeof(XtRawTick);
        spilled[victim->first] += victim->second.size();
        report.spilled_bytes += bytes;
        buffered_bytes -= bytes;
        victim->second.clear();
        victim->second.shrink_to_fit();
        return true;
    };

    XtRawTick t{};
    while (reader.next_tick(t)) {
        ++report.ticks_read;
        if (t.trading_date == 0) {
            // 归组日为 0 的行（TradingDay 缺失/脏值）没有可归属的分片，且其 sort key
            // 排在所有真实交易日之前——硬塞进任何一天都会改变回放序列。整批放弃。
            report.status = XtTickShardBuildStatus::DirtyTradingDay;
            return report;
        }
        if (!known.empty() && known.count(t.trading_date) == 0) {
            // 预判之外的交易日 → 这一轮读的源子集不足以复现它（写出去就缺一截）。
            report.status = XtTickShardBuildStatus::UnexpectedTradingDay;
            return report;
        }
        if (!wanted.empty() && wanted.count(t.trading_date) == 0) {
            // 这天的源没读全（只读了缺失日的闭包），产出会缺一截——只能整条丢。
            ++report.ticks_skipped;
            continue;
        }
        auto& bucket = buckets[t.trading_date];
        bucket.push_back(t);
        buffered_bytes += sizeof(XtRawTick);
        if (buffered_bytes > budget && !spill_largest()) {
            report.status = XtTickShardBuildStatus::WriteFailed;
            return report;
        }
    }
    if (reader.has_io_error()) {
        report.status = XtTickShardBuildStatus::WriteFailed;
        return report;
    }
    if (report.ticks_read == 0) {
        report.status = XtTickShardBuildStatus::NoTicks;
        return report;
    }
    // 源已读到 EOF：每个文件含哪些交易日到此才是完整答案，可以交给调用方入册。
    report.source_days = reader.source_trading_days();
    report.source_days.resize(opts.ordered_sources.size());

    // day_filter 点名却一条数据都没有的天：补一个空桶，收尾照样落空分片。缺这一步，
    // 「这天确实没数据」就没有实体记录，下次装载会把它当缺片、把同一批源再读一遍。
    for (uint32_t day : opts.day_filter) buckets.emplace(day, std::vector<XtRawTick>{});

    // 收尾：逐日读回 → 按 sort key 稳定排序 → 写分片。sort key 的高位是交易日
    // （见 make_tick_sort_key），故「按日升序拼接分片」与「整批全局排序」同序；
    // 日内用 stable_sort 保持读取顺序，平局次序同样与整批排序一致。
    for (auto& kv : buckets) {
        const uint32_t day = kv.first;
        std::vector<XtRawTick> recs;
        const auto it = spilled.find(day);
        const size_t spill_n = (it != spilled.end()) ? static_cast<size_t>(it->second) : 0;
        recs.reserve(spill_n + kv.second.size());
        const std::string spill = spill_path_for_day(generation_dir, day);
        if (spill_n != 0 && !read_records(spill, spill_n, recs)) {
            report.status = XtTickShardBuildStatus::WriteFailed;
            return report;
        }
        recs.insert(recs.end(), kv.second.begin(), kv.second.end());
        kv.second.clear();
        kv.second.shrink_to_fit();
        {
            std::error_code ec;
            std::filesystem::remove(spill, ec);
        }

        report.peak_day_records = std::max<uint64_t>(report.peak_day_records, recs.size());
        std::stable_sort(recs.begin(), recs.end(),
                         [](const XtRawTick& a, const XtRawTick& b) {
                             return make_tick_sort_key(a.trading_date, a.action_time) <
                                    make_tick_sort_key(b.trading_date, b.action_time);
                         });

        // generation 目录在 commit_current 之前对读者不可见，半截分片不会被读到；
        // 失败时整个 generation 作废（调用方删目录或留给 GC 计划）。
        std::vector<char> buf(kStreamBufBytes);
        std::ofstream out;
        out.rdbuf()->pubsetbuf(buf.data(), static_cast<std::streamsize>(buf.size()));
        out.open(shard_path_for_day(generation_dir, day), std::ios::binary | std::ios::trunc);
        if (!out.is_open() || !xt_raw_shard::write_shard(out, day, recs)) {
            report.status = XtTickShardBuildStatus::WriteFailed;
            return report;
        }
        out.flush();
        if (!out.good()) {
            report.status = XtTickShardBuildStatus::WriteFailed;
            return report;
        }
        report.ticks_written += recs.size();
        report.days.push_back(day);
        report.day_records.push_back(recs.size());
    }

    // 交易日清单最后写：调用方据此把分片逐日搬进池（见 read_shard_day_list）。
    {
        std::ofstream list(join(generation_dir, kShardListFile),
                           std::ios::binary | std::ios::trunc);
        if (!list.is_open()) {
            report.status = XtTickShardBuildStatus::WriteFailed;
            return report;
        }
        for (uint32_t day : report.days) {
            char buf[16];
            std::snprintf(buf, sizeof(buf), "%08u\n", day);
            list << buf;
        }
        list.flush();
        if (!list.good()) {
            report.status = XtTickShardBuildStatus::WriteFailed;
            return report;
        }
    }

    report.status = XtTickShardBuildStatus::Ok;
    return report;
}
