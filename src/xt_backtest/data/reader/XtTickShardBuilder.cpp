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

constexpr uint64_t kDefaultBufferBudgetBytes = 256ull * 1024 * 1024;

constexpr size_t kStreamBufBytes = 1u << 20;

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

}

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

    std::map<uint32_t, std::vector<XtRawTick>> buckets;
    std::map<uint32_t, uint64_t> spilled;
    uint64_t buffered_bytes = 0;

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

            report.status = XtTickShardBuildStatus::DirtyTradingDay;
            return report;
        }
        if (!known.empty() && known.count(t.trading_date) == 0) {

            report.status = XtTickShardBuildStatus::UnexpectedTradingDay;
            return report;
        }
        if (!wanted.empty() && wanted.count(t.trading_date) == 0) {

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

    report.source_days = reader.source_trading_days();
    report.source_days.resize(opts.ordered_sources.size());

    for (uint32_t day : opts.day_filter) buckets.emplace(day, std::vector<XtRawTick>{});

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
