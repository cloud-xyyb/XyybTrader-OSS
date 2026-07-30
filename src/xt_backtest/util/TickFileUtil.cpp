#include "TickFileUtil.h"

#include "xt_core/market/XtTickBinCodec.h"
#include "XtLogger.h"
#include "XtParseUtil.h"
#include "xt_util/path/PathUtil.h"
#include "TickCsvUtil.h"
#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_util/time/XtTime.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>

bool extract_date_from_filename(const std::string& name, uint32_t& out_date) {

    for (size_t i = 0; i + 7 < name.size(); ++i) {
        bool ok = true;
        for (size_t j = 0; j < 8; ++j) {
            if (!std::isdigit(static_cast<unsigned char>(name[i + j]))) { ok = false; break; }
        }
        if (!ok) continue;
        return XtParseUtil::parse_u32(std::string_view(name.data() + i, 8), out_date);
    }

    for (size_t i = 0; i + 9 < name.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(name[i]))) continue;
        bool match = true;
        for (size_t j : {0,1,2,3}) {
            if (!std::isdigit(static_cast<unsigned char>(name[i+j]))) { match = false; break; }
        }
        if (!match || name[i+4] != '-') continue;
        if (!std::isdigit(static_cast<unsigned char>(name[i+5])) ||
            !std::isdigit(static_cast<unsigned char>(name[i+6]))) continue;
        if (name[i+7] != '-') continue;
        if (!std::isdigit(static_cast<unsigned char>(name[i+8])) ||
            !std::isdigit(static_cast<unsigned char>(name[i+9]))) continue;
        char buf[9];
        buf[0] = name[i]; buf[1] = name[i+1]; buf[2] = name[i+2]; buf[3] = name[i+3];
        buf[4] = name[i+5]; buf[5] = name[i+6];
        buf[6] = name[i+8]; buf[7] = name[i+9];
        buf[8] = '\0';
        return XtParseUtil::parse_u32(std::string_view(buf, 8), out_date);
    }
    return false;
}

std::string sanitize_filename_part(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (std::isalnum(static_cast<unsigned char>(c))) {
            out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
    }
    if (out.empty()) out = "all";
    return out;
}

std::string build_tick_cache_path(const std::string& cache_dir,
                                  uint32_t start_date,
                                  uint32_t end_date,
                                  const std::vector<std::string>& tick_symbols,
                                  double qty_base,
                                  XtTickVolumeMode vol_mode,
                                  const std::vector<std::string>& sources) {
    if (cache_dir.empty()) return "";
    std::string dir = cache_dir;
    if (dir.back() != '/' && dir.back() != '\\') dir += "/";
    std::string sym = "all";
    if (!tick_symbols.empty()) {
        std::string sec;
        for (const auto& s : tick_symbols) {
            if (!sec.empty()) sec += "_";
            sec += sanitize_filename_part(s);
        }
        if (!sec.empty()) sym = sec;
    }

    uint64_t phash = xt_core::sync::kFnv1a64OffsetBasis;
    auto mix = [&](const void* d, size_t n) {
        phash = xt_core::sync::fnv1a_update(phash, d, n);
    };
    std::vector<std::string> sorted = sources;
    std::sort(sorted.begin(), sorted.end());
    for (const auto& p : sorted) {

        const uint32_t len = static_cast<uint32_t>(p.size());
        mix(&len, sizeof(len));
        mix(p.data(), p.size());
    }
    mix(&qty_base, sizeof(qty_base));
    const int32_t vm = static_cast<int32_t>(vol_mode);
    mix(&vm, sizeof(vm));
    char ph[12];
    std::snprintf(ph, sizeof(ph), "%08x",
                  static_cast<uint32_t>(phash ^ (phash >> 32)));
    std::string key = std::to_string(start_date) + "_" + std::to_string(end_date) +
                      "_" + sym + "_p" + ph;
    return dir + "ticks_" + key + ".bin";
}

namespace {

bool is_all_digits(const std::string& s, size_t n) {
    if (s.size() != n) return false;
    for (char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    }
    return true;
}

int month_index_of(uint32_t yyyymmdd) {
    return static_cast<int>(yyyymmdd / 10000) * 12 + static_cast<int>((yyyymmdd / 100) % 100);
}

std::string month_dir_path(const std::string& root, int month_index) {
    int yy = month_index / 12;
    int mm = month_index % 12;
    if (mm == 0) { mm = 12; yy -= 1; }
    char buf[8];
    std::snprintf(buf, sizeof(buf), "%02d", mm);
    return PathUtil::join(PathUtil::join(root, std::to_string(yy)), buf);
}

}

uint32_t derive_night_carry_guard_days(std::vector<uint32_t> filename_days) {
    const size_t input_files = filename_days.size();
    filename_days.erase(std::remove_if(filename_days.begin(), filename_days.end(),
                                       [](uint32_t d) { return !xt_time::is_valid_yyyymmdd(d); }),
                        filename_days.end());
    std::sort(filename_days.begin(), filename_days.end());
    filename_days.erase(std::unique(filename_days.begin(), filename_days.end()),
                        filename_days.end());
    if (filename_days.size() < 2) {
        if (input_files != 0 && filename_days.empty()) {

            static std::atomic<bool> warned{false};
            if (!warned.exchange(true, std::memory_order_relaxed)) {
                XtLogger::warn_m("MD",
                    "tick corpus has no parsable filename dates ({} file(s)); night-carry "
                    "guard falls back to {} calendar day(s); later occurrences are silent",
                    input_files, kTickNightCarryGuardDays);
            }
        }
        return kTickNightCarryGuardDays;
    }
    int64_t max_gap = 0;
    for (size_t i = 1; i < filename_days.size(); ++i) {
        max_gap = (std::max)(max_gap,
                             xt_time::yyyymmdd_diff_days(filename_days[i - 1], filename_days[i]));
    }

    return (std::max)(kTickNightCarryGuardDays, static_cast<uint32_t>(max_gap + 1));
}

uint32_t derive_night_carry_guard_days_for_paths(const std::vector<std::string>& paths) {

    std::vector<uint32_t> days;
    days.reserve(paths.size());
    for (const auto& p : paths) {
        uint32_t d = 0;
        if (!extract_date_from_filename(std::filesystem::path(p).filename().string(), d)) d = 0;
        days.push_back(d);
    }
    return derive_night_carry_guard_days(std::move(days));
}

void append_all_tick_files_from_root(const std::string& root, std::vector<std::string>& out) {
    if (root.empty()) return;
    std::error_code ec;
    if (!std::filesystem::exists(root, ec)) return;

    for (const auto& year : std::filesystem::directory_iterator(root, ec)) {
        if (ec) break;
        if (!year.is_directory()) continue;
        if (!is_all_digits(year.path().filename().string(), 4)) continue;
        std::error_code month_ec;
        for (const auto& month : std::filesystem::directory_iterator(year.path(), month_ec)) {
            if (month_ec) break;
            if (!month.is_directory()) continue;
            if (!is_all_digits(month.path().filename().string(), 2)) continue;
            std::error_code file_ec;
            for (const auto& entry : std::filesystem::directory_iterator(month.path(), file_ec)) {
                if (file_ec) break;
                if (!entry.is_regular_file()) continue;
                if (entry.path().extension() != ".csv") continue;
                out.push_back(entry.path().string());
            }
        }
    }
    std::sort(out.begin(), out.end());
}

void append_tick_files_from_root(const std::string& root,
                                 uint32_t start_date,
                                 uint32_t end_date,
                                 std::vector<std::string>& out) {
    if (root.empty()) return;
    std::error_code ec;
    if (!std::filesystem::exists(root, ec)) return;

    if (start_date == 0 && end_date == 0) {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(root, ec)) {
            if (ec) break;
            if (!entry.is_regular_file()) continue;
            auto p = entry.path();
            if (p.extension() == ".csv") out.push_back(p.string());
        }
        std::sort(out.begin(), out.end());
        return;
    }

    const int end_month = month_index_of(end_date);

    int first_month =
        (start_date != 0)
            ? month_index_of(xt_time::yyyymmdd_add_days(
                  start_date, -static_cast<int>(kTickNightCarryGuardDays)))
            : 0;

    std::vector<std::pair<std::string, uint32_t>> scanned;
    std::vector<uint32_t> seen_days;
    auto scan_months = [&](int from_month, int to_month) {
        for (int v = from_month; v <= to_month; ++v) {
            const std::string dir = month_dir_path(root, v);
            if (!std::filesystem::exists(dir, ec)) continue;
            for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
                if (ec) break;
                if (!entry.is_regular_file()) continue;
                auto p = entry.path();
                if (p.extension() != ".csv") continue;
                uint32_t fdate = 0;
                if (!extract_date_from_filename(p.filename().string(), fdate)) fdate = 0;
                scanned.emplace_back(p.string(), fdate);
                seen_days.push_back(fdate);
            }
        }
    };

    scan_months(first_month, end_month);
    uint32_t band_start = 0;
    while (true) {
        if (start_date == 0) break;
        band_start = xt_time::yyyymmdd_add_days(
            start_date, -static_cast<int64_t>(derive_night_carry_guard_days(seen_days)));
        const int band_month = month_index_of(band_start);
        if (band_month >= first_month) break;
        scan_months(band_month, first_month - 1);
        first_month = band_month;
    }

    for (const auto& [path, fdate] : scanned) {
        if (fdate != 0) {
            if (band_start != 0 && fdate < band_start) continue;
            if (end_date != 0 && fdate > end_date) continue;
        }
        out.push_back(path);
    }
    std::sort(out.begin(), out.end());
}

bool find_first_symbol_in_files(const std::vector<std::string>& files,
                                std::string& out,
                                double qty_base,
                                XtTickVolumeMode vol_mode) {
    for (const auto& path : files) {
        std::ifstream in(path);
        if (!in.is_open()) continue;
        std::string line;
        XtTickCsvSpec spec;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            if (line[0] == '#') continue;
            if (!spec.inited) {
                XtTickCsvSpec hdr;
                if (build_tick_spec_from_header(line, hdr)) {
                    spec       = hdr;
                    spec.inited = true;
                    continue;
                }
                spec.simple = true;
                spec.inited = true;
            }
            XtRawTick t;
            uint64_t key = 0;
            static const std::unordered_set<std::string> empty;
            if (parse_tick_csv_line(line, spec, t, key, empty, qty_base, vol_mode)) {
                if (t.code[0] != '\0') {
                    out = t.code;
                    return true;
                }
            }
        }
    }
    return false;
}

std::vector<std::string> filter_files_for_auto_main(const std::vector<std::string>& files,
                                                    uint32_t start_date) {
    if (start_date == 0) return files;
    std::vector<std::string> kept;
    kept.reserve(files.size());
    for (const auto& f : files) {
        uint32_t fdate = 0;
        const std::string name = std::filesystem::path(f).filename().string();
        if (extract_date_from_filename(name, fdate) && fdate < start_date) continue;
        kept.push_back(f);
    }

    if (kept.empty()) return files;
    return kept;
}

uint64_t compute_source_signature(const std::vector<std::string>& sources,
                                  double qty_base,
                                  XtTickVolumeMode vol_mode,
                                  uint32_t start_day,
                                  uint32_t end_day) {
    if (sources.empty()) return 0;

    uint64_t hash = xt_core::sync::kFnv1a64OffsetBasis;
    auto mix = [&](const void* data, size_t n) {
        hash = xt_core::sync::fnv1a_update(hash, data, n);
    };
    std::vector<std::string> sorted = sources;
    std::sort(sorted.begin(), sorted.end());
    for (const auto& path : sorted) {
        std::error_code ec;
        const auto sz = std::filesystem::file_size(path, ec);
        if (ec) return 0;
        const auto mt = std::filesystem::last_write_time(path, ec);
        if (ec) return 0;

        const auto mt_count = mt.time_since_epoch().count();
        mix(path.data(), path.size());
        mix(&sz, sizeof(sz));
        mix(&mt_count, sizeof(mt_count));
    }
    mix(&qty_base, sizeof(qty_base));
    const int32_t vm = static_cast<int32_t>(vol_mode);
    mix(&vm, sizeof(vm));

    const uint16_t codec_ver = xt_tick_bin::kTickBinCodecVersion;
    mix(&codec_ver, sizeof(codec_ver));

    const uint16_t builder_rev = 2;
    mix(&builder_rev, sizeof(builder_rev));
    mix(&start_day, sizeof(start_day));
    mix(&end_day, sizeof(end_day));
    return hash;
}

bool write_cache_manifest(const std::string& bin_path, uint64_t signature) {
    if (bin_path.empty() || signature == 0) return false;
    const std::string manifest_path = bin_path + ".manifest";

    std::ostringstream oss;
    oss << "version=2\n"
        << "signature=" << signature << "\n";
    return PathUtil::write_atomic(manifest_path, oss.str(), 0644);
}

bool verify_cache_manifest(const std::string& bin_path,
                           const std::vector<std::string>& sources,
                           double qty_base,
                           XtTickVolumeMode vol_mode,
                           uint32_t start_day,
                           uint32_t end_day) {
    if (bin_path.empty()) return false;
    const std::string manifest_path = bin_path + ".manifest";
    if (!PathUtil::file_exists(manifest_path)) return false;

    std::ifstream in(manifest_path);
    if (!in) return false;
    std::string line;
    uint64_t stored_sig = 0;
    bool found_sig = false;
    while (std::getline(in, line)) {
        const auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        const std::string key = line.substr(0, eq);
        const std::string val = line.substr(eq + 1);
        if (key == "signature") {
            try {
                stored_sig = std::stoull(val);
                found_sig = true;
            } catch (...) {
                return false;
            }
        }
    }
    if (!found_sig) return false;
    const uint64_t cur_sig =
        compute_source_signature(sources, qty_base, vol_mode, start_day, end_day);
    if (cur_sig == 0) return false;
    return cur_sig == stored_sig;
}
