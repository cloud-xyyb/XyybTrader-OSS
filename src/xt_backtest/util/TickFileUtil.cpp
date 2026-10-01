#include "TickFileUtil.h"

#include "xt_core/market/XtTickBinCodec.h"
#include "XtLogger.h"
#include "XtParseUtil.h"
#include "xt_util/path/PathUtil.h"
#include "TickCsvUtil.h"
#include "xt_core/sync/XtTransparentStringHash.h"  // FNV-1a 常量和流式 update。
#include "xt_util/time/XtTime.h"                   // yyyymmdd_add_days / diff_days（guard band）

#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>

// 文件名日期只作为范围过滤条件；解析失败时调用方仍可保留该文件参与扫描。
bool extract_date_from_filename(const std::string& name, uint32_t& out_date) {
    // 优先匹配 8 位连续数字（20220527.csv）。
    for (size_t i = 0; i + 7 < name.size(); ++i) {
        bool ok = true;
        for (size_t j = 0; j < 8; ++j) {
            if (!std::isdigit(static_cast<unsigned char>(name[i + j]))) { ok = false; break; }
        }
        if (!ok) continue;
        return XtParseUtil::parse_u32(std::string_view(name.data() + i, 8), out_date);
    }
    // 其次匹配 YYYY-MM-DD（2022-05-27.csv）。
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
    // 参数指纹：排序后的 source 路径集 + qty_base bit-pattern + vol_mode。
    uint64_t phash = xt_core::sync::kFnv1a64OffsetBasis;
    auto mix = [&](const void* d, size_t n) {
        phash = xt_core::sync::fnv1a_update(phash, d, n);
    };
    std::vector<std::string> sorted = sources;
    std::sort(sorted.begin(), sorted.end());
    for (const auto& p : sorted) {
        // 长度前缀消 concat 歧义（{"a","bc"} 与 {"ab","c"} 否则同指纹）。
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

/// @brief 名字是否为 `n` 位纯数字（年目录 4 位、月目录 2 位）。
bool is_all_digits(const std::string& s, size_t n) {
    if (s.size() != n) return false;
    for (char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    }
    return true;
}

/// @brief `year*12 + month` 的月序号；用于月目录区间迭代。
int month_index_of(uint32_t yyyymmdd) {
    return static_cast<int>(yyyymmdd / 10000) * 12 + static_cast<int>((yyyymmdd / 100) % 100);
}

/// @brief 月序号 → `root/YYYY/MM` 目录路径。
std::string month_dir_path(const std::string& root, int month_index) {
    int yy = month_index / 12;
    int mm = month_index % 12;
    if (mm == 0) { mm = 12; yy -= 1; }
    char buf[8];
    std::snprintf(buf, sizeof(buf), "%02d", mm);
    return PathUtil::join(PathUtil::join(root, std::to_string(yy)), buf);
}

}  // namespace

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
            // 一个文件名都解析不出日期：间隔无从测量，只能按保底值放宽。语料换成不带
            // 日期的命名后本条会持续成立，故进程内只吼一次。
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
    // +1：间隔 g 意味着上一个 session 的文件正落在 g 天之前，放宽必须够到它本身。
    return (std::max)(kTickNightCarryGuardDays, static_cast<uint32_t>(max_gap + 1));
}

uint32_t derive_night_carry_guard_days_for_paths(const std::vector<std::string>& paths) {
    // 每个文件都占一个位置（解析不出的记 0）：入参条数即语料文件数，`derive_*` 据此
    // 区分「语料为空」与「语料全是无日期命名」。
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

    // 只认 `YYYY/MM/*.csv`：有窗扫描也只看这两层，多认一层就会把窗口扫描永远看不到的
    // 文件算进 family 身份，两条路径的输入集从此对不上。
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

    // 起点向前放宽一段 guard（widen-only）：窗口首日的夜盘段落在上一个 session 的文件里，
    // 而上一个 session 与窗口首日之间可能隔着周末或长假——只放宽一天时，周一开窗会把周五
    // 文件承载的周一夜盘段整段漏掉。多读的窗外行由读端行级过滤（in_day_range）精确裁掉。
    //
    // guard 长度由语料自己说：先按保底值圈出月目录区间扫一遍，用**扫到的**文件名日集合
    // 推导 guard（零额外 I/O——这批目录本来就要枚举）；推出的起点若落进更早的月份，补扫
    // 那几个月再推一次。语料按月落盘时月数有限，循环必然收敛。
    const int end_month = month_index_of(end_date);
    // start_date == 0 = 只设上界，起点不设界（月序号 0，同 band_start 为 0 的老口径）。
    int first_month =
        (start_date != 0)
            ? month_index_of(xt_time::yyyymmdd_add_days(
                  start_date, -static_cast<int>(kTickNightCarryGuardDays)))
            : 0;

    // 先按月收好（路径 + 文件名日），过滤推迟到 guard 定下来之后。
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
        if (start_date == 0) break;  // 只设上界的窗不需要 band
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
    // 全部被剔除（如窗口配置与语料错位）时回退原列表，保守不致空推断。
    if (kept.empty()) return files;
    return kept;
}

// tick 二进制 cache manifest。

uint64_t compute_source_signature(const std::vector<std::string>& sources,
                                  double qty_base,
                                  XtTickVolumeMode vol_mode,
                                  uint32_t start_day,
                                  uint32_t end_day) {
    if (sources.empty()) return 0;
    // 对排序后的 (path, mtime, size) tuple 做流式 FNV-1a，保证输入顺序无关；
    // 再混入影响 bin 内容的解析参数（qty_base bit-pattern + vol_mode 枚举值）。
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
        // file_time_type 的 epoch 由实现决定；manifest 只要求同一环境内可复算。
        const auto mt_count = mt.time_since_epoch().count();
        mix(path.data(), path.size());
        mix(&sz, sizeof(sz));
        mix(&mt_count, sizeof(mt_count));
    }
    mix(&qty_base, sizeof(qty_base));
    const int32_t vm = static_cast<int32_t>(vol_mode);
    mix(&vm, sizeof(vm));
    // codec 修订号折进签名：解析器语义变更（bump）时旧 cache 签名必 mismatch →
    // 自动重建，消灸「源不变旧 bin 静默命中」窗口（旧格式 manifest 一次性失效）。
    const uint16_t codec_ver = xt_tick_bin::kTickBinCodecVersion;
    mix(&codec_ver, sizeof(codec_ver));
    // builder 修订号：建库语义变更（非 codec 字节格式变更）时 bump。
    // rev 2 = 建库按交易日窗过滤（cache 内容与文件名 range 一致），旧全量 cache 失效。
    const uint16_t builder_rev = 2;
    mix(&builder_rev, sizeof(builder_rev));
    mix(&start_day, sizeof(start_day));
    mix(&end_day, sizeof(end_day));
    return hash;
}

bool write_cache_manifest(const std::string& bin_path, uint64_t signature) {
    if (bin_path.empty() || signature == 0) return false;
    const std::string manifest_path = bin_path + ".manifest";
    // key=value 文本格式避免给 backtest cache 路径引入额外依赖。
    // version 仅作 forensic 标记；verify 只比 signature（算法升级靠 signature mismatch 自然失效）。
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
