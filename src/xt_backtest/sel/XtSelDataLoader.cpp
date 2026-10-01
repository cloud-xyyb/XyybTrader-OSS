#include "XtSelDataLoader.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <sstream>

#include "xt_logger/XtLogger.h"

namespace xt_bt_sel {

namespace {

bool is_digit_str(const std::string& s) {
    if (s.empty()) return false;
    for (char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    }
    return true;
}

std::string trim(const std::string& s) {
    size_t l = 0;
    while (l < s.size() && std::isspace(static_cast<unsigned char>(s[l]))) ++l;
    size_t r = s.size();
    while (r > l && std::isspace(static_cast<unsigned char>(s[r - 1]))) --r;
    return s.substr(l, r - l);
}

std::vector<uint32_t> read_trading_dates_csv(const std::string& path,
                                             size_t* out_bad_rows = nullptr) {
    std::vector<uint32_t> out;
    std::ifstream ifs(path);
    if (!ifs) return out;

    std::string line;
    bool header_skipped = false;  ///< 首个非数字行视为表头（不计坏行）。
    while (std::getline(ifs, line)) {
        line = trim(line);
        if (line.empty()) continue;
        // 兼容 csv 多列：取首字段；若非数字（header）则跳过
        const auto comma = line.find(',');
        const std::string first = (comma == std::string::npos) ? line : line.substr(0, comma);
        const std::string head = trim(first);
        if (!is_digit_str(head)) {
            // 首个非数字行 = 表头（跳过不计）；其后的非数字行 = 坏行。
            if (!header_skipped) header_skipped = true;
            else if (out_bad_rows) ++*out_bad_rows;
            continue;
        }
        try {
            out.push_back(static_cast<uint32_t>(std::stoul(head)));
        } catch (...) {
            if (out_bad_rows) ++*out_bad_rows;  // 数字形态但溢出 → 坏行。
            continue;
        }
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

std::vector<uint32_t> filter_range(const std::vector<uint32_t>& src,
                                     uint32_t from, uint32_t to) {
    if (from == 0 && to == 0) return src;
    std::vector<uint32_t> out;
    out.reserve(src.size());
    for (auto d : src) {
        if (from != 0 && d < from) continue;
        if (to != 0 && d > to)     continue;
        out.push_back(d);
    }
    return out;
}

}  // namespace

XtCsvCalendarHolidayProvider::XtCsvCalendarHolidayProvider(std::vector<uint32_t> trading_dates)
    : trading_dates_(std::move(trading_dates)) {
    std::sort(trading_dates_.begin(), trading_dates_.end());
}

bool XtCsvCalendarHolidayProvider::is_holiday(const std::string& /*trdtpl*/,
                                                uint32_t date,
                                                bool /*ifWeekend*/) const {
    return !std::binary_search(trading_dates_.begin(), trading_dates_.end(), date);
}

uint32_t XtCsvCalendarHolidayProvider::next_trading_date(const std::string& /*trdtpl*/,
                                                          uint32_t curDate) const {
    auto it = std::upper_bound(trading_dates_.begin(), trading_dates_.end(), curDate);
    if (it == trading_dates_.end()) return 0;
    return *it;
}

XtSelDataLoader::LoadResult XtSelDataLoader::load(const std::string& csv_path,
                                                    const std::string& datalake_root,
                                                    uint32_t date_from,
                                                    uint32_t date_to) {
    LoadResult result;

    // 1) trading_dates.csv 优先（fallback，cra ingest 上线前可走通）
    if (!csv_path.empty()) {
        size_t bad_rows = 0;
        const auto dates = read_trading_dates_csv(csv_path, &bad_rows);
        if (!dates.empty()) {
            result.trading_dates = filter_range(dates, date_from, date_to);
            result.raw_date_count = dates.size();
            result.calendar_bad_rows = bad_rows;
            result.holiday_provider =
                std::make_unique<XtCsvCalendarHolidayProvider>(dates);
            result.source = "csv:" + csv_path;
            XtLogger::info_m("SEL_BT",
                              "trading_dates loaded from csv: {} (raw={} filtered={})",
                              csv_path, dates.size(), result.trading_dates.size());
            return result;
        }
        XtLogger::warn_m("SEL_BT", "trading_dates.csv unavailable / empty: {}", csv_path);
    }

    // 2) datalake calendar.parquet — XtCalendarReader（cra ingest 上线后启用）
    if (!datalake_root.empty()) {
        xt_data::datalake::XtDatalakeRoot dl_root;
        dl_root.set_root(datalake_root);
        auto reader = std::make_shared<xt_data::datalake::XtCalendarReader>(dl_root);
        if (reader->load("SSE")) {
            const auto& all = reader->trading_dates();
            result.trading_dates = filter_range(all, date_from, date_to);
            result.raw_date_count = all.size();  // parquet 无坏行（calendar_bad_rows=0）。
            result.holiday_provider =
                std::make_unique<XtCalendarHolidayProvider>(reader);
            result.source = "parquet:" + datalake_root;
            XtLogger::info_m("SEL_BT",
                              "trading_dates loaded from datalake parquet: root={} (raw={} filtered={})",
                              datalake_root, all.size(), result.trading_dates.size());
            return result;
        }
        XtLogger::warn_m("SEL_BT",
                          "datalake calendar.parquet unavailable: root={}", datalake_root);
    }

    result.source = "empty";
    XtLogger::warn_m("SEL_BT", "XtSelDataLoader.load: no trading dates resolved");
    return result;
}

}  // namespace xt_bt_sel
