#include "xt_metrics/XtStrategyPnlBaseline.h"

#include "xt_util/path/PathUtil.h"

#include <chrono>
#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace ct::metrics {

namespace {

constexpr double kFpScale = 1'000'000.0;

int64_t amount_to_fp(double amount) {
    return static_cast<int64_t>(std::llround(amount * kFpScale));
}

std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (char c : s) {
        if (c == '\\' || c == '"') {
            out.push_back('\\');
            out.push_back(c);
        } else if (static_cast<unsigned char>(c) < 0x20) {
            char buf[8];
            std::snprintf(buf, sizeof(buf), "\\u%04x", c);
            out.append(buf);
        } else {
            out.push_back(c);
        }
    }
    return out;
}

std::string normalize_sid(std::string_view sid) {
    return sid.empty() ? std::string("_default") : std::string(sid);
}

bool ensure_dir(const std::string& dir) {
    if (dir.empty()) return false;
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) return false;
    return std::filesystem::is_directory(dir, ec);
}

struct ParseResult {
    std::vector<XtStrategyPnlBaseline::BaselineRow> rows;
    XtStrategyPnlBaseline::LoadStatus status = XtStrategyPnlBaseline::LoadStatus::Loaded;
};

bool extract_int_field(const std::string& body, const std::string& key, int* out) {
    auto k = body.find("\"" + key + "\"");
    if (k == std::string::npos) return false;
    auto c = body.find(':', k);
    if (c == std::string::npos) return false;
    char* end = nullptr;
    errno = 0;
    const long v = std::strtol(body.c_str() + c + 1, &end, 10);
    if (errno != 0 || end == body.c_str() + c + 1) return false;
    *out = static_cast<int>(v);
    return true;
}

ParseResult parse_baseline_json(const std::string& body, int expected_trading_day_id) {
    ParseResult result;
    int version = 0;
    if (!extract_int_field(body, "version", &version)) {
        result.status = XtStrategyPnlBaseline::LoadStatus::Corrupt;
        return result;
    }
    if (version != 1) {
        result.status = XtStrategyPnlBaseline::LoadStatus::UnsupportedVersion;
        return result;
    }
    int file_day = 0;
    if (!extract_int_field(body, "trading_day_id", &file_day) ||
        file_day != expected_trading_day_id) {
        result.status = XtStrategyPnlBaseline::LoadStatus::Corrupt;
        return result;
    }
    auto baseline_pos = body.find("\"baseline\"");
    if (baseline_pos == std::string::npos) {
        result.status = XtStrategyPnlBaseline::LoadStatus::Corrupt;
        return result;
    }
    auto arr_open = body.find('[', baseline_pos);
    if (arr_open == std::string::npos) {
        result.status = XtStrategyPnlBaseline::LoadStatus::Corrupt;
        return result;
    }
    auto arr_close = body.find(']', arr_open);
    if (arr_close == std::string::npos) {
        result.status = XtStrategyPnlBaseline::LoadStatus::Corrupt;
        return result;
    }

    std::string arr = body.substr(arr_open + 1, arr_close - arr_open - 1);
    size_t pos = 0;
    while (true) {
        auto obj_open = arr.find('{', pos);
        if (obj_open == std::string::npos) break;
        auto obj_close = arr.find('}', obj_open);
        if (obj_close == std::string::npos) {
            result.status = XtStrategyPnlBaseline::LoadStatus::Corrupt;
            return result;
        }
        std::string obj = arr.substr(obj_open + 1, obj_close - obj_open - 1);
        pos = obj_close + 1;

        XtStrategyPnlBaseline::BaselineRow r;

        auto extract_str = [&](const std::string& key) -> std::string {
            auto k = obj.find("\"" + key + "\"");
            if (k == std::string::npos) return {};
            auto c = obj.find(':', k);
            if (c == std::string::npos) return {};
            auto q1 = obj.find('"', c);
            if (q1 == std::string::npos) return {};
            auto q2 = obj.find('"', q1 + 1);
            if (q2 == std::string::npos) return {};
            return obj.substr(q1 + 1, q2 - q1 - 1);
        };
        auto extract_double = [&](const std::string& key) -> double {
            auto k = obj.find("\"" + key + "\"");
            if (k == std::string::npos) return 0.0;
            auto c = obj.find(':', k);
            if (c == std::string::npos) return 0.0;
            return std::atof(obj.c_str() + c + 1);
        };
        auto extract_int64 = [&](const std::string& key, bool& found) -> int64_t {
            found = false;
            auto k = obj.find("\"" + key + "\"");
            if (k == std::string::npos) return 0;
            auto c = obj.find(':', k);
            if (c == std::string::npos) return 0;
            found = true;
            return std::strtoll(obj.c_str() + c + 1, nullptr, 10);
        };
        r.sid      = extract_str("sid");
        r.code     = extract_str("code");

        bool has_realized_fp = false;
        const int64_t realized_fp = extract_int64("realized_fp", has_realized_fp);
        r.realized = has_realized_fp ? static_cast<double>(realized_fp) / kFpScale
                                     : extract_double("realized");
        r.fees     = extract_double("fees");
        r.fills    = static_cast<uint64_t>(extract_double("fills"));
        if (r.code.empty()) {
            result.status = XtStrategyPnlBaseline::LoadStatus::Corrupt;
            return result;
        }
        r.sid = normalize_sid(r.sid);
        result.rows.push_back(std::move(r));
    }
    return result;
}

}

XtStrategyPnlBaseline::XtStrategyPnlBaseline(std::string instance_id)
    : instance_id_(instance_id.empty() ? std::string("_default") : std::move(instance_id)) {}

bool XtStrategyPnlBaseline::load(std::string_view base_dir, int trading_day_id) {
    std::lock_guard<std::mutex> lock(mu_);
    const std::string path = path_for(base_dir, trading_day_id);
    std::ifstream ifs(path);
    if (!ifs) {
        last_load_status_ = LoadStatus::Missing;
        ready_ = false;
        return false;
    }
    std::ostringstream buf;
    buf << ifs.rdbuf();
    if (!ifs.good() && !ifs.eof()) {
        last_load_status_ = LoadStatus::IoError;
        ready_ = false;
        return false;
    }
    auto parsed = parse_baseline_json(buf.str(), trading_day_id);
    if (parsed.status != LoadStatus::Loaded) {
        last_load_status_ = parsed.status;
        ready_ = false;
        return false;
    }
    std::unordered_map<Key, BaselineRow, PairHash> next;
    for (auto& r : parsed.rows) {
        Key k{r.sid, r.code};
        next[k] = std::move(r);
    }
    baseline_ = std::move(next);
    last_load_status_ = LoadStatus::Loaded;
    ready_ = true;
    return true;
}

void XtStrategyPnlBaseline::rotate_day(const std::vector<BaselineRow>& rows) {
    std::lock_guard<std::mutex> lock(mu_);
    baseline_.clear();
    for (const auto& r : rows) {
        if (r.code.empty()) continue;
        BaselineRow row = r;
        row.sid = normalize_sid(row.sid);
        Key k{row.sid, row.code};
        baseline_[k] = std::move(row);
    }
    last_load_status_ = LoadStatus::Loaded;
    ready_ = true;
}

bool XtStrategyPnlBaseline::save(std::string_view base_dir, int trading_day_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const std::string dir  = std::string(base_dir) + "/" + instance_id_;
    if (!ensure_dir(dir)) return false;
    const std::string path = path_for(base_dir, trading_day_id);

    PathUtil::AtomicOfstream out(path);
    if (!out.is_open()) return false;

    auto& ofs = out.stream();
    ofs << "{\n";
    ofs << "  \"version\": 1,\n";
    ofs << "  \"trading_day_id\": " << trading_day_id << ",\n";
    ofs << "  \"instance_id\": \"" << json_escape(instance_id_) << "\",\n";
    ofs << "  \"baseline\": [\n";

    ofs << std::fixed << std::setprecision(8);
    bool first = true;
    for (const auto& [k, r] : baseline_) {
        if (!first) ofs << ",\n";
        first = false;
        ofs << "    {\"sid\": \"" << json_escape(r.sid)
            << "\", \"code\": \"" << json_escape(r.code)
            << "\", \"realized\": " << r.realized
            << ", \"realized_fp\": " << amount_to_fp(r.realized)
            << ", \"fees\": " << r.fees
            << ", \"fills\": " << r.fills << "}";
    }
    ofs << "\n  ]\n";
    ofs << "}\n";
    if (!ofs) {
        return false;
    }
    return out.commit();
}

XtStrategyPnlBaseline::BaselineRow
XtStrategyPnlBaseline::get(std::string_view sid, std::string_view code, bool* is_new) const {
    std::lock_guard<std::mutex> lock(mu_);
    Key k{normalize_sid(sid), std::string(code)};
    auto it = baseline_.find(k);
    if (it == baseline_.end()) {
        if (is_new) *is_new = true;
        return BaselineRow{normalize_sid(sid), std::string(code), 0.0, 0.0};
    }
    if (is_new) *is_new = false;
    return it->second;
}

int64_t XtStrategyPnlBaseline::realized_fp_for_sid(std::string_view sid) const {

    const std::string key_sid = normalize_sid(sid);
    std::lock_guard<std::mutex> lock(mu_);
    int64_t sum_fp = 0;
    for (const auto& [k, r] : baseline_) {
        if (k.first == key_sid) sum_fp += amount_to_fp(r.realized);
    }
    return sum_fp;
}

int XtStrategyPnlBaseline::compute_trading_day_id(int tz_offset_h) {
    auto now = std::chrono::system_clock::now();
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

    sec += static_cast<int64_t>(tz_offset_h) * 3600;
    std::time_t t = static_cast<std::time_t>(sec);
    std::tm tm_utc;
    if (::gmtime_r(&t, &tm_utc) == nullptr) return 0;
    return (tm_utc.tm_year + 1900) * 10000 + (tm_utc.tm_mon + 1) * 100 + tm_utc.tm_mday;
}

size_t XtStrategyPnlBaseline::size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return baseline_.size();
}

bool XtStrategyPnlBaseline::ready() const {
    std::lock_guard<std::mutex> lock(mu_);
    return ready_;
}

XtStrategyPnlBaseline::LoadStatus XtStrategyPnlBaseline::last_load_status() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_load_status_;
}

const char* XtStrategyPnlBaseline::load_status_name(LoadStatus status) noexcept {
    switch (status) {
    case LoadStatus::NotLoaded: return "not_loaded";
    case LoadStatus::Loaded: return "loaded";
    case LoadStatus::Missing: return "missing";
    case LoadStatus::Corrupt: return "corrupt";
    case LoadStatus::UnsupportedVersion: return "unsupported_version";
    case LoadStatus::IoError: return "io_error";
    }
    return "unknown";
}

std::vector<XtStrategyPnlBaseline::BaselineRow> XtStrategyPnlBaseline::snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<BaselineRow> out;
    out.reserve(baseline_.size());
    for (const auto& [k, r] : baseline_) {
        out.push_back(r);
    }
    return out;
}

std::string XtStrategyPnlBaseline::path_for(std::string_view base_dir,
                                            int trading_day_id) const {
    std::ostringstream oss;
    oss << base_dir << "/" << instance_id_ << "/pnl_baseline_" << trading_day_id << ".json";
    return oss.str();
}

}
