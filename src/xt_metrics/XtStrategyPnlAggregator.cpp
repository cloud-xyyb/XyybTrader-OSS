#include "xt_metrics/XtStrategyPnlAggregator.h"

#include "xt_util/path/PathUtil.h"
#include "XtLogger.h"

#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>

namespace ct::metrics {

namespace {

constexpr double kFpScale = 1'000'000.0;

std::string normalize_sid(std::string_view sid) {
    return sid.empty() ? std::string("_default") : std::string(sid);
}

bool is_value_terminator(char c) {
    return c == '\0' || c == ',' || c == '}' || c == ']';
}

bool parse_int64_strict(const char* s, int64_t& out) {
    errno = 0;
    char* end = nullptr;
    const long long v = std::strtoll(s, &end, 10);
    if (end == s) return false;
    if (errno == ERANGE) return false;
    while (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r') ++end;
    if (!is_value_terminator(*end)) return false;
    out = static_cast<int64_t>(v);
    return true;
}

bool parse_double_strict(const char* s, double& out) {
    errno = 0;
    char* end = nullptr;
    const double v = std::strtod(s, &end);
    if (end == s) return false;
    if (errno == ERANGE) return false;
    if (!std::isfinite(v)) return false;
    while (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r') ++end;
    if (!is_value_terminator(*end)) return false;
    out = v;
    return true;
}

std::string json_escape(std::string_view s) {
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

}

void XtStrategyPnlAggregator::on_close(std::string_view sid,
                                       std::string_view code,
                                       int64_t realized_pnl_fp) {
    if (code.empty()) return;
    if (realized_pnl_fp == 0) return;
    Key key{normalize_sid(sid), std::string(code)};
    std::unique_lock<std::shared_mutex> lock(mu_);
    rows_[key].realized_fp += realized_pnl_fp;
}

void XtStrategyPnlAggregator::on_fill(std::string_view sid,
                                      std::string_view code,
                                      double fee) {
    if (code.empty()) return;

    Key key{normalize_sid(sid), std::string(code)};
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto& st = rows_[key];
    st.fills += 1;
    st.fees  += fee;
}

std::vector<XtStrategyPnlAggregator::Row>
XtStrategyPnlAggregator::snapshot() const {
    std::vector<Row> out;
    std::shared_lock<std::shared_mutex> lock(mu_);
    out.reserve(rows_.size());
    for (const auto& [key, st] : rows_) {
        Row r;
        r.sid      = key.first;
        r.code     = key.second;
        r.realized = static_cast<double>(st.realized_fp) / kFpScale;
        r.fees     = st.fees;
        r.fills    = st.fills;
        out.push_back(std::move(r));
    }
    return out;
}

int64_t XtStrategyPnlAggregator::realized_fp_for_sid(std::string_view sid) const {
    const std::string key_sid = normalize_sid(sid);
    int64_t total = 0;
    std::shared_lock<std::shared_mutex> lock(mu_);
    for (const auto& [key, st] : rows_) {
        if (key.first == key_sid) total += st.realized_fp;
    }
    return total;
}

bool XtStrategyPnlAggregator::save_state(std::string_view path) const {
    // shutdown 路径用返回值上报持久化失败，避免异常穿透 runner 停机流程。
    // 持锁快照用本地 struct:realized_fp 以 int64 直通写出,不借 public
    // Row::realized (double, 金额语义) 中转,消除双语义往返。
    struct PersistRow {
        std::string sid;
        std::string code;
        int64_t     realized_fp = 0;
        double      fees        = 0.0;
        uint64_t    fills       = 0;
    };
    std::vector<PersistRow> rows;
    {
        std::shared_lock<std::shared_mutex> lock(mu_);
        rows.reserve(rows_.size());
        for (const auto& [key, st] : rows_) {
            rows.push_back(PersistRow{key.first, key.second,
                                      st.realized_fp, st.fees, st.fills});
        }
    }

    std::ostringstream oss;
    oss << "{\n";
    oss << "  \"version\": 1,\n";
    oss << "  \"saved_at_ms\": " << std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count() << ",\n";
    oss << "  \"rows\": [";
    bool first = true;
    for (const auto& r : rows) {
        if (!first) oss << ",";
        first = false;
        oss << "\n    {";
        oss << "\"sid\": \"" << json_escape(r.sid) << "\", ";
        oss << "\"code\": \"" << json_escape(r.code) << "\", ";
        oss << "\"realized_fp\": " << r.realized_fp << ", ";
        oss << "\"fees\": " << std::fixed << std::setprecision(8) << r.fees << ", ";
        oss << "\"fills\": " << r.fills;
        oss << "}";
    }
    oss << "\n  ]\n}\n";
    const std::string body = oss.str();

    const std::string p(path);
    PathUtil::AtomicOfstream out(p);
    if (!out.is_open()) return false;
    out.stream() << body;
    if (!out.stream()) return false;
    return out.commit();
}

XtStrategyPnlAggregator::LoadResult
XtStrategyPnlAggregator::reject_and_quarantine_(const std::string& path,
                                               std::string_view reason) {
    invalid_load_count_.fetch_add(1, std::memory_order_relaxed);
    namespace fs = std::filesystem;
    std::error_code ec;

    fs::path dst(path + ".quarantine");
    if (fs::exists(dst, ec) || ec) {
        const auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        dst = fs::path(path + ".quarantine." + std::to_string(epoch_ms));
    }
    fs::rename(path, dst, ec);
    if (ec) {
        XtLogger::error_m("control",
            "PNL aggregator state INVALID ({}); quarantine rename FAILED ({}): {} -> {}; "
            "rows_ unchanged, caller must fail-closed",
            std::string(reason), ec.message(), path, dst.string());
    } else {
        XtLogger::error_m("control",
            "PNL aggregator state INVALID ({}); quarantined {} -> {}; "
            "rows_ unchanged, caller must fail-closed",
            std::string(reason), path, dst.string());
    }
    return LoadResult::kInvalid;
}

XtStrategyPnlAggregator::LoadResult
XtStrategyPnlAggregator::load_state(std::string_view path) {
    namespace fs = std::filesystem;
    const std::string p(path);

    std::error_code ec;
    const bool present = fs::exists(p, ec);
    if (ec) {
        invalid_load_count_.fetch_add(1, std::memory_order_relaxed);
        XtLogger::error_m("control",
            "PNL aggregator state cannot be stat'd ({}): {}; fail-closed kInvalid, "
            "rows_ unchanged, caller must fail-closed", ec.message(), p);
        return LoadResult::kInvalid;
    }

    if (!present) return LoadResult::kMissing;

    std::ifstream f{p};

    if (!f) return reject_and_quarantine_(p, "state file exists but cannot be opened");

    std::ostringstream ss;
    ss << f.rdbuf();
    const std::string body = ss.str();

    {
        auto vpos = body.find("\"version\"");
        if (vpos == std::string::npos)
            return reject_and_quarantine_(p, "missing top-level version");
        auto vc = body.find(':', vpos);
        if (vc == std::string::npos)
            return reject_and_quarantine_(p, "malformed version field");
        int64_t version = 0;
        if (!parse_int64_strict(body.c_str() + vc + 1, version))
            return reject_and_quarantine_(p, "non-integer version value");
        if (version != 1)
            return reject_and_quarantine_(p, "unsupported version (expect 1)");
    }

    auto rows_pos = body.find("\"rows\"");
    if (rows_pos == std::string::npos)
        return reject_and_quarantine_(p, "missing rows array");
    auto arr_open = body.find('[', rows_pos);
    if (arr_open == std::string::npos)
        return reject_and_quarantine_(p, "rows array not opened");
    auto arr_close = body.find(']', arr_open);
    if (arr_close == std::string::npos)
        return reject_and_quarantine_(p, "rows array not closed (truncated)");

    std::string arr = body.substr(arr_open + 1, arr_close - arr_open - 1);
    std::unordered_map<Key, State, PairHash> parsed;
    size_t pos = 0;
    while (true) {
        auto obj_open = arr.find('{', pos);
        if (obj_open == std::string::npos) break;
        auto obj_close = arr.find('}', obj_open);
        if (obj_close == std::string::npos)
            return reject_and_quarantine_(p, "row object not closed (brace mismatch)");
        std::string obj = arr.substr(obj_open + 1, obj_close - obj_open - 1);
        pos = obj_close + 1;

        auto extract_str = [&](const std::string& key, std::string& out) -> bool {
            auto k = obj.find("\"" + key + "\"");
            if (k == std::string::npos) return false;
            auto c = obj.find(':', k);
            if (c == std::string::npos) return false;
            auto q1 = obj.find('"', c);
            if (q1 == std::string::npos) return false;
            auto q2 = obj.find('"', q1 + 1);
            if (q2 == std::string::npos) return false;
            out = obj.substr(q1 + 1, q2 - q1 - 1);
            return true;
        };

        auto value_start = [&](const std::string& key) -> const char* {
            auto k = obj.find("\"" + key + "\"");
            if (k == std::string::npos) return nullptr;
            auto c = obj.find(':', k);
            if (c == std::string::npos) return nullptr;
            return obj.c_str() + c + 1;
        };

        std::string sid;
        std::string code;
        const bool  sid_ok  = extract_str("sid", sid);
        const bool  code_ok = extract_str("code", code);
        const char* rf      = value_start("realized_fp");
        const char* fe      = value_start("fees");
        const char* fl      = value_start("fills");
        if (!sid_ok || !code_ok || rf == nullptr || fe == nullptr || fl == nullptr)
            return reject_and_quarantine_(p, "row missing required field");
        if (code.empty())
            return reject_and_quarantine_(p, "row with empty code");

        State stt;
        if (!parse_int64_strict(rf, stt.realized_fp))
            return reject_and_quarantine_(p, "invalid realized_fp");
        if (!parse_double_strict(fe, stt.fees))
            return reject_and_quarantine_(p, "invalid fees");
        int64_t fills_i = 0;
        if (!parse_int64_strict(fl, fills_i) || fills_i < 0)
            return reject_and_quarantine_(p, "invalid fills");
        stt.fills = static_cast<uint64_t>(fills_i);

        auto ins = parsed.emplace(Key{normalize_sid(sid), std::move(code)}, stt);
        if (!ins.second)
            return reject_and_quarantine_(p, "duplicate (sid, code) key");
    }

    std::unique_lock<std::shared_mutex> lock(mu_);
    rows_ = std::move(parsed);
    return LoadResult::kLoaded;
}

}
