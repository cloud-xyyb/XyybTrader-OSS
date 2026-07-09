#include "xt_metrics/XtStrategyPnlAggregator.h"

#include "xt_util/path/PathUtil.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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

bool XtStrategyPnlAggregator::load_state(std::string_view path) {
    std::ifstream f{std::string(path)};
    if (!f) return false;
    std::ostringstream ss;
    ss << f.rdbuf();
    const std::string body = ss.str();
    if (body.empty()) return false;

    auto rows_pos = body.find("\"rows\"");
    if (rows_pos == std::string::npos) return false;
    auto arr_open = body.find('[', rows_pos);
    if (arr_open == std::string::npos) return false;
    auto arr_close = body.find(']', arr_open);
    if (arr_close == std::string::npos) return false;

    std::string arr = body.substr(arr_open + 1, arr_close - arr_open - 1);
    std::unordered_map<Key, State, PairHash> parsed;
    size_t pos = 0;
    while (true) {
        auto obj_open = arr.find('{', pos);
        if (obj_open == std::string::npos) break;
        auto obj_close = arr.find('}', obj_open);
        if (obj_close == std::string::npos) break;
        std::string obj = arr.substr(obj_open + 1, obj_close - obj_open - 1);
        pos = obj_close + 1;

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
        auto extract_int64 = [&](const std::string& key) -> int64_t {
            auto k = obj.find("\"" + key + "\"");
            if (k == std::string::npos) return 0;
            auto c = obj.find(':', k);
            if (c == std::string::npos) return 0;
            return std::strtoll(obj.c_str() + c + 1, nullptr, 10);
        };
        auto extract_double = [&](const std::string& key) -> double {
            auto k = obj.find("\"" + key + "\"");
            if (k == std::string::npos) return 0.0;
            auto c = obj.find(':', k);
            if (c == std::string::npos) return 0.0;
            return std::atof(obj.c_str() + c + 1);
        };
        std::string sid  = extract_str("sid");
        std::string code = extract_str("code");
        if (code.empty()) continue;
        State st;
        st.realized_fp = extract_int64("realized_fp");
        st.fees        = extract_double("fees");
        st.fills       = static_cast<uint64_t>(extract_int64("fills"));
        parsed[Key{normalize_sid(sid), std::move(code)}] = std::move(st);
    }

    if (parsed.empty()) return false;
    std::unique_lock<std::shared_mutex> lock(mu_);
    rows_ = std::move(parsed);
    return true;
}

}
