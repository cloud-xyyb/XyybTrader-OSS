#include "StrategyUserdataStore.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>

#include "XtLogger.h"

namespace xt_live::common {

using nlohmann::json;

namespace {

constexpr int kSchemaVersion = 1;

bool is_path_component_safe(const std::string& s) {
    if (s.empty() || s == "." || s == "..") return false;
    for (const unsigned char c : s) {
        if (c < 0x20 || c == '/' || c == '\\') return false;
    }
    return true;
}

int64_t now_unix_sec() {
    return static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

}

std::string strategy_userdata_path(const std::string& state_dir, const std::string& sid) {
    namespace fs = std::filesystem;
    return (fs::path(state_dir) / ("strategy_userdata_" + sid + ".json")).string();
}

std::string derive_userdata_state_dir(const std::string& recording_file_path,
                                      const std::string& cfg_path) {
    namespace fs = std::filesystem;
    if (!recording_file_path.empty()) {

        const fs::path rec(recording_file_path);
        const fs::path recordings_dir = rec.parent_path();
        const fs::path profile_dir    = recordings_dir.parent_path();

        if (!profile_dir.empty() && profile_dir != profile_dir.root_path()) {
            return (profile_dir / "state").string();
        }
    }

    const fs::path base = fs::path(cfg_path).parent_path();
    return (base / "var" / "strategy_state").string();
}

bool write_strategy_userdata(const std::string& state_dir,
                             const std::string& sid,
                             uint32_t trading_date,
                             const std::unordered_map<std::string, std::string>& kv,
                             std::string& err) {
    if (!is_path_component_safe(sid)) {
        err = "unsafe strategy_id path component: " + sid;
        return false;
    }

    json root;
    root["schema_version"]    = kSchemaVersion;
    root["strategy_id"]       = sid;
    root["generated_at_unix"] = now_unix_sec();
    root["trading_date"]      = trading_date;
    root["userdata"]          = kv;

    const std::string path = strategy_userdata_path(state_dir, sid);
    const std::string tmp  = path + ".tmp";
    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);

    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out.good()) {
            err = "cannot open tmp for write: " + tmp;
            return false;
        }
        out << root.dump(2) << '\n';
        if (!out.good()) {
            err = "write failed: " + tmp;
            return false;
        }
    }
    std::filesystem::rename(tmp, path, ec);
    if (ec) {
        err = "rename failed: " + ec.message();
        std::error_code rm_ec;
        std::filesystem::remove(tmp, rm_ec);
        return false;
    }
    return true;
}

bool read_strategy_userdata(const std::string& state_dir,
                            const std::string& sid,
                            std::unordered_map<std::string, std::string>& out_kv,
                            std::string& err,
                            const char* log_tag) {
    if (!is_path_component_safe(sid)) {
        err = "unsafe strategy_id path component: " + sid;
        return false;
    }
    const std::string path = strategy_userdata_path(state_dir, sid);

    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {

        return false;
    }

    json root;
    try {
        in >> root;
    } catch (const std::exception& e) {
        err = std::string("json parse: ") + e.what();
        return false;
    }

    try {
        const int schema = root.value("schema_version", 0);
        if (schema != kSchemaVersion) {
            err = "unknown schema_version=" + std::to_string(schema);
            return false;
        }
        const std::string file_sid = root.value("strategy_id", std::string());
        if (file_sid != sid) {
            err = "strategy_id mismatch: file=" + file_sid + " expected=" + sid;
            return false;
        }
        std::unordered_map<std::string, std::string> parsed;
        if (root.contains("userdata")) {
            parsed = root["userdata"].get<std::unordered_map<std::string, std::string>>();
        }

        const int64_t gen_unix = root.value("generated_at_unix", int64_t{0});
        const uint32_t td       = root.value("trading_date", uint32_t{0});
        const int64_t age_sec   = gen_unix > 0 ? (now_unix_sec() - gen_unix) : -1;
        XtLogger::info_m(log_tag ? log_tag : "RT",
                         "userdata restored strategy={} keys={} trading_date={} age_sec={}",
                         sid, parsed.size(), td, age_sec);
        out_kv = std::move(parsed);
        return true;
    } catch (const std::exception& e) {
        err = std::string("schema decode: ") + e.what();
        return false;
    }
}

}
