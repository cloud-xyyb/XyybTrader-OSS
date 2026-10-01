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

/// path-component 校验：拒空 / "." / ".." / 含 '/' '\\' / 控制字符。
/// 镜像 LocalFsEventStore::is_path_component_safe，防 sid 路径注入穿越 state_dir。
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

}  // namespace

std::string strategy_userdata_path(const std::string& state_dir, const std::string& sid) {
    namespace fs = std::filesystem;
    return (fs::path(state_dir) / ("strategy_userdata_" + sid + ".json")).string();
}

std::string derive_userdata_state_dir(const std::string& recording_file_path,
                                      const std::string& cfg_path) {
    namespace fs = std::filesystem;
    if (!recording_file_path.empty()) {
        // .../live_runs/<profile>/recordings/live.jsonl → .../live_runs/<profile>/state
        const fs::path rec(recording_file_path);
        const fs::path recordings_dir = rec.parent_path();       // .../recordings
        const fs::path profile_dir    = recordings_dir.parent_path();  // .../<profile>
        // profile 必须是真实路径组件：相对浅路径推出空，绝对浅路径（如 "/live.jsonl"）
        // 推出纯根目录（若放行会落盘 "/state"），两者对称走 config 回退。
        if (!profile_dir.empty() && profile_dir != profile_dir.root_path()) {
            return (profile_dir / "state").string();
        }
    }
    // recording 禁用：回退 config 同级 var/strategy_state（镜像 CTA reconcile 默认根）。
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
    root["userdata"]          = kv;  // nlohmann 负责对任意字符串值做 JSON 转义。

    const std::string path = strategy_userdata_path(state_dir, sid);
    const std::string tmp  = path + ".tmp";
    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
    // create_directories 对已存在目录返 ec 为空且返回 false，非错误；仅当无法创建时下方写失败暴露。
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
        std::filesystem::remove(tmp, rm_ec);  // best-effort 清理，避免 tmp 残留。
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
        // 文件不存在 = clean start：err 留空，调用方静默空启动。
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
        // 快照年龄暴露给 ops：策略自负 stale KV 语义，SDK/runner 不做新鲜度 gate。
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

}  // namespace xt_live::common
