#include "XtTickCacheStoreV3.h"

#include "xt_core/market/XtRawShardCodecV3.h"
#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_util/path/PathUtil.h"

#include <nlohmann/json.hpp>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <utility>

namespace {

constexpr const char* kCurrentFile = "CURRENT";
constexpr const char* kLockFile = ".lock";
constexpr const char* kGenerationPrefix = "gen-";
constexpr const char* kShardPoolDir = "shards";
constexpr const char* kShardPrefix = "shard_";
constexpr const char* kManifestFile = "manifest.json";
constexpr const char* kClosureIndexFile = "closure_index.json";

std::string join(const std::string& dir, const std::string& leaf) {
    if (dir.empty()) return leaf;
    if (dir.back() == '/' || dir.back() == '\\') return dir + leaf;
    return dir + "/" + leaf;
}

bool is_generation_name(const std::string& name) {
    return name.rfind(kGenerationPrefix, 0) == 0;
}

/// @brief 流式 FNV-1a 混入器；字符串一律带长度前缀（消拼接歧义）。
class Mixer {
public:
    void bytes(const void* data, size_t n) {
        hash_ = xt_core::sync::fnv1a_update(hash_, data, n);
    }
    void str(const std::string& s) {
        const uint32_t len = static_cast<uint32_t>(s.size());
        bytes(&len, sizeof(len));
        bytes(s.data(), s.size());
    }
    void strings(const std::vector<std::string>& items) {
        const uint32_t count = static_cast<uint32_t>(items.size());
        bytes(&count, sizeof(count));
        for (const auto& s : items) str(s);
    }
    uint64_t value() const { return hash_; }

private:
    uint64_t hash_ = xt_core::sync::kFnv1a64OffsetBasis;
};

std::string hex16(uint64_t v) {
    char buf[24];
    std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(v));
    return std::string(buf);
}

bool parse_pooled_shard_name(const std::string& name, uint32_t& out_day) {

    if (name.rfind(kShardPrefix, 0) != 0) return false;
    const std::string body = name.substr(std::string(kShardPrefix).size());
    if (body.size() != 8 + 1 + 16 + 4) return false;
    if (body[8] != '_' || body.compare(25, 4, ".bin") != 0) return false;
    uint32_t day = 0;
    for (size_t i = 0; i < 8; ++i) {
        if (!std::isdigit(static_cast<unsigned char>(body[i]))) return false;
        day = day * 10 + static_cast<uint32_t>(body[i] - '0');
    }
    for (size_t i = 9; i < 25; ++i) {
        if (!std::isxdigit(static_cast<unsigned char>(body[i]))) return false;
    }
    if (day == 0) return false;
    out_day = day;
    return true;
}

}

uint64_t compute_family_fingerprint(const XtTickCacheFamilyKey& key) {
    Mixer m;
    m.strings(key.ordered_sources);
    m.strings(key.symbols);
    m.bytes(&key.qty_base, sizeof(key.qty_base));
    m.bytes(&key.volume_mode, sizeof(key.volume_mode));
    m.bytes(&key.codec_version, sizeof(key.codec_version));
    m.bytes(&key.builder_rev, sizeof(key.builder_rev));
    return m.value();
}

std::string family_dir_name(uint64_t fingerprint) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "fam-%016llx",
                  static_cast<unsigned long long>(fingerprint));
    return std::string(buf);
}

std::string family_dir_path(const std::string& cache_root, uint64_t fingerprint) {
    if (cache_root.empty()) return "";
    return join(join(cache_root, "v3"), family_dir_name(fingerprint));
}

bool ensure_family_dir(const std::string& family_dir) {
    if (family_dir.empty()) return false;
    std::error_code ec;
    std::filesystem::create_directories(family_dir, ec);
    if (ec) return false;
    return std::filesystem::is_directory(family_dir, ec);
}

XtTickSourceStamp stat_tick_source(const std::string& path) {
    XtTickSourceStamp st;
    st.path = path;
    std::error_code ec;
    const auto size = std::filesystem::file_size(path, ec);
    if (!ec) st.size = static_cast<uint64_t>(size);
    std::error_code mec;
    const auto mtime = std::filesystem::last_write_time(path, mec);
    if (!mec) st.mtime_ns = static_cast<uint64_t>(mtime.time_since_epoch().count());
    return st;
}

uint64_t compute_day_shard_fingerprint(const XtTickDayShardKey& key) {
    Mixer m;
    m.bytes(&key.trading_day, sizeof(key.trading_day));
    const uint32_t count = static_cast<uint32_t>(key.ordered_sources.size());
    m.bytes(&count, sizeof(count));
    for (const auto& s : key.ordered_sources) {
        m.str(s.path);
        m.bytes(&s.mtime_ns, sizeof(s.mtime_ns));
        m.bytes(&s.size, sizeof(s.size));
    }
    m.strings(key.symbols);
    m.bytes(&key.qty_base, sizeof(key.qty_base));
    m.bytes(&key.volume_mode, sizeof(key.volume_mode));
    m.bytes(&key.codec_version, sizeof(key.codec_version));
    m.bytes(&key.builder_rev, sizeof(key.builder_rev));
    return m.value();
}

std::string shard_pool_dir(const std::string& family_dir) {
    if (family_dir.empty()) return "";
    return join(family_dir, kShardPoolDir);
}

std::string pooled_shard_path(const std::string& family_dir, uint32_t trading_day,
                              uint64_t fingerprint) {
    if (family_dir.empty() || trading_day == 0) return "";
    char leaf[64];
    std::snprintf(leaf, sizeof(leaf), "%s%08u_%016llx.bin", kShardPrefix, trading_day,
                  static_cast<unsigned long long>(fingerprint));
    return join(shard_pool_dir(family_dir), leaf);
}

std::vector<uint32_t> pooled_shard_days(const std::string& family_dir) {
    std::vector<uint32_t> days;
    if (family_dir.empty()) return days;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(shard_pool_dir(family_dir), ec)) {
        if (ec) break;
        if (!entry.is_regular_file()) continue;
        uint32_t day = 0;
        if (parse_pooled_shard_name(entry.path().filename().string(), day)) days.push_back(day);
    }
    std::sort(days.begin(), days.end());
    days.erase(std::unique(days.begin(), days.end()), days.end());
    return days;
}

bool adopt_shard_into_pool(const std::string& staged_path, const std::string& family_dir,
                           uint32_t trading_day, uint64_t fingerprint) {
    const std::string dst = pooled_shard_path(family_dir, trading_day, fingerprint);
    if (dst.empty()) return false;
    std::error_code ec;
    std::filesystem::create_directories(shard_pool_dir(family_dir), ec);
    if (ec) return false;

    std::filesystem::rename(staged_path, dst, ec);
    return !ec;
}

bool write_empty_pooled_shard(const std::string& family_dir, uint32_t trading_day,
                              uint64_t fingerprint) {
    const std::string dst = pooled_shard_path(family_dir, trading_day, fingerprint);
    if (dst.empty()) return false;
    std::error_code ec;
    std::filesystem::create_directories(shard_pool_dir(family_dir), ec);
    if (ec) return false;

    const std::string tmp = dst + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) return false;
        if (!xt_raw_shard::write_shard(out, trading_day, nullptr, 0)) return false;
        out.flush();
        if (!out.good()) return false;
    }
    std::filesystem::rename(tmp, dst, ec);
    if (ec) {
        std::error_code rm_ec;
        std::filesystem::remove(tmp, rm_ec);
        return false;
    }
    return true;
}

std::string generation_manifest_path(const std::string& generation_dir) {
    return join(generation_dir, kManifestFile);
}

std::string closure_index_path(const std::string& generation_dir) {
    return join(generation_dir, kClosureIndexFile);
}

std::string serialize_generation_manifest(const XtTickGenerationManifest& manifest) {
    nlohmann::json j;
    j["schema"] = XtTickGenerationManifest::kSchema;
    j["family"] = hex16(manifest.family_fingerprint);
    j["full_coverage"] = manifest.full_coverage;
    nlohmann::json days = nlohmann::json::array();
    for (const auto& d : manifest.days) {
        nlohmann::json e;
        e["day"] = d.trading_day;
        e["shard"] = hex16(d.shard_fingerprint);
        e["records"] = d.record_count;
        days.push_back(std::move(e));
    }
    j["days"] = std::move(days);

    return j.dump(2) + "\n";
}

bool parse_generation_manifest(const std::string& text, XtTickGenerationManifest& out) {
    nlohmann::json j = nlohmann::json::parse(text, nullptr,  false);
    if (j.is_discarded() || !j.is_object()) return false;
    if (!j.contains("schema") || !j["schema"].is_number_unsigned()) return false;
    if (j["schema"].get<uint32_t>() != XtTickGenerationManifest::kSchema) return false;
    if (!j.contains("family") || !j["family"].is_string()) return false;
    if (!j.contains("full_coverage") || !j["full_coverage"].is_boolean()) return false;
    if (!j.contains("days") || !j["days"].is_array()) return false;

    auto parse_hex16 = [](const std::string& s, uint64_t& out_v) {
        if (s.size() != 16) return false;
        uint64_t v = 0;
        for (char c : s) {
            if (!std::isxdigit(static_cast<unsigned char>(c))) return false;
            const int d = (c <= '9') ? (c - '0')
                                     : (std::tolower(static_cast<unsigned char>(c)) - 'a' + 10);
            v = (v << 4) | static_cast<uint64_t>(d);
        }
        out_v = v;
        return true;
    };

    XtTickGenerationManifest parsed;
    if (!parse_hex16(j["family"].get<std::string>(), parsed.family_fingerprint)) return false;
    parsed.full_coverage = j["full_coverage"].get<bool>();

    uint32_t prev_day = 0;
    for (const auto& e : j["days"]) {
        if (!e.is_object()) return false;
        if (!e.contains("day") || !e["day"].is_number_unsigned()) return false;
        if (!e.contains("shard") || !e["shard"].is_string()) return false;
        if (!e.contains("records") || !e["records"].is_number_unsigned()) return false;
        XtTickGenerationDay d;
        d.trading_day = e["day"].get<uint32_t>();
        if (d.trading_day == 0) return false;

        if (prev_day != 0 && d.trading_day <= prev_day) return false;
        prev_day = d.trading_day;
        if (!parse_hex16(e["shard"].get<std::string>(), d.shard_fingerprint)) return false;
        d.record_count = e["records"].get<uint64_t>();
        parsed.days.push_back(d);
    }

    out = std::move(parsed);
    return true;
}

bool write_generation_manifest(const std::string& path,
                               const XtTickGenerationManifest& manifest) {
    return PathUtil::write_atomic(path, serialize_generation_manifest(manifest), 0644);
}

bool read_generation_manifest(const std::string& path, XtTickGenerationManifest& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return false;
    std::ostringstream oss;
    oss << in.rdbuf();
    if (!in.good() && !in.eof()) return false;
    return parse_generation_manifest(oss.str(), out);
}

XtTickCacheFamilyLock::~XtTickCacheFamilyLock() { unlock(); }

XtTickCacheFamilyLock::XtTickCacheFamilyLock(XtTickCacheFamilyLock&& other) noexcept
    : fd_(other.fd_) {
    other.fd_ = -1;
}

XtTickCacheFamilyLock& XtTickCacheFamilyLock::operator=(
    XtTickCacheFamilyLock&& other) noexcept {
    if (this != &other) {
        unlock();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

bool XtTickCacheFamilyLock::try_lock(const std::string& family_dir) {
    if (held()) return false;
    if (!ensure_family_dir(family_dir)) return false;
    const std::string path = join(family_dir, kLockFile);
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return false;

    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        ::close(fd);
        return false;
    }
    fd_ = fd;
    return true;
}

void XtTickCacheFamilyLock::unlock() {
    if (fd_ < 0) return;
    ::flock(fd_, LOCK_UN);
    ::close(fd_);
    fd_ = -1;
}

std::string make_generation_name(uint64_t epoch_ms, int pid, uint64_t seq) {
    char buf[80];
    std::snprintf(buf, sizeof(buf), "%s%llu-%d-%llu", kGenerationPrefix,
                  static_cast<unsigned long long>(epoch_ms), pid,
                  static_cast<unsigned long long>(seq));
    return std::string(buf);
}

bool create_generation_dir(const std::string& family_dir,
                           const std::string& generation,
                           std::string& out_path) {
    if (family_dir.empty() || !is_generation_name(generation)) return false;
    if (!ensure_family_dir(family_dir)) return false;
    const std::string path = join(family_dir, generation);
    std::error_code ec;

    if (!std::filesystem::create_directory(path, ec) || ec) return false;
    out_path = path;
    return true;
}

bool commit_current(const std::string& family_dir, const std::string& generation) {
    if (family_dir.empty() || !is_generation_name(generation)) return false;
    std::error_code ec;
    if (!std::filesystem::is_directory(join(family_dir, generation), ec)) return false;
    return PathUtil::write_atomic(join(family_dir, kCurrentFile), generation + "\n", 0644);
}

bool read_current(const std::string& family_dir, std::string& out_generation) {
    if (family_dir.empty()) return false;
    std::ifstream in(join(family_dir, kCurrentFile), std::ios::binary);
    if (!in.is_open()) return false;
    std::string line;
    if (!std::getline(in, line)) return false;
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) line.pop_back();
    if (!is_generation_name(line)) return false;
    out_generation = line;
    return true;
}

XtTickCacheGcPlan plan_gc(const std::string& family_dir) {
    XtTickCacheGcPlan plan;
    if (family_dir.empty()) return plan;

    std::vector<std::string> generations;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(family_dir, ec)) {
        if (ec) break;
        if (!entry.is_directory()) continue;
        const std::string name = entry.path().filename().string();
        if (is_generation_name(name)) generations.push_back(name);
    }
    std::sort(generations.begin(), generations.end());

    bool references_complete = true;
    std::unordered_set<std::string> referenced;
    for (const auto& g : generations) {
        XtTickGenerationManifest manifest;
        if (!read_generation_manifest(generation_manifest_path(join(family_dir, g)), manifest)) {
            references_complete = false;
            break;
        }
        for (const auto& d : manifest.days) {
            char leaf[64];
            std::snprintf(leaf, sizeof(leaf), "%s%08u_%016llx.bin", kShardPrefix, d.trading_day,
                          static_cast<unsigned long long>(d.shard_fingerprint));
            referenced.emplace(leaf);
        }
    }
    if (references_complete) {
        std::error_code pool_ec;
        for (const auto& entry :
             std::filesystem::directory_iterator(shard_pool_dir(family_dir), pool_ec)) {
            if (pool_ec) break;
            if (!entry.is_regular_file()) continue;
            const std::string name = entry.path().filename().string();
            uint32_t day = 0;
            if (!parse_pooled_shard_name(name, day)) continue;
            if (referenced.count(name) == 0) plan.orphan_shards.push_back(name);
        }
        std::sort(plan.orphan_shards.begin(), plan.orphan_shards.end());
    }

    std::string current;
    if (!read_current(family_dir, current)) {

        return plan;
    }
    plan.current = current;
    if (std::find(generations.begin(), generations.end(), current) == generations.end()) {
        plan.current_missing = true;
        return plan;
    }
    for (const auto& g : generations) {
        if (g != current) plan.removable.push_back(g);
    }
    return plan;
}
