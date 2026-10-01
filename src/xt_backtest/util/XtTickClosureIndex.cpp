#include "XtTickClosureIndex.h"

#include "TickFileUtil.h"           // extract_date_from_filename / kTickNightCarryGuardDays
#include "xt_util/path/PathUtil.h"  // write_atomic
#include "xt_util/time/XtTime.h"    // yyyymmdd_add_days / is_valid_yyyymmdd

#include <nlohmann/json.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace {

void sort_unique(std::vector<uint32_t>& v) {
    std::sort(v.begin(), v.end());
    v.erase(std::unique(v.begin(), v.end()), v.end());
}

/// @brief 文件名解析出的自然日；无法识别或非法日期返回 0。
uint32_t filename_day_of(const std::string& path) {
    uint32_t day = 0;
    const std::string name = std::filesystem::path(path).filename().string();
    if (!extract_date_from_filename(name, day)) return 0;
    if (!xt_time::is_valid_yyyymmdd(day)) return 0;
    return day;
}

/// @brief 冷启（一天都没入册）时的候选上界：文件名日往后 `guard_days` 天。
/// @return 全 1 表示不设上界（`filename_day` 非法，算不出自然日偏移时宁可多圈去读）。
uint32_t guard_band_ceiling(uint32_t filename_day, uint32_t guard_days) {
    if (!xt_time::is_valid_yyyymmdd(filename_day)) {
        return (std::numeric_limits<uint32_t>::max)();
    }
    return xt_time::yyyymmdd_add_days(filename_day, static_cast<int>(guard_days));
}

}  // namespace

XtTickClosureIndex build_closure_index(const std::string& root,
                                       const std::vector<std::string>& ordered_files,
                                       const std::vector<XtClosureStamp>& stamps) {
    XtTickClosureIndex index;
    index.root = root;
    index.sources.reserve(ordered_files.size());

    std::unordered_set<std::string> seen;
    seen.reserve(ordered_files.size());
    for (size_t i = 0; i < ordered_files.size(); ++i) {
        const std::string& path = ordered_files[i];
        if (!seen.insert(path).second) continue;  // 重复路径：首次出现胜出
        XtClosureSource src;
        src.path = path;
        if (i < stamps.size()) src.stamp = stamps[i];
        src.filename_day = filename_day_of(path);
        index.sources.push_back(std::move(src));
    }
    return index;
}

void carry_over_registrations(const XtTickClosureIndex& prior, XtTickClosureIndex& index) {
    std::unordered_map<std::string, const XtClosureSource*> by_path;
    by_path.reserve(prior.sources.size());
    for (const auto& s : prior.sources) by_path.emplace(s.path, &s);

    for (auto& dst : index.sources) {
        const auto it = by_path.find(dst.path);
        if (it == by_path.end()) continue;
        const XtClosureSource& src = *it->second;
        if (!src.registered) continue;
        // 签名背书清单：mtime/size 任一不同就认为内容变了，清单作废等重扫。
        if (src.stamp.mtime_ns != dst.stamp.mtime_ns || src.stamp.size != dst.stamp.size) {
            // 签名不符 = 内容可能变了，清单不能再当身份用；但那些天此刻只有这一份线索，
            // 降级留作 provisional 圈范围，等这一轮重扫用实测集合顶掉。
            dst.provisional_days = src.days;
            continue;
        }
        dst.registered = true;
        dst.days = src.days;
    }
}

bool register_source_days(XtTickClosureIndex& index, const std::string& path,
                          std::vector<uint32_t> days) {
    for (auto& s : index.sources) {
        if (s.path != path) continue;
        sort_unique(days);
        s.days = std::move(days);
        s.registered = true;
        s.provisional_days.clear();  // 实测已到手，旧线索让位
        return true;
    }
    return false;
}

std::vector<std::string> closure_files_for_range(const XtTickClosureIndex& index,
                                                 uint32_t start_day,
                                                 uint32_t end_day) {
    std::vector<std::string> out;
    for (const auto& s : index.sources) {
        if (!s.registered) continue;
        const bool hit = std::any_of(s.days.begin(), s.days.end(), [&](uint32_t d) {
            if (start_day != 0 && d < start_day) return false;
            if (end_day != 0 && d > end_day) return false;
            return true;
        });
        if (hit) out.push_back(s.path);
    }
    return out;
}

std::vector<uint32_t> closure_days(const XtTickClosureIndex& index) {
    std::vector<uint32_t> out;
    for (const auto& s : index.sources) {
        if (!s.registered) continue;
        out.insert(out.end(), s.days.begin(), s.days.end());
    }
    sort_unique(out);
    return out;
}

std::vector<uint32_t> closure_candidate_days(const XtTickClosureIndex& index) {
    // 地标 = 索引已知的「有东西的日子」：已入册的真交易日 + 全部文件名日。未入册文件的
    // 夜盘段属于它之后的第一个 session，没读过就只能拿地标里的下一天顶上。
    std::vector<uint32_t> landmarks;
    std::vector<uint32_t> out;
    for (const auto& s : index.sources) {
        if (s.registered) {
            out.insert(out.end(), s.days.begin(), s.days.end());
            landmarks.insert(landmarks.end(), s.days.begin(), s.days.end());
        }
        if (s.filename_day != 0) landmarks.push_back(s.filename_day);
    }
    sort_unique(landmarks);

    for (const auto& s : index.sources) {
        if (s.registered) continue;
        // 旧线索先进：某天可能只在这个源里出现过（它自己那个 session 的文件根本不存在），
        // 丢掉就是交易日全集缺口 —— 建库读到那天会撞 known_days 闸，整批放弃。
        // 刻意不进 landmarks：地标只用来挑「下一个 session」，多一个地标只会把上界压低。
        out.insert(out.end(), s.provisional_days.begin(), s.provisional_days.end());
        if (s.filename_day == 0) continue;
        out.push_back(s.filename_day);
        // 地标之后再无日子（语料末尾那个文件）→ 退回日历次日。多圈一天最多让调用方
        // 多规划一个空分片；少圈一天则是那一天彻底没人建，代价不对等。
        const auto it = std::upper_bound(landmarks.begin(), landmarks.end(), s.filename_day);
        out.push_back(it != landmarks.end()
                          ? *it
                          : xt_time::yyyymmdd_add_days(s.filename_day, 1));
    }
    sort_unique(out);
    return out;
}

std::vector<std::string> undated_sources(const XtTickClosureIndex& index) {
    std::vector<std::string> out;
    for (const auto& s : index.sources) {
        if (s.filename_day == 0) out.push_back(s.path);
    }
    return out;
}

std::vector<std::string> unregistered_sources(const XtTickClosureIndex& index) {
    std::vector<std::string> out;
    for (const auto& s : index.sources) {
        if (!s.registered) out.push_back(s.path);
    }
    return out;
}

bool has_unregistered_undated_source(const XtTickClosureIndex& index) {
    return std::any_of(index.sources.begin(), index.sources.end(),
                       [](const XtClosureSource& s) {
                           return s.filename_day == 0 && !s.registered;
                       });
}

XtClosureDayLookup::XtClosureDayLookup(const XtTickClosureIndex& index) : index_(index) {
    std::vector<uint32_t> filename_days;
    filename_days.reserve(index.sources.size());
    for (uint32_t i = 0; i < index.sources.size(); ++i) {
        const XtClosureSource& s = index.sources[i];
        filename_days.push_back(s.filename_day);  // 无日期记 0：条数即语料规模
        if (s.registered) {
            for (uint32_t day : s.days) by_day_[day].push_back(i);
            continue;
        }
        for (uint32_t day : s.provisional_days) provisional_by_day_[day].push_back(i);
        if (s.filename_day != 0) {
            pending_dated_.emplace_back(s.filename_day, i);
        } else {
            pending_undated_.push_back(i);
        }
    }
    std::sort(pending_dated_.begin(), pending_dated_.end());
    known_days_.reserve(by_day_.size());
    for (const auto& kv : by_day_) known_days_.push_back(kv.first);
    std::sort(known_days_.begin(), known_days_.end());
    // guard 由语料自己的文件名日集合推出（索引已持有，无额外 I/O），见
    // derive_night_carry_guard_days：休市多长，冷启候选就往后放宽多长。
    guard_days_ = derive_night_carry_guard_days(filename_days);
}

uint32_t XtClosureDayLookup::candidate_ceiling(uint32_t filename_day) const {
    // 按自然日 filename_day 命名的文件最多贡献两个交易日：它自己那天，以及当晚夜盘所属
    // 的下一个 session。已入册那部分给出的是真交易日（内容派生），取其中大于
    // filename_day 的最小者即上界；它只是真交易日集合的子集，故上界只会偏大不会偏小。
    const auto it = std::upper_bound(known_days_.begin(), known_days_.end(), filename_day);
    if (it != known_days_.end()) return *it;
    return guard_band_ceiling(filename_day, guard_days_);
}

void XtClosureDayLookup::collect_candidates(uint32_t day, std::vector<uint32_t>& picked) const {
    picked.insert(picked.end(), pending_undated_.begin(), pending_undated_.end());
    // 旧线索命中：这个源上一代就登记过 day，比文件名圈定硬——即便它的文件名日已经被
    // 上界挡在门外（例如该 session 自己的文件不存在，行全靠更早的文件承载）也要读。
    const auto prov = provisional_by_day_.find(day);
    if (prov != provisional_by_day_.end()) {
        picked.insert(picked.end(), prov->second.begin(), prov->second.end());
    }
    // 文件名日 > day 的文件承载的最早交易日就是它自己那天，含不了 day —— 从 day 往前扫。
    auto it = std::upper_bound(pending_dated_.begin(), pending_dated_.end(),
                               std::make_pair(day, (std::numeric_limits<uint32_t>::max)()));
    while (it != pending_dated_.begin()) {
        --it;
        // 上界对文件名日单调不减，故一旦越界，更早的文件只会更越界 —— 可以收手。
        if (candidate_ceiling(it->first) < day) break;
        picked.push_back(it->second);
    }
}

std::vector<std::string> XtClosureDayLookup::files_for_day(uint32_t day) const {
    const auto it = by_day_.find(day);
    if (it == by_day_.end()) return {};
    return resolve(it->second);
}

std::vector<std::string> XtClosureDayLookup::candidates_for_day(uint32_t day) const {
    std::vector<uint32_t> picked;
    collect_candidates(day, picked);
    return resolve(std::move(picked));
}

std::vector<std::string> XtClosureDayLookup::read_set_for_day(uint32_t day) const {
    std::vector<uint32_t> picked;
    const auto it = by_day_.find(day);
    if (it != by_day_.end()) picked = it->second;
    collect_candidates(day, picked);
    return resolve(std::move(picked));
}

std::vector<std::string> XtClosureDayLookup::resolve(std::vector<uint32_t> picked) const {
    sort_unique(picked);
    std::vector<std::string> out;
    out.reserve(picked.size());
    for (uint32_t i : picked) {
        if (i < index_.sources.size()) out.push_back(index_.sources[i].path);
    }
    return out;
}

std::string serialize_closure_index(const XtTickClosureIndex& index) {
    nlohmann::json j;
    j["schema"] = XtTickClosureIndex::kSchema;
    j["root"] = index.root;
    nlohmann::json sources = nlohmann::json::array();
    for (const auto& s : index.sources) {
        nlohmann::json e;
        e["path"] = s.path;
        e["mtime_ns"] = s.stamp.mtime_ns;
        e["size"] = s.stamp.size;
        e["registered"] = s.registered;
        e["days"] = s.days;
        sources.push_back(std::move(e));
    }
    j["sources"] = std::move(sources);
    // dump(2)：索引是人要读的 forensic 产物；nlohmann::json 的对象键按字典序输出，
    // 同一索引恒得同一字节串（generation digest 对照依赖这一点）。
    return j.dump(2) + "\n";
}

bool parse_closure_index(const std::string& text, XtTickClosureIndex& out) {
    nlohmann::json j = nlohmann::json::parse(text, nullptr, /*allow_exceptions=*/false);
    if (j.is_discarded() || !j.is_object()) return false;

    if (!j.contains("schema") || !j["schema"].is_number_unsigned()) return false;
    if (j["schema"].get<uint32_t>() != XtTickClosureIndex::kSchema) return false;
    if (!j.contains("root") || !j["root"].is_string()) return false;
    if (!j.contains("sources") || !j["sources"].is_array()) return false;

    XtTickClosureIndex parsed;
    parsed.root = j["root"].get<std::string>();
    std::unordered_set<std::string> seen;
    for (const auto& e : j["sources"]) {
        if (!e.is_object()) return false;
        if (!e.contains("path") || !e["path"].is_string()) return false;
        if (!e.contains("mtime_ns") || !e["mtime_ns"].is_number_unsigned()) return false;
        if (!e.contains("size") || !e["size"].is_number_unsigned()) return false;
        if (!e.contains("registered") || !e["registered"].is_boolean()) return false;
        if (!e.contains("days") || !e["days"].is_array()) return false;

        XtClosureSource s;
        s.path = e["path"].get<std::string>();
        if (s.path.empty()) return false;
        // 路径重复 = 索引与源集不配套（清单归属不唯一），整份不要。
        if (!seen.insert(s.path).second) return false;
        s.stamp.mtime_ns = e["mtime_ns"].get<uint64_t>();
        s.stamp.size = e["size"].get<uint64_t>();
        s.registered = e["registered"].get<bool>();

        uint32_t prev = 0;
        for (const auto& d : e["days"]) {
            if (!d.is_number_unsigned()) return false;
            const auto day = d.get<uint64_t>();
            if (day == 0 || day > 99999999ull) return false;
            // 升序且无重复：清单要倒排成 day → files，乱序说明写它的不是本实现。
            if (prev != 0 && day <= prev) return false;
            prev = static_cast<uint32_t>(day);
            s.days.push_back(static_cast<uint32_t>(day));
        }
        // 未入册就没有内容派生的日集合可言；带着日集合说明这份索引不是本实现写的。
        if (!s.registered && !s.days.empty()) return false;

        // 文件名日是路径的函数，不入 JSON——存了就多一份可能与路径矛盾的真相。
        s.filename_day = filename_day_of(s.path);
        parsed.sources.push_back(std::move(s));
    }

    out = std::move(parsed);
    return true;
}

bool write_closure_index(const std::string& path, const XtTickClosureIndex& index) {
    return PathUtil::write_atomic(path, serialize_closure_index(index), 0644);
}

bool read_closure_index(const std::string& path, XtTickClosureIndex& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return false;
    std::ostringstream oss;
    oss << in.rdbuf();
    if (!in.good() && !in.eof()) return false;
    return parse_closure_index(oss.str(), out);
}
