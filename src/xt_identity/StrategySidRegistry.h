#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <vector>

namespace xt_identity {

/// @brief 为策略字符串 ID 分配稳定的 sid_id。
///
/// StrategySidRegistry 在 bootstrap 阶段按配置顺序注册策略 ID，并把字符串 sid
/// 转为适合热路径和 wire/report 传递的 uint32 sid_id。
/// sid_id 0 保留给空 sid / single-strategy。
///
/// `freeze()` 后 registry 只服务 lookup 和 reverse lookup；
/// `global()` 是 non-owning process-global 指针，由装配方设置和清理。
class StrategySidRegistry {
public:

    static constexpr uint32_t kNoSidId = 0;

    StrategySidRegistry() : id_to_name_{""} {
    }

    uint32_t register_or_lookup(const std::string& sid_string) {
        if (sid_string.empty()) return kNoSidId;
        auto it = name_to_id_.find(sid_string);
        if (it != name_to_id_.end()) return it->second;
        if (frozen_) {

            std::fprintf(stderr,
                "[xt_identity::StrategySidRegistry] register_or_lookup('%s') after freeze() — "
                "rejecting new sid (returning kNoSidId=0);ops: add sid to yaml then restart\n",
                sid_string.c_str());
            return kNoSidId;
        }
        const uint32_t id = static_cast<uint32_t>(id_to_name_.size());
        id_to_name_.push_back(sid_string);
        name_to_id_[sid_string] = id;
        return id;
    }

    void freeze() noexcept { frozen_ = true; }

    bool is_frozen() const noexcept { return frozen_; }

    uint32_t lookup(const std::string& sid_string) const {
        auto it = name_to_id_.find(sid_string);
        return it != name_to_id_.end() ? it->second : kNoSidId;
    }

    const std::string& name(uint32_t sid_id) const {
        return sid_id < id_to_name_.size() ? id_to_name_[sid_id] : id_to_name_[0];
    }

    std::size_t sid_slot_count() const noexcept { return id_to_name_.size(); }

    static StrategySidRegistry* global() noexcept { return g_global_; }

    static void set_global(StrategySidRegistry* r) noexcept { g_global_ = r; }

    static const char* resolve_sid_cstr_or_null(uint32_t sid_id) noexcept {
        if (sid_id == kNoSidId) return nullptr;
        auto* reg = global();
        if (!reg) return nullptr;
        const std::string& s = reg->name(sid_id);
        return s.empty() ? nullptr : s.c_str();
    }

private:

    std::vector<std::string> id_to_name_;

    std::unordered_map<std::string, uint32_t> name_to_id_;

    bool frozen_ = false;

    inline static StrategySidRegistry* g_global_ = nullptr;
};

}
