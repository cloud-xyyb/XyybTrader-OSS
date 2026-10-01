#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <vector>

namespace xt_identity {

/// @brief 策略字符串 ID 到整数 sid_id 的注册表。
///
/// 多策略部署里，订单和回报都要标明归属哪个策略；直接传 std::string 策略名在热路径和
/// wire/report 上都嫌贵。本注册表在 bootstrap 阶段按配置顺序给每个策略名分配一个稳定的
/// uint32 sid_id，热路径此后只传这个整数。sid_id 0 保留给空 sid / single-strategy。
///
/// `freeze()` 后只服务 lookup 和反查；`global()` 是 non-owning process-global 指针，由装配方
/// 设置和清理。
class StrategySidRegistry {
public:
    /// @brief 空 sid / single-strategy 的 reserved id。
    static constexpr uint32_t kNoSidId = 0;

    /// @brief 操作面手工单（panel manual order）的固定保留 sid_id。
    ///
    /// 保留域高值，永不由顺序分配产生（顺序分配从 1 起、每策略 +1，触及此值需 ~4.29e9
    /// 个策略，不可达）；不占顺序 1..N 域、不进 `id_to_name_`、不计入 `sid_slot_count()`、
    /// 与 `freeze()` 无耦合。内建 lookup/name/resolve 特判承载，路由侧据此豁免策略派发。
    static constexpr uint32_t kManualSidId = 0xFFFF'FFFEu;
    /// @brief 保留 sid_id 对应的策略字符串标识。
    static constexpr const char* kManualStrategyId = "_manual";

    StrategySidRegistry() : id_to_name_{""} {
    }

    /// @brief 注册或查询 sid；用于 bootstrap 期 string -> uint32 翻译。
    /// @return 空 sid 返回 0；保留名 `_manual` 恒返 kManualSidId（内建映射，不注册进
    ///         顺序域）；已注册 sid 返回原 id；freeze 后新 sid 返回 0 并写 stderr。
    uint32_t register_or_lookup(const std::string& sid_string) {
        if (sid_string == kManualStrategyId) return kManualSidId;
        if (sid_string.empty()) return kNoSidId;
        auto it = name_to_id_.find(sid_string);
        if (it != name_to_id_.end()) return it->second;
        if (frozen_) {
            // Fail-soft：已注册 sid 仍在上方命中；这里只拒绝 freeze 后新增 sid。
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

    /// @brief 锁定 registry，禁止新增 sid；可重复调用。
    void freeze() noexcept { frozen_ = true; }

    /// @brief registry 是否已经只读。
    bool is_frozen() const noexcept { return frozen_; }

    /// @brief 只读查询 string sid -> uint32 id。
    /// @return 保留名 `_manual` 恒返 kManualSidId；未注册或空 sid 返回 kNoSidId。
    uint32_t lookup(const std::string& sid_string) const {
        if (sid_string == kManualStrategyId) return kManualSidId;
        auto it = name_to_id_.find(sid_string);
        return it != name_to_id_.end() ? it->second : kNoSidId;
    }

    /// @brief 反查 sid_id -> string sid。
    /// @return 保留值返回静态 `_manual`（不入 `id_to_name_`）；越界返回 reserved empty string。
    const std::string& name(uint32_t sid_id) const {
        if (sid_id == kManualSidId) return manual_reserved_name();
        return sid_id < id_to_name_.size() ? id_to_name_[sid_id] : id_to_name_[0];
    }

    /// @brief sid slot 容量，包含 reserved sid_id 0。
    ///
    /// 用于按 sid_id 直接索引的 per-sid vector 初始化；不表示已注册策略数量。
    std::size_t sid_slot_count() const noexcept { return id_to_name_.size(); }

    /// @brief 返回进程级 registry 指针；nullptr 表示尚未装配。
    static StrategySidRegistry* global() noexcept { return g_global_; }

    /// @brief 设置进程级 registry 指针；调用方持有 registry 生命周期。
    /// @warning 仅用于 bootstrap / test setup / teardown；运行期读侧不加锁。
    static void set_global(StrategySidRegistry* r) noexcept { g_global_ = r; }

    /// @brief 通过 global registry 将 sid_id 解析为 C string。
    /// @return sid_id 为 0、global 未设置或名称为空时返回 nullptr。
    /// @warning 返回指针指向 registry 内部 string；registry 析构前必须清空 global。
    static const char* resolve_sid_cstr_or_null(uint32_t sid_id) noexcept {
        if (sid_id == kNoSidId) return nullptr;
        // 保留值特判置于 global-null 判断之前：无需 registry 即可反解静态串。
        if (sid_id == kManualSidId) return kManualStrategyId;
        auto* reg = global();
        if (!reg) return nullptr;
        const std::string& s = reg->name(sid_id);
        return s.empty() ? nullptr : s.c_str();
    }

private:
    /// @brief 保留值 `_manual` 的静态 std::string 载体（进程生命周期，线程安全一次初始化）。
    ///
    /// `name()` 返回 `const std::string&`，保留值特判需一个不入 `id_to_name_` 的稳定 string。
    static const std::string& manual_reserved_name() {
        static const std::string s(kManualStrategyId);
        return s;
    }

    /// index = sid_id；index 0 保留为空 sid。
    std::vector<std::string> id_to_name_;
    /// string sid -> sid_id。
    std::unordered_map<std::string, uint32_t> name_to_id_;
    /// 仅 bootstrap 线程写，读侧依赖 freeze 后不再变更。
    bool frozen_ = false;

    /// non-owning process-global pointer。
    inline static StrategySidRegistry* g_global_ = nullptr;
};

} // namespace xt_identity
