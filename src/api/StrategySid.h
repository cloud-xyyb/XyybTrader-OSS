#pragma once

#include <cstdint>
#include <type_traits>

/// @brief 策略 sid 的强类型包装。
///
/// raw id `0` 是 no-sid / single-strategy sentinel。multi-strategy caller
/// 必须显式传 `StrategySid`，避免 API 默认参数静默退回 sid 0。
///
/// @note 非 0 sid_id 的规范来源是 `StrategySidRegistry`；`from_raw()` 用于把
///       已分配、已持久化或 wire DTO 中的 raw sid_id 重新包装为 typed value。
struct StrategySid {
    constexpr StrategySid() noexcept = default;

    static constexpr StrategySid legacy() noexcept { return StrategySid{}; }
    static constexpr StrategySid from_raw(uint32_t sid_id) noexcept {
        return StrategySid(sid_id);
    }

    [[nodiscard]] constexpr uint32_t raw() const noexcept { return sid_id_; }
    [[nodiscard]] constexpr bool has_value() const noexcept { return sid_id_ != 0; }

private:
    explicit constexpr StrategySid(uint32_t sid_id) noexcept : sid_id_(sid_id) {}

    uint32_t sid_id_ = 0;
};

static_assert(std::is_trivially_copyable<StrategySid>::value,
              "StrategySid must stay trivially copyable");
