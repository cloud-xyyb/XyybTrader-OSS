#pragma once

/// @file
/// @brief AccountStateStore 的 hazard 发布帧与 slot 表容量常量。
///
/// 属于 store 的并发实现细节，不是账户对外快照 API：业务读侧一律走
/// AccountStateStore::snapshot_account() / snapshot_position() /
/// snapshot_account_and_position()。

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "xt_account/api/AccountSnapshot.h"

namespace xt_account {

inline constexpr std::size_t kMaxSymbols = 100;
inline constexpr std::size_t kInvalidSymbolIndex = static_cast<std::size_t>(-1);

/// @brief 账户级 hazard 发布帧，承载 account 快照和 per-symbol slot 表。
///
/// `universe` 指向 store 的 frozen symbol universe（字典序）；`position_slots`
/// 与其同下标。本对象的 hazard guard 只保护容器和 slot 数组本身，不保护 slot
/// 指向的 `PositionSnapshot` 生命周期。
/// @warning 跨线程读取单合约快照必须走 AccountStateStore::snapshot_position() /
///          snapshot_account_and_position()；它们会做二级 hazard protect 和 current re-confirm。
struct FullSnapshot {
    AccountSnapshot account{};
    mutable std::array<std::atomic<const PositionSnapshot*>, kMaxSymbols> position_slots{};

    const std::vector<std::string>* universe = nullptr;

    FullSnapshot() noexcept {
        reset_slots();
    }

    void reset_slots() noexcept {
        for (auto& slot : position_slots) {
            slot.store(nullptr, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] std::size_t find_symbol_idx(std::string_view code) const noexcept {
        if (!universe) {
            return kInvalidSymbolIndex;
        }
        const auto it = std::lower_bound(
            universe->begin(),
            universe->end(),
            code,
            [](const std::string& lhs, std::string_view rhs) {
                return std::string_view(lhs) < rhs;
            });
        if (it == universe->end() || std::string_view(*it) != code) {
            return kInvalidSymbolIndex;
        }
        const auto idx = static_cast<std::size_t>(std::distance(universe->begin(), it));
        return idx < kMaxSymbols ? idx : kInvalidSymbolIndex;
    }
};

}
