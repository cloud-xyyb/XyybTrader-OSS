#pragma once

/// @file
/// @brief AccountStateStore 的 hazard 发布帧与容量常量。
///
/// 属于 store 的并发实现细节，不是账户对外快照 API：业务读侧一律走
/// AccountStateStore::snapshot_account() / snapshot_position() /
/// snapshot_account_and_position()。

#include <array>
#include <atomic>
#include <cstddef>
#include <string_view>

#include "xt_account/api/AccountSnapshot.h"
#include "xt_account/state/detail/AccountSymbolTable.h"
#include "xt_core/order/XtInstrumentRegistry.h"

namespace xt_account {

/// DeclaredUniverse 规模上限（超过则启动期 fail-fast）。mark registry 的 intern id
/// 共 `xt_core::kMaxInstruments`(8192) 个，其中 `kMaxPositionSlots`(1024) 个结构性
/// 预留给「持有仓位但未声明」的 foreign symbol，使「slot 持有者 mark 注册失败」
/// 不可达；declared 上限因此为 8192 − 1024 = 7168。
/// @note 与 `xt_live::kMaxDeclaredSymbols` 同值；config 侧 validator 与 store 侧
///       注册/冻结双闸各自持有一份判据。
inline constexpr std::size_t kMaxDeclaredSymbols = 7168;
static_assert(kMaxDeclaredSymbols + kMaxPositionSlots == xt_core::kMaxInstruments,
              "容量算术：declared 上限 = mark intern 总量 − foreign slot 结构性预留；"
              "任一常量变更必须另立 revision 联动重算（F02 spec D3）");

/// @brief 账户级 hazard 发布帧，承载 account 快照和 per-slot 持仓表。
///
/// `universe` 是本帧采纳的符号表世代，`position_slots` 与其 slot 下标同序。本对象的
/// hazard guard 只保护容器和 slot 数组本身，不保护 slot 指向的 `PositionSnapshot`
/// 生命周期。
/// @warning 帧内 slot 定位一律用**本帧自己的** `universe`（不变量 2）：活世代可能已
///          追加新 slot，用它去索引旧帧会越过该帧的有效前缀。
/// @warning 跨线程读取单合约快照必须走 AccountStateStore::snapshot_position() /
///          snapshot_account_and_position()；它们会做二级 hazard protect 和 current re-confirm。
struct FullSnapshot {
    AccountSnapshot account{};
    mutable std::array<std::atomic<const PositionSnapshot*>, kMaxPositionSlots> position_slots{};
    /// non-owning：本帧采纳的符号表世代。世代 immutable 且进程内常驻，生命周期覆盖
    /// 任意发布帧，故存指针而非逐帧拷贝。nullptr 表示该帧无 universe（池内未初始化）。
    const SymbolGeneration* universe = nullptr;

    FullSnapshot() noexcept {
        reset_slots();
    }

    /// @brief 清空 slot 指针，供 snapshot pool 复用对象前调用。
    /// @note 全容量置 null（不只清 active 前缀）：新帧采纳的世代可能比旧帧短或长，
    ///       残留指针会让越界 slot 读到上一轮的快照。
    void reset_slots() noexcept {
        for (auto& slot : position_slots) {
            slot.store(nullptr, std::memory_order_relaxed);
        }
    }

    /// @brief 在本帧世代中查找 symbol 的 slot 下标；未绑定世代时返回无效下标。
    [[nodiscard]] std::size_t find_symbol_idx(std::string_view code) const noexcept {
        if (!universe) {
            return kInvalidSymbolIndex;
        }
        const auto idx = universe->find_slot(code);
        return idx < kMaxPositionSlots ? idx : kInvalidSymbolIndex;
    }
};

}  // namespace xt_account
