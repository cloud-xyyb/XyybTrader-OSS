#pragma once

/// @file
/// @brief PositionUniverse 的世代化符号表：append-only slot 表 + 单指针世代发布。
///
/// 属于 AccountStateStore 的并发实现细节，不是账户对外 API：业务读侧一律走
/// AccountStateStore::snapshot_position() / snapshot_tracked_codes()。

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace xt_account {

/// position slot 容量：`FullSnapshot::position_slots` 定长数组长度，也是
/// PositionUniverse 的硬上限。消耗口径 = 三个 slot 创建源（recovery 恢复仓 /
/// worker fill 与 set_position / submit 预留）的 distinct code 并集。
inline constexpr std::size_t kMaxPositionSlots = 1024;

/// 无效 slot 下标；`find_slot` / `FullSnapshot::find_symbol_idx` 的未命中返回值。
inline constexpr std::size_t kInvalidSymbolIndex = static_cast<std::size_t>(-1);

/// @brief 符号表的一个世代；发布后 immutable，且进程生命周期内不释放。
///
/// 读者只经 `SymbolTable::active()` 拿到本对象的 const 指针，随后无 pin 直接读：
/// 世代常驻使 use-after-free 结构性不可达（不变量 1）。`by_slot` 是发布/构帧循环
/// 取 code 的唯一途径——读者永远不触碰 SymbolTable 内部的 deque。
struct SymbolGeneration {
    /// @brief 按 code 字典序排列的查找项。
    struct Entry {
        const std::string* code;  ///< 指向 SymbolTable 持有的 code（地址永久稳定）
        uint32_t slot;            ///< 该 code 的 slot 下标
    };

    uint32_t n = 0;                              ///< active slot 数；active slot 恒为 [0,n) 稠密前缀
    std::vector<const std::string*> by_slot;     ///< slot → code（追加序）
    std::vector<Entry> sorted_by_code;           ///< code → slot（字典序，二分查找源）

    /// @brief 查 code 的 slot 下标；未命中返回 kInvalidSymbolIndex。
    /// @note 读路径（每次 snapshot_position 一次）：留在 header 内联，省一次跨 TU 调用。
    [[nodiscard]] std::size_t find_slot(std::string_view code) const noexcept {
        const auto it = std::lower_bound(sorted_by_code.begin(), sorted_by_code.end(), code,
                                         [](const Entry& lhs, std::string_view rhs) {
                                             return std::string_view(*lhs.code) < rhs;
                                         });
        if (it == sorted_by_code.end() || std::string_view(*it->code) != code) {
            return kInvalidSymbolIndex;
        }
        return static_cast<std::size_t>(it->slot);
    }
};

/// @brief PositionUniverse 的唯一 owner：append-only slot 分配 + 世代发布。
///
/// 并发契约：
/// - 写（`ensure_slot` / `ensure_slots`）由内部 `append_mu_` 串行，**任意线程可调**
///   （submit 线程预留 slot 与 worker 线程入账走同一入口）；
/// - 读（`active` / `find_slot` / `size`）无锁：单指针 acquire load 后直接用 immutable
///   世代对象，读者永不触碰 `codes_`；
/// - slot 下标跨世代永久稳定：slot i ≡ 第 i 个被收编的 symbol，只增不改不回收；
/// - 世代常驻：已发布世代挂在 `retained_` 上直到本对象析构，读者手里的指针恒有效。
///
/// @warning `initialize()` 与析构不得与任何并发读写同时发生（装配 / 停机边界专用）。
class SymbolTable {
public:
    SymbolTable() = default;
    ~SymbolTable() = default;
    SymbolTable(const SymbolTable&) = delete;
    SymbolTable& operator=(const SymbolTable&) = delete;
    SymbolTable(SymbolTable&&) = delete;
    SymbolTable& operator=(SymbolTable&&) = delete;

    /// @brief 发布 gen-0（空世代），启用 slot 语义。
    /// @pre 只调用一次，且此刻无并发读写（symbol 集合冻结点）。
    /// @note gen-0 不预分配 slot：PositionUniverse 的成员只来自三个创建源，
    ///       declared 但未 touched 的 symbol 无 slot，读侧与「零值 slot」值级等价。
    void initialize();

    /// @brief 当前活世代；nullptr 表示尚未 initialize（slot 语义未启用）。
    [[nodiscard]] const SymbolGeneration* active() const noexcept {
        return gen_.load(std::memory_order_acquire);
    }

    /// @brief 已发布世代数（含 gen-0）；世代常驻内存上界 ≈ Σ(各世代 slot 数)×24B。
    /// @note 诊断/钉用：批 admission 的「整批只造一个世代」从外部只能经本计数观测。
    /// @note 计数在世代发布之后 release 写入：acquire 读到 K ⇒ 第 K 代已对 active() 可见。
    [[nodiscard]] std::size_t generation_count() const noexcept {
        return generations_.load(std::memory_order_acquire);
    }

    /// @brief 活世代的 active slot 数（`xt_position_slots_used` 的取数口）。
    [[nodiscard]] std::size_t size() const noexcept {
        const auto* g = active();
        return g ? g->n : 0;
    }

    /// @brief 活世代成员判定；未 initialize 或未命中返回 kInvalidSymbolIndex。
    /// @note 只用于成员判定与幂等短路；**帧内 slot 定位必须用帧自身的世代**（不变量 2）。
    [[nodiscard]] std::size_t find_slot(std::string_view code) const noexcept {
        const auto* g = active();
        return g ? g->find_slot(code) : kInvalidSymbolIndex;
    }

    /// @brief 幂等地为 code 分配 slot。
    /// @param code 合约代码；空 code 返回 kInvalidSymbolIndex。
    /// @return slot 下标；容量耗尽或未 initialize 返回 kInvalidSymbolIndex。
    /// @note 任意线程可调；已有 slot 时走无锁快路径不进锁。
    std::size_t ensure_slot(std::string_view code);

    /// @brief 批量 admission：一次锁内追加全批，只发布一个新世代。
    /// @param codes 候选 code；调用方负责去重（重复项在批内被跳过，不占额外 slot）。
    /// @param out_rejected 因容量耗尽未获 slot 的 code（按输入序追加）；可为 nullptr。
    /// @return 本批新建的 slot 数。
    /// @note 批 admission 把「N 个新 code」压成一个新世代，既降低世代常驻内存，
    ///       也让 recovery 批只对读侧产生一次成员面跃迁。
    std::size_t ensure_slots(const std::vector<std::string_view>& codes,
                             std::vector<std::string>* out_rejected);

private:
    /// @brief 用 `codes_` 当前内容构造并发布新世代。
    /// @pre 持有 `append_mu_`，且 `codes_` 已完成本次追加。
    void publish_generation_locked_();

    std::mutex append_mu_;                                   ///< 只串行追加者；读路径不取。
    std::deque<std::string> codes_;                          ///< slot → code 的权威存储；**仅持锁写者触碰**（元素地址永不失效）。
    std::vector<std::unique_ptr<SymbolGeneration>> retained_; ///< 常驻世代链；只在持锁追加时 push，析构统一回收。
    std::atomic<const SymbolGeneration*> gen_{nullptr};       ///< 唯一发布点：单指针 release/acquire。
    std::atomic<std::size_t> generations_{0};                ///< 已发布世代数（诊断）。
};

}  // namespace xt_account
