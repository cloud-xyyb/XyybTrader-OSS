#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string_view>

#include "xt_core/order/XtInstrumentRegistry.h"

namespace xt_account {

/// @brief AccountStateStore 内部的 mark price side table。
///
/// 管理 code -> dense id 映射和两个独立估值源；估值源由 store worker 单线程写入，
/// id 注册由 registry 内部锁串行（`intern_for_slot` 可从 submit 线程调）。
/// freeze() 后行情/持仓/成交入口不再为未知 code 建 id；新 id 只经 `intern_for_slot`
/// （position slot admission）产生，其余未知 code 的更新被丢弃并诊断。
///
/// 双数组语义（估值与执行定价分流的基座）：
/// - `marks_[id]`：**只**由行情 tick 写入，0 = 从未收到过真实行情。任何 fallback 都不得
///   写进该数组——venue 真值一经落地永不被回退值覆盖。
/// - `cost_hint_[id]`：成本价 fallback，由 store 在事件 apply 之后按持仓终态派生写入
///   （单腿 → 该腿均价；双腿锁仓 / 空仓 → 0）；0 = 无可用 fallback（失效时显式写 0，
///   不留陈旧值）。事件入口只登记 symbol，不直接把 caller 给的价格当 hint。
/// - 估值读 `effective_mark()`（venue 优先、次 cost hint）；下单定价读
///   `venue_mark_price()`，缺 venue 真值时由调用方显式拒绝，不按成本价出单。
///
/// @note symbol universe 的唯一 owner 是 AccountStateStore；本表只维护 mark 语义，
///       并通过 on_* 的返回值告诉 store 哪些 code 需要并入 universe。
class MarkPriceTable {
public:
    /// @brief on_* 事件入口对 code 的 universe 归属判定。
    enum class TrackResult : uint8_t {
        kIgnored,     ///< 空 code / 非正行情价 / frozen 后未知 code：不进 universe，非错误
        kTracked,     ///< 该 code 已有 instrument id（既有或本次注册）
        kInternFull,  ///< pre-freeze 惰性注册时 intern 表容量耗尽（调用方必须 fail-fast）
    };

    /// @brief 清空 pre-freeze 的 registry、venue mark 与 cost hint；冻结后为 no-op。
    /// @note 仅用于测试或启动期初始化；账户 recovery 的 Reset 事件走 clear_cost_hints()，
    ///       不清 venue-truth mark。不影响 store 侧 symbol universe——universe 由
    ///       AccountStateStore 单独持有。
    void reset();
    /// @brief 启动期单 symbol 预注册；用于 AccountStateStore::register_symbol_() 的冷路径。
    /// @param code 合约代码；空 code 会被忽略（返回 true），string_view 可非 null-terminated。
    /// @return false 表示 intern 表容量耗尽，该 code 拿不到 id（调用方必须 fail-fast）。
    /// @warning 必须在 freeze() 前调用；冻结后继续注册会破坏 snapshot 下标稳定契约。
    [[nodiscard]] bool pre_warm_one(std::string_view code);
    /// @brief 为 position slot admission 注册 code；冻结后仍生效。
    /// @param code 合约代码；空 code 会被忽略（返回 true）。
    /// @return false 表示 intern 表容量耗尽，该 code 拿不到 id（调用方必须 fail-closed）。
    /// @note 冻结后**唯一**允许 intern 的入口：slot 持有者必须先有 mark id，否则它的
    ///       估值恒 0 且不可修复（frozen 后 on_market_tick 不再为未知 code 建 id）。
    ///       行情/成交/持仓事件入口一律不在冻结后 intern。
    /// @note 任意线程可调：registry 写入由其内部锁串行。
    [[nodiscard]] bool intern_for_slot(std::string_view code);
    /// @brief 结束启动期注册，切换为 frozen update 语义。
    /// @note frozen 后 on_market_tick()/on_set_position()/on_trade() 不再为未知 code 建 slot。
    void freeze() noexcept { frozen_.store(true, std::memory_order_release); }
    /// @brief 返回 symbol universe 是否已冻结。
    [[nodiscard]] bool is_frozen() const noexcept { return frozen_.load(std::memory_order_acquire); }

    /// @brief 清空全部成本价 fallback，保留 venue mark。
    /// @note 账户 recovery 的 Reset 语义：持仓域整体擦除重建，旧持仓的成本价随之失效，
    ///       留着会让 venue 缺失的 code 按陈旧成本价估值；venue 行情锚点与持仓无关必须留存。
    void clear_cost_hints() noexcept;

    /// @brief 用行情 tick 更新 symbol 的 venue mark price。
    /// @param code non-owning 合约代码；nullptr 或空串会被忽略。
    /// @param mark_price fixed-point mark；必须大于 0 才会覆盖已有值。
    /// @return 该 code 的 universe 归属判定；kTracked 表示调用方需保证已并入。
    /// @note pre-freeze 未知 code 会被注册；frozen 后未知 code 会被丢弃。cost hint 不受影响。
    TrackResult on_market_tick(const char* code, int64_t mark_price);
    /// @brief 登记持仓快照涉及的 symbol，不改变任何估值源。
    /// @param code non-owning 合约代码；nullptr 或空串会被忽略。
    /// @return 该 code 的 universe 归属判定；kTracked 表示调用方需保证已并入。
    /// @note 持仓快照携带的价格不是行情，也不直接作为 cost hint：hint 由调用方在
    ///       reducer apply 之后经 update_cost_hint() 按持仓终态派生（与成交同路）。
    TrackResult on_set_position(const char* code);
    /// @brief 登记成交涉及的 symbol，不改变任何估值源。
    /// @param code non-owning 合约代码；nullptr 或空串会被忽略。
    /// @return 该 code 的 universe 归属判定；kTracked 表示调用方需保证已并入。
    /// @note 成交事件本身没有可靠 mark 输入；持仓变动后的 cost hint 由调用方在 reducer
    ///       apply 之后经 update_cost_hint() 按新仓位重算写入。
    TrackResult on_trade(const char* code);

    /// @brief 覆盖已注册 symbol 的成本价 fallback。
    /// @param code non-owning 合约代码；未注册 code 为 no-op。
    /// @param cost_hint fixed-point 成本价；0 表示显式失效。
    void update_cost_hint(const char* code, int64_t cost_hint) noexcept;

    /// @brief 返回 symbol 的 venue 真实行情价（下单定价的唯一合法来源）。
    /// @param code non-owning 合约代码；nullptr 或空串返回 0。
    /// @return fixed-point mark；0 表示未知 symbol 或从未收到过真实行情。
    [[nodiscard]] int64_t venue_mark_price(const char* code) const;
    /// @brief 返回估值口径的 mark：venue 优先，缺失时回退成本价。
    /// @param code non-owning 合约代码；nullptr 或空串返回 0。
    /// @return fixed-point mark；0 表示两个来源都没有可用值。
    /// @note 仅供估值/展示（unrealized、equity、帧内 mark_price、对账）。两次原子读不构成
    ///       整体原子：venue 恰落地的一拍内可能仍返回 hint，下一次读即收敛，无撕裂值。
    [[nodiscard]] int64_t effective_mark(const char* code) const;
    /// @brief 返回 symbol 当前成本价 fallback（诊断/测试用）。
    [[nodiscard]] int64_t cost_hint(const char* code) const;
    /// @brief code 是否已有 mark id（估值可用性判据）；lookup-only，不注册。
    /// @note 无 id ⇒ venue mark 与 cost hint 都无处可挂，`effective_mark` 恒 0——
    ///       消费方据此区分「真无价」与「价为 0」。任意线程可读（探测链无锁）。
    [[nodiscard]] bool has_mark_id(const char* code) const noexcept {
        return registry_.id_of(code) != xt_core::kInvalidInstrumentId;
    }

private:
    /// @brief frozen 后遇到未知 code 的统一告警出口（整表一次）。
    /// @note 不 assert：freeze 后收到未知合约在生产上真实可达（如 CTP 跨日新合约经
    ///       recovery 快照灌入）。逐 code 记名由 AccountStateStore 的有界 quarantine
    ///       注册表承担，这里只发一次 mark 侧的丢弃说明，不再自持 code 集合。
    void warn_unknown_frozen_code_(const char* origin, const char* code);
    /// @brief 解析 code 的 instrument id：frozen 下未知 code 只告警，pre-freeze 惰性注册。
    /// @param origin 事件入口名，进 frozen 未知 code 告警。
    /// @param out_id 命中或新注册的 id；非 kTracked 时为 kInvalidInstrumentId。
    TrackResult resolve_id_(const char* origin, const char* code,
                            xt_core::XtInstrumentId& out_id);

    /// venue 真实行情价（fp）；0 = 从未收到过真实行情。只由 on_market_tick 写。
    alignas(64) std::array<std::atomic<int64_t>, xt_core::kMaxInstruments + 1> marks_{};
    /// 成本价 fallback（fp）；0 = 无可用 fallback。venue 缺失时供估值回退。
    alignas(64) std::array<std::atomic<int64_t>, xt_core::kMaxInstruments + 1> cost_hint_{};

    xt_core::XtInstrumentRegistry registry_; ///< code -> dense id；写入由 registry 内部锁串行。

    bool warned_unknown_frozen_ = false; ///< freeze 后未知 code 告警只发一次。

    std::atomic<bool> frozen_{false}; ///< true 表示 symbol 集合已封口，on_* 不再注册未知 code。
};

}  // namespace xt_account
