#pragma once

/// @file
/// @brief 策略换月辅助组件

#include "api/IStrategyContext.h"
#include "xt_core/order/XtOrderTypes.h"

#include <algorithm>  // std::max for max_observed_order_qty_fp_ tracking
#include <cassert>    // debug-build invariant assertion in on_update
#include <cstdint>
#include <cstdlib>    // std::llabs for delta-vs-threshold check
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace xt_rollover {

/// @brief 跨物理合约的 logical position 聚合器。
///
/// 换月后新旧合约可能同时有仓位，策略下单判断通常需要跨合约 net position。
/// 本类型只维护策略本地 `code -> qty_fp` 槽位；写入为 REPLACE semantics。
///
/// 线程语义：策略线程单写，策略回调内读；不提供内部同步。
class PositionAggregator {
public:
    /// @brief 仓位跳变诊断回调。
    ///
    /// `PositionAggregator` 采用 REPLACE semantics：同一 `(sid, code)` 只能有一个
    /// canonical emitter。大幅跳变通常意味着重复 emitter 或 qty 口径混用；debug build
    /// 会 assert，release build 通过本 hook 上报 non-fatal 计数。
    ///
    /// 使用函数指针是为了让策略 plugin 不直接链接 xt_metrics / prometheus-cpp。
    using JumpReporter = void(*)(const char* sid, const char* code,
                                 int64_t prev_qty_fp, int64_t new_qty_fp);

    /// @brief 进程级 jump reporter hook；nullptr 表示 no-op。
    ///
    /// runner 在策略 worker 创建前设置一次；策略线程只读，因此普通函数指针足够。
    static void set_jump_reporter(JumpReporter reporter) { jump_reporter_ = reporter; }

    /// @brief 跳变阈值下限；避免首次 observe_order_qty 前的冷启动仓位触发误报。
    static constexpr int64_t kMinJumpThresholdFp = 1'000'000;  // 1.0 in fp

    /// @brief 设置 jump reporter 使用的 strategy_id label；空字符串表示默认标签。
    void set_sid(std::string sid) { sid_ = std::move(sid); }
    const std::string& sid() const { return sid_; }

    /// @brief 记录策略已提交过的最大订单量，用于按真实 lot size 缩放跳变阈值。
    void observe_order_qty(int64_t qty_fp) {
        const int64_t abs_q = qty_fp < 0 ? -qty_fp : qty_fp;
        if (abs_q > max_observed_order_qty_fp_) {
            max_observed_order_qty_fp_ = abs_q;
        }
    }
    int64_t max_observed_order_qty_fp() const { return max_observed_order_qty_fp_; }

    /// @brief 写入/创建某合约的仓位槽。
    ///
    /// REPLACE semantics：新 qty 覆盖旧 qty。若 delta 超过动态阈值，debug build
    /// assert，release build 通过 jump_reporter_ 做 non-fatal 观测。
    void on_update(const char* code, int64_t qty_fp) {
        if (!code || code[0] == '\0') return;
        const std::string code_s(code);
        if (auto it = slots_.find(code_s); it != slots_.end()) {
            const int64_t prev = it->second;
            // fp lot size 远小于 INT64_MAX/2，差值绝对值不会在正常交易量下溢出。
            const int64_t delta = std::llabs(qty_fp - prev);
            const int64_t threshold = std::max(max_observed_order_qty_fp_ * 2,
                                               kMinJumpThresholdFp);
            if (delta >= threshold) {
#ifdef XT_DEBUG_INVARIANTS
                assert(delta < threshold &&
                       "PositionAggregator: implausibly large jump — "
                       "mixed emitters or inconsistent qty scope?");
#endif
                if (jump_reporter_) {
                    jump_reporter_(sid_.c_str(), code, prev, qty_fp);
                }
            }
        }
        slots_[std::move(code_s)] = qty_fp;
    }

    /// @brief 确保某合约的槽存在（不改 qty）。on_roll 后标记 old code 槽已追踪。
    void touch(const char* code) {
        if (!code || code[0] == '\0') return;
        slots_.try_emplace(std::string(code), int64_t{0});
    }

    /// @brief 跨合约净仓汇总（logical position）。
    int64_t logical_fp() const {
        int64_t total = 0;
        for (const auto& kv : slots_) total += kv.second;
        return total;
    }

    /// @brief 清空全部物理合约槽；换月状态由调用方自行重建。
    void reset() { slots_.clear(); }

    /// @brief 已追踪的物理合约槽数量。
    size_t size() const { return slots_.size(); }

    /// @brief 是否尚未追踪任何物理合约。
    bool empty() const { return slots_.empty(); }

    /// @brief 只读暴露槽位快照；供策略诊断和单测检查，不应绕过 on_update 写入。
    const std::unordered_map<std::string, int64_t>& slots() const { return slots_; }

private:
    std::unordered_map<std::string, int64_t> slots_;  ///< 物理合约 code -> confirmed qty_fp。
    std::string sid_;                                 ///< jump reporter label；空串表示默认策略标签。
    int64_t max_observed_order_qty_fp_ = 0;            ///< 跳变阈值按已提交最大订单量动态放大。

    // Header-only 跨 DSO hook：runner 写入、plugin 读取同一个 inline static 实例依赖
    // GCC 默认 STB_GNU_UNIQUE 符号合并。若构建切到 -fno-gnu-unique、
    // -fvisibility=hidden 或非默认 clang 行为，plugin 可能读到独立副本并保持 nullptr；
    // 影响仅限跳变诊断缺失，money path 不依赖该 hook。若需要跨工具链稳定上报，
    // 应改为 host 注入 reporter 指针，而不是依赖链接器合并语义。
    inline static JumpReporter jump_reporter_ = nullptr;
};

/// @brief 策略自发订单元数据。
///
/// ctx->get_open_orders() 不暴露 tag/tif/offset 等迁移字段，策略 submit 时需在本地 sidecar
/// 保存一份，on_order 回调再同步 leaves 和终态清理。
struct WorkingOrderMeta {
    uint64_t      id         = 0;
    std::string   code;
    bool          is_buy     = true;
    int64_t       price_fp   = 0;
    int64_t       leaves_fp  = 0;
    XtTimeInForce tif        = IStrategyContext::TIF_GTC;
    std::string   tag;
    XtOffset      offset     = XtOffset::Open;
};

/// @brief 策略本地 working order sidecar。
///
/// 线程语义：策略线程单写，策略回调内读；不提供内部同步。
class WorkingOrderRegistry {
public:
    /// @brief 写入或更新订单元数据；id==0 表示 submit 未受理并被忽略。
    void upsert(uint64_t id, const char* code, bool is_buy,
                int64_t price_fp, int64_t leaves_fp,
                XtTimeInForce tif, const char* tag, XtOffset offset) {
        if (id == 0) return;
        auto& ord = orders_[id];
        ord.id        = id;
        ord.code      = code ? code : "";
        ord.is_buy    = is_buy;
        ord.price_fp  = price_fp;
        ord.leaves_fp = leaves_fp;
        ord.tif       = tif;
        ord.tag       = tag ? tag : "";
        ord.offset    = offset;
    }

    /// @brief on_order 回调驱动：同步 leaves，终态或 leaves<=0 时 erase。
    void apply_update(const XtOrderExecUpdate& upd) {
        auto it = orders_.find(upd.id);
        if (it == orders_.end()) return;
        it->second.leaves_fp = upd.leaves_fp;
        if (order_status_is_final(upd.status) || upd.leaves_fp <= 0) {
            orders_.erase(it);
        }
    }

    /// @brief 按订单 id 查询策略本地迁移元数据；未登记返回 nullptr。
    const WorkingOrderMeta* find(uint64_t id) const {
        auto it = orders_.find(id);
        return it == orders_.end() ? nullptr : &it->second;
    }

    /// @brief 删除订单 sidecar；用于终态回报或迁移 cancel 后清理。
    void erase(uint64_t id) { orders_.erase(id); }

    /// @brief 清空全部 working-order 元数据；通常在策略重置或 session 重建时调用。
    void reset() { orders_.clear(); }

    /// @brief 已登记的 working order 数量。
    size_t size() const { return orders_.size(); }

    /// @brief 是否没有本地 working order 元数据。
    bool empty() const { return orders_.empty(); }

    /// @brief 只读暴露全部订单 sidecar；供诊断和单测检查，不应绕过 upsert/apply_update 写入。
    const std::unordered_map<uint64_t, WorkingOrderMeta>& all() const { return orders_; }

private:
    std::unordered_map<uint64_t, WorkingOrderMeta> orders_;  ///< order id -> 策略本地迁移 sidecar。
};

/// @brief 提交一笔订单并在 registry 里登记元数据。
inline uint64_t submit_and_track(IStrategyContext* ctx,
                                 WorkingOrderRegistry& reg,
                                 const char* code,
                                 bool is_buy,
                                 int64_t price_fp,
                                 int64_t qty_fp,
                                 XtTimeInForce tif,
                                 const char* tag,
                                 XtOffset offset) {
    const uint64_t id = is_buy
        ? ctx->buy_fp(code, price_fp, qty_fp, tif, tag, offset)
        : ctx->sell_fp(code, price_fp, qty_fp, tif, tag, offset);
    reg.upsert(id, code, is_buy, price_fp, qty_fp, tif, tag, offset);
    return id;
}

/// @brief 提交订单并同步更新 PositionAggregator 的最大订单量观测值。
///
/// 使用 PositionAggregator + WorkingOrderRegistry 的策略优先走本 overload，让跳变阈值
/// 跟随策略真实 lot size。未使用 PositionAggregator 的策略可继续使用上方 overload。
///
/// @note observe_order_qty 在 venue submit 前执行，即使订单立即成交，阈值也已包含本次 qty。
inline uint64_t submit_and_track(IStrategyContext* ctx,
                                 WorkingOrderRegistry& reg,
                                 PositionAggregator& positions,
                                 const char* code,
                                 bool is_buy,
                                 int64_t price_fp,
                                 int64_t qty_fp,
                                 XtTimeInForce tif,
                                 const char* tag,
                                 XtOffset offset) {
    positions.observe_order_qty(qty_fp);
    return submit_and_track(ctx, reg, code, is_buy, price_fp, qty_fp, tif, tag, offset);
}

/// @brief GTC 单跨合约迁移：cancel old_code 所有 GTC open orders，resubmit 到 new_code。
///
/// 只迁移 tif=GTC 的单（IOC 已经同 tick 成交或 cancel，不存活到换月）。
/// 保留 price / leaves / tag / offset，不修改策略方向。
///
/// @note ctx->get_open_orders 读取 XtOrderRuntimeBook，包含 submit-未-ack（Submitted）
///   与 recovery（Recovered）单。这类非终态 GTC 单换月时也会被 cancel+resubmit
///   到 new_code，避免在旧合约留孤儿单。leaves<=0 与终态单仍跳过。
///
/// @param max_open_per_code  get_open_orders 单次读取 buffer 上限（默认 1024）。
///   高频市商场景下可通过策略 init_config 覆盖。超出此上限的 open orders 将被截断，
///   留给 on_order 回调后续清理。
inline void migrate_gtc_orders(IStrategyContext* ctx,
                               WorkingOrderRegistry& reg,
                               const char* old_code,
                               const char* new_code,
                               size_t max_open_per_code = 1024) {
    if (!ctx || !old_code || old_code[0] == '\0' || !new_code || new_code[0] == '\0') return;
    if (max_open_per_code == 0) return;

    std::vector<XtOrderExecUpdate> buf(max_open_per_code);
    const size_t n = ctx->get_open_orders(old_code, buf.data(), buf.size());
    for (size_t i = 0; i < n; ++i) {
        const XtOrderExecUpdate& upd = buf[i];
        if (upd.id == 0 || upd.leaves_fp <= 0 || order_status_is_final(upd.status)) continue;

        const WorkingOrderMeta* meta = reg.find(upd.id);
        const XtTimeInForce tif = meta ? meta->tif : IStrategyContext::TIF_GTC;
        if (tif != IStrategyContext::TIF_GTC) continue;

        const std::string tag = meta ? meta->tag : std::string{};
        ctx->cancel(upd.id);
        reg.erase(upd.id);
        submit_and_track(ctx, reg, new_code, upd.is_buy, upd.price_fp, upd.leaves_fp,
                         tif, tag.c_str(), upd.close_offset);
    }
}

} // namespace xt_rollover
