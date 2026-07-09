#pragma once

/// @file
/// @brief 策略换月辅助组件

#include "api/IStrategyContext.h"
#include "xt_core/order/XtOrderTypes.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
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

    using JumpReporter = void(*)(const char* sid, const char* code,
                                 int64_t prev_qty_fp, int64_t new_qty_fp);

    static void set_jump_reporter(JumpReporter reporter) { jump_reporter_ = reporter; }

    static constexpr int64_t kMinJumpThresholdFp = 1'000'000;

    void set_sid(std::string sid) { sid_ = std::move(sid); }
    const std::string& sid() const { return sid_; }

    void observe_order_qty(int64_t qty_fp) {
        const int64_t abs_q = qty_fp < 0 ? -qty_fp : qty_fp;
        if (abs_q > max_observed_order_qty_fp_) {
            max_observed_order_qty_fp_ = abs_q;
        }
    }
    int64_t max_observed_order_qty_fp() const { return max_observed_order_qty_fp_; }

    void on_update(const char* code, int64_t qty_fp) {
        if (!code || code[0] == '\0') return;
        const std::string code_s(code);
        if (auto it = slots_.find(code_s); it != slots_.end()) {
            const int64_t prev = it->second;

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

    void touch(const char* code) {
        if (!code || code[0] == '\0') return;
        slots_.try_emplace(std::string(code), int64_t{0});
    }

    int64_t logical_fp() const {
        int64_t total = 0;
        for (const auto& kv : slots_) total += kv.second;
        return total;
    }

    void reset() { slots_.clear(); }

    size_t size() const { return slots_.size(); }

    bool empty() const { return slots_.empty(); }

    const std::unordered_map<std::string, int64_t>& slots() const { return slots_; }

private:
    std::unordered_map<std::string, int64_t> slots_;
    std::string sid_;
    int64_t max_observed_order_qty_fp_ = 0;

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

    void apply_update(const XtOrderExecUpdate& upd) {
        auto it = orders_.find(upd.id);
        if (it == orders_.end()) return;
        it->second.leaves_fp = upd.leaves_fp;
        if (order_status_is_final(upd.status) || upd.leaves_fp <= 0) {
            orders_.erase(it);
        }
    }

    const WorkingOrderMeta* find(uint64_t id) const {
        auto it = orders_.find(id);
        return it == orders_.end() ? nullptr : &it->second;
    }

    void erase(uint64_t id) { orders_.erase(id); }

    void reset() { orders_.clear(); }

    size_t size() const { return orders_.size(); }

    bool empty() const { return orders_.empty(); }

    const std::unordered_map<uint64_t, WorkingOrderMeta>& all() const { return orders_; }

private:
    std::unordered_map<uint64_t, WorkingOrderMeta> orders_;
};

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

}
