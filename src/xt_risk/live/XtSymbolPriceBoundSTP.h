#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "api/ISTPChecker.h"
#include "xt_core/order/XtOrder.h"
#include "xt_core/sync/XtTransparentStringHash.h"

/// @brief 基于己方最优买卖边界的轻量 STP 检查器（per-order active set 版）。
///
/// 每个已注册品种维护两条 published 边界：己方最高买价（max_bid_fp）和最低卖价
///（min_ask_fp）。新买单若价格不低于己方最低卖价，或新卖单若价格不高于己方最高
/// 买价，即判定会穿越己方反向挂单并拒绝。price_fp<=0 保守视作穿越己方任一同名对侧
/// 挂单，是深防兜底规则（非 live 行为面）：生产路径下 price_fp<=0 到不了本检查器——
/// 策略市价意图在 context 已解析为正价 IOC（XtStrategyContext resolve_market_order），
/// 无行情解析失败时 live gateway 以 invalid input 在 STP/risk 前先拒（is_valid_order_input）。
///
/// 边界不再依赖调用方在终态时回传剩余活跃单：本类内部维护 per-order active set
///（id -> {is_buy, price}）+ 全局 id 索引，submit/terminal/seed/clear 全在单一
/// `active_mu_` 临界区串行，terminal 按 id erase 后即时重算该品种边界并经既有
/// atomic 发布。未注册品种返回 false，等价于该品种未启用本检查。
///
/// C3（终态重建 vs off-worker submit 的 lost-update）消除论证：terminal 不再读
/// 订单簿 snapshot；stamp / terminal / seed / clear 全走 `active_mu_` 单锁 →
/// submit-stamp 与 terminal 线性化，off-worker（panel/closeout）线程 stamp 的新
/// 边界不会被 worker 线程 terminal 的 rebuild 覆盖。terminal-before-stamp（终态先
/// 于 stamp 到达）由有界 tombstone 环拦截：stamp 前查 tombstone，命中则跳过插入，
/// 防止 active entry 泄漏 → stale bound 复活。
///
/// @warning 不是 lock-free。`map_mu_`（shared）只保护 bounds_ 的 key set 与
/// SymbolBounds 节点生命周期；单品种 price bound 通过 atomic 发布，读侧
///（would_self_trade）只取 shared_lock + 2 次 atomic load，不遍历订单簿、不取
/// `active_mu_`。所有 mutation 走 `active_mu_` 单锁（速率 ~5-10 ops/s，含
/// conformance 压测），不做分片锁。
/// @note register_symbol() 应在启动期完成，运行期注册会短暂阻塞 submit 检查路径。
class XtSymbolPriceBoundSTP : public ISTPChecker {
public:
    XtSymbolPriceBoundSTP() = default;

    void register_symbol(const std::string& code) {
        std::unique_lock lock(map_mu_);
        if (bounds_.find(code) == bounds_.end()) {
            bounds_.emplace(std::piecewise_construct,
                            std::forward_as_tuple(code),
                            std::forward_as_tuple());
        }
    }

    bool is_registered(std::string_view code) const {
        std::shared_lock lock(map_mu_);
        return bounds_.find(std::string(code)) != bounds_.end();
    }

    size_t registered_count() const {
        std::shared_lock lock(map_mu_);
        return bounds_.size();
    }

    size_t active_order_count() {
        std::lock_guard active(active_mu_);
        return id_index_.size();
    }

    bool would_self_trade(const xt_core::XtOrder& req) const override {
        std::shared_lock lock(map_mu_);
        auto it = bounds_.find(req.code);
        if (it == bounds_.end()) return false;

        const SymbolBounds& b = it->second;
        if (req.is_buy) {
            const int64_t min_ask = b.min_ask_fp.load(std::memory_order_acquire);
            if (min_ask == kNoAsk) return false;
            if (req.price_fp <= 0) return true;
            return req.price_fp >= min_ask;
        } else {
            const int64_t max_bid = b.max_bid_fp.load(std::memory_order_acquire);
            if (max_bid == kNoBid) return false;
            if (req.price_fp <= 0) return true;
            return req.price_fp <= max_bid;
        }
    }

    void on_order_submitted(const std::string& code, uint64_t id, bool is_buy, int64_t price_fp) {
        SymbolBounds* sym = lookup_bounds_(code);
        if (!sym) return;
        std::lock_guard active(active_mu_);
        if (tombstone_contains_(id)) return;
        id_index_[id] = ActiveEntry{sym, is_buy, price_fp};
        tighten_(*sym, is_buy, price_fp);
    }

    void on_order_terminal(uint64_t id) {
        std::lock_guard active(active_mu_);
        tombstone_record_(id);
        auto it = id_index_.find(id);
        if (it == id_index_.end()) return;
        SymbolBounds* sym = it->second.sym;
        id_index_.erase(it);
        recompute_bounds_(sym);
    }

    void seed_active_order(const std::string& code, uint64_t id, bool is_buy, int64_t price_fp) {
        register_symbol(code);
        SymbolBounds* sym = lookup_bounds_(code);
        if (!sym) return;
        std::lock_guard active(active_mu_);
        if (tombstone_contains_(id)) return;
        id_index_[id] = ActiveEntry{sym, is_buy, price_fp};
        tighten_(*sym, is_buy, price_fp);
    }

    void rearm_active_order(const std::string& code, uint64_t id, bool is_buy, int64_t price_fp) {
        register_symbol(code);
        SymbolBounds* sym = lookup_bounds_(code);
        if (!sym) return;
        std::lock_guard active(active_mu_);
        tombstone_erase_(id);
        id_index_[id] = ActiveEntry{sym, is_buy, price_fp};
        tighten_(*sym, is_buy, price_fp);
    }

    void clear_all_active() {
        std::shared_lock map(map_mu_);
        std::lock_guard active(active_mu_);
        id_index_.clear();
        tombstone_set_.clear();
        tombstone_fifo_.clear();
        for (auto& kv : bounds_) {
            kv.second.min_ask_fp.store(kNoAsk, std::memory_order_release);
            kv.second.max_bid_fp.store(kNoBid, std::memory_order_release);
        }
    }

private:
    static constexpr int64_t kNoAsk = std::numeric_limits<int64_t>::max();
    static constexpr int64_t kNoBid = std::numeric_limits<int64_t>::min();
    static constexpr size_t  kTombstoneCap = 1024;

    /// @brief 单品种己方最优反向挂单边界（bounds_ 节点，append-only，指针稳定）。
    struct SymbolBounds {
        std::atomic<int64_t> min_ask_fp{kNoAsk};
        std::atomic<int64_t> max_bid_fp{kNoBid};
    };

    /// @brief 全局 id 索引条目：品种边界指针 + 方向 + 价格（active_mu_ 保护）。
    struct ActiveEntry {
        SymbolBounds* sym = nullptr;
        bool          is_buy = true;
        int64_t       price_fp = 0;
    };

    SymbolBounds* lookup_bounds_(const std::string& code) {
        std::shared_lock lock(map_mu_);
        auto it = bounds_.find(code);
        return it == bounds_.end() ? nullptr : &it->second;
    }

    void tighten_(SymbolBounds& b, bool is_buy, int64_t price_fp) {
        if (is_buy) {
            if (price_fp > b.max_bid_fp.load(std::memory_order_relaxed))
                b.max_bid_fp.store(price_fp, std::memory_order_release);
        } else {
            if (price_fp < b.min_ask_fp.load(std::memory_order_relaxed))
                b.min_ask_fp.store(price_fp, std::memory_order_release);
        }
    }

    void recompute_bounds_(SymbolBounds* sym) {
        int64_t new_max_bid = kNoBid;
        int64_t new_min_ask = kNoAsk;
        for (const auto& kv : id_index_) {
            if (kv.second.sym != sym) continue;
            if (kv.second.is_buy) {
                if (kv.second.price_fp > new_max_bid) new_max_bid = kv.second.price_fp;
            } else {
                if (kv.second.price_fp < new_min_ask) new_min_ask = kv.second.price_fp;
            }
        }
        sym->max_bid_fp.store(new_max_bid, std::memory_order_release);
        sym->min_ask_fp.store(new_min_ask, std::memory_order_release);
    }

    bool tombstone_contains_(uint64_t id) const {
        return tombstone_set_.find(id) != tombstone_set_.end();
    }

    void tombstone_record_(uint64_t id) {
        if (id == 0) return;
        if (tombstone_set_.insert(id).second) tombstone_fifo_.push_back(id);
        while (tombstone_set_.size() > kTombstoneCap && !tombstone_fifo_.empty()) {
            tombstone_set_.erase(tombstone_fifo_.front());
            tombstone_fifo_.pop_front();
        }
    }

    void tombstone_erase_(uint64_t id) {
        if (tombstone_set_.erase(id) == 0) return;
        for (auto it = tombstone_fifo_.begin(); it != tombstone_fifo_.end(); ++it) {
            if (*it == id) { tombstone_fifo_.erase(it); break; }
        }
    }

    mutable std::shared_mutex map_mu_;

    xt_core::sync::TransparentStringMap<SymbolBounds> bounds_;

    std::mutex active_mu_;
    std::unordered_map<uint64_t, ActiveEntry> id_index_;
    std::unordered_set<uint64_t> tombstone_set_;
    std::deque<uint64_t>         tombstone_fifo_;
};
