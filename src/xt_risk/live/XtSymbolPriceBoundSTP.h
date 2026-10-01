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

    /// @brief 启动期预注册品种价格边界。
    /// @warning 运行期注册会阻塞 STP 热路径，并改变未注册品种的 bypass 语义。
    void register_symbol(const std::string& code) {
        std::unique_lock lock(map_mu_);
        if (bounds_.find(code) == bounds_.end()) {
            bounds_.emplace(std::piecewise_construct,
                            std::forward_as_tuple(code),
                            std::forward_as_tuple());
        }
    }

    /// @brief 品种是否已注册价格边界。
    /// @return true 表示该品种已注册（would_self_trade 对其生效）。
    /// @note shared_lock 快查（O(1)），供 submit 热路径「先查后注册」兜底用；未命中才走
    ///       register_symbol 的 unique_lock（每品种一次性）。短合约码走 std::string SSO 无堆分配。
    bool is_registered(std::string_view code) const {
        std::shared_lock lock(map_mu_);
        return bounds_.find(std::string(code)) != bounds_.end();
    }

    /// @brief 已注册品种数（观测用）。
    /// @note shared_lock 快照；用于 STP 上膛度量（0 = 未预热 = self-trade 防护旁路）。
    size_t registered_count() const {
        std::shared_lock lock(map_mu_);
        return bounds_.size();
    }

    /// @brief 活跃挂单条目数（观测/诊断用；active_mu_ 锁内快照，非热路径）。
    /// @note 终态协议闭合的可观测锚：全部订单终态后应为 0（残留 = 有单未走 on_order_terminal）。
    size_t active_order_count() {
        std::lock_guard active(active_mu_);
        return id_index_.size();
    }

    /// @brief 检查新单是否会穿越己方反向挂单。
    /// @return true 表示存在 self-trade 风险，应拒绝该单。
    /// @note 复杂度 O(1)；未注册品种返回 false。price_fp<=0 保守视作穿越己方任一同名
    ///       对侧挂单——深防兜底（defensive fallback），非 live 行为面：生产路径下策略
    ///       市价意图在 context 已解析为正价 IOC，无行情时 live gateway 以 invalid input
    ///       在本检查前先拒，故 price_fp<=0 到不了这里。规则本身对 BT / 直连 checker 单测
    ///       与未来可能的 px==0 直达路径保守正确。
    bool would_self_trade(const xt_core::XtOrder& req) const override {
        std::shared_lock lock(map_mu_);
        auto it = bounds_.find(req.code);
        if (it == bounds_.end()) return false;

        const SymbolBounds& b = it->second;
        if (req.is_buy) {
            const int64_t min_ask = b.min_ask_fp.load(std::memory_order_acquire);
            if (min_ask == kNoAsk) return false;
            if (req.price_fp <= 0) return true;  // 市价买：穿越己方任一 ask（保守）
            return req.price_fp >= min_ask;
        } else {
            const int64_t max_bid = b.max_bid_fp.load(std::memory_order_acquire);
            if (max_bid == kNoBid) return false;
            if (req.price_fp <= 0) return true;  // 市价卖：穿越己方任一 bid（保守）
            return req.price_fp <= max_bid;
        }
    }

    /// @brief 新挂单进入活跃集合后更新本品种价格边界。
    /// @param code   合约代码（须已注册；未注册时 no-op，等价旁路）。
    /// @param id     订单唯一 id（venue local_id）——terminal 时按此 id erase。
    /// @param is_buy true 更新 max_bid_fp；false 更新 min_ask_fp。
    /// @param price_fp 挂单价格（fixed-point）。
    /// @note active_mu_ 单锁；terminal-before-stamp（tombstone 命中）时跳过插入。
    void on_order_submitted(const std::string& code, uint64_t id, bool is_buy, int64_t price_fp) {
        SymbolBounds* sym = lookup_bounds_(code);
        if (!sym) return;
        std::lock_guard active(active_mu_);
        if (tombstone_contains_(id)) return;  // 终态先于 stamp → 跳过，防 stale bound 复活
        id_index_[id] = ActiveEntry{sym, is_buy, price_fp};
        tighten_(*sym, is_buy, price_fp);
    }

    /// @brief 订单终态后按 id 释放该单对边界的贡献。
    /// @param id 终态订单 id；未知 id 记入 tombstone + no-op（幂等，double-terminal 无害）。
    /// @note 非热路径；不读订单簿，不需要 code。erase 后即时重算该品种剩余边界。
    void on_order_terminal(uint64_t id) {
        std::lock_guard active(active_mu_);
        tombstone_record_(id);  // id 不复用 → 无条件记录，兜底 terminal-before-stamp 的迟到 stamp
        auto it = id_index_.find(id);
        if (it == id_index_.end()) return;  // 未 stamp / 已 erase：幂等 no-op
        SymbolBounds* sym = it->second.sym;
        id_index_.erase(it);
        recompute_bounds_(sym);
    }

    /// @brief cold reverse-sync 反向灌入一笔 alive 在途单（语义同 submitted）。
    /// @note R4 后仅 cold reverse-sync 路径调用（warm 回调改 no-op、re-arm 走 rearm_active_order）。
    ///       cold 先 clear_all_active() 清 tombstone → 此处 tombstone 检查天然 no-op；保留检查为
    ///       防御性（cold clear→seed 两次取锁间隙若有并发终态，命中 tombstone 则跳过防 stale
    ///       bound 复活；id 全局单调不复用 → 不错杀真活单）。配 stp_rearm_on_reverse_sync() 的
    ///       cold（clear+seed）分叉使用。
    void seed_active_order(const std::string& code, uint64_t id, bool is_buy, int64_t price_fp) {
        register_symbol(code);  // 幂等；unique_lock(map_mu_) 与下方 active_mu_ 不嵌套
        SymbolBounds* sym = lookup_bounds_(code);
        if (!sym) return;
        std::lock_guard active(active_mu_);
        if (tombstone_contains_(id)) return;  // cold 清场间隙并发终态 → skip 防 stale bound 复活
        id_index_[id] = ActiveEntry{sym, is_buy, price_fp};
        tighten_(*sym, is_buy, price_fp);
    }

    /// @brief book 接受点驱动的 re-arm——book 真值覆盖 STP 私有 tombstone（跟随 book 裁决）。
    /// @param code   合约代码（未注册时先注册，幂等）。
    /// @param id     订单唯一 id（venue local_id）。
    /// @param is_buy true 更新 max_bid_fp；false 更新 min_ask_fp。
    /// @param price_fp 挂单价格（fixed-point）。
    /// @note 与 seed_active_order 的唯一区别：**显式清除该 id 的 tombstone**。reverse-sync
    ///       fillup 中 book 实际接受/复活一单（经 book 自身 terminal-tombstone/dedup 裁决之后）
    ///       即视为「该单活着」，STP 必须上膛——即便 STP 私有 tombstone 曾记该 id 为终态
    ///       （book exchange-as-truth 真值优先）。id 全局不复用 → 清 tombstone 不会错误复活他单。
    ///       upsert 幂等：同 id 重复 rearm 只覆盖条目 + tighten（GTC 限价单价不变，无害）。
    ///       由 engine 在 worker 线程对 fillup 返回的 accepted 清单逐单调用；active_mu_ 串行。
    void rearm_active_order(const std::string& code, uint64_t id, bool is_buy, int64_t price_fp) {
        register_symbol(code);  // 幂等；unique_lock(map_mu_) 与下方 active_mu_ 不嵌套
        SymbolBounds* sym = lookup_bounds_(code);
        if (!sym) return;
        std::lock_guard active(active_mu_);
        tombstone_erase_(id);  // book 真值覆盖 STP 私有 tombstone（book 说活即活）
        id_index_[id] = ActiveEntry{sym, is_buy, price_fp};
        tighten_(*sym, is_buy, price_fp);
    }

    /// @brief 清空所有活跃单与 tombstone，并把所有已注册品种边界重置为 sentinel。
    /// @note reverse-sync replace 前清场用（空 batch reseed = 边界全清，正确）。
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
    static constexpr int64_t kNoAsk = std::numeric_limits<int64_t>::max(); ///< 无挂卖单
    static constexpr int64_t kNoBid = std::numeric_limits<int64_t>::min(); ///< 无挂买单
    static constexpr size_t  kTombstoneCap = 1024;  ///< terminal-before-stamp 有界环容量

    /// @brief 单品种己方最优反向挂单边界（bounds_ 节点，append-only，指针稳定）。
    struct SymbolBounds {
        std::atomic<int64_t> min_ask_fp{kNoAsk};  ///< 己方最低挂卖价（fixed-point，published）
        std::atomic<int64_t> max_bid_fp{kNoBid};  ///< 己方最高挂买价（fixed-point，published）
    };

    /// @brief 全局 id 索引条目：品种边界指针 + 方向 + 价格（active_mu_ 保护）。
    struct ActiveEntry {
        SymbolBounds* sym = nullptr;
        bool          is_buy = true;
        int64_t       price_fp = 0;
    };

    /// @brief 取品种边界节点指针（map_mu_ shared 下 find；bounds_ append-only 保指针稳定）。
    SymbolBounds* lookup_bounds_(const std::string& code) {
        std::shared_lock lock(map_mu_);
        auto it = bounds_.find(code);
        return it == bounds_.end() ? nullptr : &it->second;
    }

    /// @brief 单笔挂单只紧固边界（插入只可能收紧；active_mu_ 已持有）。
    void tighten_(SymbolBounds& b, bool is_buy, int64_t price_fp) {
        if (is_buy) {
            if (price_fp > b.max_bid_fp.load(std::memory_order_relaxed))
                b.max_bid_fp.store(price_fp, std::memory_order_release);
        } else {
            if (price_fp < b.min_ask_fp.load(std::memory_order_relaxed))
                b.min_ask_fp.store(price_fp, std::memory_order_release);
        }
    }

    /// @brief 从剩余活跃单重算该品种边界（terminal erase 可放松边界；active_mu_ 已持有）。
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

    /// @brief 记入终态 tombstone 环（FIFO 淘汰，容量 kTombstoneCap；active_mu_ 已持有）。
    void tombstone_record_(uint64_t id) {
        if (id == 0) return;
        if (tombstone_set_.insert(id).second) tombstone_fifo_.push_back(id);
        while (tombstone_set_.size() > kTombstoneCap && !tombstone_fifo_.empty()) {
            tombstone_set_.erase(tombstone_fifo_.front());
            tombstone_fifo_.pop_front();
        }
    }

    /// @brief 从 tombstone 环移除一个 id（rearm_active_order 用：book 真值覆盖；active_mu_ 已持有）。
    /// @note set 是 tombstone_contains_ 的权威；同步从 fifo 删除以保 cap 计数精确（deque 中段
    ///       erase O(n≤1024)，rearm 走 recovery 冷路径，非热路径）。
    void tombstone_erase_(uint64_t id) {
        if (tombstone_set_.erase(id) == 0) return;
        for (auto it = tombstone_fifo_.begin(); it != tombstone_fifo_.end(); ++it) {
            if (*it == id) { tombstone_fifo_.erase(it); break; }
        }
    }

    mutable std::shared_mutex map_mu_;  ///< 保护 bounds_ key set 和 SymbolBounds 节点生命周期
    // TransparentStringHash 与全仓纪律一致；C++20 后 would_self_trade(req.code)
    // 异构查找免临时 string（C++17 下行为不变）。
    xt_core::sync::TransparentStringMap<SymbolBounds> bounds_;  ///< code -> price bounds（append-only）

    std::mutex active_mu_;  ///< 串行所有 mutation（submit/terminal/seed/clear）
    std::unordered_map<uint64_t, ActiveEntry> id_index_;   ///< id -> {sym, is_buy, price}（active_mu_）
    std::unordered_set<uint64_t> tombstone_set_;           ///< 终态 id 集（active_mu_）
    std::deque<uint64_t>         tombstone_fifo_;           ///< tombstone FIFO 淘汰序（active_mu_）
};
