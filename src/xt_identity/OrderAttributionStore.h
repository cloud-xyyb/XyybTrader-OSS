#pragma once

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iterator>  // std::next（retired 表 cap 淘汰最旧扫描）
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "api/StrategySid.h"
#include "xt_identity/StrategySidRegistry.h"  // kManualSidId（retired 表 manual 长 TTL 判据）
#include "xt_logger/XtLogger.h"

namespace xt_identity {

/// @brief 记录本地订单归属的进程级 StrategySid store。
///
/// OrderAttributionStore 让提交路径和回报路径共享同一份订单归属：
/// - 发单前用 thread-local 暂存当前策略 sid，venue gateway 在 submit 边界消费。
/// - submit 成功后按 order_id 记录 sid，回报和 recovery 再按 order_id 反查。
///
/// 配置 journal 路径时，OrderAttributionStore 首次访问会 replay journal；空路径只保留
/// 进程内映射。共享映射由内部 mutex 保护；thread-local sid 只对当前发单线程可见。
class OrderAttributionStore {
public:
    // ── 订单归属反查 ───────────────────────────

    /// @brief 记录本地订单归属的 StrategySid。
    ///
    /// sid 有值时写入或覆盖映射；sid 为 no-sid 时删除映射。journal 开启时同步
    /// 追加一条恢复记录。
    static void remember_order_strategy_sid(uint64_t order_id, StrategySid sid) {
        if (order_id == 0) return;
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        load_order_sid_journal_locked();
        auto& map = order_sid_map();
        if (sid.has_value()) {
            map[order_id] = sid.raw();
            append_order_sid_journal_locked(order_id, sid.raw());
        } else {
            map.erase(order_id);
            append_order_sid_journal_locked(order_id, 0);
        }
    }

    /// @brief 查询本地订单归属；未命中或 order_id=0 返回 no-sid。
    ///
    /// 先查 active map；miss 后查内存 retired 表（终态→成交乱序窗内的精确归因，见
    /// retire_order_strategy_sid）。retired 条目按 order_id（venue 单调）键，命中即该单的
    /// 正确归属；过期条目由 retire-插入时的 amortized sweep 按 sid 类型分档 TTL（manual 长 /
    /// 策略短）清理，热路径零新增扫描。
    static StrategySid lookup_order_strategy_sid(uint64_t order_id) {
        if (order_id == 0) return StrategySid::legacy();
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        load_order_sid_journal_locked();
        const auto& map = order_sid_map();
        const auto it = map.find(order_id);
        if (it != map.end()) return StrategySid::from_raw(it->second);
        const auto& rmap = order_sid_retired_map();
        const auto rit = rmap.find(order_id);
        if (rit != rmap.end()) return StrategySid::from_raw(rit->second.sid_id);
        return StrategySid::legacy();
    }

    /// @brief 硬删除本地订单归属（双表：active + retired）；journal 追加 sid_id=0 删除记录。
    ///
    /// 用于「失败/判无归属」订单（submit 失败回滚 / orphan reap / fillup-unowned）——
    /// 这些单必须彻底遗忘、不得由 retired 表复活。终态收尾（全成/终态 update）走
    /// retire_order_strategy_sid（迁入 retired 表，manual 与策略单一视同仁），不走本函数。
    static void forget_order_strategy_sid(uint64_t order_id) {
        if (order_id == 0) return;
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        load_order_sid_journal_locked();
        order_sid_map().erase(order_id);
        order_sid_retired_map().erase(order_id);
        append_order_sid_journal_locked(order_id, 0);
    }

    /// @brief 订单终态归属退休（terminal retire）：迁入内存 retired 表（manual 与策略单一视同仁）。
    ///
    /// 由订单簿终态收尾路径（全成 / 终态 update）调用，取代旧「终态即硬删归属」：
    /// - 从 active 迁入内存 retired 表 + 写 `order_id 0` 擦除行（journal 语义完全不变；
    ///   retired 表不持久化 → 跨重启不保留 = 既有行为）。
    /// - **保留值 kManualSidId（操作面手工单）与策略 sid 走同一条路径**，唯一区别是 sweep
    ///   过期用不同 TTL：manual 走长 TTL（kManualRetiredTtlMs，~1h，覆盖 reconciliation 迟到
    ///   成交回溯窗），策略走短 TTL（order_sid_retired_ttl_ms，~30s，覆盖终态→成交乱序窗）。
    ///   收益：终态先于成交到达（cancel/fill race）时 lookup 在 TTL 窗内从「启发猜归属」
    ///   升级为精确 lookup；淘汰后回落启发 = 既有行为。
    /// - **active miss（无归属 / 已退休）**：幂等 no-op，不写 journal。
    ///
    /// 披露的行为边界（latent，非钱路）：manual 归属存活 = manual TTL 窗内；超 manual TTL /
    /// 跨重启后迟到的 manual 成交 lookup miss → 落 D4 stamp-miss 后既有 unknown_sid 路径
    /// （stale-sid 未归属或单 context 唯一 sink），记账归属可能不精确但账户/持仓无损
    /// （账簿应用先于路由判定）。
    ///
    /// @param order_id 终态订单本地 id。
    /// @param now_ms   caller 提供的 steady 毫秒时钟（retired 表 TTL/淘汰用；调用方持时钟）。
    static void retire_order_strategy_sid(uint64_t order_id, uint64_t now_ms) {
        if (order_id == 0) return;
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        load_order_sid_journal_locked();
        auto& map = order_sid_map();
        const auto it = map.find(order_id);
        if (it == map.end()) return;  // 无归属或已退休：幂等 no-op
        const uint32_t sid = it->second;
        map.erase(it);
        append_order_sid_journal_locked(order_id, 0);  // journal 语义不变（仍写 0-擦除行）
        insert_retired_locked(order_id, sid, now_ms);
    }

    // ── Submit sid 传递 ────────────────────────

    /// @brief 设置当前线程下一次 venue submit 使用的 StrategySid。
    static void set_thread_submit_strategy_sid(StrategySid sid) {
        thread_submit_strategy_sid() = sid;
    }

    /// @brief 取出当前线程 submit sid，并立即重置为 no-sid。
    static StrategySid consume_thread_submit_strategy_sid() {
        StrategySid sid = thread_submit_strategy_sid();
        thread_submit_strategy_sid() = StrategySid::legacy();
        return sid;
    }

    // ── Journal 持久化健康观测 ──────────────────

    /// @brief 自上次成功 compaction 以来 journal 持久化是否降级。
    ///
    /// append 或 compaction 落盘失败置 true；一次成功 compaction 把整份活动映射重写
    /// 落盘、治愈此前所有丢失的 append，从而清 false。降级期间内存映射仍准确，只是磁盘
    /// 副本不再权威——依赖持久归属的恢复后收尾动作（如 orphan reap）应据此改告警-only，
    /// 不要在归属可能落盘丢失时对反查不到 sid 的单执行破坏性收尾。
    /// @note 读者可能在别的线程（engine worker / 控制面 frame loop）；relaxed atomic 读。
    static bool order_sid_journal_degraded() noexcept {
        return journal_degraded_state().load(std::memory_order_relaxed);
    }

    /// @brief journal 持久化累计失败次数（append + compaction 合计，跨降级不清零）。
    static uint64_t order_sid_journal_failure_count() noexcept {
        return journal_failure_count_state().load(std::memory_order_relaxed);
    }

    /// @brief retired 表累计淘汰数（TTL 过期 + cap 淘汰合计，manual 与策略合计，观测用）。
    /// @note 淘汰仅影响内存 retired 表容量；淘汰后回落启发归因 = 既有行为，不影响 correctness。
    static uint64_t order_sid_retired_evicted_total() noexcept {
        return retired_evicted_state().load(std::memory_order_relaxed);
    }

#ifdef XT_BUILD_TESTS
    // ── Test helpers ───────────────────────────

    /// @brief 单测设置 journal 路径，并清空内存注册表与 replay 状态。
    static void set_order_strategy_sid_journal_path_for_test(std::string path) {
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        test_order_sid_journal_path() = std::move(path);
        test_order_sid_journal_path_overridden() = true;
        order_sid_map().clear();
        order_sid_retired_map().clear();
        order_sid_retired_insert_count() = 0;
        order_sid_journal_loaded() = false;
        order_sid_journal_append_count() = 0;
    }

    /// @brief 单测只丢弃内存状态，下一次 lookup 会重新 replay journal。
    /// @note retired 表是纯内存态（不持久化），随内存丢弃清空 = 生产跨重启不保留语义。
    static void drop_order_strategy_sid_memory_for_test() {
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        order_sid_map().clear();
        order_sid_retired_map().clear();  // 纯内存态随重启丢弃（跨重启 retired 归属不保留）
        order_sid_retired_insert_count() = 0;
        order_sid_journal_loaded() = false;
    }

    /// @brief 单测直接设置 journal 降级闩，供不经真实落盘失败的降级用例（如 reap）注入。
    static void set_order_sid_journal_degraded_for_test(bool degraded) {
        journal_degraded_state().store(degraded, std::memory_order_relaxed);
    }

    /// @brief 单测清空注册表、journal 测试覆盖项、compaction 阈值、env 路径缓存和降级状态。
    static void clear_order_strategy_sids_for_test() {
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        order_sid_map().clear();
        order_sid_journal_loaded() = false;
        order_sid_journal_append_count() = 0;
        order_sid_journal_compact_threshold() = 4096;
        test_order_sid_journal_path().clear();
        test_order_sid_journal_path_overridden() = false;
        cached_env_journal_path_resolved() = false;  // 重置 env path 缓存, 保测试隔离
        journal_degraded_state().store(false, std::memory_order_relaxed);
        journal_failure_count_state().store(0, std::memory_order_relaxed);
        // retired 表状态回默认，保测试隔离。
        order_sid_retired_map().clear();
        order_sid_retired_insert_count() = 0;
        order_sid_retired_ttl_ms() = kDefaultRetiredTtlMs;
        order_sid_retired_cap() = kDefaultRetiredCap;
        order_sid_retired_sweep_every() = kDefaultRetiredSweepEvery;
        retired_evicted_state().store(0, std::memory_order_relaxed);
    }

    /// @brief 单测直读 retired 表当前大小。
    static std::size_t order_sid_retired_size_for_test() {
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        return order_sid_retired_map().size();
    }
#endif

    // XT_BUILD_TESTS 下暴露白盒 helper 和内部访问级别；生产构建保持 private。
    // ODR 安全性依赖本类无数据成员、无虚函数，toggle 不改变 layout / mangling。
    // 若日后给本类增加数据成员或虚函数，必须撤掉这个访问级别 toggle。
#ifdef XT_BUILD_TESTS
public:
#else
private:
#endif
    // ── 归属表共享状态 ──────────────────────────

    /// @brief 保护进程级 order_id -> sid 注册表和 journal 状态。
    static std::mutex& order_sid_mutex() {
        static std::mutex mu;
        return mu;
    }

    /// @brief 进程级 order_id -> raw StrategySid 映射。
    static std::unordered_map<uint64_t, uint32_t>& order_sid_map() {
        static std::unordered_map<uint64_t, uint32_t> map;
        return map;
    }

    // ── Journal 状态与路径 ─────────────────────

    /// @brief 本进程是否已经 replay 过 journal。
    static bool& order_sid_journal_loaded() {
        static bool loaded = false;
        return loaded;
    }

#ifdef XT_BUILD_TESTS
    /// @brief 单测覆盖 journal 路径；空字符串表示禁用落盘。
    static std::string& test_order_sid_journal_path() {
        static std::string path;
        return path;
    }

    /// @brief 单测是否覆盖了环境变量中的 journal 路径。
    static bool& test_order_sid_journal_path_overridden() {
        static bool overridden = false;
        return overridden;
    }
#endif

    /// @brief 进程级 journal 路径缓存，避免写路径在锁内重复 getenv。
    static std::string& cached_env_journal_path() {
        static std::string path;
        return path;
    }

    /// @brief journal 路径是否已经从环境变量解析到缓存。
    static bool& cached_env_journal_path_resolved() {
        static bool resolved = false;
        return resolved;
    }

    /// @brief 返回本进程使用的 journal 路径；空字符串表示不落盘。
    /// @pre 调用方持有 order_sid_mutex()。
    static std::string configured_order_sid_journal_path_locked() {
#ifdef XT_BUILD_TESTS
        if (test_order_sid_journal_path_overridden()) return test_order_sid_journal_path();
#endif
        if (!cached_env_journal_path_resolved()) {
            const char* env = std::getenv("XT_STRATEGY_SID_OWNER_JOURNAL");
            cached_env_journal_path() = env ? std::string(env) : std::string{};
            cached_env_journal_path_resolved() = true;
        }
        return cached_env_journal_path();
    }

#ifndef _WIN32
    /// @brief 为 journal 文件创建父目录；已存在或创建失败都不向调用方报错。
    static void ensure_parent_dir_for_journal(const std::string& path) noexcept {
        const std::string::size_type slash = path.find_last_of('/');
        if (slash == std::string::npos || slash == 0) return;
        const std::string dir = path.substr(0, slash);
        for (std::string::size_type i = 1; i < dir.size(); ++i) {
            if (dir[i] == '/') {
                const std::string prefix = dir.substr(0, i);
                if (!prefix.empty()) (void)::mkdir(prefix.c_str(), 0755);
            }
        }
        (void)::mkdir(dir.c_str(), 0755);
    }
#endif

    /// @brief 自上次 compaction 以来的 append 次数，remember/forget 都会计入。
    static uint64_t& order_sid_journal_append_count() noexcept {
        static uint64_t n = 0;
        return n;
    }

    /// @brief journal compaction 阈值；达到后把文件重写为内存活动映射。
    /// @note 返回引用是为了单测下调阈值并快速触发 compaction。
    static uint64_t& order_sid_journal_compact_threshold() noexcept {
        static uint64_t t = 4096;
        return t;
    }

    // ── Journal 持久化降级状态 ──────────────────

    /// @brief journal 持久化降级闩：append/compaction 落盘失败置位，成功 compaction 清位。
    /// @note 写者持 order_sid_mutex()，读者可能跨线程无锁读；atomic 保证读到一致值。
    static std::atomic<bool>& journal_degraded_state() noexcept {
        static std::atomic<bool> degraded{false};
        return degraded;
    }

    /// @brief journal 持久化累计失败计数，仅增（跨降级保留），供限频日志与观测。
    static std::atomic<uint64_t>& journal_failure_count_state() noexcept {
        static std::atomic<uint64_t> n{0};
        return n;
    }

    /// @brief 记录一次 journal 持久化失败：累计计数 + 置降级闩 + 限频 error log。
    /// @pre 调用方持有 order_sid_mutex()。
    static void note_journal_persist_failure_locked(const char* stage, int err) noexcept {
        const uint64_t count =
            journal_failure_count_state().fetch_add(1, std::memory_order_relaxed) + 1;
        const bool was_degraded =
            journal_degraded_state().exchange(true, std::memory_order_relaxed);
        // 进入 degraded 的转换沿必打；之后按失败次数取模限频，避免只读 FS 每次 append 刷屏。
        constexpr uint64_t kLogEvery = 256;
        if (!was_degraded || (count % kLogEvery) == 0) {
            XtLogger::error_m("SID",
                "order-sid journal 持久化失败 stage={} errno={} 累计失败={} — "
                "归属落盘降级，orphan reap 将切告警-only",
                stage, err, count);
        }
    }

    /// @brief 一次成功 compaction 的自愈钩子：整文件已重写为活动映射，清降级闩。
    /// @pre 调用方持有 order_sid_mutex()。
    static void note_journal_compaction_healed_locked() noexcept {
        const bool was_degraded =
            journal_degraded_state().exchange(false, std::memory_order_relaxed);
        if (was_degraded) {
            XtLogger::info_m("SID",
                "order-sid journal compaction 成功，整文件已重写为活动映射，"
                "落盘降级自愈（累计失败={}）",
                journal_failure_count_state().load(std::memory_order_relaxed));
        }
    }

    // ── 订单归属内存 retired 表（manual 与策略单统一）────────────────
    //
    // retire 把终态订单归属从 active map 迁入内存 retired 表，让终态先于成交到达
    // （cancel/fill race）时 lookup 仍能精确反解；淘汰后回落启发 = 既有行为。manual
    // （保留值）与策略单走同一条 retire 路径，唯一区别是 sweep 过期用不同 TTL（manual 长 /
    // 策略短，见 sweep_retired_locked）。全部 state 为 function-local static，共居
    // order_sid_mutex()，不引入新 mutex，不加非静态数据成员（守本类 ODR/layout 不变量）。

    /// @brief retired 条目：退休时的 sid + steady 毫秒时戳（TTL/淘汰用）。
    struct RetiredSidEntry {
        uint32_t sid_id;
        uint64_t retire_ms;
    };

    static constexpr uint64_t kDefaultRetiredTtlMs = 30'000;     ///< ~30s 覆盖终态→成交乱序窗（策略单）
    static constexpr uint64_t kManualRetiredTtlMs = 3'600'000;   ///< ~1h 覆盖 reconciliation 迟到成交回溯窗（manual）
    static constexpr uint64_t kDefaultRetiredCap = 4096;         ///< retired 表容量上限
    static constexpr uint64_t kDefaultRetiredSweepEvery = 256;   ///< amortized sweep 插入间隔

    static std::unordered_map<uint64_t, RetiredSidEntry>& order_sid_retired_map() {
        static std::unordered_map<uint64_t, RetiredSidEntry> m;
        return m;
    }
    static uint64_t& order_sid_retired_ttl_ms() noexcept {
        static uint64_t v = kDefaultRetiredTtlMs; return v;
    }
    static uint64_t& order_sid_retired_cap() noexcept {
        static uint64_t v = kDefaultRetiredCap; return v;
    }
    static uint64_t& order_sid_retired_sweep_every() noexcept {
        static uint64_t v = kDefaultRetiredSweepEvery; return v;
    }
    /// @brief 自上次 sweep 以来的 retired 插入数（amortized sweep 触发计数）。
    static uint64_t& order_sid_retired_insert_count() noexcept {
        static uint64_t v = 0; return v;
    }
    static std::atomic<uint64_t>& retired_evicted_state() noexcept {
        static std::atomic<uint64_t> n{0}; return n;
    }

    /// @brief 插入 retired 条目 + amortized sweep（每 sweep_every 次插入或超 cap 时清理）。
    /// @pre 调用方持有 order_sid_mutex()。
    static void insert_retired_locked(uint64_t order_id, uint32_t sid_id,
                                      uint64_t now_ms) noexcept {
        auto& rmap = order_sid_retired_map();
        rmap[order_id] = RetiredSidEntry{sid_id, now_ms};
        if (++order_sid_retired_insert_count() >= order_sid_retired_sweep_every()
            || rmap.size() > order_sid_retired_cap()) {
            order_sid_retired_insert_count() = 0;
            sweep_retired_locked(now_ms);
        }
    }

    /// @brief retired 表清理：TTL 过期 + cap 淘汰最旧（by retire_ms），累计淘汰计数。
    ///
    /// TTL 按 sid 类型分档：manual（保留值 kManualSidId）用长 TTL（kManualRetiredTtlMs），
    /// 策略单用短 TTL（order_sid_retired_ttl_ms）。cap 淘汰对两类一视同仁（最旧优先）。
    /// @pre 调用方持有 order_sid_mutex()。
    static void sweep_retired_locked(uint64_t now_ms) noexcept {
        auto& rmap = order_sid_retired_map();
        const uint64_t strat_ttl = order_sid_retired_ttl_ms();
        uint64_t evicted = 0;
        for (auto it = rmap.begin(); it != rmap.end();) {
            const uint64_t entry_ttl =
                (it->second.sid_id == StrategySidRegistry::kManualSidId) ? kManualRetiredTtlMs
                                                                         : strat_ttl;
            // now_ms 单调；仅当 now 领先 retire 且超 TTL 才淘汰（防时钟异常误淘汰）。
            if (now_ms >= it->second.retire_ms && (now_ms - it->second.retire_ms) > entry_ttl) {
                it = rmap.erase(it);
                ++evicted;
            } else {
                ++it;
            }
        }
        const uint64_t cap = order_sid_retired_cap();
        while (rmap.size() > cap) {
            auto oldest = rmap.begin();
            for (auto it = std::next(rmap.begin()); it != rmap.end(); ++it) {
                if (it->second.retire_ms < oldest->second.retire_ms) oldest = it;
            }
            rmap.erase(oldest);
            ++evicted;
        }
        if (evicted) retired_evicted_state().fetch_add(evicted, std::memory_order_relaxed);
    }

    // ── Journal 写入与 replay ──────────────────

#ifndef _WIN32
    /// @brief 把 journal 原子重写为内存活动映射。
    ///
    /// 仅持久化 sid!=0 的条目；remember 是 last-write-wins，forget 是 erase。
    /// 写入使用 tmp + rename：rename 前失败保留原文件，rename 后读取新文件。
    /// @pre 调用方持有 order_sid_mutex()。
    static void compact_order_sid_journal_locked() noexcept {
        const std::string path = configured_order_sid_journal_path_locked();
        if (path.empty()) return;
        // 先消费计数器：compaction 失败时也退避一个 threshold 周期，避免只读文件系统
        // 或 inode 耗尽时每次 append 都重试失败的 open(tmp)。
        order_sid_journal_append_count() = 0;
        const std::string tmp = path + ".compact.tmp";
        const int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
        if (fd < 0) {
            note_journal_persist_failure_locked("compact_open", errno);
            return;
        }
        bool ok = true;
        int err = 0;
        char line[80];
        for (const auto& kv : order_sid_map()) {
            if (kv.second == 0) continue;  // 防御：绝不持久化 erase 哨兵
            const int len = std::snprintf(line, sizeof(line), "%llu %u\n",
                                          static_cast<unsigned long long>(kv.first),
                                          static_cast<unsigned>(kv.second));
            if (len <= 0 || static_cast<size_t>(len) >= sizeof(line)) continue;
            size_t off = 0;
            const size_t n = static_cast<size_t>(len);
            while (off < n) {
                const ssize_t wrote = ::write(fd, line + off, n - off);
                if (wrote <= 0) { ok = false; err = errno; break; }
                off += static_cast<size_t>(wrote);
            }
            if (!ok) break;
        }
        if (ok && ::fsync(fd) != 0) { ok = false; err = errno; }
        if (::close(fd) != 0 && ok) { ok = false; err = errno; }
        if (ok && ::rename(tmp.c_str(), path.c_str()) == 0) {
            // 整文件已原子替换为当前活动映射：此前所有丢失的 append 被治愈，清降级闩。
            note_journal_compaction_healed_locked();
        } else {
            // rename 失败：就地捕获真实 errno（后续 unlink 会覆盖 errno）。ok==false 时
            // err 已在写/fsync/close 失败点捕获，保留不覆盖。
            if (ok) err = errno;
            // 回滚失败的 tmp，保留原 journal；计数器已在顶部退避。
            (void)::unlink(tmp.c_str());
            note_journal_persist_failure_locked("compact_write", err);
        }
    }
#endif

    /// @brief 追加一条 order_id -> sid 记录到 journal（checked，失败置降级闩）。
    ///
    /// sid_id=0 表示删除该 order_id。POSIX 路径默认 fsync，可用
    /// `XT_STRATEGY_SID_OWNER_FSYNC=0` 关闭。open/短写/write/fsync/close 任一失败即
    /// 本条 append 未确认落盘 → 记降级（内存映射已由调用方先行更新，lookup 不受影响）。
    /// @pre 调用方持有 order_sid_mutex()。
    static void append_order_sid_journal_locked(uint64_t order_id, uint32_t sid_id) noexcept {
        const std::string path = configured_order_sid_journal_path_locked();
        if (path.empty() || order_id == 0) return;
#ifdef _WIN32
        (void)sid_id;
#else
        ensure_parent_dir_for_journal(path);
        const int fd = ::open(path.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0600);
        if (fd < 0) {
            note_journal_persist_failure_locked("append_open", errno);
            return;
        }

        // durable=false 表示这条 append 未确认落盘；err 就地捕获首个失败点的 errno
        // （close 会覆盖 errno，故不能事后再读）。
        bool durable = true;
        int err = 0;
        char line[80];
        const int len = std::snprintf(line, sizeof(line), "%llu %u\n",
                                      static_cast<unsigned long long>(order_id),
                                      static_cast<unsigned>(sid_id));
        if (len <= 0 || static_cast<size_t>(len) >= sizeof(line)) {
            durable = false;  // snprintf 编码异常（防御：正常 order_id/sid_id 不触发）
        } else {
            size_t off = 0;
            const size_t n = static_cast<size_t>(len);
            while (off < n) {
                const ssize_t wrote = ::write(fd, line + off, n - off);
                if (wrote <= 0) { durable = false; err = errno; break; }
                off += static_cast<size_t>(wrote);
            }
            if (durable) {
                const char* fsync_env = std::getenv("XT_STRATEGY_SID_OWNER_FSYNC");
                const bool fsync_enabled = !(fsync_env && fsync_env[0] == '0');
                if (fsync_enabled && ::fsync(fd) != 0) { durable = false; err = errno; }
            }
        }
        if (::close(fd) != 0 && durable) { durable = false; err = errno; }
        if (!durable) note_journal_persist_failure_locked("append_write", err);

        // 计数照旧推进（成功路径逐字节不变；失败路径也推进以维持 compaction 退避，且一次
        // 成功 compaction 会把整份活动映射重写落盘、治愈此前丢失的 append 并清降级闩）。
        if (++order_sid_journal_append_count() >= order_sid_journal_compact_threshold()) {
            compact_order_sid_journal_locked();
        }
#endif
    }

    /// @brief 首次访问时 replay journal 到内存注册表。
    ///
    /// 读取时按文件顺序折叠：sid_id=0 删除条目，非 0 写入/覆盖条目。
    /// @pre 调用方持有 order_sid_mutex()。
    static void load_order_sid_journal_locked() {
        bool& loaded = order_sid_journal_loaded();
        if (loaded) return;
        loaded = true;
        const std::string path = configured_order_sid_journal_path_locked();
        if (path.empty()) return;

        bool replayed_any = false;
        {
            std::ifstream in(path);
            if (!in.is_open()) return;
            uint64_t order_id = 0;
            uint32_t sid_id = 0;
            while (in >> order_id >> sid_id) {
                if (order_id == 0) continue;
                replayed_any = true;
                if (sid_id == 0) {
                    order_sid_map().erase(order_id);
                } else {
                    order_sid_map()[order_id] = sid_id;
                }
            }
        }
#ifndef _WIN32
        // ifstream 已离开作用域；compaction 可以用 rename 覆盖原文件，并把 append
        // 流收敛为当前活动映射。
        if (replayed_any) compact_order_sid_journal_locked();
#endif
    }

    /// @brief 发单线程上的临时 StrategySid 存储。
    ///
    /// Live order gateway 在进入 venue submit 前设置；venue gateway submit 边界消费后
    /// 立刻恢复为 no-sid，避免污染同线程后续发单。
    static StrategySid& thread_submit_strategy_sid() {
        static thread_local StrategySid sid;
        return sid;
    }
};

}  // namespace xt_identity
