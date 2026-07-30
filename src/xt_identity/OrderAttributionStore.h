#pragma once

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iterator>
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
#include "xt_identity/StrategySidRegistry.h"
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

    static void forget_order_strategy_sid(uint64_t order_id) {
        if (order_id == 0) return;
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        load_order_sid_journal_locked();
        order_sid_map().erase(order_id);
        order_sid_retired_map().erase(order_id);
        append_order_sid_journal_locked(order_id, 0);
    }

    static void retire_order_strategy_sid(uint64_t order_id, uint64_t now_ms) {
        if (order_id == 0) return;
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        load_order_sid_journal_locked();
        auto& map = order_sid_map();
        const auto it = map.find(order_id);
        if (it == map.end()) return;
        const uint32_t sid = it->second;
        map.erase(it);
        append_order_sid_journal_locked(order_id, 0);
        insert_retired_locked(order_id, sid, now_ms);
    }

    static void set_thread_submit_strategy_sid(StrategySid sid) {
        thread_submit_strategy_sid() = sid;
    }

    static StrategySid consume_thread_submit_strategy_sid() {
        StrategySid sid = thread_submit_strategy_sid();
        thread_submit_strategy_sid() = StrategySid::legacy();
        return sid;
    }

    static bool order_sid_journal_degraded() noexcept {
        return journal_degraded_state().load(std::memory_order_relaxed);
    }

    static uint64_t order_sid_journal_failure_count() noexcept {
        return journal_failure_count_state().load(std::memory_order_relaxed);
    }

    static uint64_t order_sid_retired_evicted_total() noexcept {
        return retired_evicted_state().load(std::memory_order_relaxed);
    }

#ifdef XT_BUILD_TESTS

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

    static void drop_order_strategy_sid_memory_for_test() {
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        order_sid_map().clear();
        order_sid_retired_map().clear();
        order_sid_retired_insert_count() = 0;
        order_sid_journal_loaded() = false;
    }

    static void set_order_sid_journal_degraded_for_test(bool degraded) {
        journal_degraded_state().store(degraded, std::memory_order_relaxed);
    }

    static void clear_order_strategy_sids_for_test() {
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        order_sid_map().clear();
        order_sid_journal_loaded() = false;
        order_sid_journal_append_count() = 0;
        order_sid_journal_compact_threshold() = 4096;
        test_order_sid_journal_path().clear();
        test_order_sid_journal_path_overridden() = false;
        cached_env_journal_path_resolved() = false;
        journal_degraded_state().store(false, std::memory_order_relaxed);
        journal_failure_count_state().store(0, std::memory_order_relaxed);

        order_sid_retired_map().clear();
        order_sid_retired_insert_count() = 0;
        order_sid_retired_ttl_ms() = kDefaultRetiredTtlMs;
        order_sid_retired_cap() = kDefaultRetiredCap;
        order_sid_retired_sweep_every() = kDefaultRetiredSweepEvery;
        retired_evicted_state().store(0, std::memory_order_relaxed);
    }

    static std::size_t order_sid_retired_size_for_test() {
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        return order_sid_retired_map().size();
    }
#endif

#ifdef XT_BUILD_TESTS
public:
#else
private:
#endif

    static std::mutex& order_sid_mutex() {
        static std::mutex mu;
        return mu;
    }

    static std::unordered_map<uint64_t, uint32_t>& order_sid_map() {
        static std::unordered_map<uint64_t, uint32_t> map;
        return map;
    }

    static bool& order_sid_journal_loaded() {
        static bool loaded = false;
        return loaded;
    }

#ifdef XT_BUILD_TESTS

    static std::string& test_order_sid_journal_path() {
        static std::string path;
        return path;
    }

    static bool& test_order_sid_journal_path_overridden() {
        static bool overridden = false;
        return overridden;
    }
#endif

    static std::string& cached_env_journal_path() {
        static std::string path;
        return path;
    }

    static bool& cached_env_journal_path_resolved() {
        static bool resolved = false;
        return resolved;
    }

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

    static uint64_t& order_sid_journal_append_count() noexcept {
        static uint64_t n = 0;
        return n;
    }

    static uint64_t& order_sid_journal_compact_threshold() noexcept {
        static uint64_t t = 4096;
        return t;
    }

    static std::atomic<bool>& journal_degraded_state() noexcept {
        static std::atomic<bool> degraded{false};
        return degraded;
    }

    static std::atomic<uint64_t>& journal_failure_count_state() noexcept {
        static std::atomic<uint64_t> n{0};
        return n;
    }

    static void note_journal_persist_failure_locked(const char* stage, int err) noexcept {
        const uint64_t count =
            journal_failure_count_state().fetch_add(1, std::memory_order_relaxed) + 1;
        const bool was_degraded =
            journal_degraded_state().exchange(true, std::memory_order_relaxed);

        constexpr uint64_t kLogEvery = 256;
        if (!was_degraded || (count % kLogEvery) == 0) {
            XtLogger::error_m("SID",
                "order-sid journal 持久化失败 stage={} errno={} 累计失败={} — "
                "归属落盘降级，orphan reap 将切告警-only",
                stage, err, count);
        }
    }

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

    /// @brief retired 条目：退休时的 sid + steady 毫秒时戳（TTL/淘汰用）。
    struct RetiredSidEntry {
        uint32_t sid_id;
        uint64_t retire_ms;
    };

    static constexpr uint64_t kDefaultRetiredTtlMs = 30'000;
    static constexpr uint64_t kManualRetiredTtlMs = 3'600'000;
    static constexpr uint64_t kDefaultRetiredCap = 4096;
    static constexpr uint64_t kDefaultRetiredSweepEvery = 256;

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

    static uint64_t& order_sid_retired_insert_count() noexcept {
        static uint64_t v = 0; return v;
    }
    static std::atomic<uint64_t>& retired_evicted_state() noexcept {
        static std::atomic<uint64_t> n{0}; return n;
    }

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

    static void sweep_retired_locked(uint64_t now_ms) noexcept {
        auto& rmap = order_sid_retired_map();
        const uint64_t strat_ttl = order_sid_retired_ttl_ms();
        uint64_t evicted = 0;
        for (auto it = rmap.begin(); it != rmap.end();) {
            const uint64_t entry_ttl =
                (it->second.sid_id == StrategySidRegistry::kManualSidId) ? kManualRetiredTtlMs
                                                                         : strat_ttl;

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

#ifndef _WIN32

    static void compact_order_sid_journal_locked() noexcept {
        const std::string path = configured_order_sid_journal_path_locked();
        if (path.empty()) return;

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
            if (kv.second == 0) continue;
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

            note_journal_compaction_healed_locked();
        } else {

            if (ok) err = errno;

            (void)::unlink(tmp.c_str());
            note_journal_persist_failure_locked("compact_write", err);
        }
    }
#endif

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

        bool durable = true;
        int err = 0;
        char line[80];
        const int len = std::snprintf(line, sizeof(line), "%llu %u\n",
                                      static_cast<unsigned long long>(order_id),
                                      static_cast<unsigned>(sid_id));
        if (len <= 0 || static_cast<size_t>(len) >= sizeof(line)) {
            durable = false;
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

        if (++order_sid_journal_append_count() >= order_sid_journal_compact_threshold()) {
            compact_order_sid_journal_locked();
        }
#endif
    }

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

        if (replayed_any) compact_order_sid_journal_locked();
#endif
    }

    static StrategySid& thread_submit_strategy_sid() {
        static thread_local StrategySid sid;
        return sid;
    }
};

}
