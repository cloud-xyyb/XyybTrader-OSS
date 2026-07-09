#pragma once

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <fstream>
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
        return it == map.end() ? StrategySid::legacy()
                               : StrategySid::from_raw(it->second);
    }

    static void forget_order_strategy_sid(uint64_t order_id) {
        if (order_id == 0) return;
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        load_order_sid_journal_locked();
        order_sid_map().erase(order_id);
        append_order_sid_journal_locked(order_id, 0);
    }

    static void set_thread_submit_strategy_sid(StrategySid sid) {
        thread_submit_strategy_sid() = sid;
    }

    static StrategySid consume_thread_submit_strategy_sid() {
        StrategySid sid = thread_submit_strategy_sid();
        thread_submit_strategy_sid() = StrategySid::legacy();
        return sid;
    }

#ifdef XT_BUILD_TESTS

    static void set_order_strategy_sid_journal_path_for_test(std::string path) {
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        test_order_sid_journal_path() = std::move(path);
        test_order_sid_journal_path_overridden() = true;
        order_sid_map().clear();
        order_sid_journal_loaded() = false;
        order_sid_journal_append_count() = 0;
    }

    static void drop_order_strategy_sid_memory_for_test() {
        std::lock_guard<std::mutex> guard(order_sid_mutex());
        order_sid_map().clear();
        order_sid_journal_loaded() = false;
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

#ifndef _WIN32

    static void compact_order_sid_journal_locked() noexcept {
        const std::string path = configured_order_sid_journal_path_locked();
        if (path.empty()) return;

        order_sid_journal_append_count() = 0;
        const std::string tmp = path + ".compact.tmp";
        const int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
        if (fd < 0) return;
        bool ok = true;
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
                if (wrote <= 0) { ok = false; break; }
                off += static_cast<size_t>(wrote);
            }
            if (!ok) break;
        }
        if (ok) (void)::fsync(fd);
        (void)::close(fd);
        if (!(ok && ::rename(tmp.c_str(), path.c_str()) == 0)) {

            (void)::unlink(tmp.c_str());
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
        if (fd < 0) return;

        char line[80];
        const int len = std::snprintf(line, sizeof(line), "%llu %u\n",
                                      static_cast<unsigned long long>(order_id),
                                      static_cast<unsigned>(sid_id));
        if (len > 0 && static_cast<size_t>(len) < sizeof(line)) {
            size_t off = 0;
            const size_t n = static_cast<size_t>(len);
            while (off < n) {
                const ssize_t wrote = ::write(fd, line + off, n - off);
                if (wrote <= 0) break;
                off += static_cast<size_t>(wrote);
            }
            const char* fsync_env = std::getenv("XT_STRATEGY_SID_OWNER_FSYNC");
            const bool fsync_enabled = !(fsync_env && fsync_env[0] == '0');
            if (fsync_enabled) (void)::fsync(fd);
        }
        (void)::close(fd);

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
