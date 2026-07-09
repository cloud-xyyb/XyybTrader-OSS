#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <semaphore.h>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief 线程安全 KV 参数域，支持 memory 和 shm 后端。
 *
 * @warning 不是 non-blocking 组件：memory 后端会等待 condition_variable，
 *          shm 后端可能进入 flock/sem_wait。
 * @note 本类只维护 KV、版本递增和跨进程通知；业务解释由调用方完成。
 */
class XtLiveParamDomain {
public:
    /// 后端配置（backend configuration）
    struct BackendConfig {
        std::string type   = "memory";
        std::string domain = "xyybtrader_live";
    };

    /// 参数快照（atomic snapshot），持锁期间一次性拷贝，调用方可在锁外安全使用
    struct Snapshot {
        uint64_t version = 0;
        std::unordered_map<std::string, std::string> values;
    };

    XtLiveParamDomain();
    ~XtLiveParamDomain();

    XtLiveParamDomain(const XtLiveParamDomain&) = delete;
    XtLiveParamDomain& operator=(const XtLiveParamDomain&) = delete;

    bool configure_backend(const BackendConfig& cfg, std::string* err = nullptr);

    std::string backend_name() const;

    bool wait_for_version_change(uint64_t seen_version,
                                 uint32_t timeout_ms,
                                 uint64_t* out_version = nullptr);

    bool set(const std::string& key, const std::string& value);

    bool set_checked(const std::string& key,
                     const std::string& value,
                     bool* out_changed = nullptr,
                     std::string* err = nullptr);

    bool erase(const std::string& key);

    bool get(const std::string& key, std::string& out) const;

    std::string get_or(const std::string& key, const std::string& fallback) const;

    bool        get_bool_or(const std::string& key, bool fallback) const;

    int64_t     get_int_or(const std::string& key, int64_t fallback) const;

    double      get_double_or(const std::string& key, double fallback) const;

    uint64_t version() const;

    uint64_t cached_version() const { return version_.load(std::memory_order_relaxed); }

    size_t size() const;

    Snapshot snapshot() const;

private:
    struct ShmEntry;
    struct ShmState;
    struct BackendShm;

    enum class BackendType {
        Memory,
        Shm
    };

    static bool parse_bool(const std::string& v, bool& out);

    static bool sem_wait_timeout(sem_t* sem, uint32_t timeout_ms);

    bool init_shm_backend_locked(const BackendConfig& cfg, std::string* err);
    void close_shm_backend_locked();
    bool refresh_from_shm_locked(bool force) const;
    bool apply_set_shm_locked(const std::string& key,
                              const std::string& value,
                              bool* out_changed = nullptr,
                              std::string* err = nullptr);
    bool apply_erase_shm_locked(const std::string& key,
                                bool* out_changed = nullptr,
                                std::string* err = nullptr);

    bool apply_bulk_set_shm_locked(const std::vector<std::pair<std::string, std::string>>& flat);

    void notify_param_changed_locked(bool changed);

    bool lock_shm_file_locked(int op) const;
    bool unlock_shm_file_locked() const;

    static constexpr size_t kShmEntryCap  = 1024;
    static constexpr size_t kShmKeyCap    = 96;
    static constexpr size_t kShmValueCap  = 384;

    BackendType backend_type_ = BackendType::Memory;

    mutable std::mutex              mu_;
    mutable std::condition_variable cv_;
    mutable std::unordered_map<std::string, std::string> values_;
    std::string                     backend_name_ = "memory";
    std::shared_ptr<BackendShm>     shm_;
    mutable std::atomic<uint64_t>   version_{0};
};
