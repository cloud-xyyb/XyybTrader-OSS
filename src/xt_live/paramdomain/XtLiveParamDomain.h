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
        std::string type   = "memory";           ///< "memory" | "shm"
        std::string domain = "xyybtrader_live"; ///< shm/sem 对象名的命名空间
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

    // ── 后端管理 ──────────────────────────────────────────────

    /// @brief 切换参数后端；支持 "memory"/"inmemory" 与 "shm"/"shared_memory"。
    bool configure_backend(const BackendConfig& cfg, std::string* err = nullptr);

    /// @brief 返回当前后端名称，"memory" 或 "shm"（持锁读取）
    std::string backend_name() const;

    /// @brief 等待版本号超过 `seen_version`；`timeout_ms == 0` 表示无限等待。
    bool wait_for_version_change(uint64_t seen_version,
                                 uint32_t timeout_ms,
                                 uint64_t* out_version = nullptr);

    // ── KV 读写 ──────────────────────────────────────────────

    /// @brief 写入参数（返回 true 表示值发生了变更）
    ///
    /// 等价于 set_checked(key, value, &changed) 并返回 changed。
    bool set(const std::string& key, const std::string& value);

    /// @brief 写入参数并报告是否变更（set with change detection）
    ///
    /// 值未变化时为 no-op（不递增版本、不通知）。
    ///
    /// @param key         参数键（非空，shm 后端长度 < kShmKeyCap）
    /// @param value       参数值（shm 后端长度 < kShmValueCap）
    /// @param out_changed 写入本次是否触发值变更（可为 nullptr）
    /// @param err         失败时写入错误描述（可为 nullptr）
    /// @return            true 表示操作成功（无论值是否变更）
    bool set_checked(const std::string& key,
                     const std::string& value,
                     bool* out_changed = nullptr,
                     std::string* err = nullptr);

    /// @brief 删除参数（返回 true 表示 key 存在且已删除）
    bool erase(const std::string& key);

    /// @brief 读取参数值（shm 后端下先刷新缓存）
    ///
    /// @param out  key 存在时写入对应值
    /// @return     true 表示 key 存在且 out 已写入
    bool get(const std::string& key, std::string& out) const;

    // ── 便捷读取（带类型转换与默认值） ──────────────────────────────────────────────

    /// key 不存在或读取失败时返回 fallback
    std::string get_or(const std::string& key, const std::string& fallback) const;
    /// 接受 "1"/"true"/"yes"/"on"（及其否定形式）；无法解析时返回 fallback
    bool        get_bool_or(const std::string& key, bool fallback) const;
    /// 十进制 strtoll 解析；无法解析或有余项时返回 fallback
    int64_t     get_int_or(const std::string& key, int64_t fallback) const;
    /// strtod 解析；无法解析或有余项时返回 fallback
    double      get_double_or(const std::string& key, double fallback) const;

    // ── 版本与快照 ──────────────────────────────────────────────

    /// @brief 读取当前版本号（shm 后端下先刷新缓存，持锁）
    uint64_t version() const;

    /// @brief 直接读取缓存版本号（不触发 shm 刷新，无锁，适合热路径轮询）
    uint64_t cached_version() const { return version_.load(std::memory_order_relaxed); }

    /// @brief 返回当前参数数量（shm 后端下先刷新缓存，持锁）
    size_t size() const;

    /// @brief 原子快照（持锁期间一次性拷贝版本号与全部 KV）
    Snapshot snapshot() const;

private:
    struct ShmEntry;
    struct ShmState;
    struct BackendShm;

    enum class BackendType {
        Memory, ///< 进程内 unordered_map
        Shm     ///< POSIX 共享内存
    };

    // --- 内部工具 ---
    static bool parse_bool(const std::string& v, bool& out);
    /// timeout_ms == 0 表示无限等待；EINTR 透明重试；macOS 降级为轮询
    static bool sem_wait_timeout(sem_t* sem, uint32_t timeout_ms);

    // --- shm 后端内部操作（调用方必须持有 mu_）---
    bool init_shm_backend_locked(const BackendConfig& cfg, std::string* err);
    void close_shm_backend_locked();
    bool refresh_from_shm_locked(bool force) const; ///< force=true 跳过版本号快速检查
    bool apply_set_shm_locked(const std::string& key,
                              const std::string& value,
                              bool* out_changed = nullptr,
                              std::string* err = nullptr);
    bool apply_erase_shm_locked(const std::string& key,
                                bool* out_changed = nullptr,
                                std::string* err = nullptr);
    /// 批量写入，版本号统一递增一次；容量满时静默跳过（partial write 语义）
    bool apply_bulk_set_shm_locked(const std::vector<std::pair<std::string, std::string>>& flat);
    /// changed=false 时为 no-op；changed=true 无条件唤醒 cv + 可选 sem_post
    void notify_param_changed_locked(bool changed);

    bool lock_shm_file_locked(int op) const;   ///< flock(shm_fd, op)
    bool unlock_shm_file_locked() const;        ///< flock(shm_fd, LOCK_UN)

    // shm 布局常量（Layout constants）：修改须同步升级 kShmAbi
    static constexpr size_t kShmEntryCap  = 1024; ///< 最大条目数
    static constexpr size_t kShmKeyCap    = 96;   ///< key 最大字节数（含 '\0'）
    static constexpr size_t kShmValueCap  = 384;  ///< value 最大字节数（含 '\0'）

    BackendType backend_type_ = BackendType::Memory; ///< 当前后端类型（mu_ 保护）

    mutable std::mutex              mu_;          ///< 保护以下所有可变状态
    mutable std::condition_variable cv_;          ///< memory 后端版本变更通知
    mutable std::unordered_map<std::string, std::string> values_; ///< 本地缓存
    std::string                     backend_name_ = "memory";     ///< 调试展示用
    std::shared_ptr<BackendShm>     shm_;         ///< shm 资源句柄（nullptr = 未激活）
    mutable std::atomic<uint64_t>   version_{0};  ///< 单调递增版本号
};
