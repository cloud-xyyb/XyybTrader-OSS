#pragma once

#include <atomic>
#include <string>

/// @brief 进程级工具集合。
///
/// 失败日志和业务降级策略由调用方处理。
class XtProcessUtil {
public:
    struct RuntimeTuningConfig {
        int         main_affinity   = -1;
        std::string main_priority;
        bool        enable_mlockall = false;
    };

    struct RuntimeTuningResult {
        bool mlockall_requested = false;
        bool mlockall_succeeded = false;
    };

    static void install_shutdown_signal_handlers(std::atomic<bool>& shutdown_flag,
                                                 std::atomic<int>& shutdown_sig);

    [[nodiscard]] static RuntimeTuningResult
    apply_runtime_tuning(const RuntimeTuningConfig& cfg);

    /// @brief 基于文件 `flock` 的 non-blocking 单实例进程锁。
    ///
    /// 成功后持有 `LOCK_EX | LOCK_NB` 并写入当前 PID；失败时可读取锁文件中
    /// 记录的持有者 PID，用于启动期诊断。
    class SingleInstanceLock {
    public:
        SingleInstanceLock() = default;
        ~SingleInstanceLock();

        SingleInstanceLock(const SingleInstanceLock&) = delete;
        SingleInstanceLock& operator=(const SingleInstanceLock&) = delete;

        bool acquire(const std::string& cfg_path, std::string* holder_pid);

        void release();

    private:
        int fd_ = -1;
    };

    XtProcessUtil() = delete;
};
