#pragma once

#include <atomic>
#include <string>

/// @brief 进程级工具集合。
///
/// 失败日志和业务降级策略由调用方处理。
class XtProcessUtil {
public:
    struct RuntimeTuningConfig {
        int         main_affinity   = -1;    ///< 主线程 CPU id；-1 表示跳过 CPU/NUMA 绑定
        std::string main_priority;           ///< 主线程调度优先级 label；空表示保持默认调度
        bool        enable_mlockall = false; ///< true 时尝试锁定进程地址空间；失败由调用方记录
    };

    struct RuntimeTuningResult {
        bool mlockall_requested = false;
        bool mlockall_succeeded = false;
    };

    /// @brief 安装 SIGINT/SIGTERM 等优雅退出 signal handler。
    ///
    /// handler 只写入传入的 atomic 标志；调用方必须保证两个 atomic 对象的生命周期
    /// 覆盖进程退出信号处理窗口。重复调用会覆盖上一组目标指针。
    static void install_shutdown_signal_handlers(std::atomic<bool>& shutdown_flag,
                                                 std::atomic<int>& shutdown_sig);

    /// @brief 应用进程主线程 affinity、priority 与 mlockall 调优。
    ///
    /// affinity 同时用于 NUMA 本地内存绑定；`main_affinity < 0`、空 priority 和
    /// `enable_mlockall=false` 分别表示跳过对应调优项。函数不写日志，调用方可根据
    /// 返回值记录 mlockall 诊断。
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

        /// @brief 获取 `cfg_path + ".lock"` 对应的单实例锁。
        ///
        /// 调用会先释放当前已持有的锁；返回 false 时不持有新锁。`holder_pid`
        /// 可为空，非空时仅在锁冲突且锁文件可读时写入。
        bool acquire(const std::string& cfg_path, std::string* holder_pid);

        /// @brief 释放当前持有的文件锁；未持锁时为 no-op。
        void release();

    private:
        int fd_ = -1;
    };

    XtProcessUtil() = delete;
};
