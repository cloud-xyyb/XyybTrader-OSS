#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>

#include <spdlog/spdlog.h>
#include "XtLoggerCfg.h"

/// @brief 进程级异步日志入口。
///
/// - 全部状态由 static API 管理，构造 XtLogger 对象不会创建独立 logger。
/// - 日志写入 spdlog async 队列、由后台线程落盘
/// - 队列满时丢弃最旧一条，不阻塞调用方。
/// - module filter 与 throttle 可按模块/key 控制输出量
/// - 未初始化时所有调用为 no-op
class XtLogger {
public:
    using Config = XtLoggerCfg;

    static void init_fast(const char* logfile,
                          LogLevel level = LogLevel::Info,
                          size_t max_size = 50 * 1024 * 1024,
                          size_t max_files = 10,
                          size_t queue_size = 8192,
                          size_t thread_count = 1,
                          bool console = false);

    static void init(const Config& cfg);

    static void set_level(LogLevel level);

    static void set_module_enabled(const std::string& module, bool enabled);

    static void set_throttle_ms(const std::string& key, int ms);

    static int throttle_ms(const std::string& key);

    template <typename... Args>
    static void trace(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Trace, fmt, args...);
    }

    template <typename... Args>
    static void debug(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Debug, fmt, args...);
    }

    template <typename... Args>
    static void info(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Info, fmt, args...);
    }

    template <typename... Args>
    static void warn(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Warn, fmt, args...);
    }

    template <typename... Args>
    static void error(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Error, fmt, args...);
    }

    template <typename... Args>
    static void critical(const char* fmt, const Args&... args) {
        log_impl(LogLevel::Critical, fmt, args...);
    }

    template <typename... Args>
    static void trace_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Trace, module, fmt, args...);
    }

    template <typename... Args>
    static void debug_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Debug, module, fmt, args...);
    }

    template <typename... Args>
    static void info_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Info, module, fmt, args...);
    }

    template <typename... Args>
    static void warn_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Warn, module, fmt, args...);
    }

    template <typename... Args>
    static void error_m(const char* module, const char* fmt, const Args&... args) {
        log_impl_mod(LogLevel::Error, module, fmt, args...);
    }

    template <typename... Args>
    static void info_mt(const char* module, const char* key, const char* fmt, const Args&... args) {
        log_impl_mod_throttle(LogLevel::Info, module, key, fmt, args...);
    }

    template <typename... Args>
    static void warn_mt(const char* module, const char* key, const char* fmt, const Args&... args) {
        log_impl_mod_throttle(LogLevel::Warn, module, key, fmt, args...);
    }

    template <typename... Args>
    static void error_mt(const char* module, const char* key, const char* fmt, const Args&... args) {
        log_impl_mod_throttle(LogLevel::Error, module, key, fmt, args...);
    }

    static void shutdown();

private:

    static std::shared_ptr<spdlog::logger> logger();

    static int to_spd_level(LogLevel level);

    static bool module_enabled(const std::string& module);

    static bool should_throttle(const std::string& key, int ms);

    template <typename... Args>
    static void log_impl(LogLevel level, const char* fmt, const Args&... args) {
        auto lg = logger();
        if (!lg)
            return;

        lg->log(static_cast<spdlog::level::level_enum>(to_spd_level(level)),
                SPDLOG_FMT_RUNTIME(fmt), args...);
    }

    template <typename... Args>
    static void log_impl_mod(LogLevel level, const char* module, const char* fmt, const Args&... args) {
        if (!module || !module[0]) {
            log_impl(level, fmt, args...);
            return;
        }
        if (!module_enabled(module))
            return;
        auto lg = logger();
        if (!lg)
            return;
        std::string f = std::string("[") + module + "] " + fmt;
        lg->log(static_cast<spdlog::level::level_enum>(to_spd_level(level)),
                SPDLOG_FMT_RUNTIME(f.c_str()), args...);
    }

    template <typename... Args>
    static void log_impl_mod_throttle(LogLevel level, const char* module, const char* key, const char* fmt, const Args&... args) {
        int ms = throttle_ms(key ? key : "");
        if (ms > 0 && should_throttle(std::string(module ? module : "") + ":" + (key ? key : ""), ms))
            return;
        log_impl_mod(level, module, fmt, args...);
    }
};
