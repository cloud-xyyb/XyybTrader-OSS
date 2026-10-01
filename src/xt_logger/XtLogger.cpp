#include "XtLogger.h"

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <yaml-cpp/yaml.h>

#include <chrono>
#include <cctype>
#include <mutex>
#include <unordered_map>
#include <vector>

// 全局状态生命周期：这些 static 变量在 init() 时填充，shutdown() 时由 g_logger.reset() 释放 logger。
// g_modules / g_throttles / g_last 不显式清理——进程退出时自动析构，
// 避免 shutdown 后仍有线程引用 map 导致 use-after-free。
static std::shared_ptr<spdlog::logger> g_logger;
static std::unordered_map<std::string, bool> g_modules;
static std::unordered_map<std::string, int> g_throttles;
static std::unordered_map<std::string, std::chrono::steady_clock::time_point> g_last;
// g_log_mtx 保护 modules/throttles/last 三个 map 的并发读写；
// spdlog async_logger 的【内部 sink 状态】是 thread-safe（内部有锁）；但 g_logger 这个 shared_ptr
// 非原子 shared_ptr 的并发 read/reset 是 data race。g_logger 的读写一律走
// std::atomic_load/atomic_store；logger() 返回快照副本，调用方在同一副本上检查并写日志。
static std::mutex g_log_mtx;

std::shared_ptr<spdlog::logger> XtLogger::logger() {
    return std::atomic_load(&g_logger);
}

namespace {
LogLevel parse_level_node(const YAML::Node& cfg) {
    if (cfg["level"]) {
        std::string l = cfg["level"].as<std::string>();
        for (auto& c : l) c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
        if (l == "trace") return LogLevel::Trace;
        if (l == "debug") return LogLevel::Debug;
        if (l == "info") return LogLevel::Info;
        if (l == "warn") return LogLevel::Warn;
        if (l == "error") return LogLevel::Error;
        if (l == "critical") return LogLevel::Critical;
        if (l == "off") return LogLevel::Off;
    }
    return LogLevel::Info;
}
} // namespace

int XtLogger::to_spd_level(LogLevel level) {
    switch (level) {
    case LogLevel::Trace: return spdlog::level::trace;
    case LogLevel::Debug: return spdlog::level::debug;
    case LogLevel::Info: return spdlog::level::info;
    case LogLevel::Warn: return spdlog::level::warn;
    case LogLevel::Error: return spdlog::level::err;
    case LogLevel::Critical: return spdlog::level::critical;
    case LogLevel::Off: return spdlog::level::off;
    default: return spdlog::level::info;
    }
}

void XtLogger::init_fast(const char* logfile,
                         LogLevel level,
                         size_t max_size,
                         size_t max_files,
                         size_t queue_size,
                         size_t thread_count,
                         bool console) {
    // (1) 先初始化 spdlog 全局线程池，async_logger 创建时从中取 worker
    // thread_count 通常为 1：日志写入是 IO-bound，单线程足够且避免乱序
    spdlog::init_thread_pool(queue_size, thread_count);

    // (2) sink 级别设为 trace，由 logger 级别统一控制过滤——
    // 便于运行时通过 set_level() 动态调整而不需重建 sink
    auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        logfile ? logfile : "xyybtrader.log", max_size, max_files);
    file_sink->set_level(spdlog::level::trace);

    std::shared_ptr<spdlog::logger> lg;
    if (console) {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_level(spdlog::level::trace);
        lg = std::make_shared<spdlog::async_logger>(
            "ct",
            spdlog::sinks_init_list{ console_sink, file_sink },
            spdlog::thread_pool(),
            // overrun_oldest：队列满时丢弃最旧日志而非阻塞调用线程，
            // 保证策略热路径的 log 调用不会因 IO 反压而阻塞
            spdlog::async_overflow_policy::overrun_oldest);
    } else {
        lg = std::make_shared<spdlog::async_logger>(
            "ct",
            spdlog::sinks_init_list{ file_sink },
            spdlog::thread_pool(),
            spdlog::async_overflow_policy::overrun_oldest);
    }

    lg->set_level(static_cast<spdlog::level::level_enum>(to_spd_level(level)));
    lg->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    // 仅 error 及以上触发立即 flush，降低正常日志的 IO 开销
    lg->flush_on(spdlog::level::err);
    spdlog::register_logger(lg);
    // 配置完成后原子发布：并发 log 线程经 logger() 看到的要么是旧值/null、要么是 fully-configured。
    std::atomic_store(&g_logger, lg);
}

XtLogger::Config parse_logger_config(const YAML::Node& node) {
    XtLogger::Config out;
    YAML::Node cfg = node;
    if (node["log"]) cfg = node["log"];

    out.path = cfg["file"] ? cfg["file"].as<std::string>() : out.path;
    out.console = cfg["console"] ? cfg["console"].as<bool>() : out.console;
    out.level = parse_level_node(cfg);
    out.max_size = cfg["max_size"] ? cfg["max_size"].as<size_t>() : out.max_size;
    out.max_files = cfg["max_files"] ? cfg["max_files"].as<size_t>() : out.max_files;
    out.queue_size = cfg["queue_size"] ? cfg["queue_size"].as<size_t>() : out.queue_size;
    out.thread_count = cfg["threads"] ? cfg["threads"].as<size_t>() : out.thread_count;

    if (cfg["modules"] && cfg["modules"].IsMap()) {
        for (auto it = cfg["modules"].begin(); it != cfg["modules"].end(); ++it) {
            const std::string k = it->first.as<std::string>();
            const bool v = it->second.as<bool>();
            out.modules[k] = v;
        }
    }
    if (cfg["throttle_ms"] && cfg["throttle_ms"].IsMap()) {
        for (auto it = cfg["throttle_ms"].begin(); it != cfg["throttle_ms"].end(); ++it) {
            const std::string k = it->first.as<std::string>();
            const int v = it->second.as<int>();
            out.throttle_ms[k] = v;
        }
    }
    return out;
}

void XtLogger::init(const Config& cfg) {
    init_fast(cfg.path.c_str(),
              cfg.level,
              cfg.max_size,
              cfg.max_files,
              cfg.queue_size,
              cfg.thread_count,
              cfg.console);
    for (const auto& kv : cfg.modules) {
        set_module_enabled(kv.first, kv.second);
    }
    for (const auto& kv : cfg.throttle_ms) {
        set_throttle_ms(kv.first, kv.second);
    }
}

void XtLogger::set_level(LogLevel level) {
    auto lg = std::atomic_load(&g_logger);
    if (lg) {
        lg->set_level(static_cast<spdlog::level::level_enum>(to_spd_level(level)));
    }
}

void XtLogger::shutdown() {
    auto lg = std::atomic_load(&g_logger);
    if (lg) {
        lg->flush();
    }
    spdlog::shutdown();
    std::atomic_store(&g_logger, std::shared_ptr<spdlog::logger>{});  // 原子 reset，与 logger() 的 atomic_load 配对
}

void XtLogger::set_module_enabled(const std::string& module, bool enabled) {
    std::lock_guard<std::mutex> lk(g_log_mtx);
    g_modules[module] = enabled;
}

bool XtLogger::module_enabled(const std::string& module) {
    std::lock_guard<std::mutex> lk(g_log_mtx);
    auto it = g_modules.find(module);
    if (it == g_modules.end()) return true;
    return it->second;
}

void XtLogger::set_throttle_ms(const std::string& key, int ms) {
    std::lock_guard<std::mutex> lk(g_log_mtx);
    g_throttles[key] = ms;
}

int XtLogger::throttle_ms(const std::string& key) {
    std::lock_guard<std::mutex> lk(g_log_mtx);
    auto it = g_throttles.find(key);
    if (it == g_throttles.end()) return 0;
    return it->second;
}

bool XtLogger::should_throttle(const std::string& key, int ms) {
    if (ms <= 0) return false;
    // steady_clock 在 now 获取后才加锁：减少锁内耗时，
    // 且 steady_clock 不受系统时钟回拨影响，避免 NTP 跳变导致节流失效
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(g_log_mtx);
    auto it = g_last.find(key);
    if (it != g_last.end()) {
        auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second).count();
        if (diff < ms) return true;
    }
    // 首次调用或已超过节流窗口：记录本次时间戳并放行，
    // 即每个窗口期内只有第一条日志通过
    g_last[key] = now;
    return false;
}
