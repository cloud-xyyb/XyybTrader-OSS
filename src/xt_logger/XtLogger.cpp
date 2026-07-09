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

static std::shared_ptr<spdlog::logger> g_logger;
static std::unordered_map<std::string, bool> g_modules;
static std::unordered_map<std::string, int> g_throttles;
static std::unordered_map<std::string, std::chrono::steady_clock::time_point> g_last;

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
}

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

    spdlog::init_thread_pool(queue_size, thread_count);

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

    lg->flush_on(spdlog::level::err);
    spdlog::register_logger(lg);

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
    std::atomic_store(&g_logger, std::shared_ptr<spdlog::logger>{});
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

    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(g_log_mtx);
    auto it = g_last.find(key);
    if (it != g_last.end()) {
        auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second).count();
        if (diff < ms) return true;
    }

    g_last[key] = now;
    return false;
}
