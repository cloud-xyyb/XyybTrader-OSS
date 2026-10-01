#pragma once

/// @file
/// @brief `XtLogger` 初始化配置与 YAML 解析入口。
///
/// 配置只描述文件、console、级别、async queue 和进程级 module/throttle 表；
/// 解析函数接受完整 YAML root 或其中的 `log` 节点。

#include <cstddef>
#include <string>
#include <unordered_map>

/** @brief 日志级别（log severity level）。 */
enum class LogLevel { Trace, Debug, Info, Warn, Error, Critical, Off };

/// @brief `XtLogger::init()` 使用的日志系统配置。
///
/// `queue_size` / `thread_count` 控制 spdlog async thread pool。
/// `modules` 是 module name -> enabled；`throttle_ms` 是 throttle key -> window ms。
struct XtLoggerCfg {
    /// @brief 日志文件路径（log file path）。
    std::string path = "logs/xt.log";
    /// @brief 是否同时输出到控制台（console output）。
    bool console = true;
    /// @brief 全局最低日志级别（global minimum level）。
    LogLevel level = LogLevel::Info;
    /// @brief 单文件最大字节数（max bytes per file）。
    std::size_t max_size = 50 * 1024 * 1024;
    /// @brief 滚动文件保留数（rotating file count）。
    std::size_t max_files = 10;
    /// @brief 异步队列容量（async queue capacity）。
    std::size_t queue_size = 8192;
    /// @brief 后台 IO 线程数（backend thread count）。
    std::size_t thread_count = 1;
    /// @brief 模块级启用开关（module -> enabled）。
    std::unordered_map<std::string, bool> modules;
    /// @brief throttle key 对应的限流窗口毫秒数（key -> window ms）。
    std::unordered_map<std::string, int> throttle_ms;
};

namespace YAML { class Node; }

/// @brief 从 YAML `log` 节点解析 logger 配置。
XtLoggerCfg parse_logger_config(const YAML::Node& node);
