#pragma once

/// @file
/// @brief `XtLogger` 初始化配置与 YAML 解析入口。
///
/// 配置只描述文件、console、级别、async queue 和进程级 module/throttle 表；
/// 解析函数接受完整 YAML root 或其中的 `log` 节点。

#include <cstddef>
#include <string>
#include <unordered_map>

enum class LogLevel { Trace, Debug, Info, Warn, Error, Critical, Off };

/// @brief `XtLogger::init()` 使用的日志系统配置。
///
/// `queue_size` / `thread_count` 控制 spdlog async thread pool。
/// `modules` 是 module name -> enabled；`throttle_ms` 是 throttle key -> window ms。
struct XtLoggerCfg {

    std::string path = "logs/ct.log";

    bool console = true;

    LogLevel level = LogLevel::Info;

    std::size_t max_size = 50 * 1024 * 1024;

    std::size_t max_files = 10;

    std::size_t queue_size = 8192;

    std::size_t thread_count = 1;

    std::unordered_map<std::string, bool> modules;

    std::unordered_map<std::string, int> throttle_ms;
};

namespace YAML { class Node; }

XtLoggerCfg parse_logger_config(const YAML::Node& node);
