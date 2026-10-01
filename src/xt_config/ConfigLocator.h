#pragma once
#include <string>

/// @file
/// @brief 配置文件路径定位入口。
///
/// `xt_locate_config()` 按 argv/env/search 链返回配置路径。显式 argv/env 路径原样返回；
/// 搜索链只返回实际存在的默认配置文件。

/**
 * @brief 按优先级搜索配置文件路径（config file locator）。
 *
 * 定位顺序（locate order）：
 * 1. argv：--config <path> / -c <path> / --config=<path>
 * 2. env：XT_CONFIG（默认环境变量名）
 * 3. search：逐级搜索 cwd / exe_dir 及其上层目录下的 <default_name> 和 configs/<default_name>
 *
 * argv/env 显式来源原样返回，不做存在性 fallback；路径写错时由 loader fail-closed。
 * 逐级 search 阶段只返回实际存在的文件。
 * 调用方可传不同 default_name，例如 live 使用 live.yaml，backtest 使用 backtest.yaml。
 *
 * @param env_name 环境变量名；nullptr 表示跳过环境变量查找。
 * @param default_name 搜索用文件名；nullptr 时使用 "config.yaml"。
 * @return 显式 argv/env 路径或 search 命中的配置路径；未提供显式来源且搜索不到时返回空字符串。
 */
std::string xt_locate_config(int argc, char** argv,
                             const char* env_name = "XT_CONFIG",
                             const char* default_name = "config.yaml");
