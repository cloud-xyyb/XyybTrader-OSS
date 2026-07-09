#pragma once
#include <string>

/// @file
/// @brief 配置文件路径定位入口。
///
/// `xt_locate_config()` 按 argv/env/search 链返回配置路径。显式 argv/env 路径原样返回；
/// 搜索链只返回实际存在的默认配置文件。

std::string xt_locate_config(int argc, char** argv,
                             const char* env_name = "XT_CONFIG",
                             const char* default_name = "config.yaml");
