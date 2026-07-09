#pragma once

/// @file
/// @brief provider 名 → venue capability 的纯函数 trait。
///
/// capability 判定的唯一入口；consumer 禁止各自复制前缀判定
/// （closeable gate 在 TD gateway 创建之前求值，只能按名字查）。

#include <string>

namespace xt_live::common {

inline bool provider_is_binance(const std::string& provider) {
    return provider.rfind("binance", 0) == 0;
}

inline bool provider_is_ctp(const std::string& provider) {
    return provider == "ctp";
}

inline bool provider_is_xtp(const std::string& provider) {
    return provider == "xtp";
}

inline bool provider_closeable_known_at_bootstrap(const std::string& provider) {
    return provider_is_binance(provider);
}

}
