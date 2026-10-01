#pragma once

/// @file
/// @brief provider 名 → venue capability 的纯函数 trait。
///
/// capability 判定的唯一入口；consumer 禁止各自复制前缀判定
/// （closeable gate 在 TD gateway 创建之前求值，只能按名字查）。

#include <string>

namespace xt_live::common {

/// @brief provider 是否 binance 系（"binance" / "binance_u" 等前缀形态）。
inline bool provider_is_binance(const std::string& provider) {
    return provider.rfind("binance", 0) == 0;
}

/// @brief provider 是否 CTP。
inline bool provider_is_ctp(const std::string& provider) {
    return provider == "ctp";
}

/// @brief provider 是否 XTP。
inline bool provider_is_xtp(const std::string& provider) {
    return provider == "xtp";
}

/// @brief bootstrap 期 closeable 是否可直接确认（无需等首次 position recovery）。
///
/// binance：bootstrap positionRisk 全量快照即权威，closeable 立即可信。
/// CTP：closeable 今昨拆分依赖首次 position recovery，未确认前 fail-closed
/// （平仓拒 PositionStateUnknown）。
/// XTP：按 CTP 待遇等待首次 position recovery；未确认 closeable split 前不豁免，
/// 宁可 bootstrap 窗口拒平也不放行未证实的 closeable。
inline bool provider_closeable_known_at_bootstrap(const std::string& provider) {
    return provider_is_binance(provider);
}

}  // namespace xt_live::common
