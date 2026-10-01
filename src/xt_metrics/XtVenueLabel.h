#pragma once
#include <string_view>

/// @file XtVenueLabel.h
/// @brief md_provider 到 Prometheus venue label 的单源映射。
///
/// metric/report/live 侧应统一调用本 helper，避免配置字段和解析字段各自生成
/// label，导致 panel `info()` join 失败。

namespace xt_util {

/// @brief md_provider 字符串 → canonical prometheus venue label。
///
/// 映射规则（大小写敏感，按优先级顺序）：
///   含 "binance"                                → "binance_u"
///   == "ctp" / "ctp_simnow" / "simnow"         → "ctp"
///   含 "xtp"                                   → "xtp"
///   fallback                                   → 原 md_provider（保持既有 label）
///
/// @note 返回值为 string_view，生命期与字面量相同（静态存储）或与入参相同（fallback 路径）。
///   调用方在构造 std::string 前不得销毁 md_provider 所引用的原始字符串（fallback 路径）。
inline std::string_view canonical_venue_label(std::string_view md_provider) noexcept {
    if (md_provider.find("binance") != std::string_view::npos) return "binance_u";
    if (md_provider == "ctp" || md_provider == "ctp_simnow" || md_provider == "simnow")
        return "ctp";
    if (md_provider.find("xtp") != std::string_view::npos) return "xtp";
    return md_provider;  // 保留未知 provider 的既有 label。
}

}  // namespace xt_util
