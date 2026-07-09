#pragma once
#include <string_view>

/// @file XtVenueLabel.h
/// @brief md_provider 到 Prometheus venue label 的单源映射。
///
/// metric/report/live 侧应统一调用本 helper，避免配置字段和解析字段各自生成
/// label，导致 panel `info()` join 失败。

namespace xt_util {

inline std::string_view canonical_venue_label(std::string_view md_provider) noexcept {
    if (md_provider.find("binance") != std::string_view::npos) return "binance_u";
    if (md_provider == "ctp" || md_provider == "ctp_simnow" || md_provider == "simnow")
        return "ctp";
    if (md_provider.find("xtp") != std::string_view::npos) return "xtp";
    return md_provider;
}

}
