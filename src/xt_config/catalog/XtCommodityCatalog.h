#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

#include "api/XtCommodityInfo.h"

namespace YAML {
class Node;
}

/// @brief 品种规格目录。
///
/// 以归一化品种码保存 `XtCommodityInfo`。
/// 规格查询先尝试完整合约码归一化结果，再尝试合约字母前缀。
class XtCommodityCatalog {
public:
    struct Config {
        std::unordered_map<std::string, XtCommodityInfo> map;
        std::unordered_map<std::string, double> margin_overrides;
        double default_margin_rate = 1.0;
        XtOffsetPolicy default_offset_policy = XtOffsetPolicy::OpenAllowed;
    };

    static std::string normalize_key(const std::string& key);

    static std::string extract_symbol_key(const char* code);

    XtCommodityCatalog() = default;
    explicit XtCommodityCatalog(Config cfg) { reset_from_config(std::move(cfg)); initialized_ = true; }
    XtCommodityCatalog(const XtCommodityCatalog&) = delete;
    XtCommodityCatalog& operator=(const XtCommodityCatalog&) = delete;
    XtCommodityCatalog(XtCommodityCatalog&&) = delete;
    XtCommodityCatalog& operator=(XtCommodityCatalog&&) = delete;

    void init(Config cfg);

    const std::unordered_map<std::string, XtCommodityInfo>& entries() const;

    const XtCommodityInfo* find(const char* code) const;

    double margin_rate_for(const char* code) const;

    double margin_rate_for(const XtCommodityInfo* info, const char* code) const;

    XtOffsetPolicy effective_offset_policy(const char* code) const {
        const XtCommodityInfo* info = find(code);
        if (info && info->offset_policy != XtOffsetPolicy::Inherit) return info->offset_policy;
        return default_offset_policy_;
    }

    int64_t per_unit_margin_fp(const XtCommodityInfo* info, const char* code, int64_t price_fp) const;

private:
    void reset_from_config(Config&& cfg);

    std::unordered_map<std::string, XtCommodityInfo> map_;
    std::unordered_map<std::string, double> margin_overrides_;
    double default_margin_rate_ = 1.0;
    XtOffsetPolicy default_offset_policy_ = XtOffsetPolicy::OpenAllowed;
    bool initialized_ = false;
};

XtCommodityCatalog::Config parse_commodity_catalog_config(const YAML::Node& node);
