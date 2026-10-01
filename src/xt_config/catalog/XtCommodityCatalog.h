#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

#include "api/XtCommodityInfo.h"  // XtCommodityInfo POD（契约层 DTO）

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
        std::unordered_map<std::string, XtCommodityInfo> map;  ///< 归一化品种码 → 品种规格。
        std::unordered_map<std::string, double> margin_overrides;  ///< 保证金率覆盖表。
        double default_margin_rate = 1.0;  ///< 未命中 overrides/catalog margin_rate 时的默认保证金率。
        XtOffsetPolicy default_offset_policy = XtOffsetPolicy::OpenAllowed;  ///< venue 默认开平路由策略。
    };

    // ── 合约代码归一化（symbol key normalization） ──────────────
    /// @brief 归一化键：保留字母（转大写）+ 数字，去除其他字符。
    static std::string normalize_key(const std::string& key);
    /// @brief 从完整合约代码中提取品种前缀并转大写，如 "ag2506" → "AG"。
    static std::string extract_symbol_key(const char* code);

    // ── 配置与查询接口 ──────────────────────────────────────────────
    XtCommodityCatalog() = default;
    explicit XtCommodityCatalog(Config cfg) { reset_from_config(std::move(cfg)); initialized_ = true; }
    XtCommodityCatalog(const XtCommodityCatalog&) = delete;
    XtCommodityCatalog& operator=(const XtCommodityCatalog&) = delete;
    XtCommodityCatalog(XtCommodityCatalog&&) = delete;
    XtCommodityCatalog& operator=(XtCommodityCatalog&&) = delete;

    /// @brief publication 前一次性初始化完整目录；调用后对象只读查询。
    /// @warning 只能在把 `this` 发布给 AccountStateStore / risk / gateway / view 前调用一次；
    ///          重复调用会 fail-fast 抛出 `std::logic_error`。
    void init(Config cfg);

    const std::unordered_map<std::string, XtCommodityInfo>& entries() const;

    /// @return 命中时返回目录内对象指针；code 为空或未命中时返回 nullptr。
    const XtCommodityInfo* find(const char* code) const;

    /// @brief 查询保证金率：overrides 优先，其次 catalog entry，最后默认保证金率。
    double margin_rate_for(const char* code) const;
    /// @brief 同 margin_rate_for(code)，但复用 caller 已 find 的 info 免二次查找（热路径用）。
    /// @param info 须为 find(code) 的结果（可空）；overrides 命中时 info 不参与。
    double margin_rate_for(const XtCommodityInfo* info, const char* code) const;

    /// @brief 消解后的品种开平路由策略：品种显式配置优先，Inherit/未命中 → venue 默认。
    /// 返回值永不为 Inherit（路由层不见哨兵值）。
    XtOffsetPolicy effective_offset_policy(const char* code) const {
        const XtCommodityInfo* info = find(code);
        if (info && info->offset_policy != XtOffsetPolicy::Inherit) return info->offset_policy;
        return default_offset_policy_;
    }

    /// @brief 单位数量开仓保证金 (fixed-point)：price_fp × vol_scale × margin_rate。
    /// @param info caller 已 find 的品种规格（可空 → vol_scale 按 1.0）；传入复用避免热路径重复 find。
    /// @note vol_scale 缺失/非有限/非正时按 1.0；margin_rate 走 margin_rate_for(code)（含 override 语义，故仍需 code）。
    ///       调用方应以 catalog 结果作为 instrument cost 参数单源，避免与 override 口径分叉。
    int64_t per_unit_margin_fp(const XtCommodityInfo* info, const char* code, int64_t price_fp) const;

private:
    void reset_from_config(Config&& cfg);

    // ── 合约映射缓存 ──────────────────────────────────────────────
    std::unordered_map<std::string, XtCommodityInfo> map_;      ///< 归一化品种码 → 品种规格。
    std::unordered_map<std::string, double> margin_overrides_;  ///< 保证金率覆盖索引。
    double default_margin_rate_ = 1.0;                          ///< 默认保证金率
    XtOffsetPolicy default_offset_policy_ = XtOffsetPolicy::OpenAllowed;  ///< venue 默认开平路由（Inherit 消解目标）
    bool initialized_ = false;  ///< true 表示已通过构造函数或 init() 完成 publication 前装配。
};

/// @brief 从 YAML map 解析品种目录；非法或空节点返回空 Config。
XtCommodityCatalog::Config parse_commodity_catalog_config(const YAML::Node& node);
