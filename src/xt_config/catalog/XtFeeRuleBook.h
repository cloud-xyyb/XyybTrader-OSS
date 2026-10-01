#pragma once

#include <string>
#include <unordered_map>

#include "api/IFeeRuleProvider.h"

namespace YAML {
class Node;
}

/**
 * @brief 手续费规则查询表。
 *
 * 本类按归一化品种码保存 `XtFeeRule`，并通过 `IFeeRuleProvider`
 * 向成本、风控等消费方提供只读查询。
 */
class XtFeeRuleBook : public IFeeRuleProvider {
public:
    // ── 配置模型 ──────────────────────────────────────────────
    struct Config {
        std::unordered_map<std::string, XtFeeRule> rules;  ///< 归一化品种码 → 费率规则。
    };

    // ── 费率规则访问 ──────────────────────────────────────────────
    XtFeeRuleBook() = default;
    explicit XtFeeRuleBook(const Config& cfg) { set_config(cfg); }
    void set_config(const Config& cfg);
    bool empty() const { return rules_.empty(); }
    const std::unordered_map<std::string, XtFeeRule>& rules() const { return rules_; }

    // ── IFeeRuleProvider 实现 ─────────────────────────────────────
    [[nodiscard]] bool empty_rules() const noexcept override { return rules_.empty(); }
    /// @return 命中时返回目录内规则指针；code 为空或未命中时返回 nullptr。
    [[nodiscard]] const XtFeeRule* lookup_fee_rule(const char* code) const noexcept override;

private:
    std::unordered_map<std::string, XtFeeRule> rules_;  ///< 归一化品种码 → 费率规则。
};

/// @brief 从 YAML map 解析手续费规则；非法或空节点返回空 Config。
XtFeeRuleBook::Config parse_fee_rule_book_config(const YAML::Node& node);
