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

    struct Config {
        std::unordered_map<std::string, XtFeeRule> rules;
    };

    XtFeeRuleBook() = default;
    explicit XtFeeRuleBook(const Config& cfg) { set_config(cfg); }
    void set_config(const Config& cfg);
    bool empty() const { return rules_.empty(); }
    const std::unordered_map<std::string, XtFeeRule>& rules() const { return rules_; }

    [[nodiscard]] bool empty_rules() const noexcept override { return rules_.empty(); }

    [[nodiscard]] const XtFeeRule* lookup_fee_rule(const char* code) const noexcept override;

private:
    std::unordered_map<std::string, XtFeeRule> rules_;
};

XtFeeRuleBook::Config parse_fee_rule_book_config(const YAML::Node& node);
