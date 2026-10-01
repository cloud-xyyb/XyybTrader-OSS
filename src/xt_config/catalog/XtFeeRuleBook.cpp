#include "xt_config/catalog/XtFeeRuleBook.h"

#include "xt_config/catalog/XtCommodityCatalog.h"

#include <cmath>
#include <stdexcept>
#include <string>

#include <yaml-cpp/yaml.h>

XtFeeRuleBook::Config parse_fee_rule_book_config(const YAML::Node& node) {
    XtFeeRuleBook::Config cfg;
    if (!node || node.IsNull() || !node.IsMap()) return cfg;
    // 费率字段必须有限且非负：消费方 calc_fee_from_rule 用 `>0.0` gate 会把 NaN/负值当作
    // 0-fee；解析层 fail-closed，使配置误写在启动期暴露。
    auto rate = [](const YAML::Node& item, const char* k,
                   const std::string& key, double& dst) {
        if (!item[k]) return;
        const double v = item[k].as<double>();
        if (!std::isfinite(v) || v < 0.0) {
            throw std::runtime_error("fee_rule_book[" + key + "]." + std::string(k) +
                                     " must be finite and >= 0 (got " + std::to_string(v) + ")");
        }
        dst = v;
    };
    for (auto it = node.begin(); it != node.end(); ++it) {
        if (!it->first.IsScalar()) continue;
        // fee 与 commodity catalog 共用归一化键；重复归一化 key 保持 YAML 后写覆盖语义。
        std::string key = XtCommodityCatalog::normalize_key(it->first.as<std::string>());
        if (key.empty()) continue;
        const YAML::Node item = it->second;
        XtFeeRule r;
        rate(item, "open",              key, r.open);
        rate(item, "close",             key, r.close);
        rate(item, "close_today",       key, r.close_today);
        if (item["by_volume"])    r.by_volume    = item["by_volume"].as<bool>();
        rate(item, "maker_rebate",      key, r.maker_rebate);
        rate(item, "stamp_duty_rate",   key, r.stamp_duty_rate);
        rate(item, "transfer_fee_rate", key, r.transfer_fee_rate);
        rate(item, "min_commission",    key, r.min_commission);
        cfg.rules[key] = r;
    }
    return cfg;
}

void XtFeeRuleBook::set_config(const Config& cfg) {
    rules_ = cfg.rules;
}

const XtFeeRule* XtFeeRuleBook::lookup_fee_rule(const char* code) const noexcept {
    if (!code || code[0] == '\0') return nullptr;
    const std::string exact = XtCommodityCatalog::normalize_key(code);
    auto it = rules_.find(exact);
    if (it != rules_.end()) return &it->second;

    const std::string sym = XtCommodityCatalog::extract_symbol_key(code);
    it = rules_.find(sym);
    if (it == rules_.end()) return nullptr;
    return &it->second;
}
