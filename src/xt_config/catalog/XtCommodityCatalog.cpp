#include "xt_config/catalog/XtCommodityCatalog.h"

#include <stdexcept>
#include <cmath>
#include <unordered_set>
#include <utility>
#include <vector>
#include <yaml-cpp/yaml.h>

#include "XtFixedPoint.h"  // 保证 per_unit_margin_fp 与风控侧 fixed-point 乘法口径一致。

namespace {

void append_unique(std::vector<std::string>& keys, std::string key) {
    if (key.empty()) return;
    for (const auto& existing : keys) {
        if (existing == key) return;
    }
    keys.push_back(std::move(key));
}

bool contains_digit(const std::string& key) {
    for (char c : key) {
        if (std::isdigit(static_cast<unsigned char>(c))) return true;
    }
    return false;
}

std::vector<std::string> normalized_tokens(const std::string& raw) {
    std::vector<std::string> out;
    std::string token;
    for (char c : raw) {
        const auto uc = static_cast<unsigned char>(c);
        if (std::isalnum(uc)) {
            token.push_back(static_cast<char>(std::toupper(uc)));
        } else if (!token.empty()) {
            append_unique(out, token);
            token.clear();
        }
    }
    append_unique(out, token);
    return out;
}

// 纯 venue 段 token（交易所代码）。venue-prefixed 合约键如 "SHFE.au2406" 会被 normalized_tokens
// 拆出裸 "SHFE"；若把它存成独立 override key，查同 venue 别的合约（"SHFE.rb2410"）会经 venue token
// 误命中 au2406 的费率 → 错 margin → 错冻结/accept-reject。
// 新 venue 前缀进入配置（新交易所/新市场）时必须同步扩充 kVenues，否则该 venue 重新暴露此污染面。
bool is_pure_venue_token(const std::string& t) {
    static const std::unordered_set<std::string> kVenues = {
        "SHFE", "DCE", "CZCE", "CFFEX", "INE", "GFEX", "SH", "SZ"};
    return kVenues.count(t) != 0;
}

std::vector<std::string> margin_override_storage_keys(const std::string& raw) {
    std::vector<std::string> keys;
    // 全键始终保留：venue-wide 意图（raw=="SHFE"）经此路径存为 "SHFE"，仍可被同 venue 合约查到。
    append_unique(keys, XtCommodityCatalog::normalize_key(raw));
    for (const auto& token : normalized_tokens(raw)) {
        // 跳过从合约键拆出的裸 venue 段（杜绝跨合约污染）；venue-wide 由上面全键路径覆盖。
        if (is_pure_venue_token(token)) continue;
        append_unique(keys, token);
    }
    return keys;
}

std::vector<std::string> margin_lookup_keys(const char* code) {
    std::vector<std::string> keys;
    if (!code || code[0] == '\0') return keys;

    const std::string raw(code);
    append_unique(keys, XtCommodityCatalog::normalize_key(raw));

    // overrides 兼容 "SHFE.au2406"、"au2406"、"AU"、"600000.SH" 等配置键写法。
    // 查询顺序先精确合约，再数字 token，再品种 token，避免品种级覆盖抢在合约级覆盖之前生效。
    const auto tokens = normalized_tokens(raw);
    for (const auto& token : tokens) {
        if (contains_digit(token)) append_unique(keys, token);
    }
    for (const auto& token : tokens) {
        if (!contains_digit(token)) append_unique(keys, token);
    }

    append_unique(keys, XtCommodityCatalog::extract_symbol_key(code));
    for (const auto& token : tokens) {
        append_unique(keys, XtCommodityCatalog::extract_symbol_key(token.c_str()));
    }
    return keys;
}

// 数值字段转换失败时补上「商品 key path + 原始标量文本」，替换 YAML::Clone
// 丢 mark 后 yaml-cpp 抛出的 "error at line 1, column 1: bad conversion" 无定位报错。
template <typename T>
T as_or_throw(const YAML::Node& node, const std::string& key, const char* field,
              const char* type_name) {
    try {
        return node.as<T>();
    } catch (const YAML::Exception&) {
        throw std::runtime_error("commodities." + key + "." + field + ": expected " +
                                 type_name + ", got \"" + node.Scalar() + "\"");
    }
}

std::unordered_map<std::string, XtCommodityInfo> parse_commodity_map(const YAML::Node& node) {
    std::unordered_map<std::string, XtCommodityInfo> out;
    if (!node || node.IsNull() || !node.IsMap()) return out;

    for (auto it = node.begin(); it != node.end(); ++it) {
        if (!it->first.IsScalar()) continue;

        std::string key = XtCommodityCatalog::normalize_key(it->first.as<std::string>());
        if (key.empty()) continue;

        const YAML::Node item = it->second;
        XtCommodityInfo info;
        if (item["price_tick"])
            info.price_tick = as_or_throw<double>(item["price_tick"], key, "price_tick", "double");
        if (item["vol_scale"])
            info.vol_scale = as_or_throw<double>(item["vol_scale"], key, "vol_scale", "double");
        if (item["margin_rate"])
            info.margin_rate = as_or_throw<double>(item["margin_rate"], key, "margin_rate", "double");
        if (item["slippage"]) {
            info.slippage = as_or_throw<double>(item["slippage"], key, "slippage", "double");
            if (!std::isfinite(info.slippage) || info.slippage < 0.0)
                // fail-fast：负值/NaN/±inf 无物理意义，加载期拒启动（与 offset_policy 非法值同
                // 口径）。inf 会经 xt_fill_cost::apply 原样传播成无限成交价，故与 NaN 同拦。
                throw std::runtime_error("commodities." + key + ".slippage 必须是有限非负数，得到 " +
                                         std::to_string(info.slippage));
        }
        if (item["allow_short"]) info.allow_short = item["allow_short"].as<bool>();

        if (item["has_open_interest"]) info.has_open_interest = item["has_open_interest"].as<bool>();
        if (item["has_settle_price"])  info.has_settle_price  = item["has_settle_price"].as<bool>();
        if (item["is_t_plus_1"])       info.is_t_plus_1       = item["is_t_plus_1"].as<bool>();
        if (item["min_lot"])
            info.min_lot = as_or_throw<int>(item["min_lot"], key, "min_lot", "int");
        if (item["has_broker_frozen_push"])
            info.has_broker_frozen_push = item["has_broker_frozen_push"].as<bool>();
        if (item["daily_price_limit_pct"])
            info.daily_price_limit_pct =
                as_or_throw<double>(item["daily_price_limit_pct"], key, "daily_price_limit_pct", "double");
        if (item["offset_policy"]) {
            const std::string v = item["offset_policy"].as<std::string>();
            if (v == "close_first")       info.offset_policy = XtOffsetPolicy::CloseFirst;
            else if (v == "open_allowed") info.offset_policy = XtOffsetPolicy::OpenAllowed;
            else
                // fail-fast：非法枚举值在加载期拒启动（live/BT 同一单点）。
                throw std::runtime_error(
                    "commodities." + key + ".offset_policy 非法值 '" + v +
                    "'（合法: close_first | open_allowed）");
        }

        out[key] = info;
    }
    return out;
}

}  // namespace

XtCommodityCatalog::Config parse_commodity_catalog_config(const YAML::Node& node) {
    XtCommodityCatalog::Config cfg;
    cfg.map = parse_commodity_map(node);
    return cfg;
}

std::string XtCommodityCatalog::normalize_key(const std::string& key) {
    std::string out;
    out.reserve(key.size());
    for (char c : key) {
        if (std::isalpha(static_cast<unsigned char>(c))) {
            out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
        } else if (std::isdigit(static_cast<unsigned char>(c))) {
            out.push_back(c);
        }
    }
    return out;
}

std::string XtCommodityCatalog::extract_symbol_key(const char* code) {
    if (!code) return "";
    std::string out;
    for (const char* p = code; *p; ++p) {
        if (std::isalpha(static_cast<unsigned char>(*p))) {
            out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(*p))));
        } else if (!out.empty()) {
            break;
        }
    }
    return out;
}

void XtCommodityCatalog::init(Config cfg) {
    if (initialized_) {
        throw std::logic_error("XtCommodityCatalog::init() called after catalog was already initialized");
    }
    reset_from_config(std::move(cfg));
    initialized_ = true;
}

void XtCommodityCatalog::reset_from_config(Config&& cfg) {
    map_ = std::move(cfg.map);
    margin_overrides_.clear();
    for (const auto& kv : cfg.margin_overrides) {
        for (const auto& key : margin_override_storage_keys(kv.first)) {
            margin_overrides_[key] = kv.second;
        }
    }
    default_margin_rate_ = cfg.default_margin_rate;
    default_offset_policy_ = (cfg.default_offset_policy == XtOffsetPolicy::Inherit)
                                 ? XtOffsetPolicy::OpenAllowed
                                 : cfg.default_offset_policy;
}

const std::unordered_map<std::string, XtCommodityInfo>& XtCommodityCatalog::entries() const {
    return map_;
}

const XtCommodityInfo* XtCommodityCatalog::find(const char* code) const {
    if (!code || code[0] == '\0') return nullptr;
    std::string exact = XtCommodityCatalog::normalize_key(code);
    auto it = map_.find(exact);
    if (it != map_.end()) return &it->second;
    std::string sym = XtCommodityCatalog::extract_symbol_key(code);
    it = map_.find(sym);
    if (it != map_.end()) return &it->second;
    return nullptr;
}

double XtCommodityCatalog::margin_rate_for(const char* code) const {
    // 空 override 表（多数部署）短路，免在 submit 路径白构造 lookup key 串。
    if (!margin_overrides_.empty()) {
        for (const auto& key : margin_lookup_keys(code)) {
            auto it = margin_overrides_.find(key);
            if (it != margin_overrides_.end()) return it->second;
        }
    }
    const XtCommodityInfo* info = find(code);
    if (info && info->margin_rate > 0.0) return info->margin_rate;
    return default_margin_rate_;
}

double XtCommodityCatalog::margin_rate_for(const XtCommodityInfo* info, const char* code) const {
    // override 查找与单参版逐字一致；info 替代内部 find(code)。
    if (!margin_overrides_.empty()) {
        for (const auto& key : margin_lookup_keys(code)) {
            auto it = margin_overrides_.find(key);
            if (it != margin_overrides_.end()) return it->second;
        }
    }
    if (info && info->margin_rate > 0.0) return info->margin_rate;
    return default_margin_rate_;
}

int64_t XtCommodityCatalog::per_unit_margin_fp(const XtCommodityInfo* info, const char* code,
                                              int64_t price_fp) const {
    const double vol_scale = (info && std::isfinite(info->vol_scale) && info->vol_scale > 0.0)
                                 ? info->vol_scale
                                 : 1.0;
    const double margin_rate = margin_rate_for(info, code);
    return xt_fp::mul(price_fp, xt_fp::to_fp(vol_scale * margin_rate));
}
