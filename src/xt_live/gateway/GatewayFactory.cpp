#include "GatewayFactory.h"

#include <algorithm>
#include <cctype>

#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"

#include "xt_ipc/XtShmMdGateway.h"

#ifndef XT_NO_CTP
#include "ctp/md/MdCTPGateway.h"
#ifndef XT_MD_ONLY
#include "ctp/td/TraderCTPGateway.h"
#endif
#endif

#ifndef XT_NO_BINANCE
#include "binance/md/MdBinanceUGateway.h"
#ifndef XT_MD_ONLY
#include "binance/td/TraderBinanceUGateway.h"
#endif
#endif
#ifdef XT_HAS_XTP
#include "xtp/MdXTPGateway.h"
#ifndef XT_MD_ONLY
#include "xtp/TraderXTPGateway.h"
#endif
#endif

namespace {

std::string normalize_provider(std::string provider) {
    provider.erase(
        std::remove_if(provider.begin(),
                       provider.end(),
                       [](unsigned char c) { return std::isspace(c) != 0; }),
        provider.end());
    std::transform(provider.begin(),
                   provider.end(),
                   provider.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return provider;
}

void set_provider_error(const char* kind,
                        const std::string& provider,
                        const std::vector<std::string>& known,
                        std::string* err) {
    if (!err) return;
    std::string supported;
    for (size_t i = 0; i < known.size(); ++i) {
        supported += known[i];
        if (i + 1 < known.size()) supported += ", ";
    }
    *err = std::string("unsupported ") + (kind ? kind : "gateway") +
           " provider: " + provider + " (registered: " + supported + ")";
}

void set_cfg_mismatch_error(const char* kind,
                            const std::string& provider,
                            const char* expected_cfg,
                            std::string* err) {
    if (!err) return;
    *err = std::string(kind ? kind : "gateway") + " provider '" + provider +
           "' requires " + (expected_cfg ? expected_cfg : "provider_cfg") +
           " in MdProviderCfg/TdProviderCfg::extra";
}

#ifndef XT_NO_CTP
std::unique_ptr<IMdGateway> create_md_ctp(const IMdGateway::Config& common,
                                          const gateway_factory::MdProviderCfg& extra,
                                          std::string* err) {
    auto g = std::make_unique<MdCTPGateway>();
    if (const auto* ctp = std::get_if<CTPMdCfg>(&extra.extra)) {
        g->configure_ctp(*ctp);
    } else {
        set_cfg_mismatch_error("md", "ctp", "CTPMdCfg", err);
        return nullptr;
    }
    if (!g->init(common)) {
        if (err) *err = "ctp md init failed";
        return nullptr;
    }
    return g;
}
#endif

#ifndef XT_NO_BINANCE
std::unique_ptr<IMdGateway> create_md_binance_u(const IMdGateway::Config& common,
                                                 const gateway_factory::MdProviderCfg& extra,
                                                 std::string* err) {
    auto g = std::make_unique<binance_u::MdBinanceUGateway>();
    if (const auto* bn = std::get_if<BinanceMdCfg>(&extra.extra)) {
        g->configure_binance(*bn);
    } else {
        set_cfg_mismatch_error("md", "binance_u", "BinanceMdCfg", err);
        return nullptr;
    }
    if (!g->init(common)) {
        if (err) *err = "binance_u md init failed";
        return nullptr;
    }
    return g;
}
#endif

#ifdef XT_HAS_XTP
std::unique_ptr<IMdGateway> create_md_xtp(const IMdGateway::Config& common,
                                          const gateway_factory::MdProviderCfg& extra,
                                          std::string* err) {
    auto g = std::make_unique<MdXTPGateway>();
    if (const auto* xtp = std::get_if<XTPMdCfg>(&extra.extra)) {
        g->configure_xtp(*xtp);
    } else {
        set_cfg_mismatch_error("md", "xtp", "XTPMdCfg", err);
        return nullptr;
    }
    if (!g->init(common)) {
        if (err) *err = "xtp md init failed";
        return nullptr;
    }
    return g;
}
#endif

#ifndef XT_MD_ONLY

#ifndef XT_NO_CTP
std::unique_ptr<ITradeGateway> create_td_ctp(const ITradeGateway::Config& common,
                                              const gateway_factory::TdProviderCfg& extra,
                                              const XtCommodityCatalog*  ,
                                              std::string* err) {

    auto g = std::make_unique<TraderCTPGateway>();
    if (const auto* ctp = std::get_if<CTPTdCfg>(&extra.extra)) {
        g->configure_ctp(*ctp);
    } else {
        set_cfg_mismatch_error("td", "ctp", "CTPTdCfg", err);
        return nullptr;
    }
    if (!g->init(common)) {
        if (err) *err = "ctp td init failed";
        return nullptr;
    }
    return g;
}
#endif

#ifndef XT_NO_BINANCE
std::unique_ptr<ITradeGateway> create_td_binance_u(const ITradeGateway::Config& common,
                                                    const gateway_factory::TdProviderCfg& extra,
                                                    const XtCommodityCatalog*  ,
                                                    std::string* err) {

    auto g = std::make_unique<binance_u::TraderBinanceUGateway>();
    if (const auto* bn = std::get_if<BinanceTdCfg>(&extra.extra)) {
        g->configure_binance(*bn);
    } else {
        set_cfg_mismatch_error("td", "binance_u", "BinanceTdCfg", err);
        return nullptr;
    }
    if (!g->init(common)) {
        if (err) *err = "binance_u td init failed";
        return nullptr;
    }
    return g;
}
#endif

#ifdef XT_HAS_XTP
std::unique_ptr<ITradeGateway> create_td_xtp(const ITradeGateway::Config& common,
                                              const gateway_factory::TdProviderCfg& extra,
                                              const XtCommodityCatalog* catalog,
                                              std::string* err) {
    auto g = std::make_unique<TraderXTPGateway>();
    if (const auto* xtp = std::get_if<XTPTdCfg>(&extra.extra)) {
        g->configure_xtp(*xtp);
    } else {
        set_cfg_mismatch_error("td", "xtp", "XTPTdCfg", err);
        return nullptr;
    }
    g->set_catalog(catalog);
    if (!g->init(common)) {
        if (err) *err = "xtp td init failed";
        return nullptr;
    }
    return g;
}
#endif

#endif

void ensure_builtin_providers_registered() {

    static const bool _ = []() {
        auto& md = gateway_factory::MdProviderRegistry::instance();
#ifndef XT_NO_CTP
        md.register_provider("ctp", create_md_ctp);
#endif
#ifndef XT_NO_BINANCE
        md.register_provider("binance_u", create_md_binance_u);
#endif
#ifdef XT_HAS_XTP
        md.register_provider("xtp", create_md_xtp);
#endif
#ifndef XT_MD_ONLY
        auto& td = gateway_factory::TdProviderRegistry::instance();
#ifndef XT_NO_CTP
        td.register_provider("ctp", create_td_ctp);
#endif
#ifndef XT_NO_BINANCE
        td.register_provider("binance_u", create_td_binance_u);
#endif
#ifdef XT_HAS_XTP
        td.register_provider("xtp", create_td_xtp);
#endif
#endif
        return true;
    }();
    (void)_;
}

}

namespace gateway_factory {

MdProviderRegistry& MdProviderRegistry::instance() {
    static MdProviderRegistry singleton;
    return singleton;
}

bool MdProviderRegistry::register_provider(std::string name, MdProviderFactory factory) {
    if (name.empty() || !factory) return false;
    std::lock_guard<std::mutex> g(mu_);
    auto [it, inserted] = providers_.emplace(std::move(name), std::move(factory));
    (void)it;
    return inserted;
}

bool MdProviderRegistry::unregister_provider(const std::string& name) {
    std::lock_guard<std::mutex> g(mu_);
    return providers_.erase(name) > 0;
}

std::unique_ptr<IMdGateway> MdProviderRegistry::create(
    const std::string& name,
    const IMdGateway::Config& common,
    const MdProviderCfg& extra,
    std::string* err) const {
    MdProviderFactory factory;
    {
        std::lock_guard<std::mutex> g(mu_);
        auto it = providers_.find(name);
        if (it == providers_.end()) return nullptr;
        factory = it->second;
    }
    return factory(common, extra, err);
}

std::vector<std::string> MdProviderRegistry::list_providers() const {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<std::string> out;
    out.reserve(providers_.size());
    for (const auto& kv : providers_) out.push_back(kv.first);
    std::sort(out.begin(), out.end());
    return out;
}

TdProviderRegistry& TdProviderRegistry::instance() {
    static TdProviderRegistry singleton;
    return singleton;
}

bool TdProviderRegistry::register_provider(std::string name, TdProviderFactory factory) {
    if (name.empty() || !factory) return false;
    std::lock_guard<std::mutex> g(mu_);
    auto [it, inserted] = providers_.emplace(std::move(name), std::move(factory));
    (void)it;
    return inserted;
}

bool TdProviderRegistry::unregister_provider(const std::string& name) {
    std::lock_guard<std::mutex> g(mu_);
    return providers_.erase(name) > 0;
}

std::unique_ptr<ITradeGateway> TdProviderRegistry::create(
    const std::string& name,
    const ITradeGateway::Config& common,
    const TdProviderCfg& extra,
    const XtCommodityCatalog* catalog,
    std::string* err) const {
    TdProviderFactory factory;
    {
        std::lock_guard<std::mutex> g(mu_);
        auto it = providers_.find(name);
        if (it == providers_.end()) return nullptr;
        factory = it->second;
    }
    return factory(common, extra, catalog, err);
}

std::vector<std::string> TdProviderRegistry::list_providers() const {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<std::string> out;
    out.reserve(providers_.size());
    for (const auto& kv : providers_) out.push_back(kv.first);
    std::sort(out.begin(), out.end());
    return out;
}

std::unique_ptr<IMdGateway> create_md_gateway(
    const std::string& provider,
    const IMdGateway::Config& common,
    const MdProviderCfg& extra,
    std::string* err) {

    if (extra.transport == MdTransport::Shm) {
        if (extra.shm_ring.empty()) {
            if (err) *err = "md.transport=shm requires non-empty md.shm_ring";
            return nullptr;
        }

        return std::make_unique<xt_ipc::XtShmMdGateway>(extra.shm_ring, common.subs, extra.stale_md_ms);
    }

    ensure_builtin_providers_registered();
    const std::string p = normalize_provider(provider);
    if (p.empty()) {
        if (err) *err = "md provider is required";
        return nullptr;
    }
    auto& reg = MdProviderRegistry::instance();
    auto g = reg.create(p, common, extra, err);
    if (!g && (!err || err->empty())) {
        set_provider_error("md", provider, reg.list_providers(), err);
    }
    return g;
}

std::unique_ptr<ITradeGateway> create_td_gateway(
    const std::string& provider,
    const ITradeGateway::Config& common,
    const TdProviderCfg& extra,
    const XtCommodityCatalog* catalog,
    std::string* err) {
    ensure_builtin_providers_registered();
    const std::string p = normalize_provider(provider);
    if (p.empty()) {
        if (err) *err = "td provider is required";
        return nullptr;
    }
    auto& reg = TdProviderRegistry::instance();
    auto g = reg.create(p, common, extra, catalog, err);
    if (!g && (!err || err->empty())) {
        set_provider_error("td", provider, reg.list_providers(), err);
    }
    return g;
}

}
