#include "GatewayFactory.h"

#include <algorithm>
#include <cctype>

#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
// shm transport：从外部 connector 广播的 XtShmRing 读行情（multibook 组合盘）。
// XtShmMdGateway 只依赖 IMdGateway + xt_ipc header（0 venue SDK），任何 venue build
// 组合（XT_NO_CTP/XT_NO_BINANCE/XT_MD_ONLY）下都干净，故无条件可用。
#include "xt_ipc/XtShmMdGateway.h"
// 实盘录制 tap（shm 部署下补齐 MD_TICK 流）。GatewayFactory.cpp 也被 per-venue MD-only
// connector 直编（XT_MD_ONLY）——connector 是 ring 的 writer、不走 shm 分支、且最小
// 二进制（generic connector 不 link recorder），故 recorder wire 仅在 runner 侧编入。
#ifndef XT_MD_ONLY
#include "xt_core/market/XtRawTick.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"
#include "xt_live/telemetry/recording/XtMdTickRecorderUtil.h"
#include "xt_util/time/XtSteadyClock.h"
#endif
// XT_MD_ONLY 把所有 TD (trade gateway) factory 编出去，给 MD-only 的 per-venue
// 进程（如 xt_md_connector_ctp）一个不含 order/account 路径的二进制（不链 xt_td /
// TraderCTP / xt_engine_common）。TdProviderCfg variant 与 create_td_gateway 声明
// 仍由 GatewayFactory.h 保留（POD + dispatch），但工厂体与 TD 注册全部跳过。
// CTP gateway headers pull the thost SDK, which statically embeds a 2nd OpenSSL
// (exports BN_free/RSA_free/SSL_CTX_free as global symbols). In a binance-only
// build (XT_NO_CTP) we compile CTP out entirely so the binary links a single
// OpenSSL — eliminating the symbol-collision heap corruption.
// See docs/design/binance_close_crashloop_rootcause.md.
#ifndef XT_NO_CTP
#include "ctp/md/MdCTPGateway.h"
#ifndef XT_MD_ONLY
#include "ctp/td/TraderCTPGateway.h"
#endif
#endif
// XT_NO_BINANCE 把 binance gateway + Boost.Beast/OpenSSL 编出去，给 MD-only 的
// per-venue 进程（如 xt_md_connector_ctp）一个干净符号表。BinanceMdCfg/BinanceTdCfg
// 仍由 GatewayFactory.h 提供（纯 POD），variant 编译不受影响。
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

#ifndef XT_MD_ONLY
// venue 真实盘口档数（与三处 per-gateway in-process tap 的字面量逐值一致：
// CTP 5 / XTP 10 / binance 20）；未知 provider 保守用 recorder 全宽（缺档补 0）。
int shm_recorder_depth_levels(const std::string& provider) {
    const std::string p = normalize_provider(provider);
    if (p == "ctp") return 5;
    if (p == "xtp") return 10;
    if (p == "binance_u") return 20;
    return XtLiveRecorder::kMdDepth;
}

// 构建 shm reader 的实盘录制 tap：把 ring 上的 XtRawTick 归一化后喂 XtLiveRecorder，
// 字段映射复刻 per-gateway in-process tap（ParserCTP/MdBinance/ParserXTP）。connector
// 已按 venue 填 XtRawTick（CTP 填 trading_date；binance 只填 action_date），故 trading_day
// 取 trading_date 非零否则回退 action_date。全宽定点 + fill_md_depth_fp 与既有 tap 同源。
// volume 量纲同样按 provider 分派（见 xt_live_recorder_util::to_recorder_volume_fp）：
// binance 是 coin 数量 double，需 to_recorder_fp 定点化；CTP/XTP 是原始整数手数直接截断。
std::function<void(const XtRawTick&)> make_shm_recorder_tap(const std::string& provider) {
    const int depth_levels = shm_recorder_depth_levels(provider);
    const bool volume_is_coin_qty =
        xt_live_recorder_util::venue_volume_is_coin_quantity(normalize_provider(provider));
    return [depth_levels, volume_is_coin_qty](const XtRawTick& t) {
        auto* rec = XtLiveRecorder::global();
        if (!rec) return;
        constexpr int kRecDepth = XtLiveRecorder::kMdDepth;
        const uint64_t ts_local_ns = XtSteadyClock::steady_now_ns();
        const int64_t last_fp = xt_live_recorder_util::to_recorder_fp(t.price);
        const int64_t volume_fp = volume_is_coin_qty
            ? xt_live_recorder_util::to_recorder_fp(t.total_volume)
            : static_cast<int64_t>(t.total_volume);
        int64_t bid_px_fp[kRecDepth] = {}, ask_px_fp[kRecDepth] = {},
                bid_q[kRecDepth] = {}, ask_q[kRecDepth] = {};
        xt_live_recorder_util::fill_md_depth_fp(t.bid_prices, t.ask_prices,
                                                t.bid_qty, t.ask_qty, depth_levels,
                                                bid_px_fp, ask_px_fp, bid_q, ask_q);
        const uint32_t td = t.trading_date != 0 ? t.trading_date : t.action_date;
        rec->emit_md_tick(ts_local_ns,
                          td, t.action_date, t.action_time,
                          t.code, t.exchg,
                          last_fp, volume_fp,
                          bid_px_fp, ask_px_fp, bid_q, ask_q,
                          depth_levels);
    };
}
#endif  // XT_MD_ONLY

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

// ── Built-in MD provider factories ─────────────────────────────────────

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
#endif  // XT_NO_CTP

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
#endif  // XT_NO_BINANCE

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

// ── Built-in TD provider factories ─────────────────────────────────────
// MD-only 进程（XT_MD_ONLY）整段跳过：connector 只 fan-out 行情，不持任何 trade
// gateway → 二进制不引 order/account 路径。
#ifndef XT_MD_ONLY

#ifndef XT_NO_CTP
std::unique_ptr<ITradeGateway> create_td_ctp(const ITradeGateway::Config& common,
                                              const gateway_factory::TdProviderCfg& extra,
                                              const XtCommodityCatalog* /*catalog*/,
                                              std::string* err) {
    // CTP 期货按合约本身的整数手语义下单，不走 catalog min_lot 校验。
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
#endif  // XT_NO_CTP

#ifndef XT_NO_BINANCE
std::unique_ptr<ITradeGateway> create_td_binance_u(const ITradeGateway::Config& common,
                                                    const gateway_factory::TdProviderCfg& extra,
                                                    const XtCommodityCatalog* /*catalog*/,
                                                    std::string* err) {
    // binance perp 按 stepSize/lotSize 由 venue 校验，不走 catalog min_lot。
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
#endif  // XT_NO_BINANCE

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
    g->set_catalog(catalog);  // A 股 submit_order min_lot 校验源，init 前注入
    if (!g->init(common)) {
        if (err) *err = "xtp td init failed";
        return nullptr;
    }
    return g;
}
#endif

#endif  // XT_MD_ONLY (TD provider factories)

void ensure_builtin_providers_registered() {
    /// @brief 单次注册所有 built-in providers — 首次 create_md_gateway / create_td_gateway 时触发。
    /// @note Meyer's singleton 模式；线程安全（C++11 静态局部变量保证）。
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
#endif  // XT_MD_ONLY (TD registration)
        return true;
    }();
    (void)_;
}

} // namespace

// ── Registry implementations ──────────────────────────────────────────

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

// ── Public dispatch entries ──────────────────────────────────────────

std::unique_ptr<IMdGateway> create_md_gateway(
    const std::string& provider,
    const IMdGateway::Config& common,
    const MdProviderCfg& extra,
    std::string* err) {
    // transport=shm：从 connector 广播的 ring 读行情，与 venue 无关 → 不走 provider
    // registry，直接返 XtShmMdGateway。venue provider 名仍由调用方用于 venue_kind 推导。
    if (extra.transport == MdTransport::Shm) {
        if (extra.shm_ring.empty()) {
            if (err) *err = "md.transport=shm requires non-empty md.shm_ring";
            return nullptr;
        }
        // common.subs = 本进程订阅集 → per-symbol 过滤；stale_md_ms 判 head 停滞。
        auto gw = std::make_unique<xt_ipc::XtShmMdGateway>(extra.shm_ring, common.subs,
                                                           extra.stale_md_ms);
#ifndef XT_MD_ONLY
        // runner 侧：装 recorder tap 补齐 shm 部署下缺失的 MD_TICK 录制流（connector 进程
        // 持 per-gateway in-process tap，本 runner 进程无 → tap 是「录制即数据集」的 live 半边）。
        gw->set_md_tap(make_shm_recorder_tap(provider));
#endif
        return gw;
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

} // namespace gateway_factory
