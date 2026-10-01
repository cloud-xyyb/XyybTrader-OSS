/// @file
/// @brief 按名字创建行情/交易网关的工厂。
///
/// 配置里写 "ctp" / "binance_u" / "xtp" 这样的 provider 名，runner 启动时
/// 调 `create_md_gateway()` / `create_td_gateway()`，就能拿到对应交易所的、
/// 已完成 init 的网关实例。新接一个交易所只需要注册新 provider，不用改本文件。
#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#include "ctp/md/CTPMdCfg.h"
#include "ctp/td/CTPTdCfg.h"
#include "binance/md/BinanceMdCfg.h"
#include "binance/td/BinanceTdCfg.h"
#include "xtp/XTPMdCfg.h"
#include "xtp/XTPTdCfg.h"

// XtCommodityCatalog 在 global namespace（xt_config 全局类）；TD factory 透传其指针，
// 供 venue 实现按需做 instrument-level 校验（当前仅 XTP submit_order min_lot）。
class XtCommodityCatalog;

namespace gateway_factory {

/// @brief MD 传输路径。`Direct` = 进程自连 venue 行情前置（现状）；
/// `Shm` = 从外部 connector 广播的 `XtShmRing` 段读行情（multibook 组合盘）。
enum class MdTransport { Direct, Shm };

/// @brief MD provider-specific 配置容器。
///
/// 由 `LiveConfigParsers` 按 YAML provider 字段填充；`extra` 必须与
/// `create_md_gateway()` 的 provider name 匹配，否则 factory 返回 nullptr。
///
/// `transport == Shm` 时 factory 忽略 venue `extra`，改返 `XtShmMdGateway`（从
/// `shm_ring` 段读、按 `common.subs` 过滤、`stale_md_ms` 判 head 停滞）；venue
/// 名仍驱动 per-runner venue_kind 推导（见 `XtLiveConfig.cpp`），不受 transport 影响。
struct MdProviderCfg {
    std::variant<std::monostate, CTPMdCfg, BinanceMdCfg, XTPMdCfg> extra;
    MdTransport transport = MdTransport::Direct; ///< 行情入口路径（默认 direct=现状）
    std::string shm_ring;                        ///< transport=Shm 时的 ring 段名（如 /xt_md_ctp）
    uint32_t stale_md_ms = 3000;                 ///< transport=Shm 时 head 停滞 → md_ready=false HALT 阈值
};

/// @brief TD provider-specific 配置容器。
///
/// 由 `LiveConfigParsers` 按 YAML provider 字段填充；`extra` 必须与
/// `create_td_gateway()` 的 provider name 匹配，否则 factory 返回 nullptr。
struct TdProviderCfg {
    std::variant<std::monostate, CTPTdCfg, BinanceTdCfg, XTPTdCfg> extra;
};

/// @brief MD provider factory；成功返回已完成 init 的 gateway。
/// @param err 可为空；失败时非空指针接收诊断文本。
using MdProviderFactory = std::function<std::unique_ptr<IMdGateway>(
    const IMdGateway::Config&, const MdProviderCfg&, std::string*)>;

/// @brief TD provider factory；成功返回已完成 init 的 gateway。
/// @param catalog 品种目录（可空）；venue 实现按需取用（仅 XTP 用于 min_lot 校验），
///        其余 venue 忽略。
/// @param err 可为空；失败时非空指针接收诊断文本。
using TdProviderFactory = std::function<std::unique_ptr<ITradeGateway>(
    const ITradeGateway::Config&, const TdProviderCfg&, const XtCommodityCatalog*, std::string*)>;

/// @brief MD provider 注册表：provider 名 → 行情网关工厂函数。
///
/// MdProviderRegistry 是接新交易所的扩展点：注册一个名字和工厂函数，
/// 配置里就能用这个名字创建行情网关。内置 provider 由 create_md_gateway()
/// 首次调用时自动注册。线程安全；按原始 key 查表，大小写归一化在上层入口做。
class MdProviderRegistry {
public:
    static MdProviderRegistry& instance();

    /// @return true 表示首次注册成功；空 name、空 factory 或重复 key 返回 false。
    bool register_provider(std::string name, MdProviderFactory factory);

    /// @return true 表示删除了已注册 provider；不存在时返回 false。
    bool unregister_provider(const std::string& name);

    /// @brief 按原始 key 创建 MD gateway。
    /// @note 不触发 built-in lazy registration；上层常规入口应使用 `create_md_gateway()`。
    std::unique_ptr<IMdGateway> create(const std::string& name,
                                       const IMdGateway::Config& common,
                                       const MdProviderCfg& extra,
                                       std::string* err) const;

    /// @return 已注册 provider key 的排序快照。
    std::vector<std::string> list_providers() const;
private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, MdProviderFactory> providers_;
};

/// @brief TD provider 注册表：provider 名 → 交易网关工厂函数。
///
/// 与 MdProviderRegistry 相同的扩展点，管交易侧。线程安全；
/// 按原始 key 查表，大小写归一化在上层入口做。
class TdProviderRegistry {
public:
    static TdProviderRegistry& instance();

    /// @return true 表示首次注册成功；空 name、空 factory 或重复 key 返回 false。
    bool register_provider(std::string name, TdProviderFactory factory);

    /// @return true 表示删除了已注册 provider；不存在时返回 false。
    bool unregister_provider(const std::string& name);

    /// @brief 按原始 key 创建 TD gateway。
    /// @note 不触发 built-in lazy registration；上层常规入口应使用 `create_td_gateway()`。
    std::unique_ptr<ITradeGateway> create(const std::string& name,
                                          const ITradeGateway::Config& common,
                                          const TdProviderCfg& extra,
                                          const XtCommodityCatalog* catalog,
                                          std::string* err) const;

    /// @return 已注册 provider key 的排序快照。
    std::vector<std::string> list_providers() const;
private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, TdProviderFactory> providers_;
};

/// @brief 创建并初始化 MD gateway。
///
/// `provider` 会去除空白并转小写；内置 provider 在首次调用时注册。
/// `common` 传给 gateway `init()`，`extra` 提供 provider-specific 配置。
/// @return 成功返回独占 gateway；失败返回 nullptr，`err` 非空时写入原因。
std::unique_ptr<IMdGateway> create_md_gateway(const std::string& provider,
                                              const IMdGateway::Config& common,
                                              const MdProviderCfg& extra,
                                              std::string* err);

/// @brief 创建并初始化 TD gateway。
///
/// `provider` 会去除空白并转小写；内置 provider 在首次调用时注册。
/// `common` 传给 gateway `init()`，`extra` 提供 provider-specific 配置，
/// `catalog` 为共享品种目录（可空），venue 实现按需取用（仅 XTP 用于 min_lot 校验）。
/// @return 成功返回独占 gateway；失败返回 nullptr，`err` 非空时写入原因。
std::unique_ptr<ITradeGateway> create_td_gateway(const std::string& provider,
                                                 const ITradeGateway::Config& common,
                                                 const TdProviderCfg& extra,
                                                 const XtCommodityCatalog* catalog,
                                                 std::string* err);

} // namespace gateway_factory
