#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "api/ITradeGateway.h"
#include "api/XtPositionSnapshot.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"

namespace binance_u {

class BinanceRestClient;

/// @brief Binance 交易网关里的 User Data Stream 接收器。
///
/// - Binance UDS 需要先用 REST 申请 listenKey，再用该 listenKey 建立 WS 连接。
/// - BinanceUserDataStream 把 Binance JSON 转成 ITradeGateway 统一 DTO
class BinanceUserDataStream {
public:
    struct Config {
        std::string host;          ///< UDS WS host，如 `fstream.binancefuture.com`。
        std::string port = "443";  ///< TLS 端口。
        uint32_t keepalive_ms = 30 * 60 * 1000;  ///< listenKey 续期间隔（默认 30min）
    };

    using OnOrderUpdate = std::function<void(const XtOrderExecUpdate&)>;
    /// @note 第二参 latency eligibility：live UDS 实时 = kEligible / REST gap-fill
    ///       replay（inject_gap_fill_trade）= kIneligible（构造点区分，共享 callback 不丢 source）。
    using OnTrade       = std::function<void(const XtTradeRecord&, XtLatencyEligibility)>;
    using OnAccount     = std::function<void(const ITradeGateway::AccountSnapshot&)>;
    using OnPosition    = std::function<void(const std::vector<XtPositionSnapshot>&)>;

    /// @brief 从 Binance `clientOrderId` 反解本地订单号；0 表示非本进程订单。
    using ParseLocalId  = std::function<uint64_t(std::string_view client_order_id)>;

    /// @brief 未归因 TRADE 事件处理器，用于触发 positionRisk resync。
    /// @note 典型来源是强平、ADL、手动单或外部来源订单；内部按 5s 节流触发。
    using OnUnattributedFill = std::function<void()>;

    /// @brief fill 去重判定委托；authority 是 gateway 的 BinanceOrderLedger
    ///        （per-order fills 集 + orderless `symbol|trade_id` 复合窗）。
    /// @return true = 首次记账（本流程应 emit）；false = 重复（抑制）。
    /// @note live UDS 与 REST gap-fill 注入共用同一委托 = 同一去重语义。
    ///       在 ws 线程 / reconcile 线程并发调用，实现方自持锁。
    using FillDedup = std::function<bool(uint64_t order_id, std::string_view symbol,
                                         int64_t trade_id)>;

    /// @brief 绑定 REST client；仅用于 listenKey acquire/keepalive/close，不拥有。
    BinanceUserDataStream(Config cfg, BinanceRestClient& rest);
    ~BinanceUserDataStream();

    /// @note 持有内部 WS 线程与连接状态，禁止复制。
    BinanceUserDataStream(const BinanceUserDataStream&)            = delete;
    BinanceUserDataStream& operator=(const BinanceUserDataStream&) = delete;

    /// @brief 绑定 UDS 输出回调；应在 `start()` 前完成。
    /// @warning 回调运行在内部 WS 线程，必须非阻塞；运行中 rebind 不作为稳定 contract。
    void bind_callbacks(OnOrderUpdate on_order,
                        OnTrade on_trade,
                        OnAccount on_account,
                        OnPosition on_position,
                        ParseLocalId parse_local_id);

    /// @brief 注入「无法归因成交 → resync」处理器；应在 start() 前绑定。
    /// @note 未绑定时无法归因成交只丢弃，不触发 resync。
    void set_unattributed_fill_handler(OnUnattributedFill handler);

    /// @brief 注入 fill 去重判定委托；须在 start() / 首次 inject 前绑定。
    /// @warning 未绑定 = 去重禁用（每笔按首次处理，error 日志一次），live/gap-fill
    ///          双路 emit 会让 account_store fee/realized 双计；生产装配必须绑定。
    void set_fill_dedup(FillDedup fn);

    /// @brief 启动内部 WS 线程并异步申请 listenKey；重复调用幂等。
    bool start();

    /// @brief 停止 WS 连接和 keepalive timer；重复调用幂等。
    void stop();

    /// @brief 冷路径健康检查；发现 WS stale 时异步请求 reconnect。
    bool check_health(uint32_t now_ms);

    /// @brief 由 reconcile loop 触发外部强制 WS 重连；异步、立即返回。
    void request_reconnect();

    /// @brief 查询每个 symbol 的 Binance trade id 高水位。
    /// @note 每收到一笔 `ORDER_TRADE_UPDATE x=TRADE` 更新对应 symbol 的 `t` 字段。
    ///       reconcile gap-fill 用 `last_seen + 1` 作为 userTrades fromId 起点。
    /// @return 0 表示该 symbol 当前还没有收到过 trade event。
    int64_t last_seen_trade_id(const std::string& symbol) const;

    /// @brief 列出所有已观察到 trade event 的 symbol，用于 reconcile gap-fill 迭代。
    std::vector<std::string> tracked_trade_symbols() const;

    /// @brief 注入一笔 REST userTrades 补齐的成交，并与 UDS live 路径共享同一
    ///        fill 去重委托（set_fill_dedup）。
    /// @warning 成功时在调用方线程同步触发 `on_trade`。
    /// @return true 表示实际 emit 给 on_trade；false 表示该 (symbol, trade_id)
    ///         已记账，被去重吞掉。
    bool inject_gap_fill_trade(const XtTradeRecord& tr);

#ifdef XT_BUILD_TESTS
    // 单测入口。
    /// @brief 直接喂一条 WS payload 给 message handler；仅用于离线单测。
    void inject_test_message(std::string_view payload);

    /// @brief 单测注入：绕过 WS connected 回调，直接安排一次 keepalive timer。
    void force_keepalive_for_test();
#endif

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace binance_u
