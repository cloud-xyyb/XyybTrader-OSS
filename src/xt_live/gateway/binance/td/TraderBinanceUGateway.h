#pragma once

#include <memory>
#include <optional>
#include <vector>

#ifdef XT_BUILD_TESTS
#include <cstddef>        // std::size_t：friend wrapper 签名。
#include <cstdint>        // uint64_t：friend wrapper 签名。
#include <functional>     // std::function：friend wrapper 签名。
#include <string>         // std::string：friend wrapper 签名。
#endif

#include "api/ITradeGateway.h"
#include "api/IReconcilableGateway.h"
#include "BinanceTdCfg.h"
#include "xt_live/gateway/common/TdReadinessGate.h"

namespace binance_u {

#ifdef XT_BUILD_TESTS
struct TransportAck;

class BinanceRestClient;
class BinanceOrderLedger;
#endif  // XT_BUILD_TESTS

/// @brief Binance U 本位永续的交易网关，实现 ITradeGateway / IReconcilableGateway
///
/// TraderBinanceUGateway 是 Binance 的下单出口：把 submit/cancel 发给交易所，
/// 把回报、成交和账户/持仓变化传回上层；本地状态跟交易所对不上时用 REST 对账拉回来。
///
/// submit/cancel 返回只代表请求已受理；ACK、成交、终态经 callbacks 异步返回。
/// @warning callbacks 来自内部多个线程；handler 必须非阻塞，把事件快速入队。
class TraderBinanceUGateway : public ITradeGateway, public IReconcilableGateway {
public:
    /// @brief 构造空网关；Binance 专属配置通过 configure_binance() 注入。
    TraderBinanceUGateway();

    /// @brief 停止并释放 transport、UDS 和后台 worker。
    ~TraderBinanceUGateway() override;

    // 网关持有线程、连接和回调闭包，不支持复制。
    TraderBinanceUGateway(const TraderBinanceUGateway&)            = delete;
    TraderBinanceUGateway& operator=(const TraderBinanceUGateway&) = delete;

    /// @brief 注入 Binance TD 专属配置；需在 init() 之前调用。
    void configure_binance(const BinanceTdCfg& cfg);

    // 生命周期。
    /// @brief 初始化 REST client，并创建/启动配置指定的 TD transport；不启动 UDS。
    bool init(const Config& cfg) override;

    /// @brief 启动冷队列 worker、UDS、keepalive，并投递 bootstrap snapshot。
    bool start() override;

    /// @brief 停止 transport、UDS、keepalive 和冷队列 worker；可重复调用。
    void stop() override;

    /// @brief 投递周期 time sync，检查 UDS stale，并记录 WS trade transport 断连诊断。
    bool check_health(uint32_t now_ms) override;

    /// @brief 停机冷路径 fresh 钱包余额（REST `/fapi/v2/account` totalWalletBalance）。
    /// @pre 仅 stop() 完成后调用（阻塞式 REST，见 ITradeGateway 契约）。
    bool query_wallet_balance_sync(double& out_balance) override;

    // Reconcile 兜底。
    /// @brief Binance 支持 REST reconcile，用于 lost-rtn、ghost order 和仓位漂移兜底。
    bool reconcile_supported() const override { return true; }

    /// @brief 拉取交易所 positionRisk 快照；失败返回 std::nullopt，避免误清本地仓位。
    std::optional<std::vector<XtPositionSnapshot>>
        fetch_positions_for_reconcile() override;

    /// @brief 对比本地 in-flight 与交易所 openOrders，终结或撤掉确认漂移的订单。
    std::optional<std::size_t>
        detect_and_resolve_ghost_orders_for_reconcile(uint32_t min_age_ms) override;

    /// @brief 请求一次异步 resync；同一周期内重复请求会合并。
    void request_resnap_for_reconcile() override;

    /// @brief 返回 Binance U-perp 的交易能力：无今昨仓、无 L2 orderflow，支持 IOC/FOK。
    Capabilities capabilities() const override {
        return Capabilities{
            .supports_close_today  = false,  // Binance perp 无今昨之分。
            .supports_l2_orderflow = false,
            .supports_ioc_fok      = true,
        };
    }

    /// @brief Binance TD 当前没有额外 control-plane 统计。
    QueryControlStats query_control_stats() const override { return QueryControlStats{}; }

    /// @brief local_id 是否已在本网关终态集（引擎 lost-terminal 对账探测）。
    ///
    /// 判据 = ledger 终态窗 + 订单记录 in-flight facet（in-flight 优先返回 false，
    /// 详见 BinanceOrderLedger::is_order_terminal 的判定序与淘汰兜底契约）。
    /// @note false 不可解读为「该单非终态」：未知 local_id（含跨重启且从未
    ///       recovered 的 id）恒 false（fail-safe，不据此合成）。
    [[nodiscard]] bool is_order_terminal(uint64_t local_id) const override;

    /// @brief 终态 payload 查询（转发 BinanceOrderLedger::lookup_terminal）。
    ///
    /// payload 仅在账本记录自身 status 为终态值时给出（覆盖面见 ledger 契约）；
    /// 其余终态返回 false，caller 退回 is_order_terminal 布尔探测兜底。
    [[nodiscard]] bool lookup_terminal(uint64_t local_id,
                                       XtOrderTerminalRecord& out) const override;

    /// @brief 导出账本 in-flight facet 的 local_id 集合（停机对账差集观测）。
    [[nodiscard]] bool collect_open_delivery_ids(std::vector<uint64_t>& out) const override;

    // 交易接口。
    /// @brief 本地登记并提交报单；返回 local_id，真实 ACK / 成交 / 终态通过回调异步返回。
    uint64_t submit_order(const char* code,
                          bool is_buy,
                          int64_t price_fp,
                          int64_t qty_fp,
                          XtTimeInForce tif,
                          const char* user_tag,
                          XtOffset close_offset,
                          ExecFlags exec_flags) override;

    /// @brief 本地登记并提交报单，同时显式记录已采样 intent_trace_id。
    uint64_t submit_order_with_intent_trace(const char* code,
                          bool is_buy,
                          int64_t price_fp,
                          int64_t qty_fp,
                          XtTimeInForce tif,
                          const char* user_tag,
                          XtOffset close_offset,
                          ExecFlags exec_flags,
                          uint64_t intent_trace_id) override;

    /// @brief 异步撤单；返回 true 仅表示网关接受撤单请求，最终状态以回调或 reconcile 为准。
    bool cancel_order(uint64_t local_id) override;

    // 恢复与回调。
    /// @brief 仅在 reconcile resync 发仓位快照期间短暂返回 true。
    ///
    /// Binance 不走 CTP-style startup recovery；该状态只用于让 account_store
    /// 按交易所真值执行 reset+set。Config::recovery_enabled 对本 venue 无效。
    bool is_recovering() const override;

    /// @brief 绑定交易、账户、仓位、reverse-sync 和 broker truth callbacks。
    ///
    /// 回调可能由多个 Binance 内部线程触发，调用方不得在 handler 内阻塞等待网关。
    void bind_callbacks(Callbacks callbacks) override;

    /// @brief 导出 typed TD readiness report，仅供诊断/单测观测。
    /// @note 生产侧无消费者；`on_ready` 由内部 `emit_ready_if_td_readiness_satisfied`
    ///       在 readiness gate 全绿后幂等发出，不经此 contract 放行。
    xt_live::gateway::TdReadinessReport td_readiness_report() const;

#ifdef XT_BUILD_TESTS
    /// @brief 单测访问 PImpl 的窄入口；生产构建不暴露。
    void* _test_impl_void() noexcept;
#endif  // XT_BUILD_TESTS

private:
    class Impl;
    std::unique_ptr<Impl> impl_;  ///< 隔离 Binance TD 的连接、队列、reconcile 与解析状态。

#ifdef XT_BUILD_TESTS
    // friend 仅供测试 wrapper 引用 private Impl 类型；生产构建不进入该分支。
    friend void  trader_binance_test_inject_transport_ack(void*, const TransportAck&);
    friend void  trader_binance_test_inject_uds_order(void*, const XtOrderExecUpdate&);
    friend void  trader_binance_test_emit_reconcile_ghost(void*, const XtOrderExecUpdate&);
    friend bool  trader_binance_test_emit_blank_sys_id_reject(void*, uint64_t);
    // 订单账本只读观测：测试断言直接走 BinanceOrderLedger 的生产查询 API。
    friend const BinanceOrderLedger& trader_binance_test_ledger(void*);
	    friend std::size_t trader_binance_test_cold_worker_task_count(void*);
	    friend uint64_t trader_binance_test_trace_id_for_local(void*, uint64_t);
	    friend void  trader_binance_test_rebuild_from_open_orders_body(void*, const std::string&, bool);
    // ghost / reverse-sync / resync 栅栏测试入口。
    friend long long trader_binance_test_detect_ghosts(void*, uint32_t);
    friend uint64_t trader_binance_test_user_trades_pages_capped(void*);
    friend void  trader_binance_test_set_rest(void*, std::unique_ptr<BinanceRestClient>);
    friend void  trader_binance_test_reconcile_resync(void*);
    friend void  trader_binance_test_note_tracked_symbol(void*, const std::string&);
    friend void  trader_binance_test_forward_uds_trade(void*, const XtTradeRecord&);
    friend void  trader_binance_test_bootstrap_snapshot(void*);
    friend void  trader_binance_test_request_resnap(void*);
    friend void* trader_binance_test_resync_gate_open(void*);
    friend void  trader_binance_test_resync_gate_close(void*, void*);
    // bootstrap readiness 测试入口。
    friend int   trader_binance_test_keepalive_pick(void*, long long);
    friend void  trader_binance_test_reset_td_readiness(void*);
    friend void  trader_binance_test_boot_cold_worker_with_uds(
        void*, const std::string&, std::function<void(const XtTradeRecord&, XtLatencyEligibility)>);
    friend void  trader_binance_test_install_uds_prod_wired(void*, const std::string&);
#endif  // XT_BUILD_TESTS
};

}  // namespace binance_u
