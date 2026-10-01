#pragma once
#include <functional>
#include <string>
#include <atomic>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <array>
#include <vector>
#include <utility>

#include "ThostFtdcTraderApi.h"

#include "api/ITradeGateway.h"
#include "ctp/common/CtpFrontDisconnectLogGate.h"
#include "api/AliveInflightOrder.h"
#include "xt_core/order/XtOrderTypes.h"
#include "ctp/recovery/CTPStateTracker.h"
#include "ctp/recovery/CTPRecovery.h"
#include "ctp/td/CTPTdCfg.h"
#include "ctp/td/CtpOrderRefStore.h"
#include "xt_live/gateway/common/TdReadinessGate.h"
#include "xt_live/gateway/common/DeferredTimerQueue.h"
#include "common/XtLiveRecoveryError.h"

/// @brief CTP 交易通道的核心实现：登录、恢复、下单撤单、回报归一化。
///
/// TraderCTP 实现 CTP Trader SPI，是本进程与 CTP 柜台之间的全部交易交互所在：
/// - 连接登录后先做结算确认和恢复查询（账户/持仓/在途单），凑齐才放行交易
/// - 把内部下单/撤单请求翻译成 CTP 报文发出，把 CTP 回报翻译回统一 DTO
/// - 订单号分配 crash-safe 持久化，重启不复用旧单号
///
/// @warning CTP SDK 回调线程同步进入 TraderCTP；绑定的 callbacks 必须只做
/// 轻量入队，运行中不应重新绑定。
class TraderCTP final : public CThostFtdcTraderSpi {
public:
    // ── 配置模型 ──────────────────────────────────────────────
    // provider-neutral 字段来自 ITradeGateway::Config；CTP 专属字段走 CTPTdCfg。
    using Config = ITradeGateway::Config;

    // ── 生命周期 ──────────────────────────────────────────────
    /// @brief 构造并绑定恢复流程宿主端口。
    TraderCTP();
    /// @brief 停止后台线程并释放 CTP API。
    ~TraderCTP();

    /// @brief 注入 CTPTdCfg 全部 CTP 专属字段；需在 init() 之前调用。
    /// @note 这是 CTP 专属字段的唯一配置路径。
    void configure_ctp(const CTPTdCfg& ctp_cfg);

    /// @brief 按配置初始化 TD 通道。
    bool init(const Config& cfg);
    /// @brief 启动连接与登录。
    bool start();
    /// @brief 停止通道并清理状态。
    void stop();

    /// @brief TD 通道健康检查；必要时重建 CTP API 实例。
    ///
    /// HftRunnerControl 在控制线程按周期调用。若 `connected_ == false` 的持续时间
    /// 达到 `stale_reconnect_ms`，函数会释放旧 `api_`、创建新 API 实例，并重新
    /// 执行 `RegisterSpi`、`RegisterFront`、`Init`。该路径用于处理 CTP SDK 在
    /// SimNow broker 持续拒连后不再推进内部 socket reconnect 的状态。
    ///
    /// @pre 仅允许 HftRunnerControl 控制线程串行调用；不得并发进入。
    /// @note 重建窗口内 `api_` 可为 null。submit 路径已将 `api_ == nullptr`
    ///       作为拒单条件；读到该中间状态时返回 transient reject，不产生
    ///       未定义行为。
    ///
    /// @param now_ms 当前 `XtSteadyClock::steady_now_ms()`，由 HftRunnerControl 传入。
    /// @return true 表示未触发重建；false 表示本次调用已触发 API 重建，调用方应
    ///         将本帧视为健康检查失败。
    bool check_health(uint32_t now_ms);

    // ── 类型别名 ──────────────────────────────────────────────
    using Callbacks             = ITradeGateway::Callbacks;
    using DailySettlementDelta  = ITradeGateway::DailySettlementDelta;
    using CashTransferDelta     = ITradeGateway::CashTransferDelta;
    using AccountSnapshot       = ITradeGateway::AccountSnapshot;
    using PositionSnapshot      = ITradeGateway::PositionSnapshot;

    // ── 查询类型 ──────────────────────────────────────────────
    enum class QueryKind {
        Position,
        AccountState,
        XtOrder,
        Trade
    };

    // ── 统计模型 ──────────────────────────────────────────────
    struct QueryControlStats {
        uint64_t query_pos_throttled     = 0;  ///< 持仓查询被节流次数
        uint64_t query_pos_inflight_skip = 0;  ///< 持仓查询在途时未发送 ReqQry* 的触发次数
        uint64_t query_pos_dirty_merged  = 0;  ///< 持仓查询创建 dirty retry 补发窗口的次数
    };

    // ── 回调绑定 ──────────────────────────────────────────────
    /// @brief 绑定统一 TD 回调包（order/trade/account/position/recovery-position/ready/front/reverse-sync）。
    /// @warning 不加锁，必须在 start() 前完成绑定；运行中 rebind 可能与 SDK 回调线程
    ///          或 recovery/refresh 后台线程并发。
    void bind_callbacks(Callbacks callbacks) { callbacks_ = std::move(callbacks); }

    // ── Recovery 反向灌入口 ────────────────────────────────────────────────────
    /// @brief 发布 recovery 阶段发现的 alive in-flight orders。
    ///
    /// CtpRecovery ready 前调用。函数消耗 `recovery_alive_orders_`，补齐本地
    /// state tracker / cancel routing，然后通过 callbacks_.on_reverse_sync_batch
    /// 发给上层；调用方用 is_cold_start 区分 cold-start rebuild 与 warm reconnect 合并。
    void rebuild_alive_inflight_and_emit(bool is_cold_start);

    // ── 交易接口 ──────────────────────────────────────────────
    /// @brief CTP 下单接口：校验 → OrderRef 生成 → 构造 CTP 请求（submit to CTP）。
    ///
    /// @param price_fp  内核定点价格（kPriceScale=1e6），在 TD 边界转换为 CTP double。
    /// @param qty       手数（必须为整数值；非整数时拒单返回 0）。
    /// @return 本地订单号（local_id / OrderRef）；以下情况返回 0（不下发）：
    ///         - `api_` 未初始化；price_fp/qty 无效；qty 非整数或溢出；
    ///         - `order_ref_store_.next()` 生成失败（seed 持久化写入错误，极罕见）。
    ///
    /// @note OrderRef 由 CtpOrderRefStore 恢复并原子持久化，保证同日严格单调递增。
    /// @note CloseToday / CloseYesterday 直传；Close 在 SHFE/INE 映射为
    ///       CloseYesterday，今仓由上层 build_close_slices 拆单处理。
    /// @note 可被 worker 线程或内部重连线程调用；CTP ReqOrderInsert() SDK 内部加锁。
    uint64_t submit_order(const char* code,
                          bool is_buy,
                          int64_t price_fp,
                          int64_t qty_fp,
                          XtTimeInForce tif,
                          const char* user_tag,
                          XtOffset close_offset);
    /// @brief 按本地单号撤单。
    bool cancel_order(uint64_t local_id);
    /// @brief 该 local_id 是否已在 CtpStateTracker 终态集（lost-terminal reconcile 探测，转发 state_）。
    bool is_local_terminal(uint64_t local_id) const;
    /// @brief 终态 payload 按值出参（交付投影 terminal_record；仅终态值返回 true）。
    bool lookup_terminal(uint64_t local_id, XtOrderTerminalRecord& out) const;

    // ── 恢复与连接控制 ──────────────────────────────────────────────
    /// @brief 启用/关闭登录后恢复流程。
    /// @note 该设置建议在 start 前完成；运行中切换不建议依赖其行为。
    void set_recovery_enabled(bool enabled) { recovery_.set_recovery_enabled(enabled); }

    // ── 状态与统计导出 ──────────────────────────────────────────────
    /// @brief 当前是否仍处于恢复中。
    bool is_recovering() const { return recovery_.is_recovering(); }
    /// @brief 当前交易日（YYYYMMDD，登录应答 TradingDay）；未登录为 0。
    uint32_t trading_day() const { return current_trading_day_.load(std::memory_order_acquire); }
    /// @brief 导出本地单撤单路由键快照（exchange_id/order_sys_id/front/session）；
    ///        供诊断与单测断言 reverse-sync 回灌单可撤性。
    bool cancel_target_snapshot(uint64_t local_id, CtpStateTracker::CancelTarget& out) const {
        return state_.get_cancel_target(local_id, out);
    }
    /// @brief 导出 typed TD readiness report，供启动门控与单测断言。
    xt_live::gateway::TdReadinessReport td_readiness_report() const {
        return readiness_gate_.report();
    }
    /// @brief 导出查询控制统计。
    QueryControlStats query_control_stats() const;
    /// @brief 当前 recovery 是否 wedged（re-arm 超阈仍未 ready）；诊断/单测。
    bool is_recovery_wedged() const { return recovery_wedged_.load(std::memory_order_acquire); }
    /// @brief recovery re-arm 累计调度次数；供诊断、metrics 与单测观测。
    uint64_t recovery_rearm_scheduled_total() const {
        return recovery_rearm_scheduled_total_.load(std::memory_order_acquire);
    }
    /// @brief Ready 稳态 refresh 查询超时自愈累计触发次数；进程内观测/单测（不接 prometheus）。
    uint64_t refresh_query_timeout_total() const {
        return refresh_query_timeout_total_.load(std::memory_order_acquire);
    }
    /// @brief 是否有任一 refresh 查询连续超时达 wedge 阈值（refresh wedged）；进程内观测/单测。
    /// @note 由 per-kind 连续超时计数派生（收到回包即清零），无独立 flag 漂移。
    bool is_refresh_query_wedged() const {
        if (refresh_query_wedge_rounds_ <= 0) return false;
        for (const auto& q : qry_) {
            if (q.refresh_timeout_count.load(std::memory_order_acquire) >= refresh_query_wedge_rounds_) {
                return true;
            }
        }
        return false;
    }
    /// @brief generation fence 丢弃的过期轮查询回包帧数（进程内观测/单测，不接 prometheus）。
    uint64_t stale_query_row_dropped_total() const {
        return stale_query_row_dropped_.load(std::memory_order_acquire);
    }
    /// @brief 无法比对（nRequestID<=0 或本侧未绑定）而 fail-open 接受的查询回包帧数。
    uint64_t stale_query_row_unfenceable_total() const {
        return stale_query_row_unfenceable_.load(std::memory_order_acquire);
    }
    /// @brief 查询会话代际（front 断连/session reset/rearm 时 ++）；供 provenance 记录与单测。
    uint64_t query_session_gen() const {
        return query_session_gen_.load(std::memory_order_acquire);
    }
    /// @brief 测试 seam（仅测试使用）：强制设置 connected_ 以驱动 re-arm gate；始终编译避免 #ifdef ODR 风险。
    void set_connected_for_test(bool v) { connected_.store(v, std::memory_order_release); }
    /// @brief 测试 seam：直接绑定某 kind 的在途查询 request_id
    ///        （prod 绑定点在 request_query bind-before-send，单测无 api_ 走不到；始终编译避免 #ifdef ODR 风险）。
    void set_active_request_id_for_test(QueryKind kind, int request_id) {
        qry_[qry_idx(kind)].active_request_id.store(request_id, std::memory_order_release);
    }
    /// @brief identity mismatch 闩锁是否已置位（进程级终态；供诊断与单测观测）。
    bool identity_mismatch_latched() const {
        return identity_mismatch_latched_.load(std::memory_order_acquire);
    }
    /// @brief 测试 seam：注入 expected_investor_id（prod 走 init(cfg)；始终编译避免 #ifdef ODR 风险）。
    void set_expected_investor_id_for_test(std::string v) { expected_investor_id_ = std::move(v); }
    /// @brief 测试 seam：覆盖进程启动墙钟锚（pre-restart 封印判据；prod 在 ctor 取真值）。
    void set_process_start_epoch_us_for_test(int64_t us) { process_start_epoch_us_ = us; }
    /// @brief 测试 seam：覆盖发射分区锚点（prod 在 position bIsLast 暂存快照时取墙钟）。
    void set_recovery_anchor_epoch_us_for_test(int64_t us) {
        recovery_position_anchor_us_.store(us, std::memory_order_release);
    }
    /// @brief 交付投影只读视图（诊断/单测）。
    const xt_live::gateway::XtExecDeliveryLedger& exec_ledger() const {
        return state_.exec_ledger();
    }
    /// @brief pre-restart 封印疑似泄漏计数（O7 量水位备机制，仅观测；转发 state_）。
    uint64_t watermark_breach_total() const { return state_.watermark_breach_total(); }
    /// @brief cum>=qty completion 事件派生累计（进程内观测/单测；转发 state_）。
    uint64_t synth_cum_filled_total() const { return state_.synth_cum_filled_total(); }
    /// @brief keep-Filled 冲突计数（合成 Filled 后迟到非 Filled 部分成交终态；转发 state_）。
    uint64_t cum_filled_conflict_total() const { return state_.cum_filled_conflict_total(); }

    // ── CThostFtdcTraderSpi 回调 ──────────────────────────────────────────────
    // 这些函数都在 CTP SDK 回调线程同步执行；实现负责 session 状态迁移、
    // recovery 查询推进，以及将 CTP 原始回报归一化后转发到 callbacks_。
    // ── 连接、登录与结算确认 ──────────────────────────────────────────────
    /// @brief front 连上：重置本轮 session 状态并发起 TD 登录。
    void OnFrontConnected() override;

    /// @brief front 断开：标记 TD 不可用、通知上层 front=false，并按配置启动重连。
    /// @param nReason CTP SDK 原始断连原因码，仅用于 log/recorder 与上层状态通知。
    void OnFrontDisconnected(int nReason) override;

    /// @brief 登录应答：成功后进入结算确认/恢复流程，失败时记录错误并安排重试。
    /// @param pRspUserLogin 登录成功时的 CTP session 信息，可为空。
    /// @param pRspInfo CTP 错误信息；ErrorID 非 0 表示登录失败。
    /// @param bIsLast CTP 批量应答结束标志；登录通常单条返回。
    void OnRspUserLogin(CThostFtdcRspUserLoginField* pRspUserLogin,
                        CThostFtdcRspInfoField* pRspInfo,
                        int nRequestID,
                        bool bIsLast) override;

    /// @brief 结算确认应答：成功后放行 recovery pipeline，失败时保持 TD not-ready。
    /// @param pRspInfo CTP 错误信息；ErrorID 非 0 表示确认失败。
    /// @param bIsLast CTP 批量应答结束标志；结算确认通常单条返回。
    void OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField* pConfirm,
                                    CThostFtdcRspInfoField* pRspInfo,
                                    int nRequestID,
                                    bool bIsLast) override;

    /// @brief CTP 通用错误应答：记录请求级错误，辅助定位非业务回报失败。
    /// @param bIsLast CTP 批量应答结束标志；用于与原始请求生命周期对齐。
    void OnRspError(CThostFtdcRspInfoField* pRspInfo,
                    int nRequestID,
                    bool bIsLast) override;

    // ── recovery 查询应答 ──────────────────────────────────────────────
    /// @brief 委托查询应答：recovery 阶段恢复本地订单状态并累积 alive in-flight batch。
    /// @param pOrder CTP 原始委托行；可为空，bIsLast=true 时表示本批结束。
    void OnRspQryOrder(CThostFtdcOrderField* pOrder,
                       CThostFtdcRspInfoField* pRspInfo,
                       int nRequestID,
                       bool bIsLast) override;

    /// @brief 成交查询应答：recovery 阶段重放历史成交给 state tracker 做去重/对账。
    /// @param pTrade CTP 原始成交行；可为空，bIsLast=true 时表示本批结束。
    void OnRspQryTrade(CThostFtdcTradeField* pTrade,
                       CThostFtdcRspInfoField* pRspInfo,
                       int nRequestID,
                       bool bIsLast) override;

    /// @brief 持仓查询应答：聚合 CTP 持仓行，完成后 emit PositionSnapshot batch。
    /// @param pPosition CTP 原始持仓行；可为空，bIsLast=true 时表示本批结束。
    void OnRspQryInvestorPosition(CThostFtdcInvestorPositionField* pPosition,
                                  CThostFtdcRspInfoField* pRspInfo,
                                  int nRequestID,
                                  bool bIsLast) override;

    /// @brief 资金查询应答：归一化 account snapshot，并驱动日结 delta 检测。
    /// @param pAccount CTP 原始资金快照；可为空，bIsLast=true 时表示本批结束。
    void OnRspQryTradingAccount(CThostFtdcTradingAccountField* pAccount,
                                CThostFtdcRspInfoField* pRspInfo,
                                int nRequestID,
                                bool bIsLast) override;

    // ── 下单/撤单请求错误 ──────────────────────────────────────────────
    /// @brief 下单请求错误应答：将 ReqOrderInsert reject 归一化为订单更新。
    /// @param pInputOrder 原始下单请求；用于反解 OrderRef/local_id。
    void OnRspOrderInsert(CThostFtdcInputOrderField* pInputOrder,
                          CThostFtdcRspInfoField* pRspInfo,
                          int nRequestID,
                          bool bIsLast) override;

    /// @brief 撤单请求错误应答：将 ReqOrderAction reject 归一化为订单更新。
    /// @param pInputOrderAction 原始撤单请求；用于反解 OrderRef/local_id。
    void OnRspOrderAction(CThostFtdcInputOrderActionField* pInputOrderAction,
                          CThostFtdcRspInfoField* pRspInfo,
                          int nRequestID,
                          bool bIsLast) override;

    /// @brief 异步下单错误回报：记录 broker-side insert reject。
    void OnErrRtnOrderInsert(CThostFtdcInputOrderField* pInputOrder,
                             CThostFtdcRspInfoField* pRspInfo) override;

    /// @brief 异步撤单错误回报：将 broker-side cancel reject 归一化为订单更新。
    void OnErrRtnOrderAction(CThostFtdcOrderActionField* pOrderAction,
                             CThostFtdcRspInfoField* pRspInfo) override;

    // ── 委托/成交推送 ──────────────────────────────────────────────
    /// @brief 委托状态推送：归一化 CTP order rtn；recovery 中仅更新 state tracker。
    void OnRtnOrder(CThostFtdcOrderField* pOrder) override;

    /// @brief 成交推送：委托 on_rtn_trade_（from_query=false）。recovery 窗口内的
    ///        live 成交入缓冲，recovery finish 按 TradeTime 分区发射（query 重放行
    ///        走 OnRspQryTrade 委托，按 §4.3 来源规则 absorbed 或交付）。
    void OnRtnTrade(CThostFtdcTradeField* pTrade) override;

    // ── 银期转账推送 ──────────────────────────────────────────────
    /// @brief 银期入金推送：归一化为 cash-transfer delta。
    void OnRtnFromBankToFutureByFuture(CThostFtdcRspTransferField* pRspTransfer) override;

    /// @brief 银期出金推送：归一化为 cash-transfer delta。
    void OnRtnFromFutureToBankByFuture(CThostFtdcRspTransferField* pRspTransfer) override;

private:
    // ── 会话阶段（session phase） ──────────────────────────────────────────────
    enum class SessionPhase {
        Stopped,             ///< 未启动或 stop/self-heal reset 后的静止态
        FrontConnected,      ///< front 已连上，允许发送 login 或等待 login retry
        LoginOk,             ///< login 成功，尚未完成 settlement/recovery gate
        SettlementPending,   ///< 结算确认请求在途或等待重试
        Recovering,          ///< settlement 已放行，正在查询 account/position/order/trade
        Ready,               ///< 所有 TD readiness stage 已满足，上层可交易
        FrontDisconnected    ///< 已断线；等待 reconnect/self-heal 重新进入连接流程
    };

    // ── 查询控制状态（per-kind） ──────────────────────────────────────────────
    // 聚合每类查询的 inflight / throttle / retry / timeout / 统计字段，
    // 按 qry_idx(kind) 下标访问。
    /// active_request_id 的失效哨兵：rearm 放弃旧轮后、新发送覆盖前，
    /// 任何 nRequestID>0 的回包都不匹配 → fence 丢弃。不能用 0——0 是
    /// 「未绑定 fail-open 全收」语义。
    static constexpr int kQueryRequestIdInvalidated = -1;
    struct QueryCtrl {
        std::atomic<bool>     inflight{false};         ///< 是否有同类查询在途
        std::atomic<int>      active_request_id{0};    ///< 本轮 ReqQry* 的 nRequestID（bind-before-send）；0=未绑定全收，kQueryRequestIdInvalidated=rearm 已放弃旧轮
        std::atomic<bool>     dirty{false};            ///< 在途时收到新请求，标记脏
        std::atomic<bool>     retry_pending{false};    ///< retry worker 是否待执行
        std::atomic<int64_t>  next_allowed_ms{0};      ///< 限流门控：允许发送的最早时间戳(ms)
        std::atomic<int>      backoff_level{0};        ///< 指数退避级别
        std::atomic<uint64_t> timeout_token{0};        ///< 恢复查询超时守卫 cancel token
        std::atomic<int>      timeout_count{0};        ///< 本次恢复周期已触发超时次数
        std::atomic<uint64_t> refresh_timeout_token{0}; ///< Ready 稳态 refresh 查询超时守卫 cancel token
        std::atomic<int>      refresh_timeout_count{0};  ///< 自上次成功回包以来连续 refresh 超时次数
        std::atomic<uint64_t> throttled_count{0};      ///< ReqQry* 返回 -2 后进入退避的次数（Position 导出）
        std::atomic<uint64_t> inflight_skip_count{0};  ///< 同类查询已在途，本次触发未发送 ReqQry* 的次数
        std::atomic<uint64_t> dirty_merge_count{0};    ///< dirty 从 false->true、创建补发窗口的次数

        QueryCtrl() = default;
        QueryCtrl(const QueryCtrl&) = delete;
        QueryCtrl& operator=(const QueryCtrl&) = delete;
    };

    /// @brief QueryKind 的日志和诊断标签。
    ///
    /// 仅承载稳定的可读名与 CTP SDK Req* 函数名，供查询发送、retry 与
    /// recovery timeout 路径复用；不承载 readiness、timeout 或 retry policy。
    struct QueryMeta {
        const char* name     = "unknown";    ///< 查询类型可读名（non-owning static literal）
        const char* req_name = "ReqUnknown";  ///< CTP SDK Req* 函数名（non-owning static literal）
    };

    // ── 请求编号 ──────────────────────────────────────────────
    /// @brief 生成下一个请求 id（req id）。
    int next_req_id() { return req_id_.fetch_add(1, std::memory_order_relaxed) + 1; }

    // ── 登录与会话状态 ──────────────────────────────────────────────
    /// @brief 发起交易前置登录（login request）。
    /// @return true 表示 ReqUserLogin 已被 SDK 接收；认证结果由 OnRspUserLogin 回调决定。
    bool login();
    /// @brief 处理登录失败：记录退避输入、写 recorder，并调度下一次 login。
    void handle_login_failed(CThostFtdcRspInfoField* pRspInfo);
    /// @brief Login 失败后安排重试（schedule login retry）。
    ///
    /// 触发场景：socket 已连接 (connected_=true) 但 OnRspUserLogin 报 CTP 错误
    /// (如 error 7 "还没有初始化" / error 8 "前置不活跃")。SimNow settlement 窗口
    /// (≈19:00-21:00) 常见此现象。socket 通但永远不 ready 的死锁。
    void schedule_login_retry();
    /// @brief 登录回包 identity 自证：expected_investor_id 非空时比对回包 UserID。
    /// @return true = 通过（或未配置校验）；false = 已置 identity mismatch 闩锁，
    ///         调用方必须中止本次会话推进（不进入结算确认/恢复流程）。
    /// @note fail-closed：expected 非空而回包缺 UserID 也判 mismatch。
    bool verify_login_identity_(const CThostFtdcRspUserLoginField* login);
    /// @brief 更新会话阶段（state transition）。
    void set_session_phase(SessionPhase phase);
    /// @brief 会话是否存活（stopping/connected/api 三重检查）。
    bool is_session_alive() const noexcept;
    /// @brief 重置会话状态（front_id/session_id/recovery/settlement 等）。
    void reset_session_state();
    /// @brief 重置 typed TD readiness gate（随 session/recovery 世代重置）。
    void reset_td_readiness();
    /// @brief 若 typed readiness 全满足则发 ready，否则保持 fail-closed。
    void emit_ready_if_td_readiness_satisfied();
    /// @brief 登录后进入恢复流程（enter recovery pipeline）。
    void enter_recovery_pipeline(bool settlement_confirmed);
    /// @brief recovery 查询 fail-closed 后调度有界 backoff 重发。
    /// @note timer 回调只 reset query-ctrl + 重发 query_once(kind)，不重入 pipeline、不碰 state_；
    ///   仅 connected && is_recovering && backoff>0 时调度，token 防跨 session 穿透，
    ///   mutex+pending 序列化 SDK×timer 两线程。
    void maybe_schedule_recovery_rearm_(QueryKind kind);
    /// @brief 重发前重置某 kind 的 query 控制态（解 stuck inflight）。
    void reset_query_ctrl_for_rearm_(QueryKind kind);
    /// @brief 置/清 td recovery wedged 状态并 emit gauge。
    void set_recovery_wedged_(bool wedged);
    /// @brief 过期轮持仓回包被归属栅栏拦截时的限频告警（计数在 CtpRecovery 内）。
    void maybe_log_stale_position_frame_(int request_id);
    /// @brief query generation fence：查询回包帧是否归属该 kind 当前在途轮。
    /// @return true=接受（归属当前轮，或 nRequestID<=0 / 本侧未绑定不可比对时 fail-open）；
    ///         false=过期轮，调用方整帧丢弃（已计数 + 限频告警）。
    /// @note fail-open 是硬要求：部分柜台/front 不回显 nRequestID，fence 不得成为
    ///       可用性单点（全丢 → readiness 永 fail → recovery wedge）。
    bool query_row_owned_(QueryKind kind, int request_id);
    /// @brief fence 拦截过期轮查询回包的限频告警。
    void maybe_log_stale_query_row_(QueryKind kind, int request_id);

    // ── 结算确认 ──────────────────────────────────────────────
    /// @brief 发起结算确认（settlement confirm）。
    bool confirm_settlement();
    /// @brief 发起结算确认并在成功时挂超时守卫。
    bool request_settlement_confirm_with_timeout();
    /// @brief 结算确认失败后安排重试（hard gate, no degraded ready）。
    void schedule_settlement_confirm_retry(XtLiveRecoveryError reason);
    /// @brief 结算确认超时兜底（settlement timeout fallback）。
    void arm_settlement_timeout();

    // ── 查询调度、重试与超时 ──────────────────────────────────────────────
    /// @brief QueryKind → std::array 下标。
    static constexpr size_t qry_idx(QueryKind kind) noexcept { return static_cast<size_t>(kind); }
    /// @brief 返回 QueryKind 对应的日志和诊断标签。
    QueryMeta query_meta(QueryKind kind);
    /// @brief ready 后账户/持仓刷新查询是否允许发送。
    /// @note 该 gate 不适用于 recovery 查询；recovery 阶段需要在 `Ready` 前主动发 qry*。
    bool can_run_refresh_query() const;
    /// @brief 发起委托/成交/持仓/资金查询（统一入口）。
    void query_once(QueryKind kind);
    /// @brief 处理 CTP flow-control(-2) 后的 per-kind backoff 与 retry 调度。
    /// @note 调用方必须已释放本次发送占用的 `inflight`，本函数只更新退避状态和 telemetry。
    void handle_query_throttled(QueryKind kind, QueryCtrl& q, const QueryMeta& meta, int64_t now_ms);
    /// @brief 统一查询入口（按 QueryKind 发 CTP 请求）。
    int request_query(QueryKind kind);
    /// @brief 查询回包收尾（release inflight + drain merged trigger）。
    void on_query_response_done(QueryKind kind, bool success);
    /// @brief 为失败查询安排重试（retry schedule）。
    void schedule_query_retry(QueryKind kind);
    /// @brief 处理一次查询重试；返回是否执行了重试。
    bool process_query_retry(QueryKind kind);
    /// @brief 查询重试后台线程主循环。
    void run_query_retry_worker();
    /// @brief 恢复阶段查询超时守卫（recovery query timeout guard）。
    void arm_recovery_query_timeout(QueryKind kind);
    /// @brief Ready 稳态 refresh 查询超时自愈守卫。
    /// @note 仅在 SessionPhase::Ready 且非 recovery、refresh_query_timeout_ms_>0 时挂 timer；
    ///   超时未收 bIsLast 回包 → 解 stuck inflight + 重发 query_once（不碰 state_，仅触 qry_ atomic），
    ///   连续超时达阈值置 refresh wedged 观测状态（page-then-continue）。token 防被新查询 supersede。
    void arm_refresh_query_timeout(QueryKind kind);
    /// @brief 查询完成后失效超时守卫（invalidate timeout token）。
    void clear_recovery_query_timeout(QueryKind kind);
    /// @brief 清理所有恢复查询超时状态。
    void clear_recovery_query_timeout_states();
    /// @brief 清理所有查询重试状态。
    void clear_query_retry_states();
    /// @brief 重置 per-kind 查询运行态与统计计数（inflight/dirty/backoff/metrics）。
    void reset_query_control_states();

    // ── 周期刷新 ──────────────────────────────────────────────
    /// @brief ready 后触发一次账户/持仓刷新。
    void refresh_once_after_ready();
    /// @brief 周期刷新线程主循环。
    void start_refresh_loop();

    // ── Recovery 宿主端口 ──────────────────────────────────────────────
    /// @brief 构造 recovery 宿主端口，注入到 `recovery_`（ctor 装配，捕获 self）。
    static CtpRecovery::HostPorts make_recovery_host_ports(TraderCTP* self);
    /// @brief 将 recovery 持仓快照归一化为 ITradeGateway::PositionSnapshot batch。
    ///
    /// 纯转换、无副作用；broker_frozen_last_update_ms 统一打 steady_clock 当前 ms，
    /// 与 risk path 同源，用于 staleness 检测。
    std::vector<PositionSnapshot> convert_position_snapshot(
        const std::vector<CtpRecovery::PositionSnapshot>& in) const;
    /// @brief recovery ready 发布口：执行发射分区收尾序，再开放 TD ready。
    void on_recovery_ready();
    /// @brief recovery finish 发射分区（§4.4）：pre fills（venue 时间序）→
    ///        position snapshot → reverse-sync batch → post fills →
    ///        gate 重开+二次 drain → pending terminals。
    ///
    /// 分区判据 = venue `TradeTime` vs 锚点（±margin 边带 → pre 保守）。
    /// reverse-sync handler 未绑定的 fail 路径：快照照发、缓冲整批不下发、
    /// gate 开（之后 fill 直发；td_ready 已被 ReverseSync fail 挡住）。
    void emit_recovery_finish_sequence_(bool is_cold_start);
    /// @brief recovery 期暂存 position 快照 + 记录发射分区锚点（多轮 rearm 最新轮覆盖）。
    void stage_recovery_positions_(std::vector<PositionSnapshot> out);
    /// @brief 发射暂存的 recovery position 快照（无暂存则 no-op）。
    /// @note 优先走 on_recovery_position；未绑定时回退 legacy on_position，二者恰好择一。
    void emit_staged_recovery_positions_(bool is_cold_start);
    /// @brief 重开窗口 gate 并二次 drain 发射期间新入缓冲的 fill（检查点 iv）。
    void reopen_recovery_window_gate_and_drain_();
    /// @brief flush recovery gate 期间拦下的 known-local 终态回报。
    /// @note 仅在 reverse-sync batch emit 成功后的收尾序中调用（book 先就位再
    ///       销账）；emit fail 路径不 flush，buffer 留给下轮 recovery。
    void flush_recovery_pending_terminals_();
    /// @brief 成交归一化 + 来源规则 + recovery 窗口缓冲的单一入口。
    ///
    /// exactly-once 判据在交付投影（record_fill 查+记一体，双源任一先到恰好一次）。
    /// query 重放行按 `classify_query_fill_absorbed_` 决定 absorbed（记账不下发）
    /// 或交付；交付行与 live push 同门：gate 关闭期间入缓冲（recovery finish 按
    /// TradeTime 分区发射），gate 开放时直发。
    void on_rtn_trade_(CThostFtdcTradeField* pTrade, bool from_query);
    /// @brief fill 记账后的 completion 事件派发（cum >= qty 且未终态 → Filled）。
    ///
    /// 与 fill 同线程同序：直发路径紧随 on_trade 之后 emit；recovery 中 / 窗口
    /// gate 关闭时进 recovery_pending_terminals_，由 finish 收尾序在窗口 fill
    /// flush 之后统一补发（fill→终态次序两路径都保持）。
    void maybe_emit_cum_filled_completion_(uint64_t local_id, uint64_t local_cb_us);
    /// @brief query 重放行的 absorbed 判定（§4.3 来源规则）。
    ///
    /// 回滚开关 off → 一律 absorbed（旧行为）；venue TradeTime 早于进程启动
    /// （+margin）→ pre-restart 封印；cold 轮 TradeTime ≤ 锚点(+margin) →
    /// 已被快照吸收的历史行。其余 → 交付。
    bool classify_query_fill_absorbed_(const CThostFtdcTradeField* pTrade) const;
    /// @brief 当前墙钟 UTC epoch µs（system_clock；与 venue TradeTime 同时钟域）。
    static int64_t wall_now_epoch_us_();

    // ── 回报与事件辅助 ──────────────────────────────────────────────
    /// @brief 判定 CTP 返回是否为错误（rsp error check）。
    bool is_error(CThostFtdcRspInfoField* pRspInfo);
    /// @brief 外部/历史撤单错误去重日志（避免同一 ref 日志风暴）。
    bool should_log_foreign_cancel_error_once(const char* order_ref, int error_id);
    /// @brief 尝试在首次资金查询点发布日结事件。
    void maybe_emit_daily_settlement_event(const CThostFtdcTradingAccountField* pAccount);
    /// @brief 发布银期转账事件。
    void emit_cash_transfer_event(const CThostFtdcRspTransferField* pRspTransfer, bool bank_to_future);

    // ── 通用工具 ──────────────────────────────────────────────
    /// @brief 在 ms 毫秒后于 timer_queue_ 的 owned worker 线程执行 action。
    /// @note 取代旧的 detached `std::thread.detach()`；stop() join worker 后不再触发，
    ///       从根上消除 detached `[this]` 在析构后访问已释放对象的 UAF。
    void schedule_after_ms(int ms, std::function<void()> action);

    // ── CTP 连接与配置状态 ──────────────────────────────────────────────
    CThostFtdcTraderApi* api_ = nullptr;        ///< CTP TD API 实例（owning, SDK 分配）
    mutable std::shared_mutex api_mu_;           ///< 守 api_ 生命周期：submit/cancel/query 取 shared，stop/check_health 重建取 unique
    std::string front_;                          ///< 前置地址（front address）
    std::string broker_;                         ///< 经纪商代码（broker ID）
    std::string user_;                           ///< 用户名（investor ID）
    std::string pass_;                           ///< 密码（password）
    std::string expected_investor_id_;           ///< identity 自证锚；空 = 不校验（来自 cfg.expected_investor_id）
    std::string flow_dir_;                       ///< 流文件目录（flow path）
    std::atomic<int>  front_id_{0};              ///< CTP 前置编号（front ID, 登录后赋值）
    std::atomic<int>  session_id_{0};            ///< CTP 会话编号（session ID, 登录后赋值）
    std::atomic<int>  req_id_{0};                ///< 请求 ID 自增计数器（request ID counter）
    CtpOrderRefStore order_ref_store_;           ///< OrderRef 序列分配 + crash-safe 持久化（自洽子系统）
    bool reconnect_enabled_         = true;      ///< 断连后自动重连（auto-reconnect）
    int  reconnect_interval_ms_     = 2000;      ///< settlement-confirm 重试间隔下限 ms（源自 cfg.reconnect_interval_ms）
    uint32_t                       stale_reconnect_ms_ = 0;  ///< Front 断线超过该时长后重建 TD API；0=禁用
    std::atomic<uint32_t>          last_front_active_ms_{0};  ///< front 活跃或 rebuild attempt 的 steady ms 锚点
    CtpFrontDisconnectLogGate      disc_log_gate_;            ///< OnFrontDisconnected 日志限频(SDK 回调线程独占)
    bool confirm_settlement_        = true;      ///< 是否自动确认结算（auto settlement confirm）
    bool settlement_confirmed_      = false;     ///< 本次会话结算是否已确认
    int  settlement_confirm_timeout_ms_ = 8000;  ///< 结算确认超时 ms
    int  settlement_confirm_retry_ms_   = 2000;  ///< 结算确认重试间隔 ms
    int  login_retry_ms_                = 5000;  ///< Login 失败后重试间隔 ms（CTP 限速 ≈ 1Hz）
    int  recovery_query_timeout_ms_     = 8000;  ///< 恢复查询超时 ms
    int  recovery_query_max_timeouts_   = 6;     ///< 恢复查询最大连续超时次数
    int  recovery_rearm_backoff_ms_     = 5000;  ///< fail-closed 后重发查询的基准 backoff（指数退避封顶 60s）；<=0 关闭 re-arm
    int  recovery_rearm_alert_rounds_   = 10;    ///< re-arm 轮数 ≥ 此值置 td recovery wedged 告警；re-arm 仍继续
    int  refresh_query_timeout_ms_      = 30000; ///< Ready 稳态 refresh 查询超时自愈 ms；<=0 关闭（回滚开关）
    int  refresh_query_wedge_rounds_    = 6;     ///< refresh 查询连续超时达此轮数置 refresh wedged 观测状态（仍重发）

    // ── recovery re-arm 状态 ──────────────────────────────
    // 当前 recovery FSM 严格串行（一次仅一个 stage 的 query 在途），因此单 bool pending
    // 足够；若改为 per-stage 并发查询，pending 必须拆成 per-kind 状态。
    std::mutex recovery_rearm_mu_;                            ///< 序列化 SDK×timer 两线程的 re-arm 调度决策
    int  recovery_rearm_round_    = 0;                        ///< 本 session re-arm 轮数（recovery_rearm_mu_ 保护）
    bool recovery_rearm_pending_  = false;                    ///< 单轮 in-flight 去重（recovery_rearm_mu_ 保护）
    std::atomic<uint64_t> recovery_rearm_token_{0};           ///< session 代际；reset_session_state ++，stale 回调 token 不匹配则 no-op
    std::atomic<uint64_t> recovery_rearm_scheduled_total_{0}; ///< 累计调度次数（诊断/metrics/单测）
    std::atomic<bool>     recovery_wedged_{false};            ///< re-arm 超阈仍未 ready（xt_td_recovery_wedged gauge 镜像）

    // ── 会话生命周期原子标志（session lifecycle atomics） ──────────────────────────────────────────────
    // alignas(64)：SDK 回调线程、刷新线程并发读写，防止 false sharing。
    alignas(64) std::atomic<bool>         stopping_{false};                  ///< 停止信号（stop requested）
    std::atomic<SessionPhase>             session_phase_{SessionPhase::Stopped}; ///< 当前会话阶段（session phase FSM）
    std::atomic<bool>                     connected_{false};                ///< 前置连接状态（front connected）
    /// identity mismatch 闩锁：进程级终态 sticky，reset_session_state 不复位
    /// （配置错配对整个进程生命周期成立）；enter_recovery_pipeline 据此拒绝推进。
    std::atomic<bool>                     identity_mismatch_latched_{false};
    std::atomic<bool>                     refresh_once_running_{false};     ///< ready 后首次刷新线程存活标志
    std::atomic<bool>                     refresh_running_{false};          ///< 周期刷新线程存活标志（periodic refresh alive）

    // ── 日结/资金事件标志（settlement/transfer flags） ──────────────────────────────────────────────
    // alignas(64)：SDK 回调线程写，后台线程读，独立 cache line。
    alignas(64) std::atomic<bool>         settlement_confirm_pending_{false};       ///< 结算确认请求在途（settlement confirm in-flight）
    std::atomic<bool>                     settlement_confirm_retry_pending_{false}; ///< 结算确认重试待执行
    std::atomic<bool>                     login_retry_pending_{false};              ///< Login 重试待执行（dedup flag）
    // TD login 退避输入：最近 ErrorID + 连续失败次数。
    // ErrorID 75 固定 1h；ErrorID 7 最多 30s；其它错误最多 5min。
    std::atomic<int>                      consecutive_login_failures_{0};           ///< 连续登录失败计数
    std::atomic<int>                      last_login_error_id_{0};                  ///< 上次 OnRspUserLogin pRspInfo->ErrorID
    std::atomic<uint64_t>                 settlement_confirm_token_{0};            ///< 结算确认超时 cancel token
    std::atomic<bool>                     settlement_event_pending_{false};        ///< 日结事件待发布（settlement event pending）
    std::atomic<uint32_t>                 current_trading_day_{0};                 ///< 当前交易日（YYYYMMDD, from login）
    std::atomic<uint32_t>                 last_settlement_event_day_{0};           ///< 上次已发布日结事件的交易日（dedup guard）

    // ── 查询控制（per-kind QueryCtrl 数组） ──────────────────────────────────────────────
    // 按 qry_idx(QueryKind) 下标访问，集中保存每类查询的控制状态。
    // 各 QueryCtrl 内的字段由多线程并发访问（SDK 回调写 inflight，重试线程写 retry_pending
    // 等），atomic 保证正确性；此处不强制额外 alignas，周期查询非 HFT 热路径。
    std::array<QueryCtrl, 4> qry_{};                     ///< 按 QueryKind 索引的查询控制状态数组
    std::atomic<bool> qry_retry_worker_running_{false};  ///< 查询重试 worker 线程存活标志
    // 跨-kind 全局查询节流：CTP flow-control 是所有 qry* 共享(~1/s)，非 per-kind。
    // per-kind 门(QueryCtrl.next_allowed_ms)挡不住不同 kind 背靠背连发(实测 position/
    // account/order/trade 在 6-25ms 内全发完 → 间歇 -2)；此全局时间戳确保任意两次 qry* 间隔 ≥ gap。
    std::atomic<int64_t> next_global_query_allowed_ms_{0};  ///< 任意 qry* 允许发送的最早时刻(ms，跨 kind)
    std::atomic<uint64_t> refresh_query_timeout_total_{0};  ///< Ready 稳态 refresh 查询超时自愈累计触发次数（进程内观测/单测，不接 prometheus）

    // ── query generation fence（CTP-D8）──────────────────────────────────────
    // 四类查询回包按 nRequestID 匹配该 kind 当前在途轮（bind-before-send），
    // 过期轮（超时 rearm 重发后旧代迟到）整帧丢弃：row 不混入聚合/dedup，
    // bIsLast 不释放新轮 inflight、不推进 recovery FSM。
    bool query_generation_fence_ = true;                     ///< runtime.td.query_generation_fence；false=关闭（回退全收）
    std::atomic<uint64_t> query_session_gen_{0};             ///< 查询会话代际：front 断连/session reset/rearm ++（provenance 消费）
    std::atomic<uint64_t> stale_query_row_dropped_{0};       ///< fence 丢弃的过期轮查询回包帧数（进程级，单调）
    std::atomic<uint64_t> stale_query_row_unfenceable_{0};   ///< nRequestID<=0 或本侧未绑定，无法比对而 fail-open 接受的帧数
    std::atomic<int64_t>  last_stale_query_row_log_ms_{0};   ///< fence 丢弃告警限频时间戳（steady ms）

    int account_refresh_ms_  = 0;  ///< 资金刷新间隔 ms（0 = 禁用）
    int position_refresh_ms_ = 0; ///< 持仓刷新间隔 ms（0 = 禁用；CTP 实盘要求 >0：周期持仓对账兜底）
    double account_snapshot_max_abs_ = 1e12; ///< 账户快照数值绝对值上限（来自 runtime.td.account_snapshot_max_abs）
    mutable std::mutex             foreign_cancel_warn_mu_;       ///< 外部撤单去重锁（protects warned_keys/fifo）
    std::unordered_set<std::string> foreign_cancel_warned_keys_;  ///< 已告警的外部撤单 OrderRef 集合（dedup set）
    std::deque<std::string>         foreign_cancel_warn_fifo_;    ///< 外部撤单告警淘汰队列（bounded FIFO for eviction）

    // ── 协作式延时与唤醒 ──────────────────────────────────────────────
    // timer_queue_ 拥有单个 worker 线程跑所有延时回调；wait_cv_ 给周期线程做可中断 sleep。
    // 二者都在 stop() 内被显式关停/唤醒，保证无 detached 线程残留、无秒级 join 阻塞。
    xt_live::gateway::DeferredTimerQueue timer_queue_;  ///< owned 延时回调队列（替代 detached timer；stop join）
    std::mutex                           wait_mu_;       ///< 配合 wait_cv_ 的协作 sleep 锁
    std::condition_variable              wait_cv_;       ///< 周期循环可中断 sleep（stopping_ 置位即唤醒）

    // ── 后台线程 ──────────────────────────────────────────────
    std::thread qry_retry_thread_;    ///< 查询重试后台线程（query retry worker thread）
    std::thread refresh_once_thread_; ///< ready 后首次刷新线程（one-shot refresh thread）
    std::thread refresh_thread_;      ///< 周期刷新后台线程（periodic refresh loop thread）

    // ── 恢复与状态跟踪 ──────────────────────────────────────────────
    CtpStateTracker state_;    ///< CTP 回报状态跟踪（order/trade state tracker）
    CtpRecovery     recovery_; ///< 登录后恢复流程管理（recovery pipeline）
    xt_live::gateway::TdReadinessGate readiness_gate_{"ctp"}; ///< typed TD readiness contract

    // ── 外部回调 ──────────────────────────────────────────────
    Callbacks callbacks_; ///< 统一 TD 回调包；bootstrap-only 绑定，运行中不重绑。

    // ── Recovery alive-inflight batch ────────────────────────────────────────────
    // recovery 查询期暂存 broker 侧仍 alive 的委托；ready 阶段一次性 reverse-sync。
    // mutex 只保护该冷路径 batch，兼容 SDK 将查询回调和 ready 回调调度到不同线程。
    mutable std::mutex                recovery_alive_mu_;
    std::vector<AliveInflightOrder>   recovery_alive_orders_;
    /// recovery 查询行的撤单路由字段（AliveInflightOrder 不携带 OrderSysID/
    /// FrontID/SessionID），rebuild 注册后回填 tracker —— 保证跨重启回灌单
    /// 可撤。
    struct RecoveredCancelRouting {
        std::string exchange_id;   ///< ExchangeID
        std::string order_sys_id;  ///< OrderSysID
        int front_id = 0;          ///< 原始 FrontID（0 = 行内缺失）
        int session_id = 0;        ///< 原始 SessionID（0 = 行内缺失）
    };
    std::unordered_map<uint64_t, RecoveredCancelRouting> recovery_alive_routing_;
    /// recovery batch 内跳过的 foreign OrderRef 数；reverse-sync emit metric 后清零。
    uint64_t                          recovery_skipped_foreign_ = 0;
    /// recovery gate 拦下的 known-id 终态回报缓冲（latest-wins，key=local_id）。
    /// OnRtnOrder 在 is_recovering() 时不转发，但 state_.on_order 已 mark_terminal
    /// 并消耗 dedup 预算——不缓冲则 book/账户永远收不到该终态：tracker 挡撤单
    /// （恒 no_cancel_target）而 book 留僵尸 in-flight + frozen 泄漏。
    /// 跨 recovery 重试不清空（重放行已被 dedup 折叠，不会二次入缓冲）；flush 后清空。
    mutable std::mutex recovery_term_mu_;
    /// 缓冲终态 + 其构造时 latency eligibility（per-entry 透传，flush 时携带）：
    /// OnRtnOrder 真实 push buffered 保留原 cb_us = kEligible / cum-filled completion
    /// 派生 = kIneligible（latency-terminal 批 L-1.2 CTP）。
    struct RecoveryPendingTerminal {
        XtOrderExecUpdate    upd;
        XtLatencyEligibility elig = XtLatencyEligibility::kIneligible;
    };
    std::unordered_map<uint64_t, RecoveryPendingTerminal> recovery_pending_terminals_;

    /// recovery 窗口成交缓冲（gate 关闭期间拦下待交付的 fill，recovery finish
    /// 按 venue TradeTime 分区发射）。gate 关闭区间 = enter_recovery_pipeline →
    /// emit_recovery_finish_sequence_ 的 gate 重开点。
    /// 锁面：SPI 线程写读 + reset_session_state 有控制线程调用点
    /// （check_health rebuild，api_mu_ + Release-join 序已保护，仍照
    /// recovery_term_mu_ sibling 先例持锁，全冷路径）。
    mutable std::mutex recovery_window_mu_;
    std::vector<XtTradeRecord> recovery_window_trades_;         ///< recovery_window_mu_ 保护
    bool     recovery_window_gate_closed_ = false;              ///< recovery_window_mu_ 保护
    uint64_t recovery_window_overflow_    = 0;                  ///< recovery_window_mu_ 保护；cap 丢弃计数
    /// 过期轮持仓回包告警限频时间戳（steady ms）；混轮拦截计数在 CtpRecovery 内。
    std::atomic<int64_t> last_stale_position_log_ms_{0};
    static constexpr size_t kRecoveryWindowTradeCap = 4096;
    // gate 开关判据直读 recovery_.recovery_enabled()（enabled 单一存储——init
    // 与 wrapper 两条写入路径共用，杜绝镜像漂移使 gate 永不关的构型分叉）。

    // ── 发射分区（§4.4）状态 ──────────────────────────────
    /// venue TradeTime（秒级 CST 墙上）vs 本地墙钟锚点的跨时钟域 margin；
    /// 边带内保守归 pre/absorbed（秒级 skew 下宁被快照吸收不双计）。
    static constexpr int64_t kFillSealMarginUs = 2'000'000;
    /// 发射分区锚点：recovery 期最近一轮 position 查询 bIsLast 的墙钟 epoch µs；
    /// 0 = 本轮 recovery 尚无快照（全部 fill 归 pre）。多轮 rearm 最新轮覆盖。
    std::atomic<int64_t> recovery_position_anchor_us_{0};
    /// 进程启动墙钟 epoch µs（ctor 取值）；pre-restart 封印判据。
    int64_t process_start_epoch_us_ = 0;
    /// recovery 期暂存的 position 快照（发射点后移至 recovery finish；
    /// readiness Position stage 满足点不动）。recovery_staged_pos_mu_ 保护。
    mutable std::mutex recovery_staged_pos_mu_;
    std::vector<PositionSnapshot> recovery_staged_positions_;
    bool recovery_staged_pos_valid_ = false;
    /// runtime.td.exec_ledger_query_fill_delivery；false = 回退旧行为
    /// （query 重放行一律 absorbed 记账不下发）。
    bool exec_ledger_query_fill_delivery_ = true;

    /// 进程级 cold-start 标志；首次 recovery 消费后置 false，stop() 不复位。
    bool                              is_first_recovery_ = true;
};
