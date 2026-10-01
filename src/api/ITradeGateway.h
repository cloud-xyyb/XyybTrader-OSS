#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "api/XtPositionSnapshot.h"
#include "api/AliveInflightOrder.h"
#include "api/ExecFlags.h"
#include "api/BrokerVenueTruth.h"
#include "api/XtOrderTerminalRecord.h"

/// @brief latency tracker feed 的资格判据（latency端点可采性）。
/// @note 判据只有一个：这条数据的到达时刻能否当真实延迟端点用。
///       仅供观测面 gate latency tracker；业务消费者（book/account/策略/recorder）
///       必须忽略——合成/重放数据继续走完整收尾链，eligibility 只 gate tracker feed。
enum class XtLatencyEligibility : uint8_t {
    kEligible   = 0,  ///< 到达时刻 ≈ venue 数据到达时刻的实时回报/回执转译
    kIneligible = 1,  ///< 本地合成终态、延迟重放（recovery flush / query replay）、
                      ///< 本地预发布（Submitted）等时序与 venue 脱钩的数据
};

/**
 * @brief 实盘 TD 网关抽象接口（provider-neutral）。
 *
 * @details
 * `init/start/stop` 定义 TD lifecycle；`submit_order/cancel_order` 定义报撤单
 * 命令面；`Callbacks` 定义 order/trade/account/position/ready/front/latency
 * 与 reverse-sync batch 事件的交付契约。
 *
 * @warning callback 调用线程由 gateway 实现决定。需要 thread affinity 的 consumer
 * 必须 bridge 到自身 owner thread 后再处理。
 *
 * @note 各方法在 CTP / Binance / XTP 三个实现间存在刻意保留的语义差异（返回值、
 * 恢复窗口、终态查询能力），以本文件各方法注释为契约；完整矩阵与下游消费链见
 * `docs/architecture/trade_gateway_contract.md`。
 */
class ITradeGateway {
public:
    enum class TdState : uint8_t {
        Unknown = 0,
        Live = 1,
        Recovering = 2,
    };

    // ── 生命周期 ──────────────────────────────────────────────
    virtual ~ITradeGateway() = default;

    // ── 配置模型（provider-neutral common fields only）──────────────────
    //
    // Provider-specific 字段：CTP 走 `CTPTdCfg`（via `TraderCTPGateway::configure_ctp`）；
    // Binance U-perp 走 `BinanceTdCfg`。通过 `gateway_factory::create_td_gateway`
    // 的 `TdProviderCfg` variant 参数注入。
    //
    struct Config {
        std::string front;                                         ///< 前置地址（e.g. tcp://... 或 https://...）
        std::string user;                                          ///< 用户名 / API-key holder
        /// @warning 明文凭证字段：进程内存驻留期间不受 mlock/zeroize 保护。
        /// 用 std::string 是因为 gateway 实现层广泛读 cfg.password.c_str()
        /// （TraderCTPGateway / TraderBinanceUGateway 等）。
        std::string password;                                      ///< 密码 / API secret
        bool        reconnect_enabled             = true;          ///< 断连后自动重连
        int         reconnect_interval_ms         = 2000;          ///< 初始重连间隔 ms
        int         account_refresh_ms            = 0;             ///< 资金刷新间隔 ms（0 = 禁用）
        int         position_refresh_ms           = 0;             ///< 持仓刷新间隔 ms（0 = 禁用）
        double      account_snapshot_max_abs      = 1e12;          ///< 资金快照数值绝对值上限
        // TD stuck-disconnect self-heal (对称 MD check_health).
        // 0 = 禁用；推荐 60000 (60s)。
        uint32_t    stale_reconnect_ms            = 0;
        /// @brief 登录后恢复流程开关；仅在 init() 时被各 venue 消费一次（无运行时 setter）。
        /// CTP/XTP 据此 arm 登录后 reverse-sync；binance 忽略（recovery 由 reconcile loop 内部驱动）。
        bool        recovery_enabled              = true;
        // ── 启动期 identity 自证锚（multibook 凭证错配防护；空 = 不校验）──
        // 真金盘由 deploy 层 secrets contract gate 强制非空；此处只做 runner 侧 defence-in-depth。
        std::string expected_investor_id;   ///< CTP：登录回包 UserID 必须等于该 investor 号，否则 fail-fast 不进入交易
        std::string expected_api_key_sha8;  ///< binance：sha256(api_key) 前 8 hex（小写）指纹，不符则 init 拒绝启动
    };

    struct Capabilities {
        bool supports_close_today = true;   ///< 支持平今仓（close today, SHFE/INE）
        bool supports_l2_orderflow = false; ///< 支持 Level 2 逐笔行情（orderflow data）
        bool supports_ioc_fok = true;       ///< 支持 IOC/FOK 时效指令（time-in-force）
    };
    struct QueryControlStats {
        uint64_t query_pos_throttled = 0;      ///< 持仓查询被节流次数（throttled count）
        uint64_t query_pos_inflight_skip = 0;  ///< 持仓查询在途时未发送 ReqQry* 的触发次数
        uint64_t query_pos_dirty_merged = 0;   ///< 持仓查询创建 dirty retry 补发窗口的次数
    };

    // ── 回调类型 ──────────────────────────────────────────────
    /// @note on_order_update / on_trade 第二参 `XtLatencyEligibility` 仅供观测面
    ///       gate latency tracker feed（延迟重放 / 本地合成终态 kIneligible 不喂
    ///       tracker，防假尾延迟）；业务消费者必须忽略——精确二参（非默认参）强制
    ///       编译器扫清全部发射点与 binder 转发面。
    using OrderUpdateHandler = std::function<void(const XtOrderExecUpdate&, XtLatencyEligibility)>;
    using TradeHandler = std::function<void(const XtTradeRecord&, XtLatencyEligibility)>;
    using ReadyHandler = std::function<void()>;
    using FrontHandler = std::function<void(bool connected, int reason)>;
    struct DailySettlementDelta {
        double realized_delta = 0.0; ///< 日结已实现盈亏差额（realized PnL delta）
        double fee = 0.0;            ///< 日结手续费（commission）
        uint32_t trading_day = 0;    ///< 所属交易日（trading day, YYYYMMDD）
    };
    struct CashTransferDelta {
        double amount = 0.0;      ///< 转账金额（正=入金, 负=出金; transfer amount）
        uint32_t trading_day = 0; ///< 所属交易日（trading day, YYYYMMDD）
        uint64_t serial = 0;      ///< 流水号（serial number for dedup）
    };
    using DailySettlementHandler = std::function<void(const DailySettlementDelta&)>;
    using CashTransferHandler = std::function<void(const CashTransferDelta&)>;

    /// gateway callback 使用的 PositionSnapshot alias，等价于 ::XtPositionSnapshot。
    using PositionSnapshot = ::XtPositionSnapshot;
    struct AccountSnapshot {
        double balance = 0.0;          ///< 账户余额：binance = wallet 口径（不含 uPnL）；CTP = 动态权益 Balance
        double available = 0.0;        ///< 可用资金（available margin）
        double margin = 0.0;           ///< 占用保证金（used margin）
        double frozen_margin = 0.0;    ///< 冻结保证金（frozen margin, 挂单占用）
        double frozen_cash = 0.0;      ///< 冻结手续费（frozen commission）
        double commission = 0.0;       ///< 当日手续费累计（accumulated commission）
        double close_profit = 0.0;     ///< 平仓盈亏（realized close PnL）
        double position_profit = 0.0;  ///< 持仓浮盈（unrealized position PnL）
        double pre_balance = 0.0;      ///< 上日结存（previous day balance）
        double deposit = 0.0;          ///< 当日入金（deposit）
        double withdraw = 0.0;         ///< 当日出金（withdraw）
    };
    using AccountHandler = std::function<void(const AccountSnapshot&)>;
    using PositionHandler = std::function<void(const std::vector<PositionSnapshot>&)>;
    /// @brief 恢复阶段完整 broker 持仓快照回调（position-specific recovery provenance）。
    /// @param positions 本轮恢复查询得到的全量快照；空 batch 表示 broker 权威空仓。
    /// @param is_cold_start true=进程首次 recovery，false=warm reconnect。
    /// @note `positions` 仅在同步 callback 调用期间有效；异步 consumer 必须在返回前复制入队。
    ///       producer 对同一 recovery batch 只能调用本回调或 legacy `on_position` 之一，
    ///       不得双投递。Optional；未绑定时 venue 可回退到 `on_position` 保持交付兼容，
    ///       但 legacy fallback 不携带 recovery provenance。
    using RecoveryPositionHandler =
        std::function<void(const std::vector<PositionSnapshot>& positions, bool is_cold_start)>;
    /// @brief broker venue-truth overlay 回调（available/maint/total/per_symbol）。
    /// @note 与 on_account/on_position 正交：不进 account_store reducer，供 read-side
    ///       truth source 做 store-vs-broker divergence 校验。Optional；nullptr 时不调。
    using BrokerVenueTruthHandler = std::function<void(const BrokerVenueTruth&)>;
    /// @brief Raw transport ACK 通知（dedup 之前无条件触发）。
    /// @param local_id 订单本地 id
    /// @param ack_us   transport 层 ACK 到达的 steady µs
    /// @note 区别于 `on_order_update`：后者是 first-of-arrival（UDS-first dedup
    ///       后），用于策略推进；本回调是**原始** transport ACK 时刻，仅供
    ///       latency tracker 单独统计 trade_ack（不经 UDS-first 影响），用于
    ///       Grafana 上对比 effective vs trade_ack。
    /// @note Optional callback；nullptr 时 wrapper 不调用。
    using TransportAckHandler = std::function<void(uint64_t local_id, uint64_t ack_us)>;
    /// @brief Raw UDS 到达通知（dedup 之前无条件触发，与 TransportAckHandler 配对）。
    /// @note 让 latency tracker 用物理时刻（min UDS / ACK）算 effective_ack，
    ///       而非 callback chain 触发时刻。Optional callback；nullptr 时不调用。
    using UdsArrivalHandler = std::function<void(uint64_t local_id, uint64_t uds_ts_us)>;
    /// @brief Raw transport ACK 不会到达的通知（transport timeout / disconnect）。
    /// @note 仅供 latency tracker 抑制 synthetic reconcile update fallback；不表示
    ///       交易所拒绝，也不应进入 raw trade_ack histogram。
    using TransportAckUnavailableHandler = std::function<void(uint64_t local_id, uint64_t mark_us)>;
    /// @brief Recovery 反向灌 alive in-flight orders 通知。
    /// @details
    /// batch 表示 gateway 在 cold-start 或 warm reconnect 后，从 exchange 当前
    /// 状态重建出的仍活跃 in-flight orders。`orders` 是同一 recovery 批次快照；
    /// `is_cold_start` 标记进程冷启/首次 recovery 与 warm reconnect。
    /// Optional callback；nullptr 时不调用。
    using ReverseSyncBatchHandler =
        std::function<void(const std::vector<AliveInflightOrder>& orders, bool is_cold_start)>;
    /// @par 线程契约（Thread contract）
    /// 回调的调用线程因 venue 实现而异，consumer 按「任意线程（含调用者自身线程
    /// 同步重入）」对待：
    ///   - CTP：SDK SPI 回调线程同步进入 + recovery / query-timer 后台线程
    ///     + submit/cancel 调用线程（成功路径同步发 Submitted 等回调）；
    ///   - Binance：UDS ws 线程、transport 线程、reconcile 冷路径 worker 多线程并发
    ///     + submit 调用线程（fail-closed 合成 Rejected 同步触发）；
    ///   - XTP：SDK 工作线程。
    /// handler 只做轻量入队或 O(1) 工作，不得阻塞等待网关自身，也不得假设自己
    /// 不会在 submit 调用栈内被重入（跨 submit 持非重入锁是死锁面）。
    /// 绑定必须在 start() 前完成；运行中 rebind 不安全（实现层多为无锁读取）。
    struct Callbacks {
        OrderUpdateHandler on_order_update;          ///< 订单状态变更回调（order state update）
        TradeHandler on_trade;                       ///< 成交回调（trade/fill）
        AccountHandler on_account;                   ///< 资金快照回调（account snapshot）
        PositionHandler on_position;                 ///< 持仓快照回调（position snapshot）
        BrokerVenueTruthHandler on_broker_account_truth; ///< broker venue-truth overlay（read-side，非 reducer）
        DailySettlementHandler on_daily_settlement;  ///< 日结差额回调（daily settlement delta）
        CashTransferHandler on_cash_transfer;        ///< 出入金回调（cash transfer）
        ReadyHandler on_ready;                       ///< 通道就绪回调（gateway ready）
        FrontHandler on_front;                       ///< 前置连接/断连回调（front connection state）
        TransportAckHandler on_transport_ack;        ///< Raw transport ACK 时刻（dedup 前），仅 latency 监控用
        UdsArrivalHandler on_uds_arrival;            ///< Raw UDS 到达时刻（dedup 前），与 on_transport_ack 配对
        TransportAckUnavailableHandler on_transport_ack_unavailable; ///< raw transport ACK 不会到达，仅 latency 监控用
        ReverseSyncBatchHandler on_reverse_sync_batch; ///< Recovery 反向灌 alive in-flight orders
        RecoveryPositionHandler on_recovery_position; ///< Recovery 全量持仓快照；未绑定时 producer 可回退 on_position
    };

    // ── 通道生命周期 ──────────────────────────────────────────────
    /** @brief 初始化通道资源（不发起连接）。 */
    [[nodiscard]] virtual bool init(const Config& cfg) = 0;
    /** @brief 启动连接与登录流程。 */
    [[nodiscard]] virtual bool start() = 0;
    /** @brief 停止并释放通道资源。 */
    virtual void stop() = 0;

    // ── 交易接口 ──────────────────────────────────────────────
    // 边界约定：price 与 qty 均走 fp（统一执行精度），由实现层负责转换为柜台协议格式。
    //
    // `exec_flags` 为正交 bitflag（ExecFlag::{ReduceOnly, PostOnly, Hidden}），
    // 各 provider 支持度不同：CTP 期货通常忽略所有 flag；binance perp 映射
    // `ReduceOnly` → reduceOnly=true、`PostOnly` → timeInForce=GTX。
    // 调用方必须显式传参（`exec_none()` 或 `with_flag(..., ExecFlag::...)`）。
    //
    // submit_order 返回值契约（跨 venue 不一致，调用方必须两种模式都处理）：
    //   - CTP / XTP：本地校验失败或 SDK 发送失败 → 返回 0，且不会有任何回调；
    //     非 0 仅表示请求已交给柜台 SDK，后续状态经 on_order_update 交付。
    //   - Binance：恒返回非 0 local_id（不存在返 0 路径，无本地参数校验）；
    //     唯一本地 fail-closed（venue filters 未加载）合成终态 Rejected 经
    //     on_order_update 交付（ledger 已注册，路由与清理跟真实拒单一致），
    //     其余坏参数直送 venue 由 venue 拒；transport 未启动（pre-start 边界，
    //     生产不可达）时同样返非 0 且 ledger 已注册但无任何回调——与 cancel
    //     pre-start 对称，同属「唯一可靠信号是回调终态」的静默边界。
    // 「返回非 0」不可解读为「已提交到 venue」；唯一可靠信号是回调终态。
    // @warning 回调可能在 submit_order 返回前、于调用线程同一调用栈内同步触发
    //          （CTP 成功路径同步发 Submitted；binance fail-closed 同步发 Rejected）。
    //          调用方不得假设「回调 happens-after submit_order 返回」——先登记再消费
    //          回调的状态必须在 submit 前就绪。
    [[nodiscard]] virtual uint64_t submit_order(const char* code,
                                  bool is_buy,
                                  int64_t price_fp,
                                  int64_t qty_fp,
                                  XtTimeInForce tif,
                                  const char* user_tag,
                                  XtOffset close_offset,
                                  ExecFlags exec_flags) = 0;
    /// @brief 撤单请求。
    /// @return 仅表示「网关是否接受并发出本地请求」，不保证 venue 收到或撤成：
    ///   CTP = 撤单目标存在且 ReqOrderAction 发送成功（api 不可用 / 无目标 / 已终态
    ///   → false）；XTP = SDK CancelOrder 返回非 0；Binance = 找到 local_id 并已入队
    ///   transport（transport 层失败由内部重试 / reconcile 收敛，不反映在返回值）。
    /// @note 撤单结果以 on_order_update 终态为准；对已终态订单撤单 venue 侧幂等安全
    ///       （不产生重复撤单）。CTP 对「本地已终态仍被 cancel」的 id 会再发一条
    ///       合成 Canceled on_order_update（幽灵单自愈探针，真实终态可能是 Filled），
    ///       由三方幂等（book tombstone / account 未知 id no-op / 策略终态重入）吸收。
    virtual bool cancel_order(uint64_t local_id) = 0;

    // ── 恢复控制 ──────────────────────────────────────────────
    /// @brief 当前是否处于恢复中。
    /// @warning 三个实现的时间语义不同，调用方只能按「TD 暂不可用于新指令 →
    ///          跳过 / 下一帧重试」使用，不得假设它等价于「进程启动期」：
    ///   - CTP：登录后 recovery 查询窗口（settlement/account/position/order/trade
    ///     查询全部完成前为 true）——启动期语义，秒到分钟级；
    ///   - Binance：仅 reconcile resync 发布仓位快照的瞬时窗口为 true（运行期
    ///     周期性出现，亚秒级）；startup bootstrap 期间为 false；
    ///   - XTP：恒 false（登录即用，无 recovery 窗口概念）。
    /// @note 恢复开关 `Config::recovery_enabled` 仅约束 CTP/XTP 的登录后 recovery；
    ///       binance 忽略该开关，resync 瞬窗由 reconcile loop 内部驱动。
    [[nodiscard]] virtual bool is_recovering() const = 0;
    /// @brief 当前 TD 通道状态（state-first 语义）。
    /// @warning `TdState::Recovering` 会在回调边界被即时 stamp 进事件流并改变
    ///          记账语义（XtHftChannels 以 state() stamp trade / 普通 position；
    ///          recovery position 由 on_recovery_position 显式携带来源，不反查 state()）：
    ///   - fill：Recovering 态 trade 被 XtHftEngine::on_trade 丢弃不进记账主链，
    ///     由 recovery 后的 position snapshot / reverse-sync 收敛仓位；
    ///   - realized 归因：HFT/CTA 路径均 stamp has_realized = (state != Recovering)；
    ///   - route status：sidecar 查不到归属且 Recovering → fallback_recovery
    ///     （已登记订单仍正常 routed/operator）。
    ///   Binance 的 Recovering 是运行期瞬时窗口：resync 窗口内到达的 fill 依赖
    ///   gateway 内部 ResyncFillGate 暂存并按 fence 重放保证不丢账。新增 state()
    ///   消费方必须兼容该瞬态语义，不得假设 Recovering 只发生在启动期。
    [[nodiscard]] virtual TdState state() const {
        return is_recovering() ? TdState::Recovering : TdState::Live;
    }

    /// @brief 当前交易日（YYYYMMDD，venue 权威口径）；0 = 未知/不提供。
    /// @note CTP 登录应答填充（TradingDay）；其它 venue 默认 0，caller 须对 0
    ///       做降级处理（如跳过新鲜度校验）。
    [[nodiscard]] virtual uint32_t trading_day() const { return 0; }

    // ── 通道能力 ──────────────────────────────────────────────
    [[nodiscard]] virtual Capabilities capabilities() const = 0;
    [[nodiscard]] virtual QueryControlStats query_control_stats() const = 0;

    // ── 回调绑定 ──────────────────────────────────────────────
    /// @brief 绑定订单/成交/资金/持仓/就绪回调。
    /// @pre 必须在 start() 之前完成；运行中 rebind 不安全（见 Callbacks 线程契约）。
    virtual void bind_callbacks(Callbacks callbacks) = 0;

    // ── 健康检查（冷路径，与 IMdGateway::check_health 对称） ──────────
    /// @brief 周期性健康检查。
    [[nodiscard]] virtual bool check_health(uint32_t now_ms) = 0;

    // ── venue 终态查询（append-only：放 class 末尾保 vtable ABI 不 shift 既有 slot） ──
    /// @brief 该 local_id 是否已在本网关终态集（lost-terminal reconcile 探测）。
    /// @note 默认 false；CTP/XTP 查各自 tracker 终态集；binance 查 BinanceOrderLedger
    ///       （in-flight 证据优先返回 false，详见其 is_order_terminal 契约）。
    ///       供引擎周期对账 book-open 与 venue-terminal。
    /// @warning capability 语义：false 可能是「非终态」也可能是「不支持/无记录」，
    ///          跨 venue 通用代码不得据 false 做任何推断。
    [[nodiscard]] virtual bool is_order_terminal(uint64_t /*local_id*/) const { return false; }

    /// @brief 携带已采样 intent_trace_id 的报单入口（append-only vtable slot）。
    /// @note 默认实现忽略 trace id，保持非 tracing venue 行为不变；需要 wire trace
    ///       的 TD 实现可 override 并把 trace id 写入 transport request。
    [[nodiscard]] virtual uint64_t submit_order_with_intent_trace(const char* code,
                                  bool is_buy,
                                  int64_t price_fp,
                                  int64_t qty_fp,
                                  XtTimeInForce tif,
                                  const char* user_tag,
                                  XtOffset close_offset,
                                  ExecFlags exec_flags,
                                  uint64_t intent_trace_id) {
        (void)intent_trace_id;
        return submit_order(code, is_buy, price_fp, qty_fp, tif, user_tag, close_offset, exec_flags);
    }

    // ── 停机对账 fresh 钱包余额（append-only vtable slot）────────────────
    /// @brief 同步拉一笔 fresh venue 钱包余额（停机对账直比用）。
    /// @param out_balance 成功时写入钱包余额（venue 计价币种）。binance =
    ///        `/fapi/v2/account` totalWalletBalance（wallet 口径，不含未实现盈亏，
    ///        与 runtime cash 账本及其全部锚源同口径）。
    /// @return false = venue 不支持 / REST 不可用 / 拉取解析失败；调用点回退
    ///         最后 anchor 缓存。默认不支持（CTP/XTP 停机时柜台查询通道多已
    ///         下线，走缓存回退）。
    /// @pre 仅在 gateway stop() 完成、内部线程全部 join 后的停机冷路径调用
    ///      （阻塞式 REST，与运行期 REST 用户无并发）。
    /// @warning capability 语义同 is_order_terminal：false 不携带任何账务信息。
    [[nodiscard]] virtual bool query_wallet_balance_sync(double& /*out_balance*/) { return false; }

    // ── venue 终态 payload 查询（append-only vtable slot）────────────────
    /// @brief 终态记录按值出参（is_order_terminal 的 payload 版）。
    /// @return true = 该 local_id 已终态且 payload 可信（out 填 status/filled）；
    ///         false = 非终态 / 无记录 / venue 不提供 payload。false 不携带任何
    ///         状态信息，调用方可退回 is_order_terminal 布尔探测。
    /// @note 默认 false；CTP 读交付投影 terminal_record；binance 读订单账本
    ///       （仅记录自身 status 为终态值时给 payload，见 BinanceOrderLedger）。
    [[nodiscard]] virtual bool lookup_terminal(uint64_t /*local_id*/,
                                               XtOrderTerminalRecord& /*out*/) const {
        return false;
    }

    // ── 交付投影 open 集合导出（append-only vtable slot；停机对账观测用）────
    /// @brief 导出 gateway 侧「已登记未终态」订单 id（追加写入 out）。
    /// @return false = venue 不提供该视图（out 不动）；true = 已追加（可为空）。
    /// @note 冷路径（停机对账差集观测 / 诊断），不作为交易决策输入。
    [[nodiscard]] virtual bool collect_open_delivery_ids(std::vector<uint64_t>& /*out*/) const {
        return false;
    }
};
