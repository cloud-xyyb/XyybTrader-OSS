#pragma once

#include "TraderBinanceUGateway.h"
#include "td/BinanceOrderLedger.h"  // 订单身份/状态唯一权威账本（单表 + 单锁）。
#include "td/BinanceReconciler.h"   // reconcile 机制组件 + TrackedSymbolSet + lat_now_us。
#include "td/BinanceTdParse.h"  // AccountTruthDetail / GapFillOrderAttribution / parse 自由函数。

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "rest/BinanceRestClient.h"
#include "BinanceTdCfg.h"
#include "transport/BinanceUserDataStream.h"
#include "transport/ITradeTransport.h"
#include "api/StrategySid.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_live/gateway/binance/td/EffectiveSignalDedup.h"
#include "xt_core/order/PerVenueLocalIdGenerator.h"

namespace binance_u {

/// @brief Binance U 本位 TD 网关的 PImpl 实现。
///
/// 外层 TraderBinanceUGateway 只是接口壳，真正的运行状态都在 Impl 里：
/// - 把下单/撤单交给 transport 发出去，接收 UDS 回报，跑 REST 维护任务
/// - 每笔订单是谁的、走到什么状态了，统一记在 ledger_ 里
/// - 本地跟交易所对不上时，由 reconciler_ 负责拉回一致
///
/// 多个线程会同时进出这个类，具体哪把锁保护什么见各成员分组注释。
class TraderBinanceUGateway::Impl final {
public:
    Impl()
        : reconciler_(rest, uds, ledger_, cb, readiness_gate_, tracked_symbols_,
                      BinanceReconciler::ImplHooks{
                          /*emit_through_dedup=*/
                          [this](const XtOrderExecUpdate& upd,
                                 EffectiveSignalDedup::Source src,
                                 XtLatencyEligibility elig) {
                              emit_through_dedup(upd, src, elig);
                          },
                          /*queue_reconcile_resync=*/
                          [this] { return queue_reconcile_resync_if_absent_(); },
                          /*wait_shutdown=*/
                          [this](int64_t wait_ms) {
                              return wait_shutdown_interruptible_(wait_ms);
                          },
                          /*erase_dedup_for_local_ids=*/
                          [this](const std::vector<uint64_t>& ids) {
                              std::lock_guard<std::mutex> lk(signal_state_mu_);
                              signal_dedup_.erase_for_local_ids(ids);
                          },
                          /*emit_ready_if_td_readiness_satisfied=*/
                          [this] { emit_ready_if_td_readiness_satisfied(); }}),
          // 无持久化占位；configure() 按 cfg.local_id_instance 重建为真正的
          // 发号域（configure 之前不存在 submit 路径）。
          local_id_gen_(std::in_place, std::string{}) {}

    /// @brief cold worker 的维护任务描述符。
    struct ColdWorkerTask {
        enum Kind {
            kTimeSync,           ///< 执行一次 Binance server time 同步。
            kBootstrapSnapshot,  ///< 执行一次启动期 positionRisk/account 快照拉取。
            kRefreshSnapshot,    ///< 执行一次 keepalive account truth 刷新。
            kReconcileResync,    ///< 执行一次 gap-fill + positionRisk recovery。
            kPlainCloseFallback, ///< 执行一次 reduceOnly close reject 后的 plain-close fallback。
        };
        Kind kind = kTimeSync;

        // kPlainCloseFallback payload；其他 kind 下保持默认 inert。
        std::string fb_symbol;         ///< fallback 下单 symbol。
        bool        fb_is_buy   = false; ///< fallback 下单方向。
        int64_t     fb_price_fp = 0;   ///< fallback 价格，scale 1e6。
        int64_t     fb_qty_fp   = 0;   ///< fallback 数量，scale 1e6。
    };

    /// @brief 注入 Binance TD 专用配置；会重置 readiness gate，等待下一轮 bootstrap。
    ///
    /// local_id 发号器在此按 cfg.local_id_instance 重建——非空 → per-instance
    /// tag 域（multibook 跨进程唯一）；空 → legacy venue 共享域（未配 instance 的
    /// 直连调用方保持旧行为）。configure 在 init/start 之前单线程调用，此时尚未
    /// 发号，整体重建无并发窗口。claim/flock 失败（跨实例唯一性无法验证或同名
    /// 双活）→ 发号器置空 + 记错误，init() fail-fast 拒起，绝不进入可发号状态。
    void configure(const BinanceTdCfg& cfg) {
        td_cfg = cfg;
        using Gen = xt_core::local_id::PerVenueLocalIdGenerator;
        local_id_gen_.reset();
        local_id_gen_error_.clear();
        try {
            if (!cfg.local_id_instance.empty()) {
                local_id_gen_.emplace(
                    Gen::InstanceIdentity{"binance_u", cfg.local_id_instance});
            } else {
                local_id_gen_.emplace(Gen::default_state_path("binance_u"));
            }
        } catch (const std::exception& e) {
            local_id_gen_error_ = e.what();
        }
        reset_td_readiness();
    }

    /// @brief 初始化 REST client，并验证 signed REST、listenKey 与 TD transport 可用性。
    /// @note Binance 专属配置由 configure() 注入；common cfg 仅保留统一接口签名。
    /// @return true = 配置/认证/网络/transport 校验通过；false = 网关不可启动。
    bool init(const ITradeGateway::Config& cfg);

    /// @brief 启动 cold worker、UDS 和 keepalive；投递 time sync + bootstrap snapshot。
    /// @return 已运行时返回 true；正常启动不等待账户 readiness 达成。
    bool start();

    /// @brief 停止 TD transport、UDS、keepalive 和 cold worker；唤醒等待点后 join 线程。
    void stop();
    /// @brief 停机冷路径 fresh 钱包余额；rest 活到 ~Impl，stop() 后单线程独占。
    bool query_wallet_balance_sync(double& out_balance);

    /// @brief 返回 TD readiness gate 当前快照，用于 runner 判断交易通道是否可用。
    xt_live::gateway::TdReadinessReport td_readiness_report() const {
        return readiness_gate_.report();
    }

    // ── URL / id 解析辅助 ──
    /// @brief 从 client_order_id `ct_<local_id>_<nonce>` 解析 local_id，格式不符返回 0。
    uint64_t parse_local_id_from_client_order_id(std::string_view s);

    // ── health / 回调注入 ──
    /// @brief 由 runner health loop 调用；只投递 time sync，并触发 transport/UDS 自检。
    /// @return false 仅表示 UDS health check 失败；WS trade transport 断连走诊断与自重连。
    bool check_health(uint32_t now_ms);

    /// @brief 注入上层回调；ACK、UDS、reconcile 和 bootstrap 路径都通过该回调出事件。
    void bind_callbacks(ITradeGateway::Callbacks cb_in) { cb = std::move(cb_in); }

    // ── submit / cancel 入队 (定义在 TraderBinanceUGatewayCallbacks.cpp) ──
    /// @brief 构造 Binance clientOrderId，登记本地映射后提交给当前 TD transport。
    /// @return 本地 local_id；即使 transport 异步失败也先返回，失败由 ACK/reconcile 回报。
    uint64_t submit_order_inq(const char* code, bool is_buy,
                              int64_t price_fp, int64_t qty_fp,
                              XtTimeInForce tif, const char* user_tag,
                              XtOffset close_offset, ExecFlags exec_flags,
                              uint64_t intent_trace_id);

    /// @brief 按 local_id 找回 clientOrderId/symbol 并异步撤单。
    /// @return false 表示本进程没有该 local_id 映射，未向 transport 发撤单。
    bool cancel_order_inq(uint64_t local_id);

private:
    // ── transport setup / cold worker (定义在 TraderBinanceUGatewayInit.cpp) ──
    /// @brief 根据 td_cfg.transport 创建 REST 或 WS TD transport。
    bool create_trade_transport();

    /// @brief 观测 WS TD transport 断连时长；只打诊断，不把 gateway health 置 false。
    void check_trade_transport_health(uint32_t now_ms);

    /// @brief 串行处理 REST 维护任务；submit/cancel 热路径不在此线程执行。
    void cold_worker_loop();

    /// @brief 重置 TD readiness gate 与 bootstrap retry 状态。
    void reset_td_readiness();

    /// @brief readiness 满足时只发一次 on_ready；失败时记录阻塞阶段。
    void emit_ready_if_td_readiness_satisfied();

    /// @brief 选择本轮 keepalive 要投递的 cold worker 任务。
    /// @return 已 ready 返回 `kRefreshSnapshot`；未 ready 且退避到期返回 `kBootstrapSnapshot`；
    ///         未 ready 且退避未到期返回 `std::nullopt`。
    std::optional<ColdWorkerTask::Kind> keepalive_pick_(int64_t now_ms);

    /// @brief 可被 stop() 打断的等待（shutdown_cv_）；返回 true = shutdown 已触发。
    bool wait_shutdown_interruptible_(int64_t wait_ms) {
        std::unique_lock<std::mutex> lk(shutdown_mu_);
        return shutdown_cv_.wait_for(lk, std::chrono::milliseconds(wait_ms),
            [this] { return !running.load(std::memory_order_acquire); });
    }

public:
    // cold_worker_thread 只处理维护路径；submit/cancel 热路径走 td_transport。
    std::deque<ColdWorkerTask> cold_worker_tasks;  ///< cold worker 任务队列，由 cold_worker_mu / cold_worker_cv 保护。

private:
    // ── ACK/UDS 回报状态管理 (TraderBinanceUGatewayCallbacks.cpp) ──
    /// @brief ACK/UDS 双源 effective order update 去重后转发。
    /// @pre 调用方不得持有 ledger 锁（X1）；本函数内部只获取 signal_state_mu_。
    void emit_through_dedup(const XtOrderExecUpdate& upd,
                            EffectiveSignalDedup::Source src,
                            XtLatencyEligibility elig);

    /// @brief UDS order update 入口；终态会先销本地 in-flight，再经 dedup/fence emit。
    void on_uds_order_update(const XtOrderExecUpdate& upd);

    /// @brief TD transport ACK 入口；ledger 终态判定/补齐后在锁外走 dedup emit
    ///        与 metrics/fallback 善后。
    void on_transport_ack(const TransportAck& ack,
                          const TransportLatencyTrace& trace);

    /// @brief kRejected ack 的善后：reject 日志去重 + metrics + reconcile-resync + plain-close fallback。
    /// @pre 调用方已经由 ledger 完成终态登记且 upd.status=Rejected；本函数只读 upd。
    void handle_rejected_ack(const TransportAck& ack, const XtOrderExecUpdate& upd);

public:  // ── reconcile 队列入口 + reconciler 转发 ──
    /// @brief kReconcileResync 入队；cold_worker_tasks 已有同类则跳过（队列合并）。
    /// @return true = 本次真入队；false = 已排队，调用方不应再做随附副作用
    ///         （throttle 打点 / UDS reconnect）。四个触发源（close-reject /
    ///         unattributed-fill / ghost-FILLED / request_resnap）共用本入口；
    ///         仅 request_resnap 附带 UDS reconnect。
    bool queue_reconcile_resync_if_absent_();

    /// @brief Reconcile resnap：触发 UDS reconnect，并排队 gap-fill + positionRisk recovery。
    /// @note 异步入队后立即返回，不阻塞 reconcile 线程。
    void request_resnap();

    /// @brief UDS/gap-fill fill 的唯一 emit 入口；转发到 reconciler 的 resync 栅栏。
    void forward_uds_trade(const XtTradeRecord& tr, XtLatencyEligibility elig) {
        reconciler_.forward_uds_trade(tr, elig);
    }

    /// @brief UDS ACCOUNT_UPDATE 非 ORDER 原因的 absolute position 转发（栅栏窗口内丢弃）。
    void forward_uds_position(const std::vector<XtPositionSnapshot>& positions) {
        reconciler_.forward_uds_position(positions);
    }

    /// @brief reconcile resync emit 仓位快照期间短暂为 true（recovery reset+set 语义）。
    bool is_recovering() const { return reconciler_.is_recovering(); }

#ifdef XT_BUILD_TESTS
public:  // ── 单测入口：仅供离线单测驱动私有 ACK / UDS / reconcile 路径 ──
    void test_inject_transport_ack(const TransportAck& ack,
                                   const TransportLatencyTrace& trace) {
        on_transport_ack(ack, trace);
    }

    void test_inject_uds_order(const XtOrderExecUpdate& upd) {
        on_uds_order_update(upd);
    }

    void test_emit_reconcile_ghost(const XtOrderExecUpdate& upd) {
        // 复刻 ghost 主循环终态段：ledger 决策（锁内销账）→ 锁外 emit。
        XtOrderExecUpdate u = upd;
        if (ledger_.on_reconcile_terminal(
                u.id, BinanceOrderLedger::ReconcileTerminal::kVenueTerminal, u).action ==
            BinanceOrderLedger::TerminalDecision::Action::kEmit) {
            // kVenueTerminal 复刻 ghost 主循环 venue 查单转译路径 → kEligible。
            reconciler_.emit_reconcile_terminal_update(u, XtLatencyEligibility::kEligible);
        }
    }

    bool test_emit_blank_sys_id_reject(uint64_t local_id) {
        return reconciler_.synthesize_blank_sys_id_reject(local_id);
    }

	    std::size_t test_cold_worker_task_count() {
	        std::lock_guard<std::mutex> lk(cold_worker_mu);
	        return cold_worker_tasks.size();
	    }

	    uint64_t test_trace_id_for_local(uint64_t local_id) {
	        return ledger_.trace_id_for(local_id);
	    }

	    void test_forward_uds_trade(const XtTradeRecord& tr) {
	        forward_uds_trade(tr, XtLatencyEligibility::kEligible);
	    }

    void test_bootstrap_snapshot() { reconciler_.bootstrap_snapshot(); }

    void test_request_resnap() { request_resnap(); }

    /// @brief 单测注入 openOrders JSON 字符串，不实际打 REST。
    void test_rebuild_from_open_orders_body(const std::string& body, bool is_cold_start) {
        (void)reconciler_.rebuild_from_open_orders_body(body, is_cold_start);
    }

    /// @brief 栅栏测试入口：手动开/关 ResyncFillGate（close = 析构 → 失败路径 flush：
    ///        fill 按到达序、终态排在全部 fill 之后）。
    void* test_resync_gate_open() {
        return new BinanceReconciler::ResyncFillGate(reconciler_);
    }
    void  test_resync_gate_close(void* gate) {
        delete static_cast<BinanceReconciler::ResyncFillGate*>(gate);
    }
    std::optional<ColdWorkerTask::Kind> test_keepalive_pick(int64_t now_ms) {
        return keepalive_pick_(now_ms);
    }

    void test_reset_td_readiness() { reset_td_readiness(); }

    /// @brief ghost fill 补发回归装配：只装 uds 槽位并绑生产 forward 链
    ///        （order/trade 走 on_uds_order_update / forward_uds_trade），
    ///        不 start()、不启 cold worker——task 计数断言保持可用。
    ///        seed 占 dedup 槽 tid=1，Fake REST 应返回 id>=2。
    /// @pre 已先经 trader_binance_test_set_rest 注入 Fake REST（uds 持 rest 引用）。
    void test_install_uds_prod_wired(const std::string& tracked_symbol) {
        BinanceUserDataStream::Config ucfg;
        uds = std::make_unique<BinanceUserDataStream>(ucfg, *rest);
        // 生产同款 fill 去重委托；seed 在绑定后注入才占 ledger 去重槽位。
        uds->set_fill_dedup([this](uint64_t order_id, std::string_view symbol,
                                   int64_t tid) {
            return ledger_.note_fill_seen(order_id, symbol, tid);
        });
        XtTradeRecord seed;
        seed.trade_id = "1";
        seed.code = tracked_symbol;
        (void)uds->inject_gap_fill_trade(seed);  // 登记 tracked symbol；此时回调未绑定
        uds->bind_callbacks(
            [this](const XtOrderExecUpdate& upd) { this->on_uds_order_update(upd); },
            [this](const XtTradeRecord& tr, XtLatencyEligibility elig) { forward_uds_trade(tr, elig); },
            {}, {},
            [this](std::string_view s) { return parse_local_id_from_client_order_id(s); });
    }

    /// @brief stop-while-gap-fill-inflight 回归装配：绕过 init()/start()（不碰真网络），
    ///        只装配 stop() 排序契约要素 — readiness 复位、uds 槽位、running 标志、
    ///        cold worker 线程。uds 不 start()：无 WS/io 线程，stop() 对其幂等 no-op。
    ///        seed 注入先于 bind，使 on_gap_fill_trade 只观测 resync 注入的 trade（正向
    ///        oracle）；seed 占用 dedup 槽位 tid=1，Fake REST 应返回 id>=2。
    /// @pre 已先经 trader_binance_test_set_rest 注入 Fake REST（uds 持 rest 引用）。
    void test_boot_cold_worker_with_uds(
        const std::string& tracked_symbol,
        std::function<void(const XtTradeRecord&, XtLatencyEligibility)> on_gap_fill_trade) {
        reset_td_readiness();
        BinanceUserDataStream::Config ucfg;
        uds = std::make_unique<BinanceUserDataStream>(ucfg, *rest);
        // 生产同款 fill 去重委托（seed 占用 ledger 去重槽位的前提）。
        uds->set_fill_dedup([this](uint64_t order_id, std::string_view symbol,
                                   int64_t tid) {
            return ledger_.note_fill_seen(order_id, symbol, tid);
        });
        XtTradeRecord seed;
        seed.trade_id = "1";
        seed.code = tracked_symbol;
        (void)uds->inject_gap_fill_trade(seed);  // 登记 tracked symbol；此时回调未绑定
        uds->bind_callbacks({}, std::move(on_gap_fill_trade), {}, {}, {});
        running.store(true, std::memory_order_release);
        cold_worker_thread = std::thread([this] { cold_worker_loop(); });
    }

#endif  // XT_BUILD_TESTS

private:
    // ACK/UDS effective signal 去重窗口；不要与 ledger 锁同时持有（X1）。
    EffectiveSignalDedup signal_dedup_;
    mutable std::mutex signal_state_mu_;

public:
    // 以下 public 成员仅服务 split TU 共享实现状态，不是外部 API。

    // ── 配置与外部回调 ─────────────────────────────────────────────
    BinanceTdCfg td_cfg;                       ///< Binance 专用 TD 配置；configure() 写入。
    ITradeGateway::Callbacks cb;               ///< 上层事件回调；ACK/UDS/reconcile 路径调用。
    xt_live::gateway::TdReadinessGate readiness_gate_{"binance_u"}; ///< TD 启动 readiness 阶段门。
    std::atomic<bool> ready_emitted_{false};           ///< on_ready 已发（cold worker 写 / keepalive 线程读）

    // ── transport / UDS / worker 生命周期 ─────────────────────────
    std::unique_ptr<BinanceRestClient> rest;  ///< signed REST client；cold/reconcile/UDS listenKey 共用。
    std::unique_ptr<ITradeTransport> td_transport; ///< 当前下单 transport（REST 或 WS）。
    std::unique_ptr<BinanceUserDataStream> uds; ///< listenKey + UDS WS，负责订单/成交/账户推送。
    /// uds 指针访问互斥（defense-in-depth）：check_health 的解引用与 stop() 的
    /// uds.reset() 互斥；caller 串行时仍持锁，护跨线程测试调用与线程模型演进。
    /// 锁范围严格限于这两点——不得包住 uds->stop() 或 worker join（死锁面）。
    std::mutex uds_mu_;
    std::thread cold_worker_thread;   ///< 串行执行 cold_worker_tasks 中的 REST 维护任务。
    std::thread keepalive_thread;     ///< 周期 GET account/positionRisk 的维护线程。
    std::atomic<bool> running{false}; ///< stop 信号；cold worker 和 keepalive 共同观察。

    // ── reject / resync / fallback 节流状态（callback 写，日志/入队限流用）────
    int      last_reject_err_code_logged_ = 0;   ///< 最近完整打印的 reject err_code。
    uint64_t last_reject_log_us_          = 0;   ///< 最近完整 reject 日志时间，steady µs。
    uint64_t reject_suppressed_count_     = 0;   ///< 当前日志窗口内被抑制的 reject 数。
    uint64_t last_reject_resync_us_       = 0;   ///< 最近 reject-triggered resync 入队时间。
    uint64_t last_unattributed_resync_us_ = 0;   ///< 最近无法归因成交触发 resync 入队时间。
    uint64_t last_reject_fallback_us_     = 0;   ///< 最近 plain-close fallback 入队时间。
    uint64_t last_cancel_fail_log_us_     = 0;   ///< 最近完整 cancel-fail 日志时间，steady µs。
    uint64_t cancel_fail_suppressed_count_ = 0;  ///< 当前日志窗口内被抑制的 cancel-fail 数。
    static constexpr uint64_t kLatTraceLogSampleN = 100;  ///< BinanceLat kind=trace 采样率（1/N）。
    uint64_t lat_trace_log_seq_ = 0;             ///< BinanceLat kind=trace 采样序号（首条放行）。
    uint64_t lat_t0_log_seq_ = 0;                ///< BinanceLat kind=t0 采样序号（提交线程私有）。

    // ── cold worker 队列 ──────────────────────────────────────────
    std::mutex cold_worker_mu;           ///< 保护 cold_worker_tasks；producer 只入队，cold_worker_loop 单消费者出队。
    std::condition_variable cold_worker_cv;  ///< 唤醒 cold_worker_loop 处理 cold_worker_tasks 或 stop。

    // 独立 shutdown cv 覆盖非 queue worker 的 sleep 站点（keepalive 30s,
    // bootstrap/rebuild retry wait）。stop() notify_all 后线程不必等完整周期。
    std::mutex shutdown_mu_;       ///< 仅配合 shutdown_cv_ 等待/唤醒，不保护业务状态。
    std::condition_variable shutdown_cv_;  ///< 唤醒 keepalive 与 bootstrap/retry wait。

    // ── 订单身份/状态唯一权威账本 ───────────────────────────────────
    // 身份映射、pending 短窗、in-flight 状态与终态去重窗口统一在单锁账本内；
    // emit / tracked-symbol 播种由 caller 在 ledger 调用之后锁外进行（X1/X4）。
    BinanceOrderLedger ledger_;

    // ── REST 减负：tracked symbol 集（submit 播种 + reconciler 过滤共用）──
    TrackedSymbolSet tracked_symbols_;

    // ── reconcile 机制组件 ─────────────────────────────────────────
    // bootstrap/resync、snapshot-vs-stream 栅栏、ghost 正/反 sweep、gap-fill、
    // reverse-sync 与 blank-sys-id 归 reconciler；注入 rest/uds 所有权槽位、
    // ledger、cb、readiness gate、tracked 集与 ImplHooks（dedup/队列/等待/ready）。
    // recovery 语义、栅栏、bootstrap 退避与 ghost streak 状态随机制归组件所有。
    BinanceReconciler reconciler_;

    // ── ID / health counters ──────────────────────────────────────
    /// Binance local_id 生成器。ctor 先放无持久化占位，configure() 按
    /// local_id_instance 重建（per-instance tag 域或 legacy 共享域）；
    /// claim/flock 失败时为空且 local_id_gen_error_ 非空 → init() 拒起，
    /// submit 路径只在 init 成功后可达，可直接解引用。
    std::optional<xt_core::local_id::PerVenueLocalIdGenerator> local_id_gen_;
    std::string local_id_gen_error_;  ///< configure 期发号器重建失败原因；init() 消费。
    std::atomic<uint64_t> local_id_persist_fail_seen_{0};  ///< 已 warn 的 persist 失败计数，用于日志去重。
    std::atomic<uint64_t> nonce_ctr{0};  ///< client_order_id nonce，避免同 local_id 重试时冲突。
    std::atomic<uint32_t> last_time_sync_ms{0};  ///< 最近 time sync 投递时刻，时基为 check_health(now_ms)。
    std::atomic<uint32_t> transport_down_since_ms{0};  ///< WS trade transport 首次观测断连时刻；0 表示当前健康。
    std::atomic<uint32_t> transport_down_warn_ms{0};  ///< 最近一次 transport 断连诊断日志时刻。

};

}  // namespace binance_u
