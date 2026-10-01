#pragma once

/// @file BinanceReconciler.h
/// @brief Binance TD 的对账组件：把本地订单/资金/持仓和交易所拉回一致
///
/// - 启动时拉全量快照建立基线；
/// - 运行中发现对不上（幽灵单、漏 fill、仓位漂移）就用 REST 重新核对。
/// - REST 失败时跳过本轮

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nlohmann/json_fwd.hpp>  // get_position_risk_array_ / get_open_orders_array_ 返回类型

#include "api/ITradeGateway.h"
#include "td/BinanceTdParse.h"  // AccountTruthDetail / GapFillOrderAttribution
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_live/gateway/binance/td/EffectiveSignalDedup.h"
#include "xt_live/gateway/binance/td/ResyncFillReplayer.h"  // ResyncTerminal
#include "xt_live/gateway/common/TdReadinessGate.h"
#include "xt_util/time/XtSteadyClock.h"

namespace binance_u {

class BinanceOrderLedger;
class BinanceRestClient;
class BinanceUserDataStream;

/// @brief process-monotonic 微秒时间戳，供 TD split TU 共享同一 latency 域。
inline uint64_t lat_now_us() noexcept {
    return XtSteadyClock::steady_now_us();
}

/// @brief 「可能有仓位或成交」的 symbol 集合；REST 对账时只查这些 symbol 减负。
///
/// 对账靠「没登记过的 symbol 本地必定是零仓」这个前提工作——
/// 任何让本地仓位变非零的路径都必须先 note()，漏了会让对账把真仓当零仓抹掉。
/// insert-only，自带小锁；临界区内不获取其它锁。
class TrackedSymbolSet {
public:
    /// @brief 登记可能产生 fill/持仓的 symbol（submit / 反向灌 / bootstrap 播种）。
    void note(const std::string& symbol) {
        if (symbol.empty()) return;
        std::lock_guard<std::mutex> lk(mu_);
        symbols_.insert(symbol);
    }

    /// @brief 集合快照；空或超过调用方过滤上限时调用方退回全量拉取。
    std::vector<std::string> snapshot() const {
        std::lock_guard<std::mutex> lk(mu_);
        return {symbols_.begin(), symbols_.end()};
    }

    /// @brief 是否已登记；解析失败行的 tracked/other fail-closed 分类用。
    bool contains(const std::string& symbol) const {
        std::lock_guard<std::mutex> lk(mu_);
        return symbols_.count(symbol) > 0;
    }

private:
    mutable std::mutex mu_;
    std::unordered_set<std::string> symbols_;  ///< 有 submit/fill/持仓迹象的 symbol 集。
};

/// @brief Binance TD 对账的执行组件：启动快照、定期核对、幽灵单清理和漏单回补。
///
/// BinanceReconciler 跑在冷路径（cold worker / reconcile 线程），不占下单热路径。
/// 对账期间到达的 fill 经栅栏按时间分区重放，快照和实时流不会互相覆盖。
/// 实现分在 BinanceReconcilerSync.cpp 和 BinanceReconcilerSweep.cpp 两个文件。
class BinanceReconciler {
public:
    /// @brief reconciler 需要、但仍归 gateway Impl 管的能力回调集合。
    ///
    /// 字段顺序就是初始化顺序；Impl 构造时全部绑定，不可为空。
    struct ImplHooks {
        /// ACK/UDS effective 信号去重后转发（Impl 内部自持 signal_state_mu_）。
        /// 栅栏 flush 持 resync_gate_mu_ 调用——既存嵌套
        /// resync_gate_mu_→signal_state_mu_（by design，X2 允许）。
        /// 第三参 = latency eligibility（构造源盖章，仅 gate tracker feed）。
        std::function<void(const XtOrderExecUpdate&, EffectiveSignalDedup::Source,
                           XtLatencyEligibility)>
            emit_through_dedup;
        /// kReconcileResync 入队（cold worker 队列归 Impl）；返回 false = 已排队被合并。
        std::function<bool()> queue_reconcile_resync;
        /// 可被 stop() 打断的等待；返回 true = shutdown 已触发（running=false）。
        std::function<bool(int64_t wait_ms)> wait_shutdown;
        /// warm reconnect 清理 reverse-sync 回灌订单的 dedup 状态（Impl 内部自持 signal_state_mu_）。
        std::function<void(const std::vector<uint64_t>&)> erase_dedup_for_local_ids;
        /// bootstrap 末尾 readiness 全绿时幂等 emit on_ready。
        std::function<void()> emit_ready_if_td_readiness_satisfied;
    };

    /// @brief 依赖注入构造；全部引用归 gateway Impl 所有且生命周期覆盖本组件。
    /// @param rest  REST client 所有权槽位引用：init() 才创建、单测可整体替换
    ///              （trader_binance_test_set_rest），因此注入槽位而非裸指针。
    /// @param uds   UDS 所有权槽位引用（start() 创建）；gap-fill 依赖其 trade
    ///              去重窗口，resync 依赖其 tracked trade symbol 集。
    BinanceReconciler(std::unique_ptr<BinanceRestClient>& rest,
                      std::unique_ptr<BinanceUserDataStream>& uds,
                      BinanceOrderLedger& ledger,
                      ITradeGateway::Callbacks& cb,
                      xt_live::gateway::TdReadinessGate& readiness_gate,
                      TrackedSymbolSet& tracked_symbols,
                      ImplHooks hooks);

    BinanceReconciler(const BinanceReconciler&) = delete;
    BinanceReconciler& operator=(const BinanceReconciler&) = delete;

    // ── recovery 语义 / bootstrap 退避（Impl 生命周期路径读写）────────
    /// @brief reconcile resync emit 仓位快照期间短暂为 true（recovery reset+set 语义）。
    bool is_recovering() const {
        return reconcile_in_progress_.load(std::memory_order_acquire);
    }

    /// @brief 下次 bootstrap 重试最早时刻（lat_now_us/1000 时基）；keepalive 选择器读。
    int64_t bootstrap_next_retry_ms() const {
        return bootstrap_next_retry_ms_.load(std::memory_order_relaxed);
    }

    /// @brief 重置 bootstrap 指数退避；reset_td_readiness 调用。
    void reset_bootstrap_backoff() {
        bootstrap_attempts_.store(0, std::memory_order_relaxed);
        bootstrap_next_retry_ms_.store(0, std::memory_order_relaxed);
    }

    // ── cold worker 任务体（Impl cold_worker_loop 分发）──────────────
    /// @brief Start 时拉一次 positionRisk/account，建立初始账户与持仓 truth。
    /// @note Binance UDS 只推变动；启动期必须主动 snapshot 才能满足 TD readiness。
    /// @note 任一关键快照失败会标记 readiness 失败并返回，不 emit 空 truth。
    void bootstrap_snapshot();

    /// @brief 周期性 refresh：仅 GET account（cash/balance），不 emit on_position。
    void refresh_snapshot();

    /// @brief cold worker 执行的 reconcile resync：补漏成交并发 recovery 仓位快照。
    void reconcile_resync();

    // ── reconcile 查询/收敛入口（facade reconcile loop / 单测）────────
    /// @brief 拉一次 positionRisk → XtPositionSnapshot vector
    /// @return std::nullopt 表示请求失败（http != 2xx）；空 vector 表示账户全平仓。
    std::optional<std::vector<XtPositionSnapshot>> fetch_positions();

    /// @brief 检测并处理 ghost order：本地 in-flight 但交易所 openOrders 已不存在的订单。
    /// @return 已 resolve 的 ghost 数；std::nullopt 表示 REST 失败。
    std::optional<std::size_t> detect_and_resolve_ghosts(uint32_t min_age_ms);

    /// @brief 对 ACK 前长期没有 exchange orderId 的本地订单合成 Rejected 并清理 in-flight。
    /// @return true = 本次合成并 emit；false = 已被其它路径清理或已有 sys_id。
    bool synthesize_blank_sys_id_reject(uint64_t local_id);

    /// @brief 解析 openOrders REST body，并执行本地 inflight/upsert 与 reverse sync emit。
    /// @param body `/fapi/v1/openOrders` 返回 JSON，必须为 array。
    /// @param is_cold_start cold-start vs warm-reconnect 语义，透传到 batch emit。
    bool rebuild_from_open_orders_body(const std::string& body, bool is_cold_start);

    /// @brief userTrades 分页触顶次数（轮次上限用尽仍满页 = 残量留给下一周期）。
    uint64_t user_trades_pages_capped_total() const {
        return user_trades_pages_capped_total_.load(std::memory_order_relaxed);
    }

    // ── resync 快照-流栅栏（snapshot-vs-stream fence；BinanceReconcilerSync.cpp）──
    /// @brief UDS/gap-fill fill 的唯一 emit 入口；栅栏开启时暂存，否则直发 cb.on_trade。
    /// @param elig latency eligibility：UDS 实时 = kEligible / gap-fill replay = kIneligible
    ///        （构造点区分，共享 callback 不丢 source）。栅栏 flush 的暂存 fill 一律
    ///        kIneligible（延迟重放，时序与 venue 脱钩）。
    void forward_uds_trade(const XtTradeRecord& tr, XtLatencyEligibility elig);

    /// @brief 终态 order update 的栅栏化 emit：resync gate 开启时终态暂存，其余直发。
    ///
    /// fill 被 ResyncFillGate 暂存而终态实时穿透时，策略 per-order 消账会
    ///   先见终态后见 fill（fill_sum/filled_qty 误报）—— 终态与 fill 同栅栏，
    ///   flush 时排在全部 fill 之后。非终态（NEW/PARTIAL ack）保持实时。
    /// @pre 调用方不得持有 ledger 锁（X1：判定在 ledger 锁内，emit 全在锁外）。
    /// @param elig latency eligibility（构造源盖章；栅栏暂存透传，flush 时携带）。
    void forward_order_update_gated(const XtOrderExecUpdate& upd,
                                    EffectiveSignalDedup::Source src,
                                    XtLatencyEligibility elig);

    /// @brief UDS ACCOUNT_UPDATE 非 ORDER 原因的 absolute position 转发。
    /// @note 栅栏窗口内丢弃：该事件不携带可与 fence 比较的时间戳，且 qty/entry
    ///       字段即将被 recovery 快照覆盖；放行会让 stale absolute set 排在快照之后回退仓位。
    void forward_uds_position(const std::vector<XtPositionSnapshot>& positions);

    /// @brief reconcile 合成终态的统一 emit 入口，按 ACK 来源参与 dedup。
    /// @param elig latency eligibility：venue 查单转译 = kEligible / 本地合成
    ///        （-2013 not-exist / blank-sys-id）= kIneligible。
    void emit_reconcile_terminal_update(const XtOrderExecUpdate& upd,
                                        XtLatencyEligibility elig);

    /// @brief 对账快照落地期间，把并发到达的 fill 暂存、排好序再放行的栅栏。
    ///
    /// 栅栏开着时 fill 先暂存；快照落地后按 venue 时间把暂存 fill 分成
    /// 快照前/快照后两段重放，保证既不双计仓位也不丢 PnL/fee 记账。
    /// 任何 fill 都不丢弃。未走到 finish（REST 失败早退）时，
    /// 析构按到达顺序放行并关闭栅栏。
    class ResyncFillGate {
    public:
        explicit ResyncFillGate(BinanceReconciler& reconciler);
        ~ResyncFillGate();
        ResyncFillGate(const ResyncFillGate&) = delete;
        ResyncFillGate& operator=(const ResyncFillGate&) = delete;

        /// @brief 正常路径：携快照 + fence 分区重放 emit。
        /// @param fence_update_ms  positionRisk 全部行（含 positionAmt=0）的 symbol→updateTime。
        /// @param fence_floor_ms   快照请求前采样的 venue serverTime；缺行 symbol 的兜底 fence。
        /// @param mark_recovering  true = emit 期间置 reconcile_in_progress_（resync recovery 语义）。
        void finish_with_snapshot(const std::vector<XtPositionSnapshot>& positions,
                                  const std::unordered_map<std::string, int64_t>& fence_update_ms,
                                  int64_t fence_floor_ms,
                                  bool mark_recovering);

    private:
        BinanceReconciler& reconciler_;
        bool finished_ = false;
    };

private:
    // ── ghost / leak sweep 内部类型（BinanceReconcilerSweep.cpp）──────
    /// @brief venue 当前 open order 的最小字段视图（双向 sweep 共用）。
    struct VenueOpenOrderBrief {
        std::string sys_id;            ///< exchange orderId（十进制字符串）
        std::string client_order_id;   ///< venue 侧 clientOrderId 原文
        std::string symbol;
        uint64_t    order_time_ms = 0; ///< venue 报单时间（epoch ms）；反向 sweep 的 age 基准
    };

    /// @brief 用 client_order_id 反查 open order 的结果。
    struct ClientOrderLookup {
        bool ok = false;       ///< REST 查询是否成功。
        bool found = false;    ///< 是否找到对应 open order。
        std::string sys_id;    ///< 找到时的 exchange orderId。
    };

    /// @brief Reconcile：拉取交易所当前 open orders 概要。
    /// @return std::nullopt 表示拉取/解析失败；空 vector 表示当前无 open order。
    std::optional<std::vector<VenueOpenOrderBrief>> fetch_open_orders_brief();

    /// @brief 反向 sweep：撤销 venue 上仍 open、但本地无 owner 的本进程委托。
    ///
    /// 只处理能反解为本进程 clientOrderId、且 age >= min_age_ms 的订单；
    /// 外部/手工单不碰。每周期限量，剩余订单交给后续 reconcile 周期。
    /// @return 本周期成功发出撤单的数量。
    std::size_t cancel_leaked_venue_orders(const std::vector<VenueOpenOrderBrief>& venue_open,
                                           uint32_t min_age_ms);

    /// @brief Reconcile：查询单笔订单终态（FILLED/CANCELED/EXPIRED）。
    /// @param out_elig 终态 latency eligibility：venue 查单转译 = kEligible / -2013
    ///        not-exist 本地合成 = kIneligible（供 emit_reconcile_terminal_update）。
    /// @return 可用于 ghost resolve 的 XtOrderExecUpdate；活单或查询失败返回 std::nullopt。
    std::optional<XtOrderExecUpdate> fetch_order_status(const std::string& sys_id,
                                                        const std::string& symbol,
                                                        uint64_t local_id,
                                                        XtLatencyEligibility& out_elig);

    /// @brief venue 明确回 -2013（Order does not exist）时，从本地 inflight 重建 Canceled。
    /// @note 按 0 成交终态销账；继续保留 in-flight 只会占用 margin/in-flight 投影。
    /// @return inflight 已被并发路径清掉时返回 std::nullopt（无需重复 emit）。
    std::optional<XtOrderExecUpdate>
        synthesize_not_exist_terminal_(uint64_t local_id, const std::string& sys_id);

    /// @brief ghost fetch 失败登记：连续失败计数 + 周期性升级 error 告警；绝不终态化。
    void note_ghost_fetch_failure_(uint64_t local_id, const std::string& sys_id,
                                   std::unordered_map<uint64_t, uint32_t>& next_streak);

    /// @brief 按 symbol 拉 openOrders 并查找 clientOrderId。
    /// @return ok=false 表示 REST 失败；ok=true/found=false 表示请求成功但 venue 无此 open order。
    ClientOrderLookup find_open_order_by_client_id(const std::string& symbol,
                                                   const std::string& client_order_id);

    /// @brief reconcile 处理 blank-sys-id 订单：先尝试 openOrders 反查，超龄仍找不到才 reject。
    bool resolve_blank_sys_id_for_reconcile(uint64_t local_id,
                                            const std::string& symbol,
                                            uint64_t age_ms,
                                            uint32_t min_age_ms);

    /// @brief 按 symbol 拉取 last_seen 之后的 userTrades（fromId 游标有界循环续页），
    ///        并经共享 fill 去重委托重放漏成交。
    void resync_user_trades_for_symbol(const std::string& symbol);

    /// @brief 按 orderId 拉取该单全部 userTrades（满页时 fromId 续页、有界轮次），
    ///        并经共享 fill 去重委托重放漏成交。
    /// @note 带成交 ghost 专用：per-symbol 游标可能已被后续 fill 推过漏推区间
    ///       （游标式 gap-fill 结构上查不到老 trade）；orderId 查询不受游标约束，
    ///       已见 trade_id 由共享去重委托吞掉（双投递安全）。
    void resync_user_trades_for_order(const std::string& symbol, const std::string& sys_id);

    /// @brief userTrades 数组逐笔归因并经 inject_gap_fill_trade 重放（symbol/orderId 两查询共用）。
    /// @param out_skipped 坏行（id<=0 / 解析异常）跳过数；漏成交补发路径静默丢行不可接受为不可观测。
    /// @param out_max_trade_id 本批最大 venue trade id（含被去重行；续页游标推进用），
    ///        无有效行时保持不变。
    void replay_user_trades_json_(const std::string& symbol, const nlohmann::json& trades,
                                  size_t& emitted, size_t& deduped, size_t& out_skipped,
                                  int64_t& out_max_trade_id);

    /// @brief 先查本地 in-flight，缺失时再用 `/fapi/v1/order` 反查 gap-fill 归属。
    /// @note body 解析见 td/BinanceTdParse.h::parse_gap_fill_order_attribution_body
    ///       （纯函数，单测直测）。sid 归因与 UDS handle_order_trade 共享同一
    ///       OrderAttributionStore 查表（不拆两份 lookup）。
    [[nodiscard]] std::optional<GapFillOrderAttribution>
        resolve_gap_fill_order_attribution(const std::string& symbol,
                                           int64_t exchange_order_id);

    /// @brief 从交易所 openOrders 反向灌活跃在途单。
    /// @note 反解 client_order_id 为 local_id，补齐 ledger 的 in-flight 记录，并通过
    ///       on_reverse_sync_batch 交给上层订单簿补活。
    /// @param is_cold_start  true = 进程冷启 / 首次 recovery；false = 盘中 warm reconnect。
    /// @note REST 失败时不 emit batch；reconcile loop 后续重试。
    void rebuild_alive_inflight_and_emit(bool is_cold_start);

    // ── account truth（BinanceReconcilerSync.cpp）─────────────────────
    /// @brief 拉取 `/fapi/v2/account` 并解析 cash/margin/per-symbol broker truth。
    /// @return REST 或顶层资金字段解析失败返回 std::nullopt；per-symbol 行按 best-effort 过滤。
    /// @note 解析逻辑见 td/BinanceTdParse.h::parse_account_detail_body（纯函数，单测直测）。
    [[nodiscard]] std::optional<AccountTruthDetail> fetch_account_detail();

    /// @brief 拉取 positionRisk 的 markPrice，作为 account.positions 缺失字段的补充。
    /// @return REST/解析失败返回空 map；调用方保留 mark price 为 0。
    [[nodiscard]] std::unordered_map<std::string, int64_t> fetch_position_marks();

    /// @brief resync 末尾把本地 cash 再锚定到 venue account 真值。
    /// @note positionRisk 不携带资金字段；recovery 仓位快照后补一次 account snapshot
    ///       （emit 顺序固定 position → account）。节流共享 fetch_account_detail 预算；
    ///       REST 失败只 warn 不阻塞 resync，下个 keepalive refresh 会再锚定。
    void anchor_account_truth_after_resync_();

    // ── fence 内部（BinanceReconcilerSync.cpp）────────────────────────
    /// @brief ResyncFillGate 的统一结束入口；positions 为 nullptr 时仅按到达序 flush。
    void finish_resync_fill_gate_(const std::vector<XtPositionSnapshot>* positions,
                                  const std::unordered_map<std::string, int64_t>& fence_update_ms,
                                  int64_t fence_floor_ms,
                                  bool mark_recovering);

    /// @brief 采样 venue serverTime（GET /fapi/v1/time）作 fence 地板。
    /// @return venue epoch ms；REST 失败时退回本地时钟 + 最近 offset 估计再减 1s 保守余量。
    /// @note 必须在 positionRisk 读取前调用：T <= floor 的 fill 必然已含于其后的快照。
    int64_t sample_position_fence_floor_ms_();

    // ── REST 减负：tracked symbol 过滤（BinanceReconcilerSync.cpp）────
    /// @brief positionRisk 按 tracked 集 per-symbol 拉取并合并；失败/集合过大退回全量。
    std::optional<nlohmann::json> get_position_risk_array_(const char* tag);
    /// @brief openOrders 同上（带 symbol 参数 weight 1 vs 全量 40）。
    std::optional<nlohmann::json> get_open_orders_array_(const char* tag);

    // ── 注入依赖（归 gateway Impl 所有）───────────────────────────────
    std::unique_ptr<BinanceRestClient>& rest_;      ///< REST 所有权槽位（init 创建 / 单测替换）。
    std::unique_ptr<BinanceUserDataStream>& uds_;   ///< UDS 所有权槽位（start 创建）。
    BinanceOrderLedger& ledger_;                    ///< 订单身份/状态唯一权威账本。
    ITradeGateway::Callbacks& cb_;                  ///< 上层事件回调（bind_callbacks 写入同一对象）。
    xt_live::gateway::TdReadinessGate& readiness_gate_;  ///< TD 启动 readiness 阶段门。
    TrackedSymbolSet& tracked_symbols_;             ///< REST 减负 tracked symbol 集。
    ImplHooks hooks_;                               ///< Impl 保留侧窄回调面。

    // ── recovery 语义（三层协议）───────────────────────────────────────
    // reconcile_resync 同步 emit on_position 期间置 true，让上层按 recovery 语义
    // reset+set（is_recovering → XtHftChannels 每事件快照 TdState → account_store
    // 的 recovery reset+set 与 engine 的 realized 记账跳过）。toggle 只允许发生在
    // finish_resync_fill_gate_ 持 resync_gate_mu_ 时、且 wrap 单次 cb_.on_position
    // —— 不得移动。
    std::atomic<bool> reconcile_in_progress_{false};

    // 首次 bootstrap_snapshot 使用 cold-start 语义；后续 reconcile_resync 使用 warm 语义。
    std::atomic<bool> is_first_recovery_{true};

    // ── bootstrap 指数退避 ────────────────────────────────────────────
    std::atomic<uint32_t> bootstrap_attempts_{0};       ///< bootstrap_snapshot 尝试计数（ready 后 inert）。
    std::atomic<int64_t>  bootstrap_next_retry_ms_{0};  ///< 下次 bootstrap 重试最早时刻（lat_now_us/1000 时基）。

    // fetch_account_detail 最近一次 REST 成功时刻（steady ms）；resync 末尾 account
    // anchor 的节流基准，与 keepalive refresh 共享同一 REST 预算。0 = 从未成功。
    std::atomic<uint64_t> last_account_fetch_ms_{0};

    // ── resync 快照-流栅栏状态（resync_gate_mu_ 保护）─────────────────
    // 写者：cold worker（begin/finish）与 UDS 线程（forward_uds_*）；finish 持锁
    // 跨整个分区 emit，让并发 fill 阻塞到栅栏关闭后直发（必然排在快照之后）。
    // 锁纪律（X2）：临界区内不得获取 ledger 锁；携带的 update 进栅栏前已补齐。
    std::mutex resync_gate_mu_;
    bool resync_buffering_ = false;              ///< true = resync 在途，fill 暂存待分区重放
    std::vector<XtTradeRecord> resync_fill_buffer_; ///< 栅栏窗口内暂存的 UDS/gap-fill 成交。
    /// 栅栏窗口内暂存的终态 order update（flush 时排在全部 fill 之后重放；透传构造时 eligibility）。
    std::vector<ResyncTerminal> resync_terminal_buffer_;
    uint64_t resync_pos_dropped_total_ = 0;      ///< 栅栏窗口内丢弃的 UDS absolute position 批次数

    // ── ghost fetch 失败升级（reconcile 线程私有，无锁）───────────────
    static constexpr uint32_t kGhostFetchFailEscalateCycles = 10;  ///< 连续失败升级阈值（~5min @ 30s 周期）
    std::unordered_map<uint64_t, uint32_t> ghost_fetch_fail_streak_;  ///< local_id → 连续 fetch_order_status 失败周期数。

    static constexpr std::size_t kSymbolFilterMax = 8;  ///< tracked 集超过则退回全量拉取

    // ── userTrades 有界循环分页（per-symbol gap-fill 与 per-order replay 共用）──
    static constexpr std::size_t kUserTradesPageLimit = 500;  ///< 单页 limit（venue 上限）。
    static constexpr int kUserTradesPageRoundsMax = 8;        ///< 单次补发的续页轮次上限。
    /// 轮次上限用尽仍满页的次数（残量留给下一 reconcile 周期，进程内侧道观测）。
    std::atomic<uint64_t> user_trades_pages_capped_total_{0};
};

}  // namespace binance_u
