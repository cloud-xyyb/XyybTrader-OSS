#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "api/StrategySid.h"
#include "api/IMdGateway.h"
#include "api/ITradeGateway.h"
#include "xt_core/order/XtOrderRequest.h"  // XtOrderSource enum + submit(req) 入口
#include "api/XtLiveRejectReason.h"
#include "xt_risk/gateway/XtRiskGateway.h"
#include "xt_risk/engine/XtRiskEngine.h"
#include "live/XtLiveRiskGuardContext.h"
#include "live/XtLiveRejectSink.h"
#include "live/XtSymbolPriceBoundSTP.h"
#include "live/XtVelocityLimiter.h"
#include "api/IExecutionGateway.h"
#include "api/IHftGateway.h"
#include "api/ICtaGateway.h"
#include "XtLiveOrderGatewayCfg.h"
#include "RiskStatsSnapshot.h"
#include "LogicalOrderTracker.h"
#include "xt_util/runtime/XtAtomicWindowLimiter.h"
#include "xt_strategy_engine/IStrategyExecSink.h"

// 构造期/装配期依赖：这里只暴露指针或引用，具体行为在 .cpp 中 include。
class XtLiveParamDomain;
class XtLiveLatencyTracker;
class XtCommodityCatalog;
namespace xt_account { class AccountStateStore; }
namespace xt_identity { class StrategySidRegistry; }

// 风控与指标的 late-wire 依赖：Runner/Control 装配后注入，submit 热路径只读。
namespace xt_risk { class XtRiskMonitorState; }
namespace ct::metrics {
class XtStrategyPnlAggregator;
class XtStrategyPnlBaseline;
}

// runtime book 只通过接口注入；完整类型由实现文件负责。
namespace xt_runtime::order { class XtOrderRuntimeBook; }

/// @brief gateway 进程级观测计数（advance_risk_frame 帧尾经健康面 emit 导出 prom）。
namespace xt_live_gateway_counters {

/// @brief submit slot 预留失败拒单次数（capacity/intern 耗尽的 fail-closed 开仓面）。
///
/// prom metric `xt_account_slot_reservation_rejected_total`。>0 = 容量事故进行时：
/// 新 code 的开仓被 fail-closed 拒绝（与 xt_account_position_slot_overflow_total
/// 是同一事故的两个面：彼为持仓/成交源，此为 submit 预留源）。
std::atomic<uint64_t>& slot_reservation_rejected() noexcept;

}  // namespace xt_live_gateway_counters


/// @brief 实盘下单/撤单前置网关。
///
/// `submit()`/`cancel()` 先经过 trading gate、risk gate 和 sidecar metadata，
/// 再调用 TD 通道发送请求；订单终态以 TD 回报为准。
/// 内部不加锁，`single_threaded_submit=true` 时调用方必须保证无并发提交。
class XtLiveOrderGateway final : public xt_runtime::IHftGateway,
                                 public xt_runtime::ICtaGateway,
                                 private ILiveRejectOutput {
public:
    using RiskStatsSnapshot = ::RiskStatsSnapshot;

    using Config = XtLiveOrderGatewayCfg;

    /// @brief 构造网关并绑定共享运行时状态与事件路由。
    XtLiveOrderGateway(const Config& cfg,
                       xt_strategy_engine::IStrategyExecSink* exec_sink,
                       std::atomic<bool>* trading_ready,
                       std::atomic<uint32_t>* last_tick_action_time,
                       std::atomic<bool>* td_position_ready,
                       XtLiveParamDomain* param_domain,
                       XtLiveLatencyTracker* latency_tracker,
                       std::atomic<bool>* kill_switch = nullptr,
                       XtRiskEngine::InitDeps risk_engine_deps = XtRiskEngine::InitDeps());

    /// @brief 返回当前在途挂单数。
    /// @note 实盘挂单数由 order_book_direct_ 计算；inject_components 前返回 0。
    size_t live_orders() const override;
    /// @brief 转发 venue 终态查询给当前绑定的 td_（lost-terminal reconcile；CTP 查
    ///        terminal_local_ids_，其余 venue 默认 false）。
    [[nodiscard]] bool is_order_terminal(uint64_t local_id) const override;
    /// @brief 转发 venue 终态 payload 查询给当前绑定的 td_；单一 final overrider
    ///        同时覆盖 IHftGateway 与 ICtaGateway 的同签名 vtable 槽。
    [[nodiscard]] bool lookup_terminal(uint64_t local_id,
                                       XtOrderTerminalRecord& out) const override;

    /// @brief 绑定交易通道实例。
    void bind_td(ITradeGateway* td);
    /// @brief 绑定行情通道实例。
    void bind_md(IMdGateway* md);

    /// @brief 注入执行回调 sink 与 sid registry。
    ///
    /// Runner 装配阶段、EventLoop 启动前调用一次；运行期不再修改。
    void set_exec_sinks(xt_strategy_engine::IStrategyExecSink* fallback,
                        const std::vector<xt_strategy_engine::IStrategyExecSink*>& sinks,
                        const xt_identity::StrategySidRegistry* sid_registry) noexcept {
        exec_sink_ = fallback;
        exec_sinks_ = sinks;
        sid_registry_ = sid_registry;
    }

    /// @brief 清空执行回调 raw pointer；必须在所有可能触达 submit/reject 的线程停止后调用。
    void clear_exec_sinks() noexcept {
        exec_sink_ = nullptr;
        exec_sinks_.clear();
        sid_registry_ = nullptr;
    }
    // ── risk_engine 受控访问（risk_engine_ 为 private；外部不得持有可变引用）──
    /// @brief 写入自成交黑名单；后续 submit 命中该 code 直接拒单。
    void insert_self_match_blacklist(const char* code, const char* reason) noexcept;
    /// @brief 返回 risk 配置快照（副本，只读；不暴露内部 engine）。
    XtRiskCfg risk_config() const;
    /// @brief 应用热更后的 risk 配置。
    void set_risk_config(const XtRiskCfg& cfg);
    /// @brief annual-latch 持久化快照（control 落盘用）；engine 单锁复合读，防组合撕裂。
    using AnnualLatchSnapshot = XtRiskEngine::AnnualLatchSnapshot;
    AnnualLatchSnapshot annual_latch_snapshot() const;
    /// @brief 启动期恢复 annual-latch（restore on bootstrap）。
    void restore_annual_latch(int year, int64_t year_start_equity_fp, bool cutoff_active,
                              int64_t year_start_net_pnl_fp, int64_t risk_fund_balance_fp);
    /// @brief session-begin arm annual-loss latch（年切冻结 year_start anchor）。
    void arm_annual_loss_on_session_begin(const xt_account::AccountStateStore* account_store,
                                          uint32_t trading_date, int64_t equity_fp) noexcept;
    /// @brief 注入 active risk monitor broadcast state。nullptr = 未启用。
    void set_risk_monitor_state(const xt_risk::XtRiskMonitorState* state) noexcept {
        // genuinely-late dep（HFT runner 在 event loop start 后注入）；store-release 与
        // submit 热路径的 acquire-load 配对，消除未 fence 的指针发布 race。
        risk_monitor_state_.store(state, std::memory_order_release);
    }

    /// @brief 注入 per-strategy realized-PnL aggregator。
    /// @note nullptr 时 per-strategy today PnL 保持 0，daily_loss_pct fail-safe bypass。
    void set_strategy_pnl_aggregator(
        const ct::metrics::XtStrategyPnlAggregator* aggr) noexcept;

    /// @brief 注入日初 PnL baseline，使 per-strategy daily-loss gate 用 today realized 而非 lifetime。
    /// @note nullptr/未 ready 时退化为 lifetime 累计（与未注入一致）。
    void set_strategy_pnl_baseline(
        const ct::metrics::XtStrategyPnlBaseline* baseline) noexcept;

    /// @brief 注入 per-strategy risk monitor state；nullptr 时 fail-safe bypass。
    void set_per_strategy_risk_monitor_state(
        const xt_risk::XtRiskMonitorState* monitor_state) noexcept;
    /// @brief 注入 account/order/catalog 等运行时组件指针。
    void inject_components(xt_runtime::order::XtOrderRuntimeBook* order_book,
                           xt_account::AccountStateStore* account_store,
                           std::atomic<int64_t>* close_profit_fp = nullptr,
                           const XtCommodityCatalog* catalog = nullptr,
                           const std::atomic<uint64_t>* last_position_snapshot_ms = nullptr,
                           bool stale_guard_enabled = true,
                           int  stale_ms = 120000);

    /// @brief 启动期风控配置校验；返回非空字符串表示应拒绝启动的错误。
    /// @note runner 须在 late dep（strategy_pnl_aggr / monitor_state）wire 完成后调用，
    ///       此时 per-strategy DLL 分支才可达；非空即 fail-exit。
    std::string validate_risk_config();

    /// @brief 热路径下单入口：trading gate → risk gate → TD send。
    ///
    /// `code` 指针需覆盖本次调用；`strategy_id` 非空时写入 order sidecar，
    /// 用于后续回报路由。发送前被拒时返回 `order_id=0` 和拒绝原因。
    xt_runtime::SubmitResult submit(const char* code,
                                    bool is_buy,
                                    int64_t price_fp,
                                    int64_t qty_fp,
                                    XtTimeInForce tif,
                                    std::string_view user_tag,
                                    XtOffset close_offset,
                                    ExecFlags exec_flags,
                                    std::string_view strategy_id = "") override;

    /// @brief 已采样 intent trace 的热路径下单入口。
    /// @note 未采样调用必须走 submit()；submit_with_intent_trace 只把显式 trace id
    ///       继续传到 TD，不读取进程 TLS。
    xt_runtime::SubmitResult submit_with_intent_trace(const char* code,
                                    bool is_buy,
                                    int64_t price_fp,
                                    int64_t qty_fp,
                                    XtTimeInForce tif,
                                    std::string_view user_tag,
                                    XtOffset close_offset,
                                    ExecFlags exec_flags,
                                    std::string_view strategy_id,
                                    uint64_t intent_trace_id) override;

    /// @brief XtOrderRequest 入口；把 source/audit_trace_id 等 metadata 写入 sidecar。
    xt_runtime::SubmitResult submit(const xt_core::XtOrderRequest& req,
                                    ExecFlags exec_flags) override;

    /// @brief 按本地订单号撤单；多切片 clord_id 会展开为物理切片逐一撤单。
    bool cancel(uint64_t id) override;

    /// @brief 优雅停机：拒绝新单、best-effort cancel-all，并等待挂单终态。
    ///
    /// 必须在 TD/channel stop 前调用；方法幂等，超时后带残留挂单也返回。
    void stop(std::chrono::milliseconds timeout =
                  std::chrono::seconds(5)) noexcept override;

    /// @brief 读取风控统计快照（risk stats snapshot accessor）。
    /// @note 冷路径，HftRunnerControl 每轮控制循环调用一次；各计数器原子读，无锁。
    RiskStatsSnapshot risk_stats_snapshot() const;

    /// @brief 返回逻辑订单追踪器引用，用于物理 ID → clord_id 翻译。
    LogicalOrderTracker& logical_order_tracker() { return logical_order_tracker_; }

    /// @brief 反查物理/逻辑订单号对应的 intent_trace_id；终态 grace window 内仍可命中。
    uint64_t lookup_trace_id(uint64_t order_id) const noexcept;

    /// @brief 预热 STP / velocity limiter 的 symbol slot。
    /// @note runner 装配期以 declared/subscribed symbols 调用，为 STP 自成交防护上膛
    ///       （否则未注册品种 would_self_trade 恒 false = 旁路）。运行期新合约（rollover）
    ///       由 submit 热路径 lazy 注册兜底。
    void pre_warm_risk(const std::vector<std::string>& symbols);

    /// @brief 已注册 STP 品种数（观测用；0 = STP 未上膛 = 自成交防护旁路）。
    size_t stp_registered_count() const;

    /// @brief STP 上膛度量值：stp_enabled ? registered_count() : 0（disabled 诚实归 0）。
    /// @note 1Hz frame-tail 重发 xt_stp_registered_symbols gauge 用。
    size_t stp_registered_gauge() const;

    /// @brief STP 活跃挂单条目数（诊断用；disabled 时恒 0）。
    /// @note 终态协议闭合锚：稳态/收尾时非零 = 有单 submit 后未走 notify_order_terminal。
    size_t stp_active_order_count() {
        return cfg_.stp_enabled ? stp_checker_.active_order_count() : 0;
    }

    /// @brief 订单终态后按 id 释放该单对 STP 边界的贡献（id-keyed）。
    void notify_order_terminal(uint64_t order_id) override;

    /// @brief reverse-sync fillup 接受点驱动的 STP re-arm（R4：跟随 book 裁决）。
    /// @note dual-impl（IHftGateway + ICtaGateway）单 override 覆盖两 vtable 槽；转发到
    ///       STP rearm_active_order（upsert + 清 tombstone，book 真值覆盖私有 tombstone）。
    ///       只碰 STP（不回写 book）；由 engine worker 对 fillup accepted 清单逐单调用。
    void rearm_active_order(const std::string& code, uint64_t id,
                            bool is_buy, int64_t price_fp) override;

    /// @brief Recovery reverse-sync 批量 re-arm STP：cold 清场重播 / warm no-op。
    /// @param orders  本批 exchange-alive 在途单（反解自 clientOrderId/OrderRef）。
    /// @param is_cold_start true=首次 recovery（clear+seed）；false=warm resync（no-op —— warm
    ///        re-arm 唯一来源改为 book fillup 接受点经 rearm_active_order 驱动）。
    /// @note 分叉逻辑见 live/StpReverseSyncRearm.h（R4 设计裁决：STP 跟随 book 裁决）；本函数
    ///       只加 stp_enabled 门，委托 stp_checker_ 上的分叉。在 reverse-sync 回调线程同步调用
    ///       （早于 gateway 标 satisfied → trading_ready 放行），三 runner（HFT sink / CTA
    ///       channels / SEL runner）共用。
    void stp_rearm_reverse_sync(const std::vector<AliveInflightOrder>& orders, bool is_cold_start);

    /// @brief 记录订单终态，用于 order lifecycle 撤单率统计。
    void record_order_terminal(XtOrderStatus status) override;

    /// @brief 订单终态时只 mark sidecar，不立即删除。
    ///
    /// 保留 grace window 供 recovery 二次 rtn / synthetic terminal 继续反查 sid。
    void erase_order_tracking(uint64_t order_id) override;

    /// @brief 删除终态且早于 cutoff_us 的 sidecar entries。
    std::size_t prune_terminal_strategy_ids(int64_t cutoff_us);

    /// @brief Sidecar 当前 size (诊断用,不计已 mark terminal 的)。
    std::size_t order_strategy_size() const {
        std::shared_lock<std::shared_mutex> lock(order_strategy_mu_);
        return order_strategy_.size();
    }

    /// @brief 读取 sidecar grace window，供 HftRunnerControl 计算 prune cutoff。
    int64_t order_strategy_grace_us() const noexcept {
        return order_strategy_grace_us_;
    }

    /// @brief 反查 logical_order_id → strategy_id；grace window 内终态 entry 仍可命中。
    /// @note 供 lookup_strategy_id (get_open_orders multi-strategy sid filter) 使用;
    ///       回报归属已由 venue gateway 在边界 stamp，不再经此路径。
    std::string resolve_strategy_id(uint64_t logical_order_id) const noexcept {
        std::shared_lock<std::shared_mutex> lock(order_strategy_mu_);
        const auto it = order_strategy_.find(logical_order_id);
        return it != order_strategy_.end() ? it->second.sid : std::string{};
    }

    /// @brief 根据 sidecar / TD state 推导 fill route_status。
    const char* resolve_route_status(uint64_t logical_order_id,
                                     ITradeGateway::TdState td_state) const noexcept {
        std::shared_lock<std::shared_mutex> lock(order_strategy_mu_);
        const auto it = order_strategy_.find(logical_order_id);
        if (it == order_strategy_.end()) {
            if (td_state == ITradeGateway::TdState::Recovering) return "fallback_recovery";
            return logical_order_id == 0 ? "unknown" : "unmatched";
        }
        switch (it->second.source) {
            case xt_core::XtOrderSource::Strategy:
                return it->second.sid.empty() ? "unmatched" : "routed";
            case xt_core::XtOrderSource::Manual:
            case xt_core::XtOrderSource::Closeout:
                return "operator";
            case xt_core::XtOrderSource::Recovery:
                return "fallback_recovery";
        }
        return "unknown";
    }

    /// @brief IExecutionGateway::lookup_strategy_id override (get_open_orders sid filter)。
    std::string lookup_strategy_id(uint64_t order_id) const override {
        return resolve_strategy_id(order_id);
    }

    /// @brief IExecutionGateway::translate_open_orders override（get_open_orders 逻辑行合并）。
    size_t translate_open_orders(XtOrderExecUpdate* rows, size_t n) const override {
        return logical_order_tracker_.translate_open_orders(rows, n);
    }

    /// @brief TD 持仓快照审计入口；真实 account hydration 由 event-loop pipeline 完成。
    void process_position_snapshot(const std::vector<ITradeGateway::PositionSnapshot>& pos,
                                   ITradeGateway::TdState td_state);

private:
    struct SubmitSlice {
        XtOffset close_offset = XtOffset::Open;  ///< 开平方向（Open/CloseToday/CloseYesterday）
        int64_t qty_fp = 0;                      ///< 切片数量（fixed-point）
    };

    static constexpr size_t kMaxSubmitSlices = 4;
    struct SubmitSliceArray {
        std::array<SubmitSlice, kMaxSubmitSlices> slices{};  ///< 切片数组（栈分配，最多 4 笔）
        uint8_t count = 0;                                   ///< 有效切片数
        bool truncated = false;  ///< 切片预算耗尽仍有剩余量未覆盖（caller 须拒整笔，勿静默丢量）
        bool deferred = false;   ///< close_first 品种对侧在途 Open 未决（caller 拒 OppositeOpenPending）
        void push(XtOffset offset, int64_t qty) {
            assert(count < kMaxSubmitSlices);
            slices[count++] = {offset, qty};
        }
    };

    static int64_t now_ms();
    static bool is_valid_code(const char* code);
    /// @brief 校验下单输入参数。
    static bool is_valid_order_input(const char* code, int64_t price_fp, int64_t qty_fp);

    xt_runtime::SubmitResult submit_with_metadata(const char* code,
                                                  bool is_buy,
                                                  int64_t price_fp,
                                                  int64_t qty_fp,
                                                  XtTimeInForce tif,
                                                  std::string_view user_tag,
                                                  XtOffset close_offset,
                                                  ExecFlags exec_flags,
                                                  std::string_view strategy_id,
                                                  xt_core::XtOrderSource source,
                                                  uint64_t audit_trace_id,
                                                  uint64_t intent_trace_id);
    /// @brief 按今仓/昨仓可平量生成平仓切片；返回 false 表示可平量不足。
    bool build_close_slices(const char* code,
                            bool is_buy,
                            int64_t qty_fp,
                            bool prefer_today,
                            SubmitSliceArray& out) const;

    /// @brief 将策略下单请求构建为提交切片数组。
    /// @note Open 意图且 source==Strategy 时经 offset 路由（close_first 品种
    ///       改写为平仓切片 / defer）；panel/closeout 手工单跳过路由（操作员
    ///       显式意图优先）。显式 close 入参路径逐字不变。
    SubmitSliceArray build_submit_slices(const char* code,
                                         bool is_buy,
                                         int64_t qty_fp,
                                         XtOffset close_offset,
                                         xt_core::XtOrderSource source) const;
    /// @brief 保证金拒单回放；非空 strategy_id 路由到对应 strategy exec sink。
    void emit_margin_reject(bool is_buy,
                            int64_t price_fp,
                            int64_t qty_fp,
                            int64_t tnow,
                            std::string_view strategy_id = std::string_view{});
    /// @brief 按切片执行预检和 TD send，返回物理 ID 或多切片 clord_id。
    uint64_t submit_slices(const char* code,
                           bool is_buy,
                           int64_t price_fp,
                           XtTimeInForce tif,
                           std::string_view user_tag,
                           const SubmitSliceArray& slices,
                           XtOffset orig_close_offset,
                           ExecFlags exec_flags,
                           int64_t tnow,
                           uint64_t decision_us,
                           IRejectSink& reject_sink,
                           bool is_forced_close = false,
                           std::string_view strategy_id = std::string_view{},
                           uint64_t intent_trace_id = 0);

    /// @brief submit_one_slice_ 的单切片结果，驱动 submit_slices 循环推进。
    enum class SliceOutcome : uint8_t {
        kSubmitted,    ///< 本切片已送出 TD，继续下一切片
        kRejectAll,    ///< 尚无切片送出即失败：整笔返回 0
        kStopPartial,  ///< 已有切片送出后失败：停止后续切片，进入收口
    };

    /// @brief submit_slices 跨切片累积状态（栈上，零堆开销）。
    struct SliceRunState {
        uint64_t phys_ids[kMaxSubmitSlices] = {};      ///< 成功送出的物理 ID
        int64_t  phys_qty_fps[kMaxSubmitSlices] = {};  ///< 对应切片数量（fixed-point）
        uint8_t  phys_count = 0;                       ///< 已送出切片数
        int64_t  total_submitted_qty_fp = 0;           ///< 已送出总量
        uint64_t first_phys_id = 0;                    ///< 首个物理 ID（单切片时即 clord_id）
    };

    /// @brief 单切片提交：gateway gate → 风控判定 → TD 送单 → runtime 记账 → trace/tap。
    /// @pre caller 持有 submit 串行化窗口（submit_mu_，或 single_threaded_submit 契约）。
    /// @param slice_index 当前切片下标（切片数据取 slices.slices[slice_index]，trace 归因用）。
    /// @param st 跨切片累积状态；成功送出时追加物理 ID 并累计数量（输出）。
    SliceOutcome submit_one_slice_(const char* code,
                                   bool is_buy,
                                   int64_t price_fp,
                                   XtTimeInForce tif,
                                   std::string_view user_tag,
                                   const SubmitSliceArray& slices,
                                   uint8_t slice_index,
                                   XtOffset orig_close_offset,
                                   ExecFlags exec_flags,
                                   int64_t tnow,
                                   uint64_t decision_us,
                                   IRejectSink& reject_sink,
                                   bool is_forced_close,
                                   std::string_view strategy_id,
                                   uint64_t intent_trace_id,
                                   StrategySid owner_sid,
                                   bool owner_sid_valid,
                                   SliceRunState& st);

    /// @brief 提交收口：单切片直接返回 first_phys_id（clord_id == phys_id，零开销）；
    ///        多切片经 tracker 注册逻辑 clord_id，策略只持有逻辑句柄。
    uint64_t finalize_multi_slice_(const SliceRunState& st,
                                   bool is_buy,
                                   int64_t price_fp,
                                   XtOffset orig_close_offset,
                                   uint64_t intent_trace_id);

    /// @brief Submit 后的 runtime 记账：should_skip 判定 + book slot 注册 +
    ///        account order_accepted。
    /// @pre 在 book 写线程执行（off-worker submit 经 post_to_worker 整块入队）；
    ///      order_book_direct_ 与 account_store_direct_ 非空。
    void register_submitted_order_(uint64_t phys_id,
                                   const char* code,
                                   bool is_buy,
                                   int64_t qty_fp,
                                   int64_t price_fp,
                                   XtOffset close_offset,
                                   StrategySid owner_sid,
                                   int64_t required_margin_fp,
                                   int64_t per_unit_margin_fp);

    void record_reject(XtLiveRejectReason code);
    void maybe_enter_cooldown_on_reject(int64_t now_ms);

    void publish_margin_reject(const xt_core::XtOrder& req,
                               int64_t now_ms,
                               std::string_view strategy_id = std::string_view{}) override;
    void publish_reject_reason(XtLiveRejectReason code) override;

    Config cfg_;                                                ///< 网关配置（value-owned）
    xt_runtime::order::XtOrderRuntimeBook* order_book_direct_ = nullptr;
    const XtCommodityCatalog* catalog_direct_ = nullptr;  ///< offset 路由 policy 查询（bootstrap 后只读）
    xt_account::AccountStateStore*                    account_store_direct_ = nullptr;
    std::atomic<int64_t>*                  close_profit_fp_direct_ = nullptr; ///< 今日平仓盈亏原子缓存（来自 XtRunnerSignals，non-owning）
    xt_strategy_engine::IStrategyExecSink* exec_sink_ = nullptr; ///< fallback 执行回调 sink（non-owning）
    std::atomic<bool>* trading_ready_ = nullptr;                ///< 交易就绪标志（non-owning，外部写）
    std::atomic<uint32_t>* last_tick_action_time_ = nullptr;    ///< 最近 tick action_time（non-owning）
    std::atomic<bool>* td_position_ready_ = nullptr;            ///< 持仓快照就绪标志（non-owning）
    std::atomic<bool>* kill_switch_ = nullptr;                  ///< 全局 kill switch（non-owning，ctor 存，wire() 转交 guard）
    std::atomic<ITradeGateway*> td_{nullptr};                   ///< TD 交易通道（atomic 绑定，可热切换）
    std::atomic<IMdGateway*> md_{nullptr};                      ///< MD 行情通道（atomic 绑定）
    XtLiveParamDomain* param_domain_ = nullptr;                 ///< 参数域（non-owning，gate 开关来源）
    XtLiveLatencyTracker* latency_tracker_ = nullptr;           ///< 延迟追踪器（non-owning，可 null）
    std::atomic<uint64_t> gate_seen_version_{0};                ///< 已同步的参数版本号（gate cache）
    std::atomic<bool> gate_submit_enabled_{true};               ///< 报单门禁开关（参数域驱动）
    std::atomic<bool> gate_cancel_enabled_{true};               ///< 撤单门禁开关（参数域驱动）

    AtomicWindowLimiter order_limiter_{1000};                   ///< 报单频率限流器（1s 窗口）
    AtomicWindowLimiter cancel_limiter_{1000};                  ///< 撤单频率限流器（1s 窗口）
    AtomicWindowLimiter reject_limiter_{60000};                 ///< 拒单日志限流器（60s 窗口）
    std::atomic<int64_t> block_until_ms_{0};                    ///< 冷却期截止时间 ms（拒单后退避）
    /// @brief 停机标志（IExecutionGateway::stop 触发）。一旦置位,submit/cancel
    ///        立即拒单,防止 race 在 cancel-all-on-stop 期间注入新挂单。
    /// @note  无 reset 路径——单进程生命周期内置位即终态。
    std::atomic<bool> stopping_{false};
    mutable std::mutex submit_mu_;                              ///< 报单序列化锁（多线程模式使用）
#ifndef NDEBUG
    std::atomic<bool> submit_reentry_guard_{false};             ///< 单线程模式并发误用检测（Debug only）
#endif
    XtRiskGateway risk_gateway_;                                ///< 风控网关（风控引擎适配层）
    XtRiskEngine risk_engine_;                                  ///< 风控引擎实例（value-owned）
    XtRiskPolicy risk_policy_;                                  ///< 风控策略配置（value-owned）
    XtLiveRiskGuardContext::OrderRateLimitFn order_rate_limit_fn_; ///< 报单频率限制回调（绑定 order_limiter_）
    XtLiveRiskGuardContext risk_guard_ctx_;                      ///< 实盘风控守卫上下文（聚合风控检查链）
    XtSymbolPriceBoundSTP stp_checker_;                         ///< O(1) 自成交防护检查器（STP，cfg.stp_enabled 时启用）
    XtVelocityLimiter velocity_limiter_;                        ///< 仓位变化速率限制器（cfg.velocity_enabled 时启用）
    std::atomic<const xt_risk::XtRiskMonitorState*> risk_monitor_state_{nullptr}; ///< Active risk monitor 广播状态（non-owning，atomic: HFT late wire vs hot-path read）

    /// logical_order_id → submit-time metadata sidecar。
    ///
    /// 终态时 mark `terminal_at_us` 而非立即删除，grace window 后统一 prune。
    struct OrderStrategyEntry {
        std::string sid;
        int64_t terminal_at_us = 0;  // 0 = active;非 0 = 终态时刻 (steady_us)
        // from XtOrderRequest metadata (default = Strategy)
        xt_core::XtOrderSource source = xt_core::XtOrderSource::Strategy;
    };
    mutable std::shared_mutex order_strategy_mu_;
    std::unordered_map<uint64_t, OrderStrategyEntry> order_strategy_;
    /// grace window default 60 秒。可通过 set_order_strategy_grace_us() 调整。
    int64_t order_strategy_grace_us_ = 60'000'000;

    /// phys_id / clord_id → intent_trace_id；终态后同 strategy sidecar grace 统一保留。
    struct OrderTraceEntry {
        uint64_t trace_id = 0;
        int64_t terminal_at_us = 0;  // 0 = active;非 0 = 终态时刻 (steady_us)
    };
    mutable std::shared_mutex order_trace_mu_;
    std::unordered_map<uint64_t, OrderTraceEntry> order_trace_;
    void note_trace_attribution_(uint64_t order_id, uint64_t trace_id);
    void mark_order_tracking_terminal_(uint64_t order_id, int64_t now_us);

    /// emit_margin_reject 未路由到目标 strategy 的累计次数。
    mutable std::atomic<uint64_t> dropped_margin_reject_total_{0};

    /// per-strategy exec sink ptrs；顺序与 yaml strategies[] 一致，non-owning。
    std::vector<xt_strategy_engine::IStrategyExecSink*> exec_sinks_;

    /// string sid → uint32 ID registry；Runner owned, non-owning。
    const xt_identity::StrategySidRegistry* sid_registry_ = nullptr;
public:
    /// @brief 读取 margin reject 路由失败计数。
    uint64_t dropped_margin_reject_count() const noexcept {
        return dropped_margin_reject_total_.load(std::memory_order_relaxed);
    }
private:

    static int64_t xt_steady_now_us_();
    std::atomic<uint64_t> last_halt_log_version_{0};            ///< 每个 monitor 版本只打印一次 halt 诊断日志

    std::atomic<uint64_t> submit_attempts_{0};                  ///< 报单尝试计数
    std::atomic<uint64_t> submit_success_{0};                   ///< 报单成功计数
    std::atomic<uint64_t> cancel_attempts_{0};                  ///< 撤单尝试计数
    std::atomic<uint64_t> cancel_success_{0};                   ///< 主动撤单 API 成功计数
    std::atomic<uint64_t> canceled_terminal_total_{0};          ///< 订单终态 Canceled 总数
                                                                ///  (含 IOC 超时 / 主动撤 / exchange 撤)
    std::array<std::atomic<uint64_t>, kLiveRejectReasonCount> reject_counts_{}; ///< 按拒绝原因分类计数数组

    LogicalOrderTracker logical_order_tracker_;                  ///< 逻辑订单追踪器（物理 ID→clord_id 映射）

    void refresh_param_gate_cache();
};
