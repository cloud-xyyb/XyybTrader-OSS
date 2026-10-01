#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "api/IHftGateway.h"
#include "xt_engine/sim/HFTMatchEngine.h"
#include "xt_risk/engine/XtRiskEngine.h"
#include "xt_risk/gateway/XtRiskGateway.h"
#include "xt_risk/live/XtSymbolPriceBoundSTP.h"
#include "xt_risk/config/XtRiskCfg.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_account/AccountStoreView.h"
#include "xt_cost/XtFillCostApplier.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_config/catalog/XtFeeRuleBook.h"
#include "xt_cost/XtTradeCostConfig.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/market/XtTick.h"
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "api/IHftEngineObserver.h"

class IRejectSink;
namespace xt_runtime { class XtRuntimeMarketCache; }

using xt_runtime::IHftGateway;
using xt_runtime::SubmitResult;

/// @brief HFT 回测执行链的订单 gateway。
///
/// 策略侧通过 `IHftGateway` 提交/撤单；本类持有 match、risk、cost 与 order sidecar，
/// 把撮合结果按回放时钟转成 fill/update 队列。
/// account、catalog、order book 由 Runner 持有并通过 non-owning 指针注入
class XtHftBacktestOrderGateway final : public IHftGateway {
public:
    // ── 初始化参数（Runner 构造后传入一次） ───────────────────────────────

    /// @brief 执行链初始化参数，由 Runner 组装后传入一次。
    /// @details
    /// - 配置字段用于重建 gateway-owned match/risk/cost 组件。
    /// - raw ptr 字段均为 required non-owning 依赖，生命周期必须覆盖 gateway。
    struct InitParams {
        XtRiskCfg risk;                         ///< Runner 传入的 risk 配置；init() 用它构造 risk_engine_。
        XtRiskPolicy risk_policy;               ///< risk gateway 使用的策略开关。
        XtHftMatchEngine::Config match;         ///< match engine 配置快照。
        XtTradeCostConfig cost;                 ///< fill cost applier 配置快照。
        XtFeeRuleBook::Config fees;             ///< fee rule book 配置快照。
        int64_t max_order_qty_fp = 0;           ///< 单次 submit 切片上限；0 表示不按数量拆分。
        bool self_match_check = false;          ///< true 时启用 submit 前 STP 自成交预防（默认关）。
        bool margin_call_enabled = false;       ///< true 时逐 tick 检查维持保证金并强平（默认关）。
        double maintenance_ratio = 0.9;         ///< 维持保证金系数；仅 margin_call_enabled 时读。

        // ── non-owning raw ptr；调用方持有所有权 ─────────────────────────
        xt_account::AccountStateStore* account = nullptr; ///< required；账户事件写入与快照来源。
        const XtCommodityCatalog* catalog = nullptr;      ///< required；合约元数据与费用模型引用。
        xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr; ///< required；订单 domain book。
    };

    XtHftBacktestOrderGateway() = default;

    /// @brief 初始化 backtest 执行链并绑定 Runner-owned domain 组件。
    /// @details
    /// - 可重复调用；每次都会重建 gateway-owned match/risk/cost 状态。
    /// - 会重置 order/open-order domain，使 backtest 从 cold-start book 开始。
    /// - `InitParams` 中 raw ptr 必须非空；gateway 不接管其所有权。
    void init(InitParams p);

    // ── 模型与报告链注入（bootstrap 期绑定）─────────────────────────────

    /// @brief 设置 order delay wheel（submit 到撮合引擎可见的 tick 延迟）。
    /// @details
    /// - 仅 bootstrap 调用；运行期调用会重置 delay wheel。
    /// - wheel 中尚未提交到 match engine 的 delayed orders 会被丢弃。
    void set_order_delay_ticks(uint32_t n) { match_engine_.set_order_delay_ticks(n); }

    /// @brief 注入 market cache（bootstrap 期，build_runtime_view 之后）。
    ///
    /// price_limit_enabled 时 submit 路径读 `last_tick(code)` 的涨跌停价填风控
    /// 快照（dispatch 前 cache 已持当前 tick）；nullptr 或首 tick 前 last_tick 为空
    /// → limit 0 = 检查 bypass（fail-open）。
    void set_market_cache(const xt_runtime::XtRuntimeMarketCache* cache) noexcept {
        market_cache_ = cache;
    }

    /// @brief 转发注入 latency model 给内部 match engine。
    /// @details
    /// - Runner 在 `init()` 之后按 yaml `backtest.latency_model:` 决定是否调用。
    /// - 配置后 submit eligibility latency 与 fill response latency 均由 match engine 抽样。
    void set_latency_model(std::unique_ptr<ILatencyModel> m) {
        match_engine_.set_latency_model(std::move(m));
    }

    /// @brief 转发注入 queue model 给内部 match engine。
    /// @details
    /// - Runner 在 `init()` 之后按 yaml `backtest.queue_model:` 决定是否调用。
    /// - 配置后挂单排队与成交优先级由 match engine 按该模型评估。
    void set_queue_model(std::unique_ptr<IQueueModel> m) {
        match_engine_.set_queue_model(std::move(m));
    }

    /// @brief 添加 HFT observer，order/trade 回报按 add 顺序 fan-out。
    /// @details
    /// - 非 owning；调用方保证 observer 生命周期覆盖 gateway。
    /// - nullptr 会被忽略，不改变已有 fan-out 顺序。
    void add_observer(xt_runtime::IHftEngineObserver* obs) {
        if (obs) observers_.push_back(obs);
    }
    /// @brief 清空 observer fan-out 链；不析构 observer。
    /// @details
    /// - 只清空 non-owning 指针列表，不触发任何 observer 回调。
    void clear_observers() { observers_.clear(); }

    /// @brief 绑定 reject sink，风控/输入拒单直接落地到该接口。
    /// @details
    /// - Reject 不走 observer fan-out，避免 gateway 依赖 concrete reporter 类型。
    /// - 非 owning；传 nullptr 等价于禁用 reject 落地。
    void bind_reject_sink(IRejectSink* sink) { reject_sink_ = sink; }

    // ── 热路径（Engine::on_tick 调用顺序）────────────────────────────────

    /// @brief 推进撮合引擎 market tick 时钟（双轴：event + trading）。
    /// @details
    /// - `Engine::run_tick_cycle` 在策略 dispatch 前调用，使 submit timestamp 对齐当前 tick。
    /// - 会递增 `tick_idx_`；该计数参与 ack_delay_ticks / ttl_ticks / create_tick 到期判定。
    /// - `action_date`（0 时 fallback trading_date）+ action_time 合成 event-axis 时刻；
    ///   `trading_date` 仅供 report/current_ts 戳，不入时长运算。
    /// - grid clock 注入和 EOF flush 必须走 `advance_clock_only()`，只推时间不动 market tick 计数。
    void advance_clock(uint32_t action_date, uint32_t action_time_ms,
                       uint32_t trading_date) override;

    /// @brief 推进 wall-clock 时间但不递增 market tick 计数。
    /// @details
    /// - 用于 `IReplayRuntime::on_clock` 注入的 grid clock 事件和 EOF flush。
    /// - 不推进 `XtOrderManager::cur_tick_`，避免 ack_delay_ticks / ttl_ticks 误计入 grid clock。
    /// - 不是 `IHftGateway` 接口的一部分；live 路径没有 grid clock 概念。
    void advance_clock_only(uint32_t action_date, uint32_t action_time_ms,
                            uint32_t trading_date);

    /// @brief 驱动撮合并把本 tick 成交放入 pending fill 队列。
    /// @details
    /// - 本函数只产生 match result，不同步调用 `engine.on_trade`。
    /// - 成交按 response latency 写入 pending queue，到 `visible_at_ns` 后由 `drain_fills()` 拉取。
    /// - order update 没有 fill visibility delay，会留在 gateway buffer 等 `drain_order_updates()` 拉取。
    void process_tick(const XtTick& tick) override;

    /// @brief 从 pending 队列 drain 已到期 fill（visible_at_ns <= now_ns）到 out。
    /// @details
    /// - Drain 过程中 fan-out `observer.on_trade`；具体落地行为由注册的 observer 决定。
    /// - 不执行 domain 处理；order_book / account / 策略 exec sink 由调用方通过 `engine.on_trade` 完成。
    /// - 同一 fill 只会被 drain 一次；multi-strategy 模式从 sidecar 回填
    ///   `XtTradeRecord::strategy_id` + `sid_id` 同源双键。
    size_t drain_fills(uint32_t date, uint32_t time_ms,
                       std::vector<XtTradeRecord>& out) override;

    /// @brief 丢弃所有 pending fill，不通知 observer/reporter。
    /// @details
    /// - EOF 严格因果收尾使用；未到可见时间的 fill 不进入 strategy、reporter 或 P&L。
    /// - 只清理 pending fill queue，不修改 match engine 的 in-transit order wheel。
    /// @return 丢弃的 fill 数量。
    size_t drop_pending_fills() override;

    /// @brief post-fill 风险观测快照（纯观测，不产生拒单）。
    /// @details
    /// - `position_breach_count`：某 code 的 fill 净仓绝对值穿透 `risk.max_pos_qty`
    ///   的进入次数（edge-triggered：进入超限记一次 + warn log，回到限内再穿透
    ///   才记下次）。pre-trade 的 max_pos_qty 是 confirmed 净仓语义（resting
    ///   不计入），批量成交可越限——本观测让越限对策略作者显式可见。
    ///   只观测账户级 `risk.max_pos_qty`；`per_strategy_max_pos_qty` 配额
    ///   不在观测范围。
    /// - `peak_abs_position_fp`：fill 净仓绝对值峰值（跨全部 code 取最大）。
    /// - `peak_live_orders`：在途挂单数峰值（submit 成功时刻采样撮合簿
    ///   live 数；`order_delay_ticks > 0` 时 delay wheel 内在途单不计入，
    ///   该形态下为下界观测）。
    /// - 净仓按 drained fill 带符号全额累计（BT 起始仓位恒 0，正常路径与
    ///   confirmed 同值；over-close 被账本 clamp 的角落 fill 全额计入 →
    ///   观测偏保守 over-report，对告警信号 fail-safe）；EOF 被 drop 的
    ///   未可见 fill 不计（与 P&L 口径一致）。
    struct RiskObservability {
        uint32_t position_breach_count = 0;
        int64_t  peak_abs_position_fp  = 0;
        size_t   peak_live_orders      = 0;
    };
    const RiskObservability& risk_observability() const noexcept { return risk_obs_; }

    /// @brief Replayer 主循环最后一次推进到的 trading date。
    /// @details
    /// - EOF 严格因果收尾时，Driver 用此值 + grace_ms 计算 EOF clock。
    /// - 0 表示尚未推进过 clock/tick。
    uint32_t last_clock_date() const noexcept { return last_date_; }

    /// @brief Replayer 主循环最后一次推进到的 action_time，格式 HHMMSSmmm。
    /// @details
    /// - EOF 严格因果收尾时，Driver 用此值 + grace_ms 计算 EOF clock。
    /// - 0 表示尚未推进过 clock/tick。
    uint32_t last_clock_time_ms() const noexcept { return last_time_ms_; }

    /// @brief 最近一次推进的自然日（event-axis）；EOF grace 用 event 轴做时长延展。
    /// @details 0 表示尚未推进过，或源不携带 action_date（已 fallback trading_date）。
    uint32_t last_clock_action_date() const noexcept { return last_action_date_; }

    /// @brief 返回 match engine 累计被 EOF 丢弃的 in-transit order 数。
    /// @details
    /// - `order_delay_ticks=0` 时永远为 0。
    /// - `order_delay_ticks>0` 时由 `discard_in_transit_orders()` 累加。
    std::uint64_t lost_in_transit_count() const noexcept {
        return match_engine_.lost_in_transit_count();
    }

    /// @brief EOF 收尾：丢弃 match engine wheel 中尚未提交的 delayed orders。
    /// @details
    /// - 与 `drop_pending_fills()` 对称；本函数清上行 order wheel。
    /// - 每单合成 Canceled 终态并抽到 gateway 缓冲（caller 随后
    ///   drain_order_updates 派发，frozen 释放链靠终态）。
    /// - 返回本次丢弃数，并计入 `lost_in_transit_count()`。
    std::size_t discard_in_transit_orders() {
        const std::size_t n = match_engine_.discard_in_transit_orders();
        if (n > 0) match_engine_.drain_updates(updates_buf_);
        return n;
    }

    /// @brief 进入 EOF 收尾栅栏：此后 submit 一律本地拒，直到 run 结束。
    /// @details
    /// - 由 driver flush_pending 在收尾入口调用，闭合「合成 Canceled 终态
    ///   经 on_order 触发策略重挂 → 新单进撮合再冻结」的遗留活单窗口。
    /// - BT-only；栅栏一经置位不再清除（生命周期 = flush 到 run 结束）。
    void begin_eof_closing() noexcept { eof_closing_ = true; }

    /// @brief EOF 收尾：为尚未收到 delivered 终态的在途单逐个合成 Canceled 终态。
    /// @details
    /// - 覆盖三类：已入撮合簿的 GTC 等 resting 单、以及内部已全成交/部分成交但其
    ///   终态回报被 strict-drop 的单（这类单已从撮合域归档，撮合层扫描看不到它们）。
    /// - 合成终态一律走 delivered-view 口径：filled = 已实际派发给策略/账户的累计
    ///   成交量，leaves = qty − delivered；被 strict-drop 的 fill 从策略/账户视角
    ///   「从未发生」，据此闭账不产生账本分裂。
    /// - 每单合成 Canceled 抽到 gateway 缓冲（caller 随后 drain_order_updates 派发，
    ///   策略收挂单结局 + frozen 释放链靠终态）。
    /// - 已收到 delivered 终态（drain_order_updates 见终态即摘除投影）与 wheel 单
    ///   （discard_in_transit_orders 已单独合成）不重复处理。
    /// - 返回本次合成数。
    std::size_t synthesize_eof_delivered_cancels();

    /// @brief EOF 收尾：为盘中已派终态、其未可见 fill 尚在 pending 的单补交付剩余 fill。
    /// @details
    /// - 由 driver flush_pending 在 drain_fills 之后、drop_pending_fills 之前调用。
    /// - 仅处理 terminal_delivered 投影：终态已盘中经 drain_order_updates 派发（携真实
    ///   内部 filled），此处把该单在 pending_fills_ 内仍未可见的 fill 越过 visible_at
    ///   一次性补交付，使 delivered 追平 claim、账本闭合（从 strict-drop 中救回）；终态
    ///   后不再有新 fill，故补交付累计 == claim。补交付 fill 走 sid 回填 + 风险观测 +
    ///   observer fan（mirror drain_fills 交付语义），策略在 on_order(终态) 之后收 on_fill
    ///   （late-fill after terminal，同 live tail-fill 契约同形）。
    /// - 补交付后 erase 对应投影；hold-back 的单（终态未派发、未标 terminal_delivered）不
    ///   在此列，交 synthesize_eof_delivered_cancels（两机制互斥，单终态恰一次）。
    /// - EOF 栅栏已置位 → 补交付触发的策略重挂被拒（F3 既有）。
    /// - 返回本次补交付 fill 数。
    std::size_t complete_eof_delivered_terminal_fills(std::vector<XtTradeRecord>& out);

    /// @brief 累计 margin call 触发次数（margin_call_enabled=false 恒 0）。
    uint64_t margin_call_count() const noexcept { return margin_call_count_; }

    /// @brief event 轴巨幅前跳（脏未来日期 tick）观测计数。
    /// @note 纯观测（clean data 恒 0，不改行为）：错误未来日期 tick 会永久毒化
    ///       last_event_ns_（TTL/ACK 立即到期 + fill 滞留 EOF），对称 OrderManager
    ///       的 backward clamp warn。见 note_event_forward_spike_。
    uint64_t event_forward_spike_count() const noexcept { return event_forward_spike_count_; }

    /// @brief Drain match_engine 在 process_tick 内生成的 order_update 到 out。
    /// @details
    /// - `run_tick_cycle_` 在 `process_tick()` 后立即 drain，order_update 与本 tick 同步可见。
    /// - Drain 过程中调用 observer.on_order_update，供报告层观察订单状态。
    /// - domain 处理由调用方经 `engine.on_order_update` 完成。
    /// - multi-strategy 模式会从 order sidecar 回填 `XtOrderExecUpdate::sid_id`。
    size_t drain_order_updates(std::vector<XtOrderExecUpdate>& out) override;

    /// @brief 设置 fill 可见性延迟，单位 nanoseconds；默认 0 表示立即可见。
    /// @details
    /// - 仅在未配置 latency model 时生效。
    /// - 配置 latency model 后由 match engine 抽样 response latency。
    void set_fill_latency_ns(int64_t ns) noexcept { fill_latency_ns_ = ns; }

    /// @brief 标定下一次 process_tick 的 fill timestamp（策略 dispatch tick 时间）。
    /// @details
    /// - 该时间只对下一次 `process_tick()` 生效。
    /// - `process_tick()` 消费后立即清空，避免未匹配 clock 泄漏到后续撮合。
    void set_dispatch_clock(uint32_t date, uint32_t time_ms) override;

    /// @brief 会话开始事件：账户换日、风控年初基准刷新、strategy sidecar 跨日清理。
    /// @details
    /// - 向 account 写入 session_begin，并刷新 risk annual latch。
    /// - 跨 trading date 时清理已终态的上一日 `order_strategy_` sidecar。
    void on_session_begin(uint32_t trading_date) override;

    /// @brief 订单终态后按 id 释放 STP 边界贡献（self_match_check 开启时；对齐 live）。
    void notify_order_terminal(uint64_t order_id) override;

    // ── 视图访问（Engine 构建 XtRuntimeView 时使用）──────────────────────

    /// @brief 返回 gateway-owned account view；底层 account 仍由 Runner 持有。
    xt_runtime::IAccountView*                      account_view();
    /// @brief 返回 Runner-owned runtime order book。
    xt_runtime::order::XtOrderRuntimeBook*         order_book();
    const xt_runtime::order::XtOrderRuntimeBook*   order_book()        const;
    /// @brief 返回 Runner-owned commodity catalog。
    const XtCommodityCatalog*                      commodity_catalog() const;

    // ── 账户直读（Engine 的持仓/资金查询接口使用）────────────────────────

    /// @brief 返回 Runner-owned account store；可能在 init 前为 nullptr。
    xt_account::AccountStateStore*       account()       { return account_; }
    const xt_account::AccountStateStore* account() const { return account_; }

    // ── 报告与统计 ────────────────────────────────────────────────────────

    /// @brief 返回账户 cash fixed-point 快照；init 前返回 0。
    int64_t get_cash_fp()       const;
    /// @brief 返回账户 equity fixed-point 快照；init 前返回 0。
    int64_t get_equity_fp()     const;
    /// @brief 返回账户 unrealized PnL fixed-point 快照；init 前返回 0。
    int64_t get_unrealized_fp() const;
    /// @brief 返回账户订单冻结 fixed-point 快照；init 前返回 0。
    int64_t get_frozen_fp()     const;
    /// @brief 返回 match engine 当前 alive orders 数量。
    size_t  live_orders()       const override;

    /// @brief 当前 packed timestamp（YYYYMMDD*1e9 + HHMMSSmmm）。
    /// @details
    /// - 来源为最近一次 clock/tick 推进。
    /// - 用于 RejectSink 和 observer on_order_submit 的事件时间戳。
    int64_t current_ts() const;

    // ── IExecutionGateway ─────────────────────────────────────────────────

    /// @brief 提交 HFT 回测订单。
    /// @details
    /// - `qty_fp` 会按 `max_order_qty_fp_` 切片，再逐片执行风控、close offset 路由和撮合提交。
    /// - 成功切片会登记 order book，并通过 observer fan-out 发出 submit 事件。
    /// - `strategy_id` 写入 order sidecar；multi-strategy 回放依赖它把 fill/update 路由回对应 engine。
    /// @return 返回第一片订单 id；0 表示输入、风控或切片预算拒单。
    SubmitResult submit(const char* code,
                        bool is_buy,
                        int64_t price_fp,
                        int64_t qty_fp,
                        XtTimeInForce tif,
                        std::string_view user_tag,
                        XtOffset offset,
                        ExecFlags exec_flags,
                        std::string_view strategy_id = "") override;

    /// @brief 撤销指定 id 的挂单；id 不存在或已成交时返回 false。
    /// @details
    /// - 直接转发给 match engine；不会同步触发 observer。
    /// - 后续 cancel update 通过 `drain_order_updates()` 进入 engine/report 链。
    bool cancel(uint64_t id) override;
    /// @brief 回测 gateway 无后台 worker；stop 为 no-op。
    /// @details
    /// - 保留接口兼容 `IHftGateway`。
    /// - 不等待线程、不 drain 队列、不改变撮合状态。
    void stop(std::chrono::milliseconds timeout = std::chrono::seconds(5)) noexcept override {
        (void)timeout;
    }

private:
    /// @brief 单笔提交实现：输入校验、风控、close offset 路由和逐片提交。
    /// @details
    /// - 输入 guard 与 risk guard 失败时通过 `reject_sink_` 落地拒单。
    /// - 每个成功切片的 order_id 都写 `order_strategy_` sidecar。
    /// - 首切片失败返回 0；中途失败保留已提交切片并停止后续切片。
    SubmitResult submit_single_(const char* code,
                                bool is_buy,
                                int64_t price_fp,
                                int64_t qty_fp,
                                XtTimeInForce tif,
                                std::string_view user_tag,
                                XtOffset offset,
                                std::string_view strategy_id = "",
                                bool is_forced_close = false);


    /// @brief 向 match_engine_ 提交一笔最终切片，并登记 order book 与 observer。
    /// @details
    /// - 提交成功时写入 runtime order book，使 in-flight projection 立即可见。
    /// - 使用当前 packed timestamp fan-out `on_order_submit`。
    /// @return 撮合引擎分配的订单 id（0 表示 match 内部拒单）。
    uint64_t submit_one_slice_(const char* code, bool is_buy, int64_t price_fp,
                               XtTimeInForce tif, std::string_view user_tag,
                               XtOffset sl_offset, int64_t sl_qty,
                               const xt_account::CloseableView& closeable_for_book,
                               StrategySid sid);

    // ── pending 可见性（event-axis）────────────────────────────
    // visible_at 与释放基准同挂 event 轴（自然日 epoch ns），跨午夜单调；
    // 旧 note_clock_advance_ 回跳重标绗带已随混合轴退役（倒流脏数据由
    // last_event_ns_ max-clamp 冻结，方向安全：只晚释不早释）。

    /// @brief margin_call_enabled 时每个 market tick 尾检查维持保证金。
    /// @details equity < Σ|pos|×per_unit_margin(mark)×ratio 触发强平；未归账 fill
    ///          （pending_fills_）存在时延后一 tick 评估，防旧快照误触/over-close。
    ///          in_liquidation_ 闩期：在途活动结清后残仓 → 重试强平（零成交/部分
    ///          成交/拒单路径收敛于此）；归 flat 释放。
    void check_margin_call_();

    /// @brief 强平：撤全部在途挂单 + 对手价 IOC 按 max_order_qty 分块平全部持仓。
    void execute_forced_liquidation_();

    /// @brief 全部 tracked symbol 持仓是否归零（long/short 两侧）。
    bool positions_flat_() const;

    /// @brief 观测 event 轴巨幅前跳（脏未来日期 tick）：前跳超阈值时计数 + 首次 warn。
    /// @details 不 clamp、不改 last_event_ns_ 推进——纯观测加固（对称 OrderManager
    ///          的 backward clamp warn；clean data 不可达）。首个 event（无基准）跳过。
    void note_event_forward_spike_(int64_t event_ns) noexcept;

    /// @brief 维持保证金需求：遍历持仓 symbol 求 Σ|long+short|×per_unit_margin(mark)×ratio。
    int64_t compute_maintenance_fp_() const;

    // ── 执行链（gateway-owned）─────────────────────────────────────
    XtHftMatchEngine match_engine_;                 ///< owning；撮合、延迟模型与 order delay wheel。
    // fee_book_ 必须先于 cost_applier_/risk_engine_ 声明并保持稳定地址。
    XtFeeRuleBook fee_book_;                        ///< owning；cost_applier_ 与 risk_engine_ 持非拥有指针。
    xt_fill_cost::FillCostApplier cost_applier_;     ///< owning；对本 tick trades_buf_ 应用费用与滑点成本。
    std::unique_ptr<XtRiskEngine> risk_engine_;      ///< owning；由 InitParams::risk 创建。
    XtRiskPolicy risk_policy_;                       ///< 风控策略开关快照。
    XtRiskGateway risk_gw_;                          ///< 风控入口；绑定 risk_engine_ 与 risk_policy_。

    // ── Domain 组件（non-owning，Runner 持有所有权）────────────────────
    xt_account::AccountStateStore* account_ = nullptr; ///< required；账户事件写入和 snapshot 来源。
    const XtCommodityCatalog* commodities_ = nullptr;  ///< required；合约规格与手续费查询。
    xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr; ///< required；订单 domain book。
    const xt_runtime::XtRuntimeMarketCache* market_cache_ = nullptr; ///< non-owning；涨跌停 venue-truth 来源（price_limit_enabled 时 submit 读）。
    bool price_limit_enabled_ = false;  ///< InitParams::match 快照；submit 越板拒开关。
    bool self_match_check_ = false;     ///< InitParams 快照；submit 前 STP 自成交预防开关。
    XtSymbolPriceBoundSTP stp_checker_; ///< owning；己方挂单价格边界（self_match_check 时维护）。
    std::unique_ptr<xt_account::AccountStoreView> account_view_;  ///< owning view wrapper；底层 account_ 非 owning。

    int64_t max_order_qty_fp_ = 0;  ///< 单次 submit 切片上限（fixed-point qty）；0 表示不限。
    int64_t session_begin_realized_fp_ = 0;  ///< 交易日初 realized 基线；daily_loss = realized_now − 本值。

    /// @brief 逐笔 fill 更新风险观测（净仓累计 + 峰值 + breach edge 检测）。
    void note_fill_for_risk_obs_(const XtTradeRecord& tr);

    // ── post-fill 风险观测（纯观测，不拦截）────────────────────────
    RiskObservability risk_obs_;
    std::unordered_map<std::string, int64_t> fill_net_fp_;  ///< per-code drained fill 带符号净仓
    std::unordered_map<std::string, bool> breach_state_;    ///< per-code 是否处于超限态（edge 检测）
    int64_t obs_max_pos_qty_fp_ = 0;    ///< risk.max_pos_qty 快照（fp）；<=0 关闭 breach 检测。

    // ── 爆仓模拟（margin_call_enabled 时活跃）──────────────────────
    bool margin_call_enabled_ = false;  ///< InitParams 快照；逐 tick 维持保证金检查开关。
    int64_t maintenance_ratio_fp_ = 0;  ///< 维持保证金系数（fixed-point）。
    bool daily_loss_check_enabled_ = false;  ///< risk.max_daily_loss > 0 快照；off 时 submit 路径免算 today_pnl。
    int64_t risk_max_order_qty_fp_ = 0;  ///< risk.max_order_qty 快照（fp）；强平分块 cap 取 engine/risk 两值严。
    bool in_liquidation_ = false;       ///< 强平在途闩；平仓 fill 全部归账前不重复触发。
    uint64_t margin_call_count_ = 0;    ///< 触发次数（诊断）。

    // ── Observer / reject sink(非拥有) ────────────────────────────────
    std::vector<xt_runtime::IHftEngineObserver*> observers_; ///< non-owning；按 add 顺序 fan-out。
    IRejectSink* reject_sink_ = nullptr; ///< non-owning；拒单落地接口，nullptr 表示忽略 reject report。

    // ── 时钟状态（每 tick 推进） ──────────────────────────────────────────
    uint64_t tick_idx_ = 0;     ///< market tick 计数；grid clock/EOF flush 不递增。
    uint32_t last_date_ = 0;    ///< 最近 clock/tick 的 trading date；0 表示尚未推进。
    uint32_t last_time_ms_ = 0; ///< 最近 clock/tick 的 HHMMSSmmm；current_ts() 来源。
    uint32_t last_action_date_ = 0; ///< 最近 clock/tick 的自然日（含 fallback）；EOF event 轴重建用。
    int64_t  last_event_ns_ = 0;  ///< 最近推进到的 event-axis ns（max-clamp 保单调）；pending 释放/visible base。
    uint64_t event_forward_spike_count_ = 0;  ///< event 轴巨幅前跳（脏未来日期）观测计数；不改行为。
    uint32_t pending_dispatch_date_ = 0; ///< 下一次 process_tick 的 fill 日期覆盖；0 表示未设置。
    uint32_t pending_dispatch_time_ms_ = 0; ///< 下一次 process_tick 的 HHMMSSmmm 覆盖。

    // ── 帧内暂存缓冲（每 tick 复用，无跨帧语义） ─────────────────────────
    std::vector<XtTradeRecord> trades_buf_;       ///< process_tick 帧内成交暂存；enqueue 后清空。
    std::vector<XtOrderExecUpdate> updates_buf_;  ///< process_tick 帧内订单更新暂存；drain 后清空。

    // multi-strategy routing sidecar：submit 写入 order_id -> strategy id，
    // drain_fills/drain_order_updates 反查后补回回报对象。backtest 单线程运行，不加锁。
    // 终态订单按 trading_date 标记，跨日 session_begin 清理上一日终态 entries，避免长期回测无界增长。
    /// @brief multi-strategy 回报路由的 order sidecar entry。
    struct OrderStrategyEntry {
        std::string sid;               ///< submit 传入的 strategy_id；可为空表示 single-strategy fallback。
        uint32_t sid_id = 0;           ///< submit 期 lookup 的 sid_id（0 = 未注册）；drain 反查 O(1)。
        uint32_t terminal_at_date = 0; ///< 0 表示 active；非 0 表示终态所在 trading date。
    };
    std::unordered_map<uint64_t, OrderStrategyEntry> order_strategy_; ///< order id -> strategy sidecar。
    uint32_t current_trading_date_ = 0; ///< 当前 session date；sidecar mark/prune 基准。

    // ── EOF delivered-view 投影（每 submit 建、delivered 终态即摘）──────────
    // 目的：EOF 收尾时按「已实际派发」口径为无终态在途单闭账。跟踪面只保留合成
    // Canceled 所需的最小字段 + delivered_filled 累计（drain_fills 每笔已交付 fill
    // 递增）；drain_order_updates 见 delivered 终态即 erase，投影稳态 = 当前活跃单。
    /// @brief 一笔在途单的 delivered-view 投影。
    struct DeliveredOrderState {
        bool     is_buy = true;                 ///< 方向；合成终态回报用。
        XtOffset close_offset = XtOffset::Open; ///< 开平语义；合成终态回报用。
        int64_t  price_fp = 0;                  ///< 报单价（fp）；合成终态回报用。
        int64_t  qty_fp = 0;                    ///< 报单总量（fp）；leaves = qty − delivered。
        int64_t  delivered_filled_fp = 0;       ///< 已实际派发给策略/账户的累计成交量。
        bool     terminal_delivered = false;    ///< 终态已盘中派发但该单仍有未可见 fill：投影不摘，待 fill 可见/EOF 补交付闭账。
        int64_t  claimed_filled_fp = 0;         ///< terminal_delivered 时终态携带的内部 filled（claim）；delivered 追平即闭合。
    };
    std::unordered_map<uint64_t, DeliveredOrderState> delivered_proj_; ///< order id -> delivered 投影。
    bool eof_closing_ = false; ///< EOF 收尾栅栏：置位后 submit 一律本地拒（begin_eof_closing）。

    // ── Pending fill queue（跨帧 fill 回程延迟模拟）─────────────────────
    /// @brief 已撮合但尚未到达策略可见时间的 fill。
    struct PendingFill {
        XtTradeRecord trade;       ///< match 已成交但尚未对 strategy 可见的 fill。
        int64_t visible_at_ns;     ///< 策略可见时刻，epoch ns。
    };
    std::deque<PendingFill> pending_fills_; ///< 跨 on_tick 保存尚未对 strategy 可见的 fills。

    /// @brief fill 驱动的 order update（Filled/PartiallyFilled）与对应 fill 同
    ///        visible_at 释放——update 立即可见而 fill 延迟会让策略在滞后窗口
    ///        读到「已 Filled 但仓位/冻结未变」的分裂状态。
    struct PendingUpdate {
        XtOrderExecUpdate update;  ///< 尚未到可见时间的 fill-driven update。
        int64_t visible_at_ns;     ///< 与对应 fill 同值的可见时刻，epoch ns。
    };
    std::deque<PendingUpdate> pending_updates_; ///< 延迟释放的 fill-driven updates。

    /// @brief 把 visible_at <= now 的 pending update 顺序释回 updates_buf_。
    void release_due_updates_(int64_t now_ns);

    /// @brief 某 order 是否仍有一笔未可见（EOF 会被 drop）的 pending fill。
    /// @details 未可见 = visible_at > last_event_ns_（flush 的 drain_order_updates
    ///          调用点上 last_event_ns_ 已被 advance_clock_only 推到 EOF cutoff）。
    ///          仅 EOF 收尾 hold-back 判据使用。
    bool order_has_unvisible_pending_fill_(uint64_t id) const;

    int64_t fill_latency_ns_ = 0; ///< fallback fill 回程延迟 ns；latency model 存在时不使用。

    bool initialized_ = false; ///< true 表示 init() 已绑定 domain 并完成执行链重建。
};
