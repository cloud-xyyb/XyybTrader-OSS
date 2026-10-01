#pragma once

#include "XtStrategyBase.h"
#include "XtStrategyInitConfig.h"
#include "api/IExecutionGateway.h"
#include "api/IStrategyContext.h"
#include "api/IntentResult.h"
#include "xt_metrics/XtStrategyMetricsAggregator.h"
#include "xt_strategy_engine/IStrategyExecSink.h"
#include "xt_strategy_engine/XtStopOrderBook.h"
#include "xt_strategy_engine/view/XtRuntimeView.h"

// XtRunnerSignals 由 runner 持有；context 只保存 non-owning pointer，避免 header 循环依赖。
struct XtRunnerSignals;

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>

class XtRollEngine;  // xt_util（global scope）

namespace xt_strategy_engine {
class XtDeclaredSymbolRegistry;
class IStrategyTelemetrySink;
}

/// @brief 策略回调内访问 runtime 的统一 context。
///
/// Context 是策略调用下单、行情/账户查询、参数读取、指标上报和 userdata 的入口。
///
/// @note 引擎在同一 event-loop 线程内串行调用策略回调与 context API；默认路径无锁。
///       HFT 实盘若由指标采集线程读取 metrics，需在 EventLoop 启动前调用
///       `enable_concurrent_metrics()`。
/// @note `roll_engine` 非空时，buy_fp/sell_fp 与持仓查询按逻辑标的映射到物理合约
///       （如 ag@main → ag2206）。
class XtStrategyContext final : public IStrategyContext,
                                public xt_strategy_engine::IStrategyExecSink {
public:
    /**
     * @brief 策略参数读写端口（parameter read/write port）。
     *
     * @warning 实现方必须保证 `get` / `set` / `version` 线程安全；热更新可能来自
     *          控制面或配置监控线程，策略回调在 event-loop 线程读取参数。
     * @note `version()` 必须单调递增，用于 context 检测参数变更。
     */
    class IParamPort {
    public:
        virtual ~IParamPort() = default;
        /** @brief 读取参数值（by key），找到返回 true 并写入 out。 */
        virtual bool get(const std::string& key, std::string& out) const = 0;
        /** @brief 设置参数值（by key），成功返回 true。 */
        virtual bool set(const std::string& key, const std::string& value) = 0;
        /** @brief 当前参数版本号（单调递增，用于变更检测）。 */
        virtual uint64_t version() const = 0;
    };

    /// @brief 构造配置聚合体（construction config aggregate）。
    ///
    /// 调用方必须提供 strategy；HFT/CTA 直连或 intent 模式还必须提供 runtime_view。
    /// SEL selector 可以只提供 on_weights_fn；此时 read/query API 返回安全默认值
    /// （nullptr/0）并 warn-once，submit/cancel fail-closed——任何路径不做 null deref。
    struct Config {
        /// @note 签名不携带 ExecFlags（有意）：市价意图在 submit_impl_ 已 resolve
        ///       为激进限价 IOC，intent 链只消费解析后的 price_fp/tif；
        ///       `ExecFlag::Market` 分类 tag 仅直连路径透传 gateway wire。
        using IntentFn = std::function<xt_runtime::IntentResult(
            const char* strategy_id, const char* code,
            double prev_virtual_pos, double new_virtual_pos,
            int64_t price_fp, XtTimeInForce tif, const char* tag)>;
        using WeightsFn = std::function<void(
            const std::string& strategy_id,
            const std::unordered_map<std::string, double>& weights)>;

        XtStrategyBase* strategy = nullptr;                    ///< 策略实例；必须非空。
        xt_runtime::IExecutionGateway* exec_gateway = nullptr; ///< 直连下单网关；direct mode 使用。
        IntentFn on_intent_fn;                                 ///< intent mode 回调；由 context 推进 virtual_pos。
        WeightsFn on_weights_fn;                               ///< selector mode 权重转发回调；可为空。
        xt_strategy_engine::XtRuntimeView* runtime_view = nullptr; ///< 行情/账户/时间运行时视图。
        const XtStrategyInitConfig* init_config = nullptr;         ///< YAML 初始参数；context 不拥有。
        IParamPort* param_port = nullptr;                          ///< 参数热更新端口；可为空。
        const ::XtRollEngine* roll_engine = nullptr;               ///< 逻辑标的到物理合约映射；可为空。
        /// 策略 telemetry 下沉（report_metric JSONL + 直连 intent trace）；non-owning。
        /// live runner 注入 recorder adapter；BT/单测留空 → 两路径 noop。
        xt_strategy_engine::IStrategyTelemetrySink* telemetry_sink = nullptr;

        std::string strategy_id; ///< 策略唯一标识；用于 multi-strategy 路由和指标标签。

        /// @brief multi-strategy 下按 strategy_id 过滤 get_open_orders。
        ///
        /// false 表示保留 single-strategy / backtest 语义，返回 runtime view 可见的全部 open orders。
        bool route_by_sid = false;
        /// @brief declare_position_interest 的目标 registry；nullptr 时该 API 为 no-op。
        xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry = nullptr;
        /// @brief 是否把 signed in-flight 投影进 get_position_fp。
        ///
        /// live mode 可启用防重单窗口；backtest 同步撮合没有 in-flight window，保持 false。
        bool project_inflight = false;
        /// @brief 本地条件单开关（config opt-in，默认关保 golden）。
        ///
        /// false 时 submit_stop_fp/submit_trailing_stop_fp 返 0 +
        /// CapabilityUnsupported，engine hook 早退。
        bool conditional_orders_enabled = false;
    };

    explicit XtStrategyContext(const Config& cfg);
    ~XtStrategyContext() override = default;

    /// @brief 启用并发指标模式（双缓冲 + seqlock）。
    ///
    /// 默认使用单线程无锁路径（回测/CTA 均为单线程）。
    /// HFT 实盘 EventLoop 启动前调用此方法，启用并发安全的 observe() 路径。
    void enable_concurrent_metrics() noexcept;

    // ── IStrategyContext 实现（overrides） ───────────────────────────────
    //
    // 本组方法是策略 plugin 看到的唯一 runtime facade。实现层负责把逻辑标的解析、
    // multi-strategy sid 隔离、in-flight 投影和 recorder/metric 注入收束在 context 内；
    // plugin 不应绕过 context 直接访问 gateway、runtime view 或 param port。

    // 交易指令。

    /// @brief 提交买单；direct 模式直通 exec_gateway，intent/weights 模式转为目标仓位意图。
    ///
    /// @return order_id；0 表示无可路由下单通道、风控/网关拒单或输入无效。
    uint64_t buy_fp(const char* code,
                    int64_t price_fp,
                    int64_t qty_fp,
                    XtTimeInForce tif,
                    const char* tag,
                    XtOffset offset) override;
    /// @brief 提交卖单；与 buy_fp 使用同一 submit gate、逻辑标的解析和 recorder 标注路径。
    ///
    /// @return order_id；失败语义同 buy_fp。
    uint64_t sell_fp(const char* code,
                     int64_t price_fp,
                     int64_t qty_fp,
                     XtTimeInForce tif,
                     const char* tag,
                     XtOffset offset) override;
    /// @brief direct 模式撤单入口；intent/weights 模式无可路由撤单通道时 warn-once 后 no-op。
    void cancel(uint64_t order_id) override;

    // 本地条件单（engine tick 驱动；契约见 IStrategyContext 声明处）。

    uint64_t submit_stop_fp(const char* code, bool is_buy, int64_t trigger_price_fp,
                            int64_t qty_fp, XtOffset offset, const char* tag) override;
    uint64_t submit_trailing_stop_fp(const char* code, bool is_buy, int64_t distance_fp,
                                     int64_t qty_fp, XtOffset offset, const char* tag,
                                     int64_t seed_price_fp) override;
    bool cancel_stop(uint64_t stop_id) override;

    // 只读查询：行情与订单。

    /// @brief 返回最新 tick snapshot；raw key miss 后按 roll_engine 回退到物理合约 key。
    ///
    /// @return runtime view 缺失或无行情时返回 nullptr。
    const XtTick* get_last_tick(const char* code) override;
    /// @brief 返回最新成交价（fp）；raw key miss 后按 roll_engine 回退到物理合约 key。
    ///
    /// @return runtime view 缺失或无价格时返回 0。
    int64_t get_last_price_fp(const char* code) override;
    /// @brief 复制近期 tick ring buffer 窗口；raw key miss 后回退到物理合约 key。
    ///
    /// @return 实际写入数量；runtime view 缺失或无数据时返回 0。
    size_t get_ticks(const char* code, XtTick* buf, size_t max) override;
    /// @brief 复制指定 period 的 K 线窗口；raw key miss 后回退到物理合约 key。
    ///
    /// @return 实际写入数量；runtime view 缺失或无数据时返回 0。
    size_t get_bars(const char* code, const char* period, XtBar* buf, size_t max) override;

    /// @brief 查询 open orders；按物理合约过滤，并在 route_by_sid 时只返回本 strategy_id 的订单。
    ///
    /// live gateway 会先把物理切片单合并为策略可见的逻辑 order id；buf 为空或 max=0 返回 0。
    size_t get_open_orders(const char* code, XtOrderExecUpdate* buf, size_t max) override;

    // 只读查询：持仓、账户与合约。

    /// @brief 返回 confirmed 仓位，可选叠加本策略 signed in-flight，用于 live 防重单。
    ///
    /// @return runtime view 缺失或无仓位时返回 0。
    int64_t get_position_fp(const char* code) override;
    /// @brief 返回 confirmed 持仓均价（fp），查询前把逻辑标的解析为物理合约。
    ///
    /// @return runtime view 缺失或无仓位时返回 0。
    int64_t get_position_avg_price_fp(const char* code) override;
    /// @brief 返回 confirmed 持仓浮盈（fp），查询前把逻辑标的解析为物理合约。
    ///
    /// @return runtime view 缺失或无仓位时返回 0。
    int64_t get_position_unrealized_fp(const char* code) override;
    /// @brief 当前 (strategy_id, code) 未成交挂单的 signed qty；backtest 或未启用投影时返回 0。
    int64_t projected_inflight_fp(const char* code) override;
    /// @brief 写出单合约持仓明细；仅返回第一条 physical-code detail。
    ///
    /// @return buf 为空、max=0、runtime view 缺失或无明细时返回 0；成功写入 1。
    size_t get_position_detail(const char* code,
                               xt_runtime::XtStraPositionDetail* buf,
                               size_t max) override;
    /// @brief 返回策略可见账户可用资金（fp）。
    ///
    /// @return runtime view 缺失时返回 0。
    int64_t get_cash_fp() override;
    /// @brief 返回策略可见账户权益（fp）。
    ///
    /// @return runtime view 缺失时返回 0。
    int64_t get_equity_fp() override;
    /// @brief 返回合约基础信息；catalog 按物理合约查，context 按调用 code 缓存结果。
    ///
    /// @return code 为空、runtime view 缺失或合约不存在时返回 nullptr；返回指针不要跨 callback 持久缓存。
    const xt_runtime::XtStraContractInfo* get_contract_info(const char* code) override;

    // 只读查询：时间。

    /// @brief 返回 action date（yyyymmdd）；runtime view 缺失时返回 0。
    uint32_t get_date() override;
    /// @brief 返回 trading date（yyyymmdd）；runtime view 缺失时返回 0。
    uint32_t get_trading_date() override;
    /// @brief 返回 action time（HHMMSSMMM）；runtime view 缺失时返回 0。
    uint32_t get_time_ms() override;

    // 策略配置：参数读取。

    // 参数读取优先解析 `strategy.<id>.<key>`，再回退裸 key；
    // 标量 getter 使用严格解析，策略热路径应缓存读取结果。
    /// @brief 读取字符串参数；缺失时返回 def，def 为 nullptr 时返回空串。
    std::string get_param_str(const char* key, const char* def) override;
    /// @brief 读取 int64 参数；缺失、非完整十进制或越界时返回 def。
    int64_t get_param_int(const char* key, int64_t def) override;
    /// @brief 读取 double 参数；缺失、非完整浮点字面量或解析异常时返回 def。
    double get_param_double(const char* key, double def) override;
    /// @brief 读取 bool 参数；接受 1/0、true/false、yes/no、on/off，其余返回 def。
    bool get_param_bool(const char* key, bool def) override;

    // 策略配置：订阅与持仓兴趣声明。

    /// @brief 订阅声明写入 context 本地集合；engine dispatch 时据此过滤 tick/bar。
    void sub_tick(const char* code) override;
    /// @brief 声明 bar 订阅并设置 ring buffer 容量；runtime view 缺失时只保留 dispatch 过滤声明。
    void sub_bar(const char* code, const char* period, size_t max) override;

    /// @brief 声明策略关心的持仓标的；registry 缺失时 no-op，便于 single-strategy/backtest 复用。
    void declare_position_interest(const char* code) override;

    // 观测出口：日志、指标与下一笔报单诊断字段。
    //
    // 日志统一归入 STRA category；metric 同时进入本地聚合器，live 模式额外写 recorder。
    void log_debug(const char* msg) override;
    void log_info(const char* msg) override;
    void log_warn(const char* msg) override;
    /// @brief 上报 strategy metric；空 name 丢弃。
    ///
    /// name 必须来自稳定存储，metrics 聚合器会在秒级 drain 前持有该标识。
    void report_metric(const char* name, double val) override;
    /// @brief per-ctx 标注下一笔 submit 的 recorder reason；submit_impl_ 消费后清空。
    void set_next_order_reason(const char* reason) override;

    // 进程内状态：userdata。

    /// @brief 进程内策略 KV；graceful stop 时由 runner 经 StrategyUserdataStore 落盘、
    /// 启动装配期回灌（见 IStrategyContext 契约）；crash（kill -9）丢失。
    ///
    /// 空 key 忽略；val 为 nullptr 时保存为空串。
    void save_userdata(const char* key, const char* val) override;
    /// @brief 读取进程内策略 KV；miss 或空 key 时原样返回 def。
    ///
    /// 命中时返回 ABI 兼容的 const char*；指针只保证到本 context 下一次 load_userdata 调用前有效。
    const char* load_userdata(const char* key, const char* def) override;

    // ── 引擎侧事件派发（engine-facing event dispatch）──────────────

    /** @brief 初始化策略：init_phase_ 闸门内调 strategy.on_init，随后合成
     *         channel_ready("MD"/"TD")。strategy->init(cfg) 由 runner 先行另调。 */
    void on_init();
    /** @brief 派发 tick 到策略 on_tick 回调（含订阅检查）。 */
    void dispatch_tick(const XtTick& tick);
    /** @brief 派发 bar 到策略 on_bar 回调（含订阅检查；空订阅 = 全收，BT/单策略语义不变）。
     *  @note ctx 级过滤按 code（sub_bar 只登记 code，period 仅作容量参数）；
     *    引擎级 subscribed_bars_ 先按 (code,period) 过滤。策略 sub_bar 的 code 集
     *    应覆盖引擎转发集，否则差集 bar 在 ctx 层被滤（多策略隔离的本意）。 */
    void dispatch_bar(const char* code, const char* period, const XtBar& bar);
    /** @brief 派发 bar 开盘事件到策略 on_bar_open 回调（含订阅检查）。 */
    void dispatch_bar_open(const char* code, const char* period, int64_t open_px_fp);
    /** @brief 派发委托队列快照到策略回调。 */
    void on_order_queue(const XtOrderQueue& q);
    /** @brief 派发逐笔委托到策略回调。 */
    void on_order_detail(const XtOrderDetail& d);
    /** @brief 派发逐笔成交到策略回调。 */
    void on_transaction(const XtTransaction& t);
    /** @brief 通知策略 session 开始（交易日切换）。 */
    void on_session_begin(uint32_t trading_date);
    /** @brief 通知策略 session 结束。 */
    void on_session_end(uint32_t trading_date);
    /** @brief 通知策略换月事件（旧主力 → 新主力）。 */
    void on_roll(const char* old_code, const char* new_code, uint32_t trading_date,
                 int64_t old_last_price_fp = 0);
    /** @brief 通知策略通道就绪（实盘连接建立）。 */
    void on_channel_ready(const char* channel);
    /** @brief 通知策略通道断开（实盘连接丢失）。 */
    void on_channel_lost(const char* channel, int reason);
    /** @brief 通知策略参数已更新。 */
    void on_params_updated(uint64_t version);

    // ── 条件单 engine hook（engine worker 线程，与策略回调同线程）─────────

    /// @brief per-tick 触发判定：推进 trailing 水位 + 对 Armed 命中执行 fire
    ///        （clamp → submit_impl_ → 按模式 consume/Fired）。
    /// @note engine 在派发策略 on_tick 之前调用（stop 优先于策略新意图）；
    ///       CTA 引擎必须喂 remapped tick（逻辑 code + 复权价）。
    void check_conditional_orders(const XtTick& tick);

    /// @brief 终态回报喂入（HFT direct 路径 Fired 状态机驱动）。
    ///
    /// engine 在 on_order_update 的 sid 路由处调用（live event loop 直调与
    /// BT drain 共同入口）；非终态与 unknown/duplicate id 幂等忽略。
    void notify_order_update_for_stops(const XtOrderExecUpdate& upd);

    /// @brief roll change 清仓：erase 该 code 全部条件单（engine 在 on_roll
    ///        fan-out **之前**调用，避免 wipe 策略刚重挂的 stops）。
    /// @return 清掉的数量（>0 时已 warn 日志，策略需在 on_roll 重挂）。
    std::size_t clear_stops_for_code(const char* code);

    /// @brief engine per-tick 早退信号：开关开且 book 非空（含 Fired——水位
    ///        与终态仍需驱动）。header-inline，空 book 零函数调用成本。
    [[nodiscard]] bool has_armed_stops() const noexcept {
        return conditional_orders_enabled_ && !stop_book_.empty();
    }

    /// @brief 本 context 的条件单开关（与 engine 同源 yaml）。
    /// @note engine 装配期据此校验双侧一致；`has_armed_stops()` 依赖 book 非空，
    ///       证明不了配置一致。
    [[nodiscard]] bool conditional_orders_enabled() const noexcept {
        return conditional_orders_enabled_;
    }

    // ── IStrategyExecSink 实现（engine/gateway 执行回调）──────────────
    //
    // IStrategyContext 必须保持第一基类；IStrategyExecSink 是第二个进程内接口，
    // 多继承下 sink 指针必须通过 static_cast<IStrategyExecSink*> 取得 adjusted address。

    /// @brief 派发订单回报到策略 on_order。
    void on_order(const XtOrderExecUpdate& upd) override;
    /// @brief 派发成交回报到策略 on_fill。
    void on_fill(const XtFillEvent& fill) override;
    /// @brief 派发账户变更到策略 on_account。
    void on_account(const XtAccountUpdate& upd) override;

    /// @brief 启动期注入 virtual_pos_fp_ 初值。
    ///
    /// runner / ops 工具可在策略首次决策前从 account NET 或 snapshot 文件恢复
    /// per-strategy logical position，避免重启后从 0 仓位开始重复下单。
    ///
    /// qty_fp 使用 context 维度，必须与 buy_fp/sell_fp 的 qty_fp 口径一致。
    /// @note 仅 `on_intent_fn_` 路径消费 `virtual_pos_fp_`：intent submit 会推进/读取它；
    ///       direct gateway 路径不消费，调用本方法无效果。
    void set_initial_virtual_pos(const char* code, int64_t qty_fp);

    /// @brief 暴露 virtual_pos_fp_ 只读快照（snapshot auto-dump）。
    ///
    /// runner 在 session_end / SIGTERM 时 dump 各策略 virtual_pos 到 snapshot.json。
    /// @warning copy 本身无同步：必须在 worker 已停止或与策略回调同线程时调用，
    ///          与并发写（intent 路径 buy_fp/sell_fp）同时调用是 data race。
    std::unordered_map<std::string, int64_t> snapshot_virtual_pos() const {
        return virtual_pos_fp_;
    }

    /// @brief 暴露 userdata_ 只读快照（graceful-stop dump）。
    ///
    /// runner 在 graceful shutdown（event loop 已停）时 dump 各策略 userdata 到
    /// per-sid JSON 文件，重启装配期回灌。SDK 保持零 IO：copy 交给 runner 落盘。
    /// @warning copy 本身无同步：契约与 snapshot_virtual_pos() 一致——必须在 worker
    ///          已停止或与策略回调同线程时调用，与并发写（save_userdata）同时调用是
    ///          data race。
    std::unordered_map<std::string, std::string> snapshot_userdata() const {
        return userdata_;
    }

    /// @brief 装配期回灌 userdata_（restore before on_init）。
    ///
    /// runner 从 per-sid 快照文件读回后，在 on_init 派发前、worker 未启动时调用；
    /// 直接覆盖 userdata_。策略随后在 on_init/on_tick 通过 load_userdata 读回。
    /// @warning 必须在 worker 启动前（同装配主线程）调用；worker 运行后调用与
    ///          save_userdata/load_userdata 是 data race。若 userdata_ 非空，本方法
    ///          log warn 后覆盖（镜像 set_initial_virtual_pos 的竞态暴露风格）。
    void restore_userdata(std::unordered_map<std::string, std::string> kv);

    /** @brief 是否调用过 sub_tick（tick subscribed flag，供 XtCtaEngine 查询）。 */
    bool has_tick_subscription() const noexcept { return tick_subscribed_; }

    /// @brief 策略唯一标识；multi-strategy 下作为回报路由 key。
    const std::string& strategy_id() const noexcept { return strategy_id_; }

    /// @brief 缓存的 raw StrategySid id（uint32）；与 strategy_id() 对偶，供 HFT 回报按
    ///        sid_id 路由，免去 uint32→string→线性扫的 round-trip。
    /// @pre ensure_sid_resolved() 已在 engine 装配期调用（registry 已 freeze）；否则返回 0。
    uint32_t resolved_sid_id() const noexcept { return resolved_sid_id_; }

    /// @brief 装配期（registry freeze 后、单线程）强制解析一次 sid_id，使 resolved_sid_id()
    ///        在回报路由热路径上只读缓存值，与 string 路由读 immutable strategy_id_ 对称、无竞态。
    void ensure_sid_resolved() { (void)resolved_sid_raw_(); }
    /** @brief 排出策略指标快照（report 采集用）。 */
    ct::metrics::XtStrategyMetricsSnapshot drain_strategy_metrics();

    /// @brief 注入 runner 级 submit gates。
    ///
    /// Non-owning；runner 必须保证 `s` 生命周期覆盖 context。该指针在启动装配期注入，
    /// 不支持运行期热切换；传入 nullptr 时 `can_submit()` 仅检查 control enabled gate。
    void set_signals(const XtRunnerSignals* s) noexcept { signals_ = s; }

    /// @brief 实现 `IStrategyContext::can_submit()` 的 runner gate AND 语义。
    bool can_submit() const override;

    /// @brief 最近一次 buy_fp/sell_fp 本地受理结果；见 IStrategyContext 契约。
    [[nodiscard]] xt_runtime::XtRejectCode last_reject_code() const noexcept override {
        return last_reject_code_;
    }

    /// @brief 控制面启停策略提交；false 时 can_submit() fail-closed。
    void set_control_enabled(bool enabled) noexcept {
        control_enabled_.store(enabled, std::memory_order_release);
    }

    /// @brief SEL selector 输出目标权重；按 strategy_id 单维转发给 runner 注入的 weights handler。
    void set_target_weights(
        const std::unordered_map<std::string, double>& weights) override;

    /// @brief SEL 调度本次 fire 的计划时刻（epoch ms）；调度器在派发 on_schedule
    ///        前注入，live/backtest 同路径，让策略可测调度延迟。
    [[nodiscard]] int64_t current_fire_time_ms() const noexcept override {
        return current_fire_time_ms_;
    }
    void set_current_fire_time_ms(int64_t fire_ms) noexcept {
        current_fire_time_ms_ = fire_ms;
    }

private:
    static XtStrategyBase& require_strategy(XtStrategyBase* strategy, const char* where);
    static bool is_valid_fp_input(const char* code, int64_t price_fp, int64_t qty_fp);

    /// @brief buy_fp/sell_fp 共享实现：roll 解析 → 市价 resolve → guard →
    ///        intent/直连分发 + last_reject_code 记录。
    uint64_t submit_impl_(const char* code, bool is_buy, int64_t price_fp, int64_t qty_fp,
                          XtTimeInForce tif, const char* tag, XtOffset offset);
    /// @brief 本地预检；过检返 NoError，否则返对应拒因（无执行通道→CapabilityUnsupported /
    /// init 阶段→TradingNotReady / 非法入参→InvalidInput）。
    xt_runtime::XtRejectCode guard_submit(const char* code, int64_t price_fp, int64_t qty_fp, bool is_buy) const;
    bool is_subscribed(const char* code) const;

    const std::string* find_param_value(const char* key) const;
    /// @brief 懒解析并缓存当前 strategy_id 对应的 StrategySid raw id。
    ///
    /// 未接入 registry 的 single-strategy / backtest 路径返回 0，表示按 all-sid 读取。
    uint32_t resolved_sid_raw_();
    static bool parse_bool_value(const std::string& value, bool def);
    /// 逻辑标的 → 物理合约（持仓/价格查询用；非逻辑标的原样返回）。
    const char* resolve_physical_(const char* code) const;
    /// read API 的 runtime_view 守卫：null 时 warn-once 并返回 false（调用方返安全默认）。
    bool view_or_warn_();

    /// 条件单触发执行：clamp（Close 类按可平量）→ price_fp=0 经 submit_impl_
    /// resolve → 按模式转移（HFT on_fired / CTA consume）；本地拒保留 Armed。
    void fire_stop_(uint64_t stop_id);
    /// 触发时可平量：HFT direct 走 pos_detail 侧向 closeable（回退方向净仓），
    /// CTA intent 走 virtual_pos_fp_ 方向可减量（与 delta 语义同源账本）。
    int64_t stop_closeable_qty_(const xt_strategy_engine::StopSlot& s);
    /// 条件单可观测（stop-order observability）：counter event / armed gauge。
    /// metrics target 关闭时（XT_HAVE_METRICS 未定义）为 no-op。
    void stop_emit_event_(const char* event, uint64_t delta = 1) const;
    void stop_emit_armed_gauge_() const;

    // ── 注入依赖（injected dependencies） ──────────────────
    XtStrategyBase* strategy_ = nullptr;                    ///< 策略实例；non-owning。
    xt_runtime::IExecutionGateway* exec_gateway_ = nullptr; ///< 直连下单网关（direct exec gateway）
    Config::IntentFn on_intent_fn_;                         ///< intent mode 回调；可为空。
    Config::WeightsFn on_weights_fn_;                       ///< SEL 目标权重转发回调；可为空。
    xt_strategy_engine::XtRuntimeView* runtime_view_ = nullptr;   ///< 运行时视图（runtime view）
    const XtStrategyInitConfig* init_config_ = nullptr;   ///< 初始参数（init params from YAML）
    IParamPort* param_port_ = nullptr;                    ///< 参数热更新端口（hot-reload port）
    const ::XtRollEngine* roll_engine_ = nullptr;         ///< 换月引擎（roll engine，逻辑→物理映射）
    xt_strategy_engine::IStrategyTelemetrySink* telemetry_sink_ = nullptr;  ///< telemetry 下沉（null = noop）
    /// multi-strategy 下按 strategy_id 过滤 get_open_orders；false 保留 single-strategy/backtest 全量视图。
    bool route_by_sid_ = false;
    /// declare_position_interest 的转发目标；nullptr 时 no-op。
    xt_strategy_engine::XtDeclaredSymbolRegistry* declared_registry_ = nullptr;
    /// runner 级 submit gates；nullptr 时 can_submit() 只检查 control_enabled_。
    const XtRunnerSignals* signals_ = nullptr;

    // ── 策略状态（strategy state） ──────────────────
    std::string strategy_id_;              ///< 策略唯一标识（strategy unique ID）
    bool project_inflight_ = false;        ///< true 时把 signed in-flight 投影进 cur_pos。
    bool init_phase_ = false;              ///< 初始化阶段标志（on_init 执行中为 true，阻止下单）
    uint64_t last_param_version_ = 0;      ///< 上次处理的参数版本（用于变更检测）
    uint32_t resolved_sid_id_ = 0;         ///< 缓存的 raw StrategySid id；sid_resolved_ 后有效。
    bool sid_resolved_ = false;            ///< resolved_sid_raw_ 是否已完成首次解析。

    // control-plane strategy enable gate；on_params_updated 写，can_submit() hot path 读。
    // 默认 enabled=true；backtest/no ParamDomain 路径保持允许提交。
    std::atomic<bool> control_enabled_{true};

    /// 最近一次 buy_fp/sell_fp 本地受理结果；ctx 单线程契约下 plain member。
    xt_runtime::XtRejectCode last_reject_code_ = xt_runtime::XtRejectCode::NoError;

    /// 下一单决策原因（per-ctx 承载；submit_impl_ 发单窗口注入 TLS、出口清两者，
    /// 杜绝「设置后未下单」经进程 TLS 串到同 worker 线程其他策略的下一单）。
    std::string next_order_reason_;

    /// SEL 调度本次 fire 的计划时刻（epoch ms）；0 = 非 SEL 或尚未 fire。
    int64_t current_fire_time_ms_ = 0;

    /// cancel 在 intent/weights 模式不可路由的 warn-once 闩（防刷屏）。
    bool cancel_unroutable_warned_ = false;

    /// read API 在 weights-only ctx（无 runtime_view）被调用的 warn-once 闩。
    bool null_view_warned_ = false;

    // ── 订阅管理（subscription management） ──────────────────
    std::unordered_set<std::string> subscriptions_;       ///< 已订阅合约集合（subscribed codes）
    bool tick_subscribed_ = false;                        ///< tick 订阅标志（sub_tick 被调用后置 true，供 XtCtaEngine 读取）
    mutable std::unordered_map<std::string, bool> sub_cache_; ///< 订阅匹配缓存（subscription match cache，避免重复模糊匹配）

    // ── Router 模式虚拟持仓（virtual position for router mode） ──────────────────
    std::unordered_map<std::string, int64_t> virtual_pos_fp_; ///< 虚拟净仓 fp（virtual net position per code）
    uint64_t next_virtual_id_ = 0;                            ///< 虚拟 order_id 分配计数器（virtual order ID counter）

    // ── 本地条件单（engine-hosted stop orders） ──────────────────
    bool conditional_orders_enabled_ = false;         ///< config opt-in，默认关
    xt_strategy_engine::XtStopOrderBook stop_book_;   ///< per-ctx 条件单簿（worker 线程独占）
    std::vector<uint64_t> stop_fired_scratch_;        ///< check 命中收集 scratch（免 per-tick 分配）

    // ── SEL selector 状态（selector state） ──────────────────

    // ── 缓存与持久化（cache & persistence） ──────────────────
    std::unordered_map<std::string, std::string> userdata_;   ///< 策略自定义键值对（user data kv store）
    std::unordered_map<std::string, xt_runtime::XtStraContractInfo> contract_cache_; ///< 合约信息缓存（contract info cache）
    std::string load_userdata_buf_;        ///< load_userdata 返回值缓冲（保证指针在下次调用前有效）
    mutable std::string param_value_buf_;  ///< find_param_value 返回值缓冲（param_port 读取用）
    mutable std::string resolve_phys_buf_; ///< resolve_physical_ 返回值缓冲（roll engine 映射用）
    ct::metrics::XtStrategyMetricsAggregator metrics_; ///< 策略指标聚合器（metrics aggregator）
};
