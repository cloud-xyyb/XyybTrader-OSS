#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>    // set_target_weights weights map

#include "xt_core/market/XtTick.h"
#include "xt_core/market/XtBarStruct.h"
#include "xt_core/order/XtOrderTypes.h"        // XtTimeInForce, XtOrderExecUpdate, XtOffset
#include "api/XtStrategyRuntimeTypes.h"
#include "api/XtRejectCode.h"           // last_reject_code 返回码

/**
 * @brief 统一策略上下文接口（unified strategy context interface）。
 *
 * @details
 * 策略通过 context 发起下单/撤单，读取行情、订单、持仓、账户、时间、参数，
 * 并上报日志与指标。
 *
 * @warning 策略只能在 engine callback 或 owner event-loop thread 内调用 context。
 * context 不承诺跨线程安全；跨线程使用必须先投递回 owner thread。
 */
class IStrategyContext {
public:
    virtual ~IStrategyContext() = default;

    // 报单有效期别名（time-in-force aliases，方便策略层直接使用）
    static constexpr XtTimeInForce TIF_GTC = XtTimeInForce::GTC;
    static constexpr XtTimeInForce TIF_IOC = XtTimeInForce::IOC;  // FAK
    static constexpr XtTimeInForce TIF_FOK = XtTimeInForce::FOK;

    // ── 交易接口（trading API） ──────────────────────────────────────────
    // 固定点约定：所有价格/数量均为 fp（×kOrderExecFpScale = 1e6）。
    // 返回值：order_id（0 表示被风控/网关拒单）。
    // 推荐做法：对 XtTick 的 bid_price_fp/ask_price_fp 直接使用，
    //   自行计算的 double 用 order_exec_to_fp() 转换（见 xt_core/order/XtOrderTypes.h）。

    /** @brief 限价买入（limit buy）。 */
    virtual uint64_t buy_fp(const char* code,
                            int64_t     price_fp,
                            int64_t     qty_fp,
                            XtTimeInForce tif,
                            const char* tag,
                            XtOffset    offset) = 0;

    /** @brief 限价卖出（limit sell）。 */
    virtual uint64_t sell_fp(const char* code,
                             int64_t     price_fp,
                             int64_t     qty_fp,
                             XtTimeInForce tif,
                             const char* tag,
                             XtOffset    offset) = 0;

    /** @brief 按 order_id 撤单（cancel by ID）。 */
    virtual void cancel(uint64_t order_id) = 0;

    // ── 行情查询（market data query） ───────────────────────────────────
    //
    // 逻辑标的（如 "ag@main"，roll_engine 启用时）查询语义：
    //   - 行情查询（last_tick/last_price/ticks/bars）：raw 优先，miss 回退
    //     按物理合约重查——覆盖 HFT（cache 物理键）与 CTA roll（cache 逻辑
    //     remap 键）两种布局。
    //   - 订单/持仓/合约信息查询（get_open_orders/get_position_*/
    //     get_contract_info）：store 恒为物理键，查询前解析到物理合约
    //     （与下单路径 submit 同口径）。

    /** @brief 获取合约最新 tick 快照（last tick snapshot），无数据时返回 nullptr。 */
    [[nodiscard]] virtual const XtTick* get_last_tick(const char* code) = 0;

    /** @brief 获取合约最新成交价（last price，fp）。 */
    [[nodiscard]] virtual int64_t get_last_price_fp(const char* code) = 0;

    /** @brief 获取合约近期 tick 历史（ring buffer 窗口），返回实际写入数量。 */
    [[nodiscard]] virtual size_t get_ticks(const char* code, XtTick* buf, size_t max) = 0;

    /** @brief 获取合约 K 线历史（bar history by period），返回实际写入数量。 */
    [[nodiscard]] virtual size_t get_bars(const char* code, const char* period,
                            XtBar* buf, size_t max) = 0;

    // ── 订单查询（order query） ──────────────────────────────────────────

    /** @brief 获取在途订单列表（open order list），按合约过滤，返回实际写入数量。 */
    [[nodiscard]] virtual size_t get_open_orders(const char* code,
                                   XtOrderExecUpdate* buf, size_t max) = 0;

    // ── 持仓/账户（position & account，查询只读，不维护状态） ──────────────

    /** @brief 净仓（fp）：正值=多仓，负值=空仓，0=无仓位。
     *  @note live 启用 in-flight 投影时返回值已含本策略未成交挂单的带符号净量；
     *  需要纯 confirmed 仓位时减去 projected_inflight_fp(code)。 */
    [[nodiscard]] virtual int64_t get_position_fp(const char* code) = 0;

    /** @brief 在途净仓投影（fp）：启用 in-flight 投影时=该 (sid,code) 未成交
     *  挂单带符号净量(买+/卖-)；单策略 backtest / 未启用投影的 live 恒 0。
     *  @warning 恒 0 的形态下用它做开/平防重单会静默失效（挂单不计入敞口）；
     *  需要全模式一致的敞口投影用 xt_projected_position_fp /
     *  xt_open_orders_net_leaves_fp（本文件尾 helper，get_open_orders 口径）。
     *  本接口保留给「还原纯 confirmed」的减法口径（见 get_position_fp @note）。 */
    [[nodiscard]] virtual int64_t projected_inflight_fp(const char* code) = 0;

    /** @brief 开仓均价（fp）；无仓位时返回 0。 */
    [[nodiscard]] virtual int64_t get_position_avg_price_fp(const char* code) = 0;

    /** @brief 浮动盈亏（fp）；无仓位时返回 0。 */
    [[nodiscard]] virtual int64_t get_position_unrealized_fp(const char* code) = 0;

    /** @brief 持仓明细（today/yesterday/frozen），返回实际写入数量。 */
    [[nodiscard]] virtual size_t get_position_detail(const char* code,
                                       xt_runtime::XtStraPositionDetail* buf,
                                       size_t max) = 0;

    /** @brief 可用资金（available cash，fp）。 */
    [[nodiscard]] virtual int64_t get_cash_fp() = 0;

    /** @brief 账户权益（total equity，fp）。 */
    [[nodiscard]] virtual int64_t get_equity_fp() = 0;

    /** @brief 合约基础信息（乘数/最小变动价位等）；不存在时返回 nullptr。
     *  @note 返回指针生命周期与 context 相同，调用方不得持久缓存（do not cache beyond callback scope）。
     */
    [[nodiscard]] virtual const xt_runtime::XtStraContractInfo* get_contract_info(const char* code) = 0;

    // ── 时间（time） ──────────────────────────────────────────────────────

    virtual uint32_t get_date() = 0;          ///< 自然日（yyyymmdd）
    virtual uint32_t get_trading_date() = 0;  ///< 交易日（yyyymmdd）
    virtual uint32_t get_time_ms() = 0;       ///< action_time（HHMMSSMMM，如 143025500）

    // ── 参数（parameters，typed getters，支持热更新） ─────────────────────
    /// @warning 参数热更新仅经过单元测试验证，尚未在实盘环境中启用过（not validated in live trading）。
    /// @note key 由实现自动加 namespace `strategy.<id>.<key>` 实现 multi-strategy
    ///       隔离；这 4 个 typed getter 是 plugin 唯一的参数访问入口，绕过它们直接
    ///       访问底层 param port 会破坏 multi-strategy 路由（key 串扰）。

    /** @brief 读取字符串参数（string param），不存在时返回 def。
     *  @note 返回 std::string by-value 有堆分配；on_init 读一次缓存，勿在 on_tick/on_bar 热路径轮询。 */
    virtual std::string get_param_str(const char* key, const char* def) = 0;

    /** @brief 读取整型参数（int64 param），不存在时返回 def。 */
    virtual int64_t get_param_int(const char* key, int64_t def) = 0;

    /** @brief 读取浮点参数（double param），不存在时返回 def。 */
    virtual double get_param_double(const char* key, double def) = 0;

    /** @brief 读取布尔参数（bool param），不存在时返回 def。 */
    virtual bool get_param_bool(const char* key, bool def) = 0;

    // ── 订阅（subscription） ─────────────────────────────────────────────

    /** @brief 订阅合约 tick 行情（on_init 阶段调用）。 */
    virtual void sub_tick(const char* code) = 0;

    /** @brief 订阅合约 bar（on_init 阶段调用；max 为 ring buffer 容量，0 = 默认）。 */
    virtual void sub_bar(const char* code, const char* period, size_t max) = 0;

    // ── 日志/指标（logging & metrics） ──────────────────────────────────

    /** @brief 调试日志（debug log，非热路径，不影响延迟预算）。 */
    virtual void log_debug(const char* msg) = 0;
    /** @brief 信息日志（info log）。 */
    virtual void log_info(const char* msg) = 0;
    /** @brief 警告日志（warn log）。 */
    virtual void log_warn(const char* msg) = 0;

    /** @brief 上报瞬时指标值（gauge metric report）。
     *  @warning name 必须指向持久存储（字符串字面量/static）：聚合器以非拥有
     *    string_view 持有至秒级 drain，临时串（如 `std::string(...).c_str()`）
     *    会在 drain 时读悬空内存。 */
    virtual void report_metric(const char* name, double val) = 0;

    /**
     * @brief 为下一次 buy_fp/sell_fp 标注决策原因。
     *
     * @details 写入 per-ctx 暂存，submit 发单窗口内注入 recorder TLS；默认
     * noop（非 live context 忽略）。典型用法：
     * @code
     *   ctx_->set_next_order_reason("ob_imb 0.35>0.3");
     *   ctx_->buy_fp(...);
     * @endcode
     *
     * @note reason 在下一次 buy_fp/sell_fp 调用返回时清空。**per-ctx 承载**（不进
     *       进程 TLS 残留）→ 不会串到同 worker 线程**其他策略**的订单。但**设置后
     *       未下单**时 reason 保留在本 ctx，会被**本 ctx 下一笔单**消费（非 tick 级
     *       自动清零）——推荐每次 submit 前紧挨着 set，勿「设置后跳过本 tick 发单」。
     *       仅 live 实现经 XtLiveRecorder 转发至 ORDER_SUBMIT event（诊断字段）。
     */
    virtual void set_next_order_reason(const char* /*reason*/) {}

    /// @brief 综合判断策略当前是否应该发起新的 submit。
    ///
    /// @details live `XtStrategyContext` 注入 runner signals 后按 AND 语义读取：
    /// - `in_trading_section` — HHMM 在 active_sections 内。
    /// - `trading_ready` — MD + TD 已 ready。
    /// - `!kill_switch` — 全局 emergency latch 未触发。
    /// - `!closeout_no_open_window` — 不在 [no_open, force] 闭区间禁开仓窗口
    ///   （含 force 分钟，如 14:55-14:59）。
    /// - `control_enabled` — operator 未通过控制面 pause 本策略。
    ///
    /// `closeout_no_open_window` 在本 API 中是策略侧粗粒度 submit gate；真正
    /// open-only 的兜底由 risk guard 按 `XtOffset::Open` 执行，平仓/撤单路径不应依赖
    /// 本 API 放行。`XtRunnerSignals` 不提供跨字段一致 snapshot；调用方只应把返回值
    /// 当成本次策略决策的即时短路条件。
    ///
    /// 推荐策略 `on_tick` 短路:
    /// ```cpp
    /// void on_tick(IStrategyContext* ctx, const XtTick& t) override {
    ///     if (!ctx->can_submit()) return;   // 覆盖所有 submit gate
    ///     // ... 决策逻辑 ...
    /// }
    /// ```
    /// 默认 `true` 向后兼容；`XtStrategyContext` 未注入 runner signals 时只检查
    /// `control_enabled` gate。
    /// @note 此 API 只阻止策略侧新 submit 决策，不阻止 `cancel`。
    [[nodiscard]] virtual bool can_submit() const { return true; }

    // 集合竞价限价单 reject 走 risk 层（`XtRiskEngine` 读
    // `IRiskGuardContext::EvalCtx::is_in_call_auction`，order gateway 按 session
    // 填）；SDK 不提供集合竞价 getter——未来 A 股策略要主动感知时按真需求
    // append-only 新增。

    // ── 进程内 kv store（in-process userdata，runner 侧 graceful-stop 持久化） ──

    /** @brief 保存键值对（string kv）。
     *  @warning 值在进程内内存存储；跨交易日 session 切换保留（进程不重启）。
     *           跨重启由 runner 在 **graceful stop**（event loop 已停）时落盘、
     *           下次启动装配期回灌——**crash（kill -9）丢失**。策略自负 userdata
     *           **新鲜度与兼容性校验**：回灌的 KV 可能是上次 session 的旧值，
     *           stale 语义（是否采信、如何 migrate schema）由策略决定，SDK 不做
     *           任何语义校验。 */
    virtual void save_userdata(const char* key, const char* val) = 0;

    /** @brief 读取键值（string kv），不存在时返回 def。生命周期/丢失/新鲜度语义同 save_userdata。
     *  @warning 返回指针指向 context 内部单一缓冲，下一次 load_userdata 调用即失效；
     *           不要跨调用持有（同 get_contract_info 的「勿跨回调缓存」约束）。 */
    virtual const char* load_userdata(const char* key, const char* def) = 0;

    // ─────────────────────────────────────────────────────────────────────
    // ⚠️ ABI append-only 区
    //
    // **任何**新 virtual 必须加在此处 (vtable 末尾) 而非中间,否则破坏老
    // plugin .so vtable 兼容性 (旧 plugin vtable 错位 → 调用错误 method →
    // silent corruption,e.g. trades=0)。
    //
    // 如果改动需要中间位置 virtual (e.g. removing 一个 method),必须强制
    // rebuild 所有 plugin .so 并 bump XT_STRATEGY_ABI_VERSION (StrategyABI.h)。
    // ─────────────────────────────────────────────────────────────────────

    /// @brief plugin 显式声明关心的 symbol（declared symbol set）。
    ///
    /// 声明进 XtDeclaredSymbolRegistry（runner 装配期还会从 config seed 同一
    /// registry，两路 union）。当前消费方：
    ///   - per-strategy risk：engine 按 sid × declared symbol 跟踪 unrealized
    ///     PnL，喂 per_strategy_daily_loss_pct 风控分支
    ///   - per-strategy PnL emit：legacy ledger today gauge 按 declared 集合发布
    ///   - overlap / drift 校验：跨策略 symbol 重叠检测与 1Hz drift check
    ///
    /// **使用方式** (plugin on_init 内):
    /// ```cpp
    /// void on_init(IStrategyContext* ctx) override {
    ///     ctx->sub_tick("BTCUSDT");
    ///     ctx->declare_position_interest("BTCUSDT");
    /// }
    /// ```
    ///
    /// 不调用时依赖 runner config seed 覆盖；plugin 侧 declare 让交易 symbol
    /// 与 config 漂移时上述消费方仍然完整。仓位查询走 pull
    /// （ctx->get_position_fp()），与本 API 无关。
    /// vtable 末尾 default no-op 保布局安全；跨 ABI 版本混部由 loader 版本门禁
    /// fail-fast（精确相等校验），不依赖运行期 fallback。
    virtual void declare_position_interest(const char* /*code*/) {}

    // ─────────────────────────────────────────────────────────────────────
    // SEL Selector Engine append-only 扩展
    //
    // 严格 append-only：default no-op 让 HFT/CTA 现有 plugin .so 无需 rebuild。
    // SEL 引擎对应实现见 XtStrategyContext::set_target_weights。
    // ─────────────────────────────────────────────────────────────────────

    /// @brief 设置 selector 目标权重（symbol → weight）。
    /// @note 单账户接口；多账户路由待真实需求出现再 append-only 设计
    ///   （带 account 维度的 weights → handle_pos_change_fn 路由）。
    virtual void set_target_weights(
        const std::unordered_map<std::string, double>& /*weights*/) {}

    /// @brief 本次调度的计划 fire 时刻（epoch ms），selector 测调度延迟用。
    ///   XtStrategyContext 由 SEL 引擎在派发 on_schedule 前注入、派发返回后
    ///   复位为 0（live/backtest 同路径）；非 SEL 策略与 schedule 窗口外恒 0。
    [[nodiscard]] virtual int64_t current_fire_time_ms() const noexcept { return 0; }

    /// @brief 最近一次**策略发起**操作的本地受理结果（buy_fp/sell_fp 与 stop arm）。
    ///
    /// id==0（本地拒单）时给出拒因：风控/网关拒经 SubmitResult.reject_reason，
    /// intent（executer）路径经 IntentResult.reject_reason，context 本地预检
    /// （init 阶段 / 非法入参）给出对应码；受理成功时归 NoError。
    /// venue 异步拒单不经此通道（走 on_order 的 Rejected 终态）；**engine 发起的
    /// 条件单 fire 亦不经此通道**（fire 结果由 stop book 状态机消化，不覆写策略可见值）。
    /// 默认 NoError 向后兼容；老 plugin .so 不调用即不受影响（append-only ABI）。
    /// @note 本契约由 XtStrategyContext 实现承载；interface default no-op 实现
    ///       （Mock/未 override）返 0 时不置码，恒 NoError。
    [[nodiscard]] virtual xt_runtime::XtRejectCode last_reject_code() const noexcept {
        return xt_runtime::XtRejectCode::NoError;
    }

    // ─────────────────────────────────────────────────────────────────────
    // 本地条件单（engine-hosted stop / trailing stop）
    //
    // engine tick 驱动判定（价源 = last）；进程内内存态——重启即失效、换月
    // （roll change）时该合约全部条件单被清空并计入 dropped_roll，策略需在
    // on_roll 回调里重挂。触发后的执行语义按 ctx 模式分叉：
    //   - HFT direct：转 marketable-limit IOC 走完整 submit 链（risk gate
    //     同权），终态回报驱动——足额成交消费、零/部分成交余量自动重挂
    //     （venue fire 计数封顶后丢弃）。BT 中终态同 tick 到达、live 晚
    //     N tick 到达 → 重挂节奏 BT/live 天然不同（执行延迟本质差异）。
    //   - CTA intent：转净仓 delta intent（幂等，executer 拥有收敛；Close 类为
    //     减仓，Open 为突破加仓），accepted 即消费；offset 由 executer 推导，
    //     登记值仅记录意图。
    // 命名空间：触发匹配按引擎给触发钩子的 tick.code 进行——HFT 为物理合约码
    // （逻辑标的 arm 时经 roll 引擎解析，解析与 buy_fp 同源；失败处理刻意分叉——
    // buy 保原码交下游 venue 拒单回路，stop 无下游回路故 arm fail-closed 拒，
    // InvalidInput，不静默挂不可匹配 slot）；CTA 为 remap 后的
    // 逻辑码，勿用物理码挂 CTA stop（不在 tick 命名空间内 = 永不触发）。
    // 需 config 开启（runtime.trading.conditional_orders_enabled /
    // backtest.conditional_orders_enabled，默认关）；关闭时返 0 +
    // last_reject_code()=CapabilityUnsupported（book 满同码，由 warn log
    // "stop book full" 区分）。
    // ─────────────────────────────────────────────────────────────────────

    /// @brief 登记固定价停止单（stop-market 形态）。
    /// @param trigger_price_fp 触发价（fp ×1e6）；buy-stop `last >= trigger`，
    ///        sell-stop `last <= trigger`。
    /// @param offset Close 类触发时按可平量 clamp 后发单（HFT 侧向可平 /
    ///        CTA 净仓可减）；可平量为 0 时该条件单被丢弃（仓位已平自愈）。
    /// @return stop_id（非 0，独立于 order_id 域）；失败返 0，拒因经
    ///         last_reject_code() 观察。
    virtual uint64_t submit_stop_fp(const char* /*code*/, bool /*is_buy*/,
                                    int64_t /*trigger_price_fp*/,
                                    int64_t /*qty_fp*/, XtOffset /*offset*/,
                                    const char* /*tag*/) { return 0; }

    /// @brief 登记跟踪停止单（trailing stop）。
    /// @param distance_fp 回撤距离，**绝对价格 fp（×1e6），非 tick 数**。
    /// @param seed_price_fp 非 0 时以此预播种水位（如入场价）；0 = armed 后
    ///        该 code 首个 tick 的 last 播种（当 tick 不会触发）。
    virtual uint64_t submit_trailing_stop_fp(const char* /*code*/, bool /*is_buy*/,
                                             int64_t /*distance_fp*/,
                                             int64_t /*qty_fp*/, XtOffset /*offset*/,
                                             const char* /*tag*/,
                                             int64_t /*seed_price_fp*/) { return 0; }

    /// @brief 撤销未终结的条件单（触发单在途时同时尝试撤该真实单）。
    /// @note 触发判定先于策略 on_tick 派发：同 tick 内条件已触发时本调用
    ///       作用于在途单而非阻止触发。
    /// @return 找到并撤销为 true；未知 stop_id 返 false。
    virtual bool cancel_stop(uint64_t /*stop_id*/) { return false; }
};

// ── 敞口投影 helper（header-only free function，非 vtable/ABI 面） ────────────
//
// 市商/防重单姿态的推荐敞口口径：projected = 纯 confirmed + 全部未终结挂单的
// 带符号 leaves 净额（买 +/卖 −，开平一律按方向计入净额）。数据源
// get_open_orders（含 Submitted 未 ack 与 Recovered），全执行模式单一语义
// ——区别于 projected_inflight_fp（单策略 backtest / 未启用 in-flight 投影的
// live 恒 0，见其声明注释）。仅用 confirmed 仓位判断敞口的策略在挂单未成交
// 窗口内会反复加挂（回测同步撮合下挂单堆积、成交后仓位远超策略自设上限）。

/// @brief code 上全部未终结挂单的带符号 leaves 净额（fp，买 +/卖 −）。
/// @note 仅聚合前 64 笔挂单；市商常态双边 ≤2 笔，超出说明挂单已失控，
///       返回值此时仅供参考（低估敞口 = 越界方向 fail-open）。
/// @note 继承 get_open_orders 的线程契约：非 owner 线程调用 fail-closed
///       返 0 笔 → 本函数静默退化为 0（等价 confirmed-only）。
[[nodiscard]] inline int64_t xt_open_orders_net_leaves_fp(IStrategyContext* ctx,
                                                          const char* code) {
    if (!ctx) return 0;
    XtOrderExecUpdate buf[64];
    const size_t n = ctx->get_open_orders(code, buf, 64);
    int64_t net_fp = 0;
    for (size_t i = 0; i < n; ++i) {
        if (buf[i].leaves_fp <= 0) continue;
        net_fp += buf[i].is_buy ? buf[i].leaves_fp : -buf[i].leaves_fp;
    }
    return net_fp;
}

/// @brief 投影净仓（fp）= 纯 confirmed + 挂单带符号 leaves 净额。
/// @note get_position_fp 在启用 in-flight 投影时已含 tracker 净量，
///       先减 projected_inflight_fp 还原纯 confirmed 再叠加 book 口径，
///       避免同一笔挂单双计。
[[nodiscard]] inline int64_t xt_projected_position_fp(IStrategyContext* ctx,
                                                      const char* code) {
    if (!ctx) return 0;
    const int64_t confirmed_fp =
        ctx->get_position_fp(code) - ctx->projected_inflight_fp(code);
    return confirmed_fp + xt_open_orders_net_leaves_fp(ctx, code);
}
