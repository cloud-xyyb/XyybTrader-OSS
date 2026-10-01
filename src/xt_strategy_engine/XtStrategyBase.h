#pragma once

/// @file
/// @brief 策略插件的 C++ SDK 基类与能力声明。
///
/// 写一个策略 = 继承 XtStrategyBase，override 需要的回调（on_tick/on_bar/on_fill 等），
/// 编译成 .so 由 runner 加载。capabilities() 自报策略需要哪些回调通道；
/// runner 启动期校验，部署形态不符合时直接拒绝，避免回调静默失效。

#include <cstdint>

// xt_core 类型前向声明（global scope）
struct XtTick;
struct XtBar;
struct XtFillEvent;
struct XtOrderExecUpdate;
struct XtAccountUpdate;
struct XtOrderQueue;
struct XtOrderDetail;
struct XtTransaction;

#include "XtStrategyInitConfig.h"

class IStrategyContext;

/// @brief 策略能力位（self-declared capabilities）。
///
/// 策略 plugin override `XtStrategyBase::capabilities()` 自报对回调通道
/// 的真实需求。Runner 启动期校验：当部署模式禁用了对应通道（例如 CTA 多策略
/// 或 SEL runner 的 exec sink callback path 不可用）时 abort，避免静默失效。
///
/// capabilities()=0 表示策略未声明额外回调通道需求。
enum class StrategyCapability : uint64_t {
    None              = 0,
    /// 策略 override `on_fill`，需要 engine 把成交回报 fan-out 到本策略 ctx。
    RequiresOnFill    = 1ULL << 0,

    /// 策略 override `on_order`，需要订单状态机推送。
    RequiresOnOrder   = 1ULL << 1,

    /// 策略 override `on_tick`；HFT 主驱动依赖它，CTA 可通过 sub_tick 显式订阅。
    /// @note 仅声明用途的 capability 位：tick 投递由 ctx->sub_tick 订阅驱动，
    ///       runner 不据此位做启动校验。
    SubscribesTick    = 1ULL << 2,
};

inline constexpr uint64_t to_capability_bit(StrategyCapability c) noexcept {
    return static_cast<uint64_t>(c);
}

/// @brief exec_sink callback path 不可用时无法兑现的回调通道集合。
///
/// runner 启动校验对照此掩码 abort；新增依赖 exec sink callback path 的 capability 位
/// 必须同步加入，否则该通道在多策略模式下静默失效。
inline constexpr uint64_t kExecSinkRequiredCaps =
    to_capability_bit(StrategyCapability::RequiresOnFill) |
    to_capability_bit(StrategyCapability::RequiresOnOrder);

/// @brief 策略 plugin 的统一基类。
///
/// 子类通过 override market、trade、session 和 SEL callbacks 接入 runner。
/// `init()` 接收静态配置，`on_init()` 接收运行时 context；方向下单 helper
/// 只负责把 enter/exit 语义转成 context 的 `buy_fp` / `sell_fp` 调用。
///
/// @note 引擎在同一 event-loop 线程串行调用策略回调；默认策略实现不需要为回调并发加锁。
/// @warning 策略若自行启动 background thread，并在其中访问 context、策略内部状态或
///          回调输入保存的引用，必须自行同步并保证生命周期覆盖访问期。
class XtStrategyBase {
public:
    virtual ~XtStrategyBase() = default;

    // ── 初始化（initialization） ─────────────────────────────────────────

    /** @brief 静态参数初始化（static param init）：从 YAML 加载，在 on_init 之前调用。 */
    virtual void init(const XtStrategyInitConfig&) {}

    /** @brief 运行时初始化（runtime init）：此时 context 已就绪，可在此订阅合约、注册 bar 周期。 */
    virtual void on_init(IStrategyContext*) {}

    // ── 行情推送（market data） ──────────────────────────────────────────

    /** @brief tick 推送回调（tick push callback）。
     *  @note HFT 策略通常覆盖此方法（core tick callback，主热路径）。
     *        CTA 默认主驱动是 on_bar；调用 ctx->sub_tick 的 CTA 策略也会收到
     *        on_tick，可用于 tick 级风控、trailing stop 或盘口监控。
     *
     *  Safety invariant (View Causality):
     *  - 调用 on_tick(t) 时，ctx 提供的 view 反映的世界状态严格只包含
     *    时间戳 < t 的所有回报（成交 / 撤单 / 拒单）
     *  - 本 tick 内通过 ctx 提交的订单产生的回报，只能在下一次 on_tick(t')
     *    中通过 view 观察到，其中 t' >= t + min_response_latency
     *  - 这与实盘"回报必须晚于下单一个 round-trip"的物理事实对齐
     *  - 回测中由 XtCtaMatchEngine 的延迟队列（fill_latency_ns / cancel_latency_ns）
     *    保证此契约
     */
    virtual void on_tick(IStrategyContext*, const XtTick&) {}

    /// @brief bar close 回调；CTA 策略的主驱动入口。
    virtual void on_bar(IStrategyContext* ctx,
                        const char*       code,
                        const char*       period,
                        const XtBar&      bar) {
        (void)ctx; (void)code; (void)period; (void)bar;
    }

    /** @brief 新 bar 开盘通知（bar open notification）：开盘价已知，策略可挂开盘单。 */
    virtual void on_bar_open(IStrategyContext* ctx,
                             const char*       code,
                             const char*       period,
                             int64_t           open_px_fp) {
        (void)ctx; (void)code; (void)period; (void)open_px_fp;
    }

    // ── L2 数据推送（L2 market data） ───────────────────────────────────
    /// @brief 委托队列快照回调；仅在策略订阅并且引擎接入 L2 order queue 时触发。
    virtual void on_order_queue(IStrategyContext*, const XtOrderQueue&) {}
    /// @brief 逐笔委托回调；仅在策略订阅并且行情源提供 order detail 时触发。
    virtual void on_order_detail(IStrategyContext*, const XtOrderDetail&) {}
    /// @brief 逐笔成交回调；仅在策略订阅并且行情源提供 transaction 时触发。
    virtual void on_transaction(IStrategyContext*, const XtTransaction&) {}

    // ── 交易回报（trade callbacks） ────────────────────────────────
    /** @brief 报单状态更新（order status update）。 */
    virtual void on_order(IStrategyContext*, const XtOrderExecUpdate&) {}

    /** @brief 成交通知（fill notification）：主成交回调，推荐 override 此方法。
     *
     *  同步语义：本回调触发前 account_store 已应用本 fill，策略中调用
     *  ctx->get_position_fp() 可读到 post-fill 状态。
     */
    virtual void on_fill(IStrategyContext*, const XtFillEvent&) {}

    // 仓位通知走 pull：ctx->get_position_fp() / get_position_detail()
    // （engine 不 fan-out position snapshot，无 push 回调）。

    /** @brief 账户快照推送（account snapshot）。
     *
     *  与 on_fill 独立；策略如需 fill 后账户值，应通过
     *  ctx->get_cash_fp() / get_equity_fp() 同步读取 runtime view。
     */
    virtual void on_account(IStrategyContext*, const XtAccountUpdate&) {}

    // ── 会话生命周期（session lifecycle） ───────────────────────────────

    /** @brief 交易日开始（trading day begin）：仅在 trading_date 变化时触发。 */
    virtual void on_session_begin(IStrategyContext*, uint32_t /*trading_date*/) {}

    /** @brief 交易日结束（trading day end）：仅在 trading_date 变化时触发。 */
    virtual void on_session_end(IStrategyContext*, uint32_t /*trading_date*/) {}

    /** @brief 合约换月通知（contract roll）：逻辑主力对应的真实合约发生切换时触发。
     *  @param old_code  切换前合约代码（首次设定时为空串）
     *  @param new_code  切换后合约代码
     *  @param trading_date  切换发生的交易日
     *  @param old_last_price_fp  旧合约最后已知价格（定点数；0 = 未知）
     *
     *  @note 引擎层调用 5 参数签名；策略需要换月事件时直接覆盖此方法。
     */
    virtual void on_roll(IStrategyContext* ctx,
                         const char*       old_code,
                         const char*       new_code,
                         uint32_t          trading_date,
                         int64_t           old_last_price_fp) {
        (void)ctx;
        (void)old_code;
        (void)new_code;
        (void)trading_date;
        (void)old_last_price_fp;
    }

    /// @brief 参数域变更通知（param domain update）。
    ///
    /// @warning 参数热更新回调仅经过单元测试验证，尚未在实盘环境中启用过。
    virtual void on_params(IStrategyContext*) {}

    /// @brief 策略能力位自报（self-declared capabilities）。
    ///
    /// 子类按需 override 声明对回调通道的真实依赖。CTA/SEL runner 启动期
    /// 与部署模式交叉校验（对照 kExecSinkRequiredCaps；CTA 多策略或 SEL
    /// runner 无 exec-sink dispatch path 时 abort），避免声明的通道静默失效。
    /// 校验时同步 log capability 值供审计。HFT 多策略经 per-sid exec sink 派发。
    ///
    /// 默认 0：策略未声明额外回调通道需求。
    virtual uint64_t capabilities() const noexcept { return 0; }

    // ── 通道状态（channel state） ────────────────────────────────────────
    // 统一契约：ctx.on_init 后必达合成 ready("MD") + ready("TD") 各一次（三引擎
    // 一致，早于真实连接）；真实连接 transition（lost / re-ready，edge 去重）
    // 由有通路的引擎追加投递——HFT/CTA live 投递；SEL live 只合成 ready，
    // backtest 无连接语义也只有合成。
    // 合成与真实两路径互不去重：策略可能在真实连接就绪时收到重复 ready，需幂等。

    /// @brief 通道就绪事件；合成 ready（on_init 后必达）或真实连接 ready。
    /// @param channel "MD" / "TD"。
    virtual void on_channel_ready(IStrategyContext*,
                                  const char* /*channel*/) {}

    /// @brief 通道断连事件（真实 transition；HFT/CTA live 投递，SEL/backtest 无）。
    virtual void on_channel_lost(IStrategyContext*,
                                 const char* /*channel*/,
                                 int /*reason*/) {}

    // ─────────────────────────────────────────────────────────────────────
    // SEL Selector Engine append-only 扩展
    // ─────────────────────────────────────────────────────────────────────

    /// @brief Selector 调度回调（SEL 引擎专属）。
    /// @param fireTime_ms  实际触发 ms 时间戳（live 异步延迟监控关键）
    virtual void on_schedule(IStrategyContext* /*ctx*/,
                             uint32_t /*curDate*/,
                             uint32_t /*curTime*/,
                             int64_t  /*fireTime_ms*/) {}

    /// @brief Session 结束 hook（SEL 引擎日终回调）。
    virtual void on_session_finish(IStrategyContext* /*ctx*/) {}

    // ── 便利方法（convenience helpers，非虚，内部调用 ctx->buy_fp/sell_fp） ──
    // 注：方法体定义在 XtStrategyBase.inl，需 IStrategyContext 完整定义。

    /** @brief 开多仓（enter long）：以指定价格/数量买入开仓。 */
    static uint64_t enter_long(IStrategyContext* ctx,
                               const char* code,
                               int64_t price_fp,
                               int64_t qty_fp,
                               const char* tag = nullptr);

    /** @brief 平多仓（exit long）：以指定价格/数量卖出平仓。 */
    static uint64_t exit_long(IStrategyContext* ctx,
                              const char* code,
                              int64_t price_fp,
                              int64_t qty_fp,
                              const char* tag = nullptr);

    /** @brief 开空仓（enter short）：以指定价格/数量卖出开仓。 */
    static uint64_t enter_short(IStrategyContext* ctx,
                                const char* code,
                                int64_t price_fp,
                                int64_t qty_fp,
                                const char* tag = nullptr);

    /** @brief 平空仓（exit short）：以指定价格/数量买入平仓。 */
    static uint64_t exit_short(IStrategyContext* ctx,
                               const char* code,
                               int64_t price_fp,
                               int64_t qty_fp,
                               const char* tag = nullptr);
};
