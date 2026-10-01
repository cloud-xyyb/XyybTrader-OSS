#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "SelTaskScheduler.h"
#include "api/XtStrategyRuntimeTypes.h"
#include "api/IStrategyContext.h"
#include "xt_core/order/XtOrderTypes.h"  // XtTimeInForce

class XtStrategyContext;
class XtStrategyBase;

namespace xt_engine_sel {

/// @brief 单个 (fire, code) 目标的规划期判定（执行前，与 tradability gate 正交）。
///
/// 优先级自上而下：无价 → min_lot 截零 → 与现仓一致 → 正常。`kMinLotZero` 只描述
/// 「raw target 非零但整 lot 截断后为零」，此时若已有持仓仍会产生清仓 diff。
enum class SelPlanningStatus {
    kOk = 0,        ///< 产出非零 diff，进入执行路径
    kNoPrice,       ///< price_resolver 无价 → 整个 symbol 跳过（target 未知）
    kMinLotZero,    ///< raw target 非零但整 lot 向零截断后为 0
    kNoChange,      ///< target == 当前持仓，无 diff
    kNegativeWeight,///< 负目标权重 → 拒（SEL v1 long-only，无做空腿）
};

/// @brief `SelPlanningStatus` 的稳定字符串名（BT 产物列值，改名即破 schema）。
[[nodiscard]] const char* sel_planning_status_name(SelPlanningStatus s) noexcept;

/// @brief 一次 `on_target_weights` 调用的整体记录（BT 观测 sink 载荷）。
///
/// 每次调用恰好一条（含各条早退路径），`fire_seq` 在 engine 实例内单调递增，
/// 是与 `SelTargetDecision` 关联的唯一键。
struct SelFireRecord {
    uint64_t    fire_seq = 0;
    std::string strategy_id;
    /// 触发本次 fire 的 scheduler task id；0 = 非 schedule 路径（策略直调 ctx
    /// set_target_weights 等），消费方按 strategy_id 回退归因。
    uint32_t    task_id = 0;
    int64_t     equity_fp = 0;             ///< equity_resolver 取值；未进真 diff 路径时为 0
    bool        equity_nonpositive = false; ///< true = equity<=0 导致整个 fire 被跳过
    size_t      n_targets = 0;             ///< 本次 weights 的 symbol 数
};

/// @brief 单个 (fire, code) 的规划期决策（BT 观测 sink 载荷）。
///
/// 在 `on_target_weights` 的每个 symbol 决策点发射，含 `continue` 早退路径；
/// `kNoPrice` 行的 target/diff 字段无意义（消费方须按 status 判空）。
struct SelTargetDecision {
    uint64_t          fire_seq = 0;
    std::string       strategy_id;
    std::string       code;
    double            weight = 0.0;
    int64_t           price_fp = 0;           ///< 0 = 无价
    int64_t           target_qty_raw_fp = 0;  ///< 整 lot 截断前
    int64_t           target_qty_fp = 0;      ///< 整 lot 截断后
    int64_t           pre_qty_fp = 0;         ///< 决策时的净持仓
    int64_t           diff_qty_fp = 0;        ///< target - pre
    SelPlanningStatus status = SelPlanningStatus::kOk;
};

/// @brief 整数投影 hook 的单条目（一次 fire 内一个 code 的规划期快照）。
///
/// `target_qty_fp` 是 engine 算出的基线（`weight × equity / price` 向零截断到
/// min_lot 整数倍）；hook 拿到整批后可按自己的约束（费用、可用现金、余数分配）
/// 重写目标。早退条目（负权重 / 无价）不进批——它们没有可投影的目标。
struct SelTargetProjectionEntry {
    std::string code;
    double      weight = 0.0;
    int64_t     price_fp = 0;           ///< 规划价（回测口径 = T-1 close）
    int         min_lot = 1;            ///< 该 code 的最小交易单位（股）
    int64_t     target_qty_raw_fp = 0;  ///< 整 lot 截断前的原始目标
    int64_t     target_qty_fp = 0;      ///< 基线目标（整 lot 倍数）
    int64_t     pre_qty_fp = 0;         ///< 决策时的净持仓
};

/**
 * @brief 提供 selector 策略的定时调度和目标权重执行入口。
 *
 * `on_minute_tick()` 驱动 scheduler 触发策略 schedule hook；
 * `on_target_weights()` 将目标权重转换为待执行的持仓差额。
 * 调用方通过 `flush_pending()` 或 `fire_one_pending()` 分阶段提交这些差额。
 *
 * 同一实例按 driver clock 串行调用；执行入口和行情/账户解析能力由 Config 注入。
 *
 * 线程契约：`bind_driver_thread()` 绑定后，调度 / 权重 / session / 参数 / task 注册
 * 入口只接受来自 driver 线程的调用，跨线程调用 fail-closed（丢弃 + 计数 + log-once），
 * 因为这些入口写的是 pending 组、scheduler 表和 ctx fan-out 等无锁 mutable 状态。
 * 未绑定时全部放行：装配期主线程与单线程 backtest driver 都落在这一态。
 */
class XtSelEngine {
public:
    /// @brief 配置 selector engine 的调度器、执行回调和解析器。
    ///
    /// scheduler 为必填；resolver 和执行回调为空时，对应能力按默认语义退化。
    struct Config {
        std::unique_ptr<ISelTaskScheduler> scheduler;        ///< 调度器（必填）
        std::unique_ptr<IHolidayProvider> holiday_provider;  ///< 交易日历（随 Config move 进 engine 持有；null 时节假日跳过失效）

        /// @brief 执行持仓差额的回调。
        ///
        /// `on_target_weights()` 生成 diff_pos_fp 后，flush/fire 路径通过本回调提交。
        using HandlePosChangeFn = std::function<void(
            const std::string& strategy_id,
            const std::string& code,
            int64_t diff_pos_fp,
            int64_t price_fp,
            XtTimeInForce tif,
            const std::string& tag)>;
        HandlePosChangeFn handle_pos_change_fn;

        /// @note 三个 resolver 全注入时，on_target_weights 按
        ///       weight × equity / close_price 计算 target qty，再与当前持仓求 diff。
        ///       任一 resolver 缺省时退化为 passthrough（diff_pos_fp=0，price_fp=0）。

        /// @brief 价格解析：code → close_price_fp（用 T 日 close 价）。
        using PriceResolverFn = std::function<int64_t(const std::string& code)>;
        PriceResolverFn price_resolver_fn;

        /// @brief 当前净持仓解析：code → qty_fp（带方向，long > 0, short < 0）。
        using PositionResolverFn = std::function<int64_t(const std::string& code)>;
        PositionResolverFn position_resolver_fn;

        /// @brief 当前 equity 解析：返回 equity_fp（用于 weight × equity = target_value）。
        using EquityResolverFn = std::function<int64_t()>;
        EquityResolverFn equity_resolver_fn;

        /// @brief min_lot 解析（A 股 SH/SZ 主板 100，期货/加密 1）。
        /// @note 注入后 `on_target_weights` 先计算 raw target，再向零截断到 min_lot
        ///       整数倍（非四舍五入）；输出的 diff_pos_fp 已是整 lot 倍数。
        /// @note 缺省时按 min_lot=1 处理，等价于不做 lot 约束。
        using MinLotResolverFn = std::function<int(const std::string& code)>;
        MinLotResolverFn min_lot_resolver_fn;

        /// @brief 当前持仓 symbol 集合解析：返回非零净仓的合约代码集合。
        /// @note 仅供 sparse-weights 观测护栏（R4 契约要求 dense weights）。集合中
        ///       不在本次 weights keys 里的 symbol（即将被静默踢出、不带显式 0 权重、
        ///       不会被平出的旧持仓）触发 `on_target_weights` warn + 计数，
        ///       **不改行为**（等数量换集合、少数覆盖同样触发——按集合差集判定，
        ///       非按数量比较）。缺省（nullptr）时护栏惰性，计数恒 0。
        using HeldSymbolsResolverFn = std::function<std::vector<std::string>()>;
        HeldSymbolsResolverFn held_symbols_resolver_fn;

        /// @brief 策略 on_init 失败回调（per-strategy fail-closed，对称 HFT/CTA engine）。
        ///
        /// SEL 的 on_init 由 runner 装配期调用（strat->on_init）；失败时 runner 调
        /// `mark_init_failed(idx)`，engine 标记该 ctx disabled（后续 schedule/session/weights
        /// fan-out 跳过）并调用本回调（sid_id + 策略名）。空回调时仅本地 disable + 计数；
        /// live 组合根注入以置 per-sid kill latch。
        std::function<void(uint32_t /*sid_id*/, const std::string& /*strategy_name*/)>
            on_strategy_init_failure;

        /// @brief 每次 `on_target_weights` 调用发射一条 fire 记录（回测产物观测面）。
        /// @note nullptr（live 默认）时零调用、零行为差异。
        using FireSinkFn = std::function<void(const SelFireRecord&)>;
        FireSinkFn fire_sink;

        /// @brief 每个 (fire, code) 决策点发射一条（含 `continue` 早退路径）。
        /// @note nullptr（live 默认）时零调用、零行为差异。
        using TargetDecisionSinkFn = std::function<void(const SelTargetDecision&)>;
        TargetDecisionSinkFn target_decision_sink;

        /// @brief 整批目标的整数投影 hook（回测口径注入；live 不注入）。
        ///
        /// `plan[i].target_qty_fp` 是 engine 的基线目标，`targets_inout` 入参与之
        /// 逐位相同；hook 只改写 `targets_inout`。分手/费用/现金这类市场语义留在
        /// 注入方，engine 只负责「批的组织 + 契约校验」。
        /// @note nullptr（live 默认）时整段跳过，基线即终值、零行为差异。
        /// @note 契约：写回值须**非负**、与基线之差须是 `plan[i].min_lot` 的整数倍，
        ///       且 `targets_inout.size()` 不得改变。违约条目被还原为基线并计数
        ///       （fail-closed —— 投影不该有能力破 long-only 与整手约束）。
        ///       判「差额是整手」而非「写回值是整手」：`min_lot == 1` 时基线本就不做
        ///       整手截断，按绝对值判会把未改动的基线也判违约。
        using TargetProjectionFn = std::function<void(
            const std::vector<SelTargetProjectionEntry>& plan,
            std::vector<int64_t>& targets_inout)>;
        TargetProjectionFn target_projection_fn;
    };

    explicit XtSelEngine(Config cfg);
    ~XtSelEngine();

    XtSelEngine(const XtSelEngine&) = delete;
    XtSelEngine& operator=(const XtSelEngine&) = delete;

    /// @brief 绑定 driver 线程；此后受守卫入口以该 tid 为权威。
    ///
    /// 冷路径（live 由 driver 线程在 driver_loop_ 首行自绑）。绑定前所有
    /// 入口无条件放行，不做 first-touch 记忆：装配期主线程的 add_context /
    /// add_task / mark_init_failed 与从不绑定的单线程 backtest driver 都落在这一态。
    /// @param tid driver 线程 id。可重复调用（restart 后新 driver 自绑重绑）；
    ///        tid 为 atomic，重绑与并发守卫读无 race。
    void bind_driver_thread(std::thread::id tid) noexcept;

    /// @brief 注册 strategy + ctx（engine owns ctx；strategy 由 caller own non-owning ptr）。
    /// @return 该 strategy 在 engine 内的 index（0-based）。
    size_t add_context(std::unique_ptr<XtStrategyContext> ctx,
                       XtStrategyBase* strategy);

    /// @brief Driver 调用：当前 (curDate, curTime) 触发 scheduler 检查 fire。
    /// @note live driver 每分钟调；backtest driver 按 clock 推进调。
    /// @warning 绑定后 driver 线程专用；跨线程调用被丢弃 + 计数（见类注释线程契约）。
    void on_minute_tick(uint32_t curDate, uint32_t curTime);

    /// @brief 调度回调：对应 task fire 时调每个 strategy 的 on_schedule。
    void on_schedule_fire(const SelTaskInfo& task,
                          uint32_t curDate,
                          uint32_t curTime,
                          int64_t fireTime_ms);

    /// @brief Strategy 调 ctx->set_target_weights 时进入此函数。
    /// @note 本方法只计算 per-code diff，并把本次 fire 的 SELL / BUY 差额**追加**
    ///       为一个 pending group；不直接 submit。调用方通过
    ///       `flush_pending(SellOnly/BuyOnly)` 或 `fire_one_pending` 控制实际 fire。
    /// @note 同一分钟多个 task 触发时，各 fire 各自成组、先来先 fire；本方法不清空
    ///       既有 pending（清空语义会让先触发 task 的差额被后者静默吞掉）。
    /// @note pending 存的是**绝对目标**：差额在 dispatch 时按最新持仓重算，所以同槽
    ///       两个 target 是幂等替换（后者覆盖前者的效果）而不是差额相加。分桶只按
    ///       规划期方向决定**执行时点**；重算后方向翻转的条目按真方向归位（SELL 桶
    ///       翻 BUY 推迟到 BUY phase），故先卖后买的现金序不被同槽复合破坏。
    /// @note `strategy_id` 未注册（无对应 ctx）时整次调用被丢弃 + 计数：engine 不为
    ///       自己不管理的策略挂 target，否则该 target 绕过 per-ctx disabled 闸直接
    ///       进 pending（装配期 on_init 早于 add_context，是这条逃逸的可达窗口）。
    /// @warning 绑定后 driver 线程专用；跨线程调用被丢弃 + 计数（见类注释线程契约）。
    void on_target_weights(const std::string& strategy_id,
                            const std::unordered_map<std::string, double>& weights);

    /// @brief Phased fire 阶段标识。
    enum class Phase {
        SellOnly = 0,  ///< 仅 fire 缓存的 SELL 单（diff_pos_fp < 0）
        BuyOnly  = 1,  ///< 仅 fire 缓存的 BUY 单（diff_pos_fp > 0）
        All      = 2,  ///< 一次性 fire 全部（先 SELL 后 BUY）
    };

    /// @brief 触发缓存的 pending diffs 按 phase fire。
    /// @warning 调用方必须在 `SellOnly` 后完成 gateway update、fill drain 和 account apply，
    ///          再调用 `BuyOnly`；否则 BUY phase 可能读取到未释放的 cash。
    /// @note 触发后清空对应 phase cache；唯一例外是重算后翻 BUY 的 SELL 条目——它
    ///       留在 BUY 桶里等 `BuyOnly`（`SellOnly` 之后 `pending_buy_count()` 可能上升）。
    /// @warning 绑定后 driver 线程专用；跨线程调用被丢弃 + 计数（见类注释线程契约）。
    void flush_pending(Phase phase);

    /// @brief  fire **单个** pending diff (从 buys 或 sells 头部弹出
    ///        一个并 fire)。返回 true 表示队列前进了一条, false 表示对应 phase cache 空。
    /// @note true 不等于「提交了一单」：重算差额归零（noop）或 SELL 桶翻 BUY（改挂
    ///       BUY 桶延后执行）时同样返回 true——判据是队列单调收敛，caller 的
    ///       `while (pending_*_count() > 0)` 循环仍必然终止。
    ///
    /// 用途: SEL backtest BUY phase 内逐笔 fire。caller 用 while 循环
    ///        fire_one_pending(BuyOnly) → drain + apply (cash 更新) → fire_one_pending → ...
    ///        让每个 BUY 单的 risk check 看到 actual post-prev-fill cash, 与实盘 OMS 行为对齐
    ///        (批量一次性 fire 时多笔 BUY 看同一 cash → 同 batch 已占用 cash 未计 → 累积透支)。
    bool fire_one_pending(Phase phase);

    [[nodiscard]] size_t pending_sell_count() const noexcept;
    [[nodiscard]] size_t pending_buy_count()  const noexcept;

    /// @brief 正在 fire 的 pending 所属 fire_seq；非 fire 期为 0。
    /// @note 供 `handle_pos_change_fn` 内把 submit 归因到发起它的那次 fire
    ///       （回调签名不带 fire 身份，回测产物的执行列靠此关联）。
    [[nodiscard]] uint64_t current_fire_seq() const noexcept { return current_fire_seq_; }

    /// @brief Session begin / end forward 到所有 strategy。
    /// @warning 绑定后 driver 线程专用；跨线程调用被丢弃 + 计数（见类注释线程契约）。
    void on_session_begin(uint32_t trading_date);
    void on_session_end(uint32_t trading_date);

    /// @brief 注册 task 到内部 scheduler。
    /// @return task id；0 = 未注册（无 scheduler，或绑定后被跨线程调用丢弃）。
    /// @warning 绑定后 driver 线程专用；生产装配在绑定前完成注册。
    uint32_t add_task(SelTaskInfo info);

    /// @brief sparse-weights 观测计数：weights 未覆盖当前持仓中 >=1 个 symbol 的调度次数。
    /// @note held_symbols_resolver_fn 未注入时恒 0。
    [[nodiscard]] uint64_t sparse_weights_events() const noexcept { return sparse_weights_events_; }

    /// @brief 被拒的负目标权重累计数（SEL v1 long-only 防御闸）。
    [[nodiscard]] uint64_t negative_weight_rejects() const noexcept {
        return negative_weight_rejects_;
    }

    /// @brief dispatch 时按最新持仓重算后差额归零、未派发的 pending 数。
    /// @note 同槽后一个 task 下了与前一个相同的 target 时出现（幂等语义的正解）。
    [[nodiscard]] uint64_t pending_dispatch_noops() const noexcept {
        return pending_dispatch_noops_;
    }

    /// @brief dispatch 重算后方向与分桶相反的 pending 数。
    /// @note 同槽多 target 复合时出现（后一个 target 落在已成交量的另一侧）。数量
    ///       以重算值为准（幂等优先）；执行时点按重算后的真方向归位——BUY 桶翻
    ///       SELL 立即执行，SELL 桶翻 BUY 推迟到 BUY phase（不在 SELL phase 插买单）。
    [[nodiscard]] uint64_t pending_dispatch_direction_flips() const noexcept {
        return pending_dispatch_direction_flips_;
    }

    /// @brief 同一 code 被不同 strategy_id 同时挂 target 的次数（语义冲突观测）。
    [[nodiscard]] uint64_t cross_sid_same_code_pendings() const noexcept {
        return cross_sid_same_code_pendings_;
    }

    /// @brief 同槽同 (sid,code) 后发目标就地覆盖未 dispatch pending 的次数
    ///（幂等替换的可见面：含 diff==0 的 kNoChange/kMinLotZero 覆盖）。
    [[nodiscard]] uint64_t same_slot_target_replacements() const noexcept {
        return same_slot_target_replacements_;
    }

    /// @brief session 边界清掉的跨日残留 pending 组数（应恒 0）。
    [[nodiscard]] uint64_t stale_pending_groups_dropped() const noexcept {
        return stale_pending_groups_dropped_;
    }

    /// @brief 投影 hook 改写掉基线目标的 (fire, code) 条目数。
    /// @note hook 未注入时恒 0。
    [[nodiscard]] uint64_t projection_adjusted_targets() const noexcept {
        return projection_adjusted_targets_;
    }

    /// @brief 投影 hook 违约（负值 / 非整手 / 改了批长度 / 抛异常）被还原为基线的条目数。
    /// @note 恒应为 0；非零 = 注入方的投影实现破了契约，产物按基线口径继续。
    [[nodiscard]] uint64_t projection_rejected_entries() const noexcept {
        return projection_rejected_entries_;
    }

    [[nodiscard]] size_t context_count() const noexcept { return contexts_.size(); }
    [[nodiscard]] XtStrategyContext* context(size_t idx) noexcept;
    [[nodiscard]] const XtStrategyContext* context(size_t idx) const noexcept;
    [[nodiscard]] bool has_strategy(const std::string& strategy_id) const;

    /// @brief 控制面启停指定策略的提交闸。
    /// @return false = 无该 strategy_id 的 ctx。
    /// @note 不受 driver 线程守卫约束，可从控制面（gRPC）线程直调：查表读的是装配后
    ///       不再变动的 strategy_ids_/contexts_，写的是 ctx 内单个 `std::atomic<bool>`，
    ///       不碰 pending 组 / scheduler 表等无锁 mutable 状态。
    bool set_strategy_control_enabled(const std::string& strategy_id, bool enabled);

    /// @brief 刷新各 ctx 的 ParamDomain 视图并 fan-out strategy.on_params。
    /// @param version 目标参数版本（param domain 单调版本号）。
    /// @warning 绑定后 driver 线程专用；跨线程调用被丢弃 + 计数（见类注释线程契约）。
    ///          live 的参数投递口是 scheduler 的 params inbox，由 driver 线程 drain。
    void update_params(uint64_t version);

    /// @brief 标记 ctx[idx] fail-closed disabled（strategy on_init 失败）。
    /// @details SEL 的 on_init 由 runner 装配期调用；runner 守卫捕获异常后调本方法。
    ///          置位 disabled（schedule/session/weights fan-out 跳过该 ctx）+ 计数 +
    ///          critical 日志 + 触发 on_strategy_init_failure 回调。幂等；idx 越界 no-op。
    void mark_init_failed(size_t idx);

    /// @brief 因 on_init 失败被 fail-closed disable 的 context 累计数。
    /// @note backtest fail-fast / live 观测据此判定。
    [[nodiscard]] uint64_t strategy_init_failure_count() const noexcept {
        return strategy_init_failure_total_.load(std::memory_order_relaxed);
    }

    /// @brief 绑定 driver 线程后，被跨线程调用而丢弃的受守卫入口调用累计数。
    /// @note 稳态恒 0；>0 表示有非 driver 线程直调 engine（丢弃前那就是 data race）。
    [[nodiscard]] uint64_t cross_thread_dropped() const noexcept {
        return cross_thread_dropped_.load(std::memory_order_relaxed);
    }

    /// @brief 因 strategy_id 无对应 ctx 而整次丢弃的 `on_target_weights` 累计数。
    [[nodiscard]] uint64_t unregistered_sid_weight_drops() const noexcept {
        return unregistered_sid_weight_drops_;
    }

private:
    /// @brief driver 线程契约检查（release 也生效）。
    ///
    /// 未绑定时无条件放行；绑定后只放行 driver 线程，其余调用累加
    /// `cross_thread_dropped_` + log-once 后返回 false，caller 必须 fail-closed。
    /// @param api 被拦截的入口名（日志用，须为字面量）。
    [[nodiscard]] bool on_driver_thread_or_drop_(const char* api) const noexcept;

    /// @brief 按 strategy_id 找到 ctx index。
    /// @note context 数量很小，线性查找即可。
    [[nodiscard]] size_t find_ctx_idx_(const std::string& strategy_id) const;

    /// @brief ctx[idx] 是否已因 on_init 失败被 fail-closed disable。
    /// @note disabled 集恒空（正常路径）时对每个 fan-out 只多一个廉价 false 分支。
    [[nodiscard]] bool is_ctx_disabled_(size_t idx) const noexcept {
        return idx < ctx_disabled_.size() && ctx_disabled_[idx] != 0;
    }

    /// @brief pending 目标缓存（SELL 桶 / BUY 桶 separate）。
    ///
    /// 存**绝对目标**而非规划期差额：差额在 dispatch 时按最新持仓重算。target weights
    /// 是幂等语义——同槽两个 task 各下 target=100 时终仓必须是 100 而不是 200，冻结
    /// 差额会把「幂等替换」变成「差额可加」。`planned_diff_fp` 只决定 SELL/BUY 分桶
    /// （即执行顺序），不参与数量计算。
    struct PendingDiff {
        std::string strategy_id;
        std::string code;
        int64_t     target_qty_fp   = 0;  ///< 绝对目标（整 lot 倍数）
        int64_t     planned_diff_fp = 0;  ///< 规划期差额；仅用于分桶与日志
        int64_t     price_fp        = 0;
        std::string tag;
    };

    /// @brief 一次 fire 产出的差额组；组间 FIFO，组内 SELL 先于 BUY。
    struct PendingGroup {
        uint64_t                 fire_seq = 0;
        std::vector<PendingDiff> sells;
        std::vector<PendingDiff> buys;
        [[nodiscard]] bool empty() const noexcept { return sells.empty() && buys.empty(); }
    };

    /// @brief `dispatch_` 的处置结果（跨桶方向翻转的执行时点裁定）。
    enum class DispatchOutcome {
        kDispatched = 0,  ///< 已调 handle_pos_change_fn
        kNoop,            ///< 重算差额归零（更早的成交已达成 target），未派发
        kDeferToBuy,      ///< SELL 桶重算后翻 BUY：本 phase 不派发，须改挂 BUY 桶
    };

    /// @brief 按 phase 取第一个含待 fire 条目的组；无则 nullptr。
    [[nodiscard]] PendingGroup* first_group_with_(bool want_sells) noexcept;
    /// @brief 提交单条 pending 并置/复位 current_fire_seq_。
    /// @param from_sell_bucket 该条目出自 SELL 桶；重算后翻 BUY 时返回 kDeferToBuy
    ///        （不派发），由 caller 调 `requeue_as_buy_` 归位，保住先卖后买的现金序。
    /// @param out_diff_fp 出参：本次重算出的差额（kDeferToBuy 时即归位后的新分桶依据）。
    DispatchOutcome dispatch_(const PendingDiff& e, uint64_t fire_seq,
                              bool from_sell_bucket, int64_t& out_diff_fp);
    /// @brief 把翻向条目挂回同 fire_seq 组的 BUY 桶（组已摘空被丢弃时按同 fire_seq 重建）。
    /// @param replanned_diff_fp 归位后的分桶依据（重算值）：覆盖 `planned_diff_fp`，
    ///        否则 BUY phase 再判一次仍与原始负差额反向，同一次翻转被计两遍。
    void requeue_as_buy_(const PendingDiff& e, uint64_t fire_seq,
                         int64_t replanned_diff_fp);
    /// @brief 丢弃已 fire 空的组（组数 = 同分钟 task 数，线性扫足够）。
    void drop_empty_groups_() noexcept;

    Config cfg_;
    std::vector<std::unique_ptr<XtStrategyContext>> contexts_;  ///< owns ctx
    std::vector<XtStrategyBase*> strategies_;  ///< non-owning, parallel to contexts_
    std::vector<std::string> strategy_ids_;    ///< strategy_id cache, parallel to contexts_
    std::vector<uint8_t> ctx_disabled_;        ///< parallel to contexts_；on_init 失败置 1，fan-out 跳过。
    std::atomic<uint64_t> strategy_init_failure_total_{0};  ///< on_init 失败被 fail-closed disable 的 ctx 累计数。

    /// @brief per-fire 差额组队列（FIFO；由 phase fire）。
    std::vector<PendingGroup> pending_groups_;

    /// @brief on_target_weights 调用序号（run 内单调，>0）。
    uint64_t fire_seq_next_ = 0;
    /// @brief 当前正在 dispatch 的组 fire_seq；非 fire 期 0。
    uint64_t current_fire_seq_ = 0;

    /// @brief sparse-weights 观测计数（held_symbols_resolver_fn 注入时才累加）。
    uint64_t sparse_weights_events_ = 0;

    /// @brief 负目标权重被拒次数（long-only 防御闸）。
    uint64_t negative_weight_rejects_ = 0;

    /// @brief 当前 schedule 派发中的 task id；非 schedule 期 0（fire 记录归因用）。
    uint32_t current_task_id_ = 0;

    // ── pending 语义观测（target 幂等替换的可见面）────────────────────────────
    uint64_t pending_dispatch_noops_ = 0;
    uint64_t pending_dispatch_direction_flips_ = 0;
    uint64_t cross_sid_same_code_pendings_ = 0;
    uint64_t same_slot_target_replacements_ = 0;
    uint64_t stale_pending_groups_dropped_ = 0;

    // ── 整数投影 hook 观测 ─────────────────────────────────────────────────
    uint64_t projection_adjusted_targets_ = 0;
    uint64_t projection_rejected_entries_ = 0;

    /// @brief 未注册 strategy_id 的 on_target_weights 丢弃数（driver 线程写）。
    uint64_t unregistered_sid_weight_drops_ = 0;

    // ── driver 线程闩（fail-closed）──────────────────────────────────
    // 两字段均 atomic：driver 线程自绑（含 restart 重绑）与其它线程的守卫读
    // 并发，tid 非 atomic 会在重绑窗口构成 data race。
    std::atomic<bool> driver_bound_{false};          ///< bind_driver_thread 已调
    std::atomic<std::thread::id> driver_tid_{};      ///< 权威 driver tid（可重绑）
    mutable std::atomic<uint64_t> cross_thread_dropped_{0};  ///< 跨线程丢弃累计
};

}  // namespace xt_engine_sel
