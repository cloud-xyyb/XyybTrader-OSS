#include "XtSelEngine.h"

#include <algorithm>
#include <atomic>
#include <climits>
#include <cstdio>
#include <exception>
#include <utility>
#include <vector>

#include "XtStrategyBase.h"
#include "XtStrategyContext.h"
#include "XtLogger.h"
#include "xt_logger/XtCallbackGuard.h"
#include "xt_core/XtFixedPoint.h"  // xt_fp::to_fp / mul / div

namespace xt_engine_sel {

namespace {

/// @brief  qty_fp 向零截断到 min_lot 整数倍（非四舍五入）。
/// @details A 股 SH 主板 lot=100：125000 → 125000；124937.6 → 124900；-2350 → -2300。
[[nodiscard]] int64_t round_to_min_lot_fp(int64_t qty_fp, int min_lot) noexcept {
    if (min_lot <= 1) return qty_fp;
    const int64_t lot_fp = static_cast<int64_t>(min_lot) * kPriceFpScale;
    if (lot_fp == 0) return qty_fp;
    const int64_t abs_fp = (qty_fp < 0) ? -qty_fp : qty_fp;
    const int64_t rounded = (abs_fp / lot_fp) * lot_fp;
    return (qty_fp < 0) ? -rounded : rounded;
}

// SEL 策略回调守卫吞掉的异常累计 (swallow + count); SEL 无 metric pipeline，用 log + counter。
std::atomic<uint64_t> g_sel_strategy_exc_total_{0};

}  // namespace

const char* sel_planning_status_name(SelPlanningStatus s) noexcept {
    switch (s) {
        case SelPlanningStatus::kOk:          return "ok";
        case SelPlanningStatus::kNoPrice:     return "no_price";
        case SelPlanningStatus::kMinLotZero:  return "min_lot_zero";
        case SelPlanningStatus::kNoChange:    return "no_change";
        case SelPlanningStatus::kNegativeWeight: return "negative_weight";
    }
    return "ok";
}

XtSelEngine::XtSelEngine(Config cfg) : cfg_(std::move(cfg)) {
    if (!cfg_.scheduler) {
        XtLogger::error_m("SEL", "XtSelEngine: scheduler is null");
    }
    if (!cfg_.holiday_provider) {
        XtLogger::warn_m("SEL", "XtSelEngine: holiday_provider is null (节假日跳过失效)");
    }
    if (!cfg_.handle_pos_change_fn) {
        XtLogger::warn_m("SEL", "XtSelEngine: handle_pos_change_fn is null "
                                 "(target_weights 转发禁用)");
    }
}

XtSelEngine::~XtSelEngine() = default;

void XtSelEngine::bind_driver_thread(std::thread::id tid) noexcept {
    // tid 存 atomic：restart（stop→start 新 driver 线程）时重绑会在 driver_bound_
    // 已为 true 的情况下改写 tid，此刻其它线程的守卫读与本写必须无 race。
    driver_tid_.store(tid, std::memory_order_release);
    driver_bound_.store(true, std::memory_order_release);
}

bool XtSelEngine::on_driver_thread_or_drop_(const char* api) const noexcept {
    // 未绑定 = 装配期主线程 / 单线程 backtest driver：无条件放行，也不记忆首触线程
    // （首触会把装配主线程锁成权威，随后 driver 线程的正常调用全被误拦）。
    if (!driver_bound_.load(std::memory_order_acquire)) return true;
    if (std::this_thread::get_id() == driver_tid_.load(std::memory_order_acquire)) return true;
    // 这些入口写的是 pending 组 / scheduler 表 / ctx fan-out 等无锁 mutable 状态，
    // 跨线程进来就是 data race；丢弃比让一笔来路不明的调仓落进 pending 安全。
    cross_thread_dropped_.fetch_add(1, std::memory_order_relaxed);
    // 走 stderr 而非 XtLogger：本函数 noexcept，格式化/落盘抛异常会直接 terminate。
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
        std::fprintf(stderr,
                     "[SEL-ENGINE][error] cross-thread call blocked: api='%s' — "
                     "schedule/weights/session/params/task entries accept the driver "
                     "thread only; this call was dropped (repeats counted, not logged)\n",
                     api);
    }
    return false;
}

size_t XtSelEngine::add_context(std::unique_ptr<XtStrategyContext> ctx,
                                 XtStrategyBase* strategy) {
    if (!ctx) {
        XtLogger::error_m("SEL", "add_context: ctx is null");
        return SIZE_MAX;
    }
    if (!strategy) {
        XtLogger::error_m("SEL", "add_context: strategy is null");
        return SIZE_MAX;
    }
    const size_t idx = contexts_.size();
    const std::string strategy_id = ctx->strategy_id();
    if (strategy_id.empty()) {
        XtLogger::error_m("SEL", "add_context: strategy_id is empty");
        return SIZE_MAX;
    }
    if (find_ctx_idx_(strategy_id) != SIZE_MAX) {
        XtLogger::error_m("SEL", "add_context: duplicate strategy_id='{}'", strategy_id);
        return SIZE_MAX;
    }
    strategy_ids_.push_back(strategy_id);
    contexts_.push_back(std::move(ctx));
    strategies_.push_back(strategy);
    ctx_disabled_.push_back(0);  // parallel；on_init 失败前恒 0（fan-out 等价旧路径）。
    // registry 已 freeze（live 装配 finalize_sid_registry 先于 add_context）：
    // 装配单线程内预解析 sid_id，mark_init_failed / 回报归因读 resolved_sid_id()
    // 拿真值而非未解析的 0（mirror HFT）。BT 无全局 registry → 解析得 0，与
    // 惰性首触等价。
    contexts_.back()->ensure_sid_resolved();
    XtLogger::info_m("SEL", "add_context: idx={} strategy_id={}",
                     idx, strategy_ids_.back());
    return idx;
}

XtStrategyContext* XtSelEngine::context(size_t idx) noexcept {
    return idx < contexts_.size() ? contexts_[idx].get() : nullptr;
}

const XtStrategyContext* XtSelEngine::context(size_t idx) const noexcept {
    return idx < contexts_.size() ? contexts_[idx].get() : nullptr;
}

bool XtSelEngine::has_strategy(const std::string& strategy_id) const {
    return find_ctx_idx_(strategy_id) != SIZE_MAX;
}

bool XtSelEngine::set_strategy_control_enabled(const std::string& strategy_id,
                                               bool enabled) {
    const size_t idx = find_ctx_idx_(strategy_id);
    if (idx == SIZE_MAX || idx >= contexts_.size() || !contexts_[idx]) return false;
    contexts_[idx]->set_control_enabled(enabled);
    return true;
}

void XtSelEngine::mark_init_failed(size_t idx) {
    if (idx >= contexts_.size() || idx >= ctx_disabled_.size()) return;
    if (ctx_disabled_[idx]) return;  // 幂等
    ctx_disabled_[idx] = 1;
    strategy_init_failure_total_.fetch_add(1, std::memory_order_relaxed);
    const std::string& name = strategy_ids_[idx];
    const uint32_t sid_id = contexts_[idx] ? contexts_[idx]->resolved_sid_id() : 0;
    XtLogger::critical(
        "[SEL-ENGINE] strategy '{}' (sid_id={}) on_init 失败 — fail-closed disabled："
        "schedule/session/target_weights fan-out 跳过该策略；兄弟策略不受影响。"
        "若该策略有恢复持仓则无人管理，需 operator 处置。",
        name.c_str(), sid_id);
    if (cfg_.on_strategy_init_failure) cfg_.on_strategy_init_failure(sid_id, name);
}

void XtSelEngine::update_params(uint64_t version) {
    if (!on_driver_thread_or_drop_("update_params")) return;
    for (size_t i = 0; i < contexts_.size(); ++i) {
        if (is_ctx_disabled_(i)) continue;
        auto* ctx = contexts_[i].get();
        if (!ctx) continue;
        const std::string sid = ctx->strategy_id();
        xt_log::run_guarded("SEL", "strategy.on_params_updated",
                            xt_log::guard_item(i, sid.c_str(), &g_sel_strategy_exc_total_),
                            [&] { ctx->on_params_updated(version); });
    }
}

void XtSelEngine::on_minute_tick(uint32_t curDate, uint32_t curTime) {
    if (!on_driver_thread_or_drop_("on_minute_tick")) return;
    if (!cfg_.scheduler) return;
    cfg_.scheduler->on_minute_end(
        curDate, curTime,
        [this](const SelTaskInfo& task, uint32_t cd, uint32_t ct, int64_t ft_ms) {
            on_schedule_fire(task, cd, ct, ft_ms);
        });
}

void XtSelEngine::on_schedule_fire(const SelTaskInfo& task,
                                     uint32_t curDate,
                                     uint32_t curTime,
                                     int64_t fireTime_ms) {
    // 公开入口独立过闸（不只靠 on_minute_tick 外闸）：写 current_task_id_ /
    // fire_seq 并直接回调 strategy，跨线程进来同样是 race。
    if (!on_driver_thread_or_drop_("on_schedule_fire")) return;
    const size_t idx = find_ctx_idx_(task.strategy_id);
    if (idx == SIZE_MAX) {
        XtLogger::warn_m("SEL", "on_schedule_fire: no context for strategy_id='{}'",
                         task.strategy_id);
        return;
    }
    if (is_ctx_disabled_(idx)) return;  // fail-closed：disabled 策略不 fire schedule。
    auto* strategy = strategies_[idx];
    auto* ctx = contexts_[idx].get();
    if (!strategy || !ctx) return;

    // 派发前注入计划 fire 时刻（真 epoch ms，由 curDate/curTime 经 CST 换算，
    // live/backtest 同式确定性——见 SelTaskScheduler 计算处）：策略经
    // ctx->current_fire_time_ms() 可与 now_ms 做 fire-latency 延迟差值。
    ctx->set_current_fire_time_ms(fireTime_ms);
    // task 身份随派发窗口下传：同 sid 多 task 时 fire 记录必须能区分是哪个 task
    // 触发的（只按 strategy_id 归因会把两次 fire 混成一条）。
    current_task_id_ = task.id;
    // 调策略 on_schedule callback（4 参签名 wt parity）；守卫吞异常防 worker 线程崩。
    xt_log::run_guarded("SEL", "strategy.on_schedule",
                        xt_log::guard_item(idx, task.strategy_id.c_str(),
                                           &g_sel_strategy_exc_total_),
                        [&] { strategy->on_schedule(ctx, curDate, curTime, fireTime_ms); });
    // 派发返回即复位：非 schedule 回调读 0（契约「0 = 非 SEL 或尚未 fire」），
    // 不泄漏上一次调度的陈旧值。
    current_task_id_ = 0;
    ctx->set_current_fire_time_ms(0);
}

void XtSelEngine::on_target_weights(
    const std::string& strategy_id,
    const std::unordered_map<std::string, double>& weights) {
    if (!on_driver_thread_or_drop_("on_target_weights")) return;
    XtLogger::info_m("SEL", "on_target_weights: strategy_id='{}' n_symbols={}",
                     strategy_id, weights.size());

    // fire 身份先分配：所有早退路径都要发一条 fire 记录，消费方才能把「回调执行
    // 了但没产出调仓」与「压根没触发」区分开。RAII 保证不遗漏 return 分支。
    SelFireRecord fire;
    fire.fire_seq    = ++fire_seq_next_;
    fire.strategy_id = strategy_id;
    fire.task_id     = current_task_id_;
    fire.n_targets   = weights.size();
    struct FireEmitter {
        const Config::FireSinkFn* sink;
        const SelFireRecord*      rec;
        // 析构里调用户 sink：抛出会在 unwinding 期 terminate。观测通道不值当搭上
        // 整个进程，吞掉并留日志。
        ~FireEmitter() {
            if (!*sink) return;
            try {
                (*sink)(*rec);
            } catch (const std::exception& e) {
                XtLogger::error_m("SEL", "fire_sink threw (swallowed): {}", e.what());
            } catch (...) {
                XtLogger::error_m("SEL", "fire_sink threw (swallowed, non-std)");
            }
        }
    } fire_emitter{&cfg_.fire_sink, &fire};

    // fail-closed：engine 不为自己不管理的 strategy_id 挂 target。装配期
    // `strat->on_init` 早于 `add_context`，此刻 ctx 尚未注册——放行等于让 on_init
    // 里发出的权重绕过 per-ctx disabled 闸直接落进 pending，随后被首个分钟管线执行
    // （即便该策略 on_init 失败已被 mark_init_failed）。
    // disabled 策略同样拒：正常路径其 on_schedule 已被跳过，此为防御性二道闸。
    {
        const size_t sidx = find_ctx_idx_(strategy_id);
        if (sidx == SIZE_MAX) {
            ++unregistered_sid_weight_drops_;
            XtLogger::warn_m("SEL",
                              "on_target_weights: no context for strategy_id='{}'; "
                              "整次 target 丢弃（engine 只为已注册策略挂 target）",
                              strategy_id);
            return;
        }
        if (is_ctx_disabled_(sidx)) return;
    }

    // sparse-weights 观测护栏（R4 契约要求 dense weights）：held symbol 集合中
    // 不在本次 weights keys 里的 symbol（被静默踢出、不带显式 0 权重、不会被平出的
    // 旧持仓）触发告警。按集合差集判定而非数量比较——"等数量换集合"（新权重覆盖数
    // 相同但 symbol 集不同）同样会漏掉被踢出的旧持仓，纯数量比较捕不到这种情形。
    // 仅 warn + 计数，不改行为；resolver 缺省时护栏惰性跳过。
    if (cfg_.held_symbols_resolver_fn) {
        const std::vector<std::string> held_codes = cfg_.held_symbols_resolver_fn();
        std::vector<std::string> missing;
        missing.reserve(held_codes.size());
        for (const auto& code : held_codes) {
            if (weights.find(code) == weights.end()) missing.push_back(code);
        }
        if (!missing.empty()) {
            ++sparse_weights_events_;
            std::sort(missing.begin(), missing.end());  // 日志确定性
            constexpr size_t kMaxShown = 5;
            std::string shown;
            const size_t show_n = std::min(missing.size(), kMaxShown);
            for (size_t i = 0; i < show_n; ++i) {
                if (i) shown += ", ";
                shown += missing[i];
            }
            if (missing.size() > show_n) shown += ", ...";
            XtLogger::warn_m("SEL",
                              "sparse weights: sid='{}' {} held symbol(s) missing from "
                              "weights (showing up to {}): {} "
                              "(R4 要求 dense；未覆盖的旧持仓不会被平出，仅观测不改行为)",
                              strategy_id, missing.size(), kMaxShown, shown);
        }
    }

    if (!cfg_.handle_pos_change_fn) return;

    // 真 diff 模式 — 三 resolver 全注入时计算
    //   target_qty_fp = weight × equity / close_price
    //   diff_pos_fp   = target_qty_fp - current_qty_fp
    // 任一 resolver 缺省 → 退化 trivial passthrough。
    const bool real_diff_mode =
        cfg_.price_resolver_fn && cfg_.position_resolver_fn && cfg_.equity_resolver_fn;

    if (!real_diff_mode) {
        // trivial passthrough（向后兼容）
        for (const auto& kv : weights) {
            cfg_.handle_pos_change_fn(strategy_id, kv.first, /*diff_pos_fp=*/0,
                                       /*price_fp=*/0, XtTimeInForce::GTC, "sel_weights");
        }
        return;
    }

    const int64_t equity_fp = cfg_.equity_resolver_fn();
    fire.equity_fp = equity_fp;
    if (equity_fp <= 0) {
        fire.equity_nonpositive = true;
        XtLogger::warn_m("SEL",
                          "on_target_weights: equity_fp={} <= 0; skip diff for sid='{}'",
                          equity_fp, strategy_id);
        return;
    }

    // SELL→BUY phased fire (实盘 PMC 标准, 与 BlackRock
    //   Aladdin / Bloomberg PORT / 国内迅投等对齐):
    //     Pass 1: compute diff per code, split 成本次 fire 的 sells / buys
    //     Pass 2 (caller drove): flush_pending(SellOnly) → drain → flush_pending(BuyOnly)
    //   不在本 method 内调 handle_pos_change_fn — 交给 flush_pending。
    //
    //   sort 保 deterministic：按 code 字典序，确保跨编译器 / std lib 版本输出一致。
    PendingGroup group;
    group.fire_seq = fire.fire_seq;

    std::vector<std::pair<std::string, double>> sorted_weights(weights.begin(), weights.end());
    std::sort(sorted_weights.begin(), sorted_weights.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // 决策发射：sink 未注入（live 默认）时整段不构造载荷。
    auto emit_decision = [&](const std::string& code, double weight, int64_t price_fp,
                             int64_t raw_fp, int64_t target_fp, int64_t pre_fp,
                             int64_t diff_fp, SelPlanningStatus status) {
        if (!cfg_.target_decision_sink) return;
        SelTargetDecision d;
        d.fire_seq          = fire.fire_seq;
        d.strategy_id       = strategy_id;
        d.code              = code;
        d.weight            = weight;
        d.price_fp          = price_fp;
        d.target_qty_raw_fp = raw_fp;
        d.target_qty_fp     = target_fp;
        d.pre_qty_fp        = pre_fp;
        d.diff_qty_fp       = diff_fp;
        d.status            = status;
        cfg_.target_decision_sink(d);
    };

    // 一次 fire 内单个 code 的发射槽：与 `sorted_weights` 逐位平行。早退条目
    // （负权重 / 无价）在此占位而不进投影批——它们没有可投影的目标。
    struct DecisionSlot {
        size_t            plan_idx     = 0;  ///< `early == false` 时的 plan/targets 下标
        SelPlanningStatus early_status = SelPlanningStatus::kOk;
        bool              early        = false;
    };

    // Pass 1：逐 code 算基线目标。**只收集不发射**——决策行统一由 Pass 3 按
    // `sorted_weights` 原序（= code 字典序）发出。早退条目若在此就地发射，产物的
    // 行序会变成「先全部早退、后全部正常」，而 target_weights.csv / events_full.csv
    // 的落地序是下游消费方的契约（normalize.py 依赖它），不能随内部分批而变。
    std::vector<DecisionSlot> slots;
    slots.reserve(sorted_weights.size());
    std::vector<SelTargetProjectionEntry> plan;
    plan.reserve(sorted_weights.size());
    for (const auto& kv : sorted_weights) {
        const std::string& code = kv.first;
        const double       weight = kv.second;

        // long-only 防御闸：负权重经 round-to-lot 会算出负 target，diff 跨零后
        // 整单撞 ShortNotAllowed（连平多腿一起丢），静默失真。显式拒该 symbol。
        if (weight < 0.0) {
            ++negative_weight_rejects_;
            XtLogger::warn_m("SEL",
                              "on_target_weights: code='{}' weight={} < 0 rejected "
                              "(SEL v1 long-only，无做空腿)", code, weight);
            slots.push_back({0, SelPlanningStatus::kNegativeWeight, /*early=*/true});
            continue;
        }

        const int64_t price_fp = cfg_.price_resolver_fn(code);
        if (price_fp <= 0) {
            XtLogger::warn_m("SEL",
                              "on_target_weights: code='{}' price_fp={} <= 0; skip",
                              code, price_fp);
            slots.push_back({0, SelPlanningStatus::kNoPrice, /*early=*/true});
            continue;
        }

        // weight 转 fp scale (~1e6)；xt_fp::mul/div 用 __int128 防溢出
        const int64_t weight_fp       = xt_fp::to_fp(weight);
        const int64_t target_value_fp = xt_fp::mul(weight_fp, equity_fp);

        SelTargetProjectionEntry e;
        e.code              = code;
        e.weight            = weight;
        e.price_fp          = price_fp;
        // 截断 TARGET 到整 lot（向零取整；工业标准, 与 wt SelMocker / Lean / zipline 对齐）。
        e.min_lot           = cfg_.min_lot_resolver_fn ? cfg_.min_lot_resolver_fn(code) : 1;
        e.target_qty_raw_fp = xt_fp::div(target_value_fp, price_fp);
        e.target_qty_fp     = round_to_min_lot_fp(e.target_qty_raw_fp, e.min_lot);
        e.pre_qty_fp        = cfg_.position_resolver_fn(code);
        slots.push_back({plan.size(), SelPlanningStatus::kOk, /*early=*/false});
        plan.push_back(std::move(e));
    }

    // Pass 2：整批投影（hook 未注入 = live/未配置 → 整段跳过，基线即终值）。
    std::vector<int64_t> targets;
    targets.reserve(plan.size());
    for (const auto& e : plan) targets.push_back(e.target_qty_fp);
    if (cfg_.target_projection_fn && !plan.empty()) {
        std::vector<int64_t> projected = targets;
        bool usable = true;
        try {
            cfg_.target_projection_fn(plan, projected);
        } catch (const std::exception& ex) {
            usable = false;
            XtLogger::error_m("SEL", "target_projection_fn threw: {} —— 整批还原基线",
                              ex.what());
        } catch (...) {
            usable = false;
            XtLogger::error_m("SEL", "target_projection_fn threw (non-std) —— 整批还原基线");
        }
        if (usable && projected.size() != plan.size()) {
            usable = false;
            XtLogger::error_m("SEL",
                              "target_projection_fn resized targets ({} → {}) —— 整批还原基线",
                              plan.size(), projected.size());
        }
        if (!usable) {
            projection_rejected_entries_ += plan.size();
        } else {
            for (size_t i = 0; i < plan.size(); ++i) {
                const int64_t lot_fp =
                    static_cast<int64_t>(plan[i].min_lot > 1 ? plan[i].min_lot : 1) *
                    kPriceFpScale;
                // 判据是「与基线之差是整手」，不是「写回值本身是整手」：min_lot=1
                // 时基线压根不做整手截断（`weight × equity / price` 通常带零头），
                // 按绝对值判会把没被改过的基线也判违约、整批空转。差额判在两种
                // min_lot 下都保住同一个不变量——基线整手 ⇒ 结果整手。
                if (projected[i] < 0 || (projected[i] - targets[i]) % lot_fp != 0) {
                    ++projection_rejected_entries_;
                    XtLogger::error_m("SEL",
                                      "target_projection_fn: code='{}' 写回 {} 违约"
                                      "（负值，或与基线 {} 之差不是 {} 的整数倍）—— 还原基线",
                                      plan[i].code, projected[i], targets[i],
                                      plan[i].min_lot);
                    continue;
                }
                if (projected[i] != targets[i]) ++projection_adjusted_targets_;
                targets[i] = projected[i];
            }
        }
    }

    // Pass 3：发决策行 + 分桶（数量口径以投影终值为准）。遍历序 = sorted_weights
    // 原序，早退条目就地发射 —— 产物行序因此与「未配置投影」时逐字节相同。
    for (size_t si = 0; si < sorted_weights.size(); ++si) {
        if (slots[si].early) {
            emit_decision(sorted_weights[si].first, sorted_weights[si].second,
                          /*price_fp=*/0, 0, 0, 0, 0, slots[si].early_status);
            continue;
        }
        const size_t pi = slots[si].plan_idx;
        const std::string& code = plan[pi].code;
        const double  weight          = plan[pi].weight;
        const int64_t price_fp        = plan[pi].price_fp;
        const int64_t target_qty_raw  = plan[pi].target_qty_raw_fp;
        const int64_t target_qty_fp   = targets[pi];
        const int64_t current_qty_fp  = plan[pi].pre_qty_fp;
        const int64_t diff_pos_fp     = target_qty_fp - current_qty_fp;

        XtLogger::debug_m("SEL",
                           "diff: code='{}' weight={} eq_fp={} px_fp={} target_raw={} "
                           "target_round={} current_qty_fp={} diff_pos_fp={}",
                           code, weight, equity_fp, price_fp,
                           target_qty_raw, target_qty_fp,
                           current_qty_fp, diff_pos_fp);

        const SelPlanningStatus status =
            (target_qty_raw != 0 && target_qty_fp == 0) ? SelPlanningStatus::kMinLotZero
            : (diff_pos_fp == 0)                        ? SelPlanningStatus::kNoChange
                                                        : SelPlanningStatus::kOk;
        emit_decision(code, weight, price_fp, target_qty_raw, target_qty_fp,
                      current_qty_fp, diff_pos_fp, status);

        // 幂等替换的完整实现：同 (sid,code) 已有未 dispatch 的 pending → 从旧 fire
        // 组中摘除（其目标已被本次决策取代；摘除而非就地改值，否则该腿仍挂在旧
        // fire_seq 名下——trace 归组与下游按 fire 新旧判断的消费方都会把它当旧目标）。
        // 摘除后按本次决策重建：diff 非零照常入本 fire 组；diff==0（kNoChange/
        // kMinLotZero）不新建 = 后发「维持现状」真正生效，而不是被跳过后任由旧
        // 目标的剩余部分继续执行。
        for (auto& g : pending_groups_) {
            const auto scrub = [&](std::vector<PendingDiff>& batch) {
                for (auto it = batch.begin(); it != batch.end();) {
                    if (it->code == code && it->strategy_id == strategy_id) {
                        ++same_slot_target_replacements_;
                        XtLogger::info_m("SEL",
                                          "same-slot target replaced: sid='{}' code='{}' "
                                          "old_target_fp={} new_target_fp={}",
                                          strategy_id, code, it->target_qty_fp,
                                          target_qty_fp);
                        it = batch.erase(it);
                    } else {
                        ++it;
                    }
                }
            };
            scrub(g.sells);
            scrub(g.buys);
        }
        if (diff_pos_fp == 0) continue;  // 无（或已摘除）旧 pending 且无差额 = 真 no-op

        // 同一 code 已被别的 sid 挂了 target：两个策略各自认为自己拥有这份持仓，
        // 后 dispatch 的会把先 dispatch 的成交量当成「已有仓」再抹平。只观测不改
        // 行为（真正的解是账户维度隔离，见 same-symbol 多策略架构裁定）。
        for (const auto& g : pending_groups_) {
            const auto conflicts = [&](const std::vector<PendingDiff>& batch) {
                for (const auto& p : batch) {
                    if (p.code == code && p.strategy_id != strategy_id) return true;
                }
                return false;
            };
            if (conflicts(g.sells) || conflicts(g.buys)) {
                ++cross_sid_same_code_pendings_;
                XtLogger::warn_m("SEL",
                                  "pending target conflict: code='{}' 已被 sid='{}' 挂单，"
                                  "本次 sid='{}' 的 target 会以 dispatch 时的实时持仓重算"
                                  "（同 code 跨策略 target 语义冲突，仅观测）",
                                  code, g.sells.empty() ? g.buys.front().strategy_id
                                                        : g.sells.front().strategy_id,
                                  strategy_id);
                break;
            }
        }

        // Split 成 SELL / BUY 缓存（存绝对 target；差额在 dispatch 时重算）
        PendingDiff entry;
        entry.strategy_id     = strategy_id;
        entry.code            = code;
        entry.target_qty_fp   = target_qty_fp;
        entry.planned_diff_fp = diff_pos_fp;
        entry.price_fp        = price_fp;
        entry.tag             = "sel_weights";
        if (diff_pos_fp < 0) group.sells.push_back(std::move(entry));
        else                 group.buys.push_back(std::move(entry));
    }

    XtLogger::info_m("SEL",
                      "on_target_weights cached: sid='{}' fire_seq={} sells={} buys={}",
                      strategy_id, group.fire_seq, group.sells.size(), group.buys.size());
    // 幂等替换可能把旧 fire 组摘空：就地清掉，否则空组活到次日 session 边界被
    // 误计进 stale_pending_groups_dropped（观测假阳性；派单不受影响）。
    drop_empty_groups_();
    if (!group.empty()) pending_groups_.push_back(std::move(group));
}

size_t XtSelEngine::pending_sell_count() const noexcept {
    size_t n = 0;
    for (const auto& g : pending_groups_) n += g.sells.size();
    return n;
}

size_t XtSelEngine::pending_buy_count() const noexcept {
    size_t n = 0;
    for (const auto& g : pending_groups_) n += g.buys.size();
    return n;
}

XtSelEngine::PendingGroup* XtSelEngine::first_group_with_(bool want_sells) noexcept {
    for (auto& g : pending_groups_) {
        if (want_sells ? !g.sells.empty() : !g.buys.empty()) return &g;
    }
    return nullptr;
}

XtSelEngine::DispatchOutcome XtSelEngine::dispatch_(const PendingDiff& e,
                                                   uint64_t fire_seq,
                                                   bool from_sell_bucket,
                                                   int64_t& out_diff_fp) {
    // 差额在此重算（pending 存绝对 target）：target weights 是幂等替换语义，同槽
    // 后一个 target 必须覆盖前一个的效果，而不是与之相加。resolver 缺省的
    // passthrough 形态没有持仓真值，退回规划期差额。
    int64_t diff_fp = e.planned_diff_fp;
    out_diff_fp = diff_fp;
    if (cfg_.position_resolver_fn) {
        diff_fp = e.target_qty_fp - cfg_.position_resolver_fn(e.code);
        out_diff_fp = diff_fp;
        if (diff_fp == 0) {
            ++pending_dispatch_noops_;
            XtLogger::info_m("SEL",
                              "pending dropped at dispatch: sid='{}' code='{}' "
                              "target={} 已由更早的成交达成（幂等替换）",
                              e.strategy_id, e.code, e.target_qty_fp);
            return DispatchOutcome::kNoop;
        }
        // 分桶按规划期方向，重算后可能反向：数量以重算值为准（幂等优先），但**执行
        // 时点按重算后的真方向归位**，否则 SELL phase 内会插进一笔买单，先卖后买的
        // 现金序被破坏（BUY 用的是还没释放出来的钱）。
        //   SELL 桶翻 BUY → 改挂 BUY 桶，延迟到 BUY phase 执行（本 phase 不派发）；
        //   BUY 桶翻 SELL → 立即执行（卖出只释放现金，任何时点都不透支）。
        if ((diff_fp > 0) != (e.planned_diff_fp > 0)) {
            ++pending_dispatch_direction_flips_;
            if (from_sell_bucket) {
                XtLogger::warn_m("SEL",
                                  "pending direction flipped at dispatch: sid='{}' "
                                  "code='{}' planned={} recomputed={} —— 推迟到 BUY "
                                  "phase 执行（不在 SELL phase 插买单）",
                                  e.strategy_id, e.code, e.planned_diff_fp, diff_fp);
                return DispatchOutcome::kDeferToBuy;
            }
            XtLogger::warn_m("SEL",
                              "pending direction flipped at dispatch: sid='{}' code='{}' "
                              "planned={} recomputed={} —— 立即执行（卖出现金安全）",
                              e.strategy_id, e.code, e.planned_diff_fp, diff_fp);
        }
    }
    current_fire_seq_ = fire_seq;
    cfg_.handle_pos_change_fn(e.strategy_id, e.code, diff_fp,
                               e.price_fp, XtTimeInForce::GTC, e.tag);
    current_fire_seq_ = 0;
    return DispatchOutcome::kDispatched;
}

void XtSelEngine::requeue_as_buy_(const PendingDiff& e, uint64_t fire_seq,
                                  int64_t replanned_diff_fp) {
    // 归位后该条目的桶方向就是 BUY：`planned_diff_fp` 改写成重算值（它只决定分桶与
    // 日志），否则 BUY phase 再算一次仍与原始负差额反向，同一次翻转被计两遍。
    PendingDiff moved = e;
    moved.planned_diff_fp = replanned_diff_fp;
    // 归位到原 fire 的 BUY 桶（组内 code 序仍是字典序追加）；该组可能已因摘空被
    // 丢弃，此时按同 fire_seq 重建一组挂到队尾（组数 = 同分钟 task 数，线性扫足够）。
    for (auto& g : pending_groups_) {
        if (g.fire_seq == fire_seq) {
            g.buys.push_back(std::move(moved));
            return;
        }
    }
    PendingGroup fresh;
    fresh.fire_seq = fire_seq;
    fresh.buys.push_back(std::move(moved));
    pending_groups_.push_back(std::move(fresh));
}

void XtSelEngine::drop_empty_groups_() noexcept {
    pending_groups_.erase(
        std::remove_if(pending_groups_.begin(), pending_groups_.end(),
                       [](const PendingGroup& g) { return g.empty(); }),
        pending_groups_.end());
}

bool XtSelEngine::fire_one_pending(Phase phase) {
    if (!cfg_.handle_pos_change_fn) return false;
    // 组间 FIFO、组内保持 on_target_weights 的 code 字典序；单组时与旧的
    // 单一 sells_/buys_ 队列逐条 pop 完全同序。
    auto fire_front = [this](bool want_sells) -> bool {
        PendingGroup* g = first_group_with_(want_sells);
        if (!g) return false;
        auto& batch = want_sells ? g->sells : g->buys;
        const uint64_t fire_seq = g->fire_seq;
        const PendingDiff e = batch.front();  // dispatch 前取值：回调内可能追加新组
        batch.erase(batch.begin());
        drop_empty_groups_();
        // 方向翻转的 SELL 条目改挂 BUY 桶：本 phase 不派发，但队列已前进（SELL 桶
        // 计数递减），caller 的 while(pending_sell_count()>0) 仍单调收敛。
        int64_t diff_fp = 0;
        if (dispatch_(e, fire_seq, /*from_sell_bucket=*/want_sells, diff_fp) ==
            DispatchOutcome::kDeferToBuy) {
            requeue_as_buy_(e, fire_seq, diff_fp);
        }
        return true;
    };
    switch (phase) {
        case Phase::SellOnly: return fire_front(/*want_sells=*/true);
        case Phase::BuyOnly:  return fire_front(/*want_sells=*/false);
        case Phase::All:
            if (pending_sell_count() > 0) return fire_front(/*want_sells=*/true);
            return fire_front(/*want_sells=*/false);
    }
    return false;
}

void XtSelEngine::flush_pending(Phase phase) {
    if (!on_driver_thread_or_drop_("flush_pending")) return;
    if (!cfg_.handle_pos_change_fn) {
        pending_groups_.clear();
        return;
    }

    // 先整体摘走待 fire 的条目再 dispatch：回调内若再触发 on_target_weights
    // （追加新组）不会让本次 flush 变成无界循环。
    auto take = [this](bool want_sells) {
        std::vector<std::pair<uint64_t, PendingDiff>> out;
        for (auto& g : pending_groups_) {
            auto& batch = want_sells ? g.sells : g.buys;
            for (auto& e : batch) out.emplace_back(g.fire_seq, std::move(e));
            batch.clear();
        }
        drop_empty_groups_();
        return out;
    };
    auto fire_batch = [this, &take](bool want_sells) {
        for (const auto& [fire_seq, e] : take(want_sells)) {
            // 翻向的 SELL 条目改挂 BUY 桶（本 phase 不派发）：SellOnly 调用返回后
            // 由随后的 BuyOnly（或 All 的第二段 take(buys)）消费。
            int64_t diff_fp = 0;
            if (dispatch_(e, fire_seq, /*from_sell_bucket=*/want_sells, diff_fp) ==
                DispatchOutcome::kDeferToBuy) {
                requeue_as_buy_(e, fire_seq, diff_fp);
            }
        }
    };

    switch (phase) {
        case Phase::SellOnly:
            fire_batch(/*want_sells=*/true);
            break;
        case Phase::BuyOnly:
            fire_batch(/*want_sells=*/false);
            break;
        case Phase::All:
            // @warning 整 phase 先摘队再回调：若 SELL 回调内又触发 on_target_weights
            //          追加新组，第二段 take(buys) 会消费新组的 BUY 却留下它的 SELL，
            //          组内 SELL-first 不再成立。生产 caller（BT runner 逐笔 fire、
            //          live scheduler 分两次 SellOnly/BuyOnly）都不用这种重入形态。
            fire_batch(/*want_sells=*/true);   // SELL first (cash 释放)
            fire_batch(/*want_sells=*/false);
            break;
    }
}

void XtSelEngine::on_session_begin(uint32_t trading_date) {
    if (!on_driver_thread_or_drop_("on_session_begin")) return;
    // 跨日残留 pending：正常路径每日 fire 后被 drain 干净；残留说明上一日的 flush/
    // fire 没跑完。带着昨日 target 进新一天会按新价新仓再执行一次，故在日界丢弃。
    if (!pending_groups_.empty()) {
        stale_pending_groups_dropped_ += pending_groups_.size();
        XtLogger::warn_m("SEL",
                          "session_begin {}: dropping {} stale pending group(s) from the "
                          "previous session（上一日未 drain 完，昨日 target 不跨日执行）",
                          trading_date, pending_groups_.size());
        pending_groups_.clear();
    }
    for (size_t i = 0; i < strategies_.size(); ++i) {
        if (is_ctx_disabled_(i)) continue;
        if (strategies_[i] && contexts_[i]) {
            xt_log::run_guarded("SEL", "strategy.on_session_begin",
                                xt_log::guard_item(i, strategy_ids_[i].c_str(),
                                                   &g_sel_strategy_exc_total_),
                                [&] { strategies_[i]->on_session_begin(contexts_[i].get(), trading_date); });
        }
    }
}

void XtSelEngine::on_session_end(uint32_t trading_date) {
    if (!on_driver_thread_or_drop_("on_session_end")) return;
    for (size_t i = 0; i < strategies_.size(); ++i) {
        if (is_ctx_disabled_(i)) continue;
        if (strategies_[i] && contexts_[i]) {
            xt_log::run_guarded("SEL", "strategy.on_session_end",
                                xt_log::guard_item(i, strategy_ids_[i].c_str(),
                                                   &g_sel_strategy_exc_total_),
                                [&] {
                strategies_[i]->on_session_end(contexts_[i].get(), trading_date);
                // SEL 专属 hook（与 wt on_session_end 模式对齐）。
                strategies_[i]->on_session_finish(contexts_[i].get());
            });
        }
    }
}

uint32_t XtSelEngine::add_task(SelTaskInfo info) {
    if (!on_driver_thread_or_drop_("add_task")) return 0;
    if (!cfg_.scheduler) return 0;
    return cfg_.scheduler->add_task(std::move(info));
}

size_t XtSelEngine::find_ctx_idx_(const std::string& strategy_id) const {
    for (size_t i = 0; i < strategy_ids_.size(); ++i) {
        if (strategy_ids_[i] == strategy_id) return i;
    }
    return SIZE_MAX;
}

}  // namespace xt_engine_sel
