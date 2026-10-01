#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "xt_engine/sel/XtSelEngine.h"
#include "xt_engine/sel/SelTaskScheduler.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_strategy_engine/XtStrategyContext.h"

namespace xt_engine_sel {
namespace {

class FakeHolidayProvider : public IHolidayProvider {
public:
    bool is_holiday(const std::string&, uint32_t, bool) const override { return false; }
    uint32_t next_trading_date(const std::string&, uint32_t curDate) const override { return curDate + 1; }
};

class RecordingStrategy final : public XtStrategyBase {
public:
    void on_schedule(IStrategyContext*,
                     uint32_t curDate,
                     uint32_t curTime,
                     int64_t fireTime_ms) override {
        ++fire_count;
        last_date = curDate;
        last_time = curTime;
        last_fire_time_ms = fireTime_ms;
    }

    int fire_count = 0;
    uint32_t last_date = 0;
    uint32_t last_time = 0;
    int64_t last_fire_time_ms = 0;
};

class ParamRecordingStrategy final : public XtStrategyBase {
public:
    void on_params(IStrategyContext*) override { ++params_count; }

    int params_count = 0;
};

class FakeParamPort final : public XtStrategyContext::IParamPort {
public:
    bool get(const std::string& key, std::string& out) const override {
        auto it = values.find(key);
        if (it == values.end()) return false;
        out = it->second;
        return true;
    }

    bool set(const std::string& key, const std::string& value) override {
        values[key] = value;
        ++version_value;
        return true;
    }

    uint64_t version() const override { return version_value; }

    std::unordered_map<std::string, std::string> values;
    uint64_t version_value = 1;
};

XtSelEngine make_engine(int* callback_count = nullptr) {
    auto holiday = std::make_unique<FakeHolidayProvider>();
    auto sched = std::make_unique<SelTaskScheduler>(holiday.get());
    XtSelEngine::Config cfg;
    cfg.scheduler = std::move(sched);
    cfg.holiday_provider = std::move(holiday);
    if (callback_count) {
        cfg.handle_pos_change_fn = [callback_count](const std::string&, const std::string&,
                                                      int64_t, int64_t,
                                                      XtTimeInForce, const std::string&) {
            (*callback_count)++;
        };
    } else {
        cfg.handle_pos_change_fn = [](const std::string&, const std::string&,
                                       int64_t, int64_t,
                                       XtTimeInForce, const std::string&) {};
    }
    return XtSelEngine(std::move(cfg));
}

std::unique_ptr<XtStrategyContext> make_sel_context(XtStrategyBase& strategy,
                                                    const std::string& strategy_id,
                                                    XtStrategyContext::IParamPort* param_port = nullptr) {
    XtStrategyContext::Config cfg;
    cfg.strategy = &strategy;
    cfg.strategy_id = strategy_id;
    cfg.param_port = param_port;
    cfg.on_weights_fn = [](const std::string&,
                            const std::unordered_map<std::string, double>&) {};
    return std::make_unique<XtStrategyContext>(cfg);
}

}  // namespace

TEST(XtSelEngine, ConstructEmpty) {
    auto engine = make_engine();
    EXPECT_EQ(engine.context_count(), 0u);
}

TEST(XtSelEngine, AddTask_ForwardsToScheduler) {
    auto engine = make_engine();

    SelTaskInfo task;
    task.period = TaskPeriod::Daily;
    task.time = 1500;
    task.strategy_id = "demo_sid";
    uint32_t id = engine.add_task(task);
    EXPECT_GT(id, 0u);
}

TEST(XtSelEngine, OnTargetWeights_InvokesHandlePosChange) {
    int callback_count = 0;
    auto engine = make_engine(&callback_count);
    RecordingStrategy strat;
    ASSERT_EQ(engine.add_context(make_sel_context(strat, "test_sid"), &strat), 0u);

    std::unordered_map<std::string, double> weights = {
        {"600000.SH", 0.3},
        {"000001.SZ", 0.4},
    };
    engine.on_target_weights("test_sid", weights);
    EXPECT_EQ(callback_count, 2);  // 2 symbols → 2 callbacks
}

TEST(XtSelEngine, OnMinuteTick_NoTasks_NoCallback) {
    int callback_count = 0;
    auto engine = make_engine(&callback_count);

    // 没注册 task，on_minute_tick 不应触发任何 handle_pos_change
    engine.on_minute_tick(20260120, 1500);
    EXPECT_EQ(callback_count, 0);
}

TEST(XtSelEngine, OnScheduleFireRoutesNamedStrategyId) {
    auto engine = make_engine();
    RecordingStrategy alpha;
    RecordingStrategy beta;

    ASSERT_EQ(engine.add_context(make_sel_context(alpha, "alpha_sid"), &alpha), 0u);
    ASSERT_EQ(engine.add_context(make_sel_context(beta, "beta_sid"), &beta), 1u);

    SelTaskInfo task;
    task.strategy_id = "beta_sid";
    engine.on_schedule_fire(task, 20260120, 1500, 202601201500LL);

    EXPECT_EQ(alpha.fire_count, 0);
    EXPECT_EQ(beta.fire_count, 1);
    EXPECT_EQ(beta.last_date, 20260120u);
    EXPECT_EQ(beta.last_time, 1500u);
    EXPECT_EQ(beta.last_fire_time_ms, 202601201500LL);
}

TEST(XtSelEngine, SetStrategyControlEnabledTargetsNamedContextOnly) {
    auto engine = make_engine();
    RecordingStrategy alpha;
    RecordingStrategy beta;

    ASSERT_EQ(engine.add_context(make_sel_context(alpha, "alpha_sid"), &alpha), 0u);
    ASSERT_EQ(engine.add_context(make_sel_context(beta, "beta_sid"), &beta), 1u);
    ASSERT_TRUE(engine.context(0));
    ASSERT_TRUE(engine.context(1));

    EXPECT_TRUE(engine.context(0)->can_submit());
    EXPECT_TRUE(engine.context(1)->can_submit());

    EXPECT_TRUE(engine.set_strategy_control_enabled("beta_sid", false));
    EXPECT_TRUE(engine.context(0)->can_submit());
    EXPECT_FALSE(engine.context(1)->can_submit());

    EXPECT_TRUE(engine.set_strategy_control_enabled("beta_sid", true));
    EXPECT_TRUE(engine.context(1)->can_submit());
    EXPECT_FALSE(engine.set_strategy_control_enabled("missing_sid", false));
}

// ── T1.135 + 集合比对升级 sparse-weights 观测护栏 ─────────────────────────
TEST(XtSelEngine, SparseWeightsGuardCountsUncoveredHeldSymbols) {
    auto holiday = std::make_unique<FakeHolidayProvider>();
    auto sched = std::make_unique<SelTaskScheduler>(holiday.get());
    XtSelEngine::Config cfg;
    cfg.scheduler = std::move(sched);
    cfg.holiday_provider = std::move(holiday);
    cfg.handle_pos_change_fn = [](const std::string&, const std::string&,
                                   int64_t, int64_t, XtTimeInForce, const std::string&) {};
    std::vector<std::string> held;
    cfg.held_symbols_resolver_fn = [&held]() -> std::vector<std::string> { return held; };
    XtSelEngine engine(std::move(cfg));
    RecordingStrategy strat;
    ASSERT_EQ(engine.add_context(make_sel_context(strat, "sid"), &strat), 0u);

    const std::unordered_map<std::string, double> two = {
        {"600000.SH", 0.5}, {"000001.SZ", 0.5}};

    // held={}：空集合 → 全覆盖 → 不触发
    engine.on_target_weights("sid", two);
    EXPECT_EQ(engine.sparse_weights_events(), 0u);

    // held={600000.SH, 000001.SZ}：held ⊆ weights keys → 全覆盖 → 不触发
    held = {"600000.SH", "000001.SZ"};
    engine.on_target_weights("sid", two);
    EXPECT_EQ(engine.sparse_weights_events(), 0u);

    // held 多一个不在 weights 里的 symbol（少覆盖场景）→ 触发一次，再来一次累加
    held = {"600000.SH", "000001.SZ", "600519.SH"};
    engine.on_target_weights("sid", two);
    EXPECT_EQ(engine.sparse_weights_events(), 1u);
    engine.on_target_weights("sid", two);
    EXPECT_EQ(engine.sparse_weights_events(), 2u);
}

// 回归护栏从"数量比较"升级为"集合比较"：等数量换集合（新 weights 覆盖数与
// held 数量相同，但 symbol 集合不同）必须触发——旧的 `weights.size() < held`
// 纯数量比较会漏掉这种情形（2 == 2 不触发），是本次升级要堵的漏洞。
TEST(XtSelEngine, SparseWeightsGuardCatchesSameCountDifferentSet) {
    auto holiday = std::make_unique<FakeHolidayProvider>();
    auto sched = std::make_unique<SelTaskScheduler>(holiday.get());
    XtSelEngine::Config cfg;
    cfg.scheduler = std::move(sched);
    cfg.holiday_provider = std::move(holiday);
    cfg.handle_pos_change_fn = [](const std::string&, const std::string&,
                                   int64_t, int64_t, XtTimeInForce, const std::string&) {};
    // held 2 symbols，weights 也覆盖 2 symbols，但集合不同（600519.SH 被静默换掉）。
    cfg.held_symbols_resolver_fn = []() -> std::vector<std::string> {
        return {"600000.SH", "600519.SH"};
    };
    XtSelEngine engine(std::move(cfg));
    RecordingStrategy strat;
    ASSERT_EQ(engine.add_context(make_sel_context(strat, "sid"), &strat), 0u);

    const std::unordered_map<std::string, double> two_different = {
        {"600000.SH", 0.5}, {"000001.SZ", 0.5}};  // 000001.SZ 换掉了 600519.SH

    engine.on_target_weights("sid", two_different);
    EXPECT_EQ(engine.sparse_weights_events(), 1u);  // 600519.SH 未被覆盖 → 触发
}

TEST(XtSelEngine, SparseWeightsGuardInertWithoutResolver) {
    int cb = 0;
    auto engine = make_engine(&cb);  // 未注入任何 held resolver
    RecordingStrategy strat;
    ASSERT_EQ(engine.add_context(make_sel_context(strat, "sid"), &strat), 0u);
    const std::unordered_map<std::string, double> one = {{"600000.SH", 1.0}};
    engine.on_target_weights("sid", one);
    EXPECT_EQ(engine.sparse_weights_events(), 0u);
    EXPECT_EQ(cb, 1);  // 护栏惰性但正常 passthrough
}

// 集合口径钉：held 数量 < weights 覆盖数时依然要检测出未覆盖 symbol——护栏纯按
// 集合差集判定，不受两侧相对数量大小影响（只按数量比较会误判 held=1 < 覆盖 2
// 为"数量足够、不触发"，掩盖 held 中确实存在的未覆盖 symbol）。
TEST(XtSelEngine, SparseWeightsGuardDetectsMissingHeldRegardlessOfCount) {
    auto holiday = std::make_unique<FakeHolidayProvider>();
    auto sched = std::make_unique<SelTaskScheduler>(holiday.get());
    XtSelEngine::Config cfg;
    cfg.scheduler = std::move(sched);
    cfg.holiday_provider = std::move(holiday);
    cfg.handle_pos_change_fn = [](const std::string&, const std::string&,
                                   int64_t, int64_t, XtTimeInForce, const std::string&) {};
    // held 只有 1 个 symbol，weights 覆盖 2 个——held 唯一的 symbol 不在 weights
    // 里，即便 held.size() < weights.size() 仍应触发（纯集合判定）。
    cfg.held_symbols_resolver_fn = []() -> std::vector<std::string> {
        return {"600519.SH"};
    };
    XtSelEngine engine(std::move(cfg));
    RecordingStrategy strat;
    ASSERT_EQ(engine.add_context(make_sel_context(strat, "sid"), &strat), 0u);

    const std::unordered_map<std::string, double> two = {
        {"600000.SH", 0.5}, {"000001.SZ", 0.5}};

    engine.on_target_weights("sid", two);
    EXPECT_EQ(engine.sparse_weights_events(), 1u);
}

TEST(XtSelEngine, UpdateParamsRefreshesContextsAndEnabledGate) {
    auto engine = make_engine();
    ParamRecordingStrategy alpha;
    ParamRecordingStrategy beta;
    FakeParamPort port;
    port.values["strategy.beta_sid.enabled"] = "false";

    ASSERT_EQ(engine.add_context(make_sel_context(alpha, "alpha_sid", &port), &alpha), 0u);
    ASSERT_EQ(engine.add_context(make_sel_context(beta, "beta_sid", &port), &beta), 1u);

    engine.update_params(port.version() + 1);

    EXPECT_EQ(alpha.params_count, 1);
    EXPECT_EQ(beta.params_count, 1);
    ASSERT_TRUE(engine.context(0));
    ASSERT_TRUE(engine.context(1));
    EXPECT_TRUE(engine.context(0)->can_submit());
    EXPECT_FALSE(engine.context(1)->can_submit());
}

// ── Fail-closed per-strategy disable on on_init failure（G4）───────────────────

namespace {

class SelSpyStrategy final : public XtStrategyBase {
public:
    void on_schedule(IStrategyContext*, uint32_t, uint32_t, int64_t) override { ++fires; }
    void on_session_begin(IStrategyContext*, uint32_t) override { ++sessions; }
    int fires = 0;
    int sessions = 0;
};

}  // namespace

// SEL 的 on_init 由 runner 装配期调用；失败时 runner 调 engine.mark_init_failed(idx)。
// 标记后 A fail-closed disabled：schedule fire / session fan-out 跳过 A，B 正常；
// failure 回调收到 A；failure_count==1；mark_init_failed 幂等。
TEST(XtSelEngine_InitFailure, MarkInitFailedDisablesCtxFailClosed) {
    int cap_count = 0;
    std::string cap_name;

    auto holiday = std::make_unique<FakeHolidayProvider>();
    auto sched = std::make_unique<SelTaskScheduler>(holiday.get());
    XtSelEngine::Config cfg;
    cfg.scheduler = std::move(sched);
    cfg.holiday_provider = std::move(holiday);
    cfg.handle_pos_change_fn = [](const std::string&, const std::string&,
                                   int64_t, int64_t, XtTimeInForce, const std::string&) {};
    cfg.on_strategy_init_failure =
        [&](uint32_t, const std::string& name) { ++cap_count; cap_name = name; };
    XtSelEngine engine(std::move(cfg));

    SelSpyStrategy stra_a, stra_b;
    const size_t idx_a = engine.add_context(make_sel_context(stra_a, "A"), &stra_a);
    const size_t idx_b = engine.add_context(make_sel_context(stra_b, "B"), &stra_b);
    ASSERT_NE(idx_a, SIZE_MAX);
    ASSERT_NE(idx_b, SIZE_MAX);

    // 模拟 A 的 on_init 失败（runner 装配期守卫捕获异常后调用）。
    engine.mark_init_failed(idx_a);
    EXPECT_EQ(engine.strategy_init_failure_count(), 1u);
    EXPECT_EQ(cap_count, 1);
    EXPECT_EQ(cap_name, "A");

    // schedule fire：disabled A 不 fire on_schedule；B 正常。
    SelTaskInfo task_a;
    task_a.strategy_id = "A";
    engine.on_schedule_fire(task_a, 20260401, 1500, 0);
    SelTaskInfo task_b;
    task_b.strategy_id = "B";
    engine.on_schedule_fire(task_b, 20260401, 1500, 0);
    EXPECT_EQ(stra_a.fires, 0);
    EXPECT_EQ(stra_b.fires, 1);

    // session fan-out：disabled A 跳过；B 正常。
    engine.on_session_begin(20260401);
    EXPECT_EQ(stra_a.sessions, 0);
    EXPECT_EQ(stra_b.sessions, 1);

    // mark_init_failed 幂等（重复调用不重复计数 / 不重复回调）。
    engine.mark_init_failed(idx_a);
    EXPECT_EQ(engine.strategy_init_failure_count(), 1u);
    EXPECT_EQ(cap_count, 1);
}

namespace {

/// @brief 进程级 sid registry 指针的 RAII 装/卸（失败路径也保证复位，防污染其它 case）。
struct GlobalSidRegistryGuard {
    explicit GlobalSidRegistryGuard(xt_identity::StrategySidRegistry* r) {
        xt_identity::StrategySidRegistry::set_global(r);
    }
    ~GlobalSidRegistryGuard() { xt_identity::StrategySidRegistry::set_global(nullptr); }
};

}  // namespace

// ── F-F driver 线程守卫 + 未注册 sid 丢弃 ──────────────────────────────────
//
// 守卫的 pre-bind 全放行是硬约束：XtSelBacktestRunner 单线程、从不 bind，守卫
// 必须对它完全惰性（下面第一条钉住这点）。

namespace {

/// @brief 装一个「真 diff 模式 + 已注册 ctx」的 engine（守卫钉共用）。
///
/// 三 resolver 全注入才会走 pending 分桶路径（passthrough 形态不进 pending，
/// 断言不到「跨线程调用没污染 pending」）。equity 100 万 / 价 10 元 / 现仓 0：
/// weight 1.0 → BUY 10 万股一条。
[[nodiscard]] std::unique_ptr<XtSelEngine> make_guard_case_engine(
    RecordingStrategy& strat, int* callback_count) {
    auto holiday = std::make_unique<FakeHolidayProvider>();
    auto sched   = std::make_unique<SelTaskScheduler>(holiday.get());
    XtSelEngine::Config cfg;
    cfg.scheduler        = std::move(sched);
    cfg.holiday_provider = std::move(holiday);
    cfg.handle_pos_change_fn = [callback_count](const std::string&, const std::string&,
                                                int64_t, int64_t, XtTimeInForce,
                                                const std::string&) {
        if (callback_count) ++(*callback_count);
    };
    cfg.price_resolver_fn    = [](const std::string&) -> int64_t { return 10'000'000; };
    cfg.position_resolver_fn = [](const std::string&) -> int64_t { return 0; };
    cfg.equity_resolver_fn   = []() -> int64_t { return 1'000'000'000'000; };
    cfg.min_lot_resolver_fn  = [](const std::string&) { return 1; };
    auto engine = std::make_unique<XtSelEngine>(std::move(cfg));
    EXPECT_EQ(engine->add_context(make_sel_context(strat, "sid"), &strat), 0u);
    return engine;
}

const std::unordered_map<std::string, double> kOneWeight = {{"600000.SH", 1.0}};

}  // namespace

// 未绑定态（= backtest driver / 装配期主线程）：全部入口照常执行、不计数。
TEST(XtSelEngine_ThreadGuard, PreBindCallsPassThroughUncounted) {
    int cb = 0;
    RecordingStrategy strat;
    auto engine = make_guard_case_engine(strat, &cb);

    SelTaskInfo info;
    info.strategy_id = "sid";
    info.period      = TaskPeriod::Daily;
    info.time        = 1500;
    EXPECT_GT(engine->add_task(info), 0u);
    engine->on_session_begin(20260401);
    engine->on_minute_tick(20260401, 1500);
    engine->update_params(2);
    engine->on_target_weights("sid", kOneWeight);
    ASSERT_EQ(engine->pending_buy_count(), 1u);
    engine->flush_pending(XtSelEngine::Phase::All);
    engine->on_session_end(20260401);

    EXPECT_EQ(cb, 1);  // pending 被 flush 派发
    EXPECT_EQ(engine->pending_buy_count(), 0u);
    EXPECT_EQ(engine->cross_thread_dropped(), 0u);
}

// 必败钉：bind 后 off-thread on_target_weights 必须被丢弃。旧实现无守卫 → 权重
// 直接 append 进 pending，下一个 flush 就把这笔来路不明的调仓发出去。
TEST(XtSelEngine_ThreadGuard, BoundOffThreadTargetWeightsDropped) {
    int cb = 0;
    RecordingStrategy strat;
    auto engine = make_guard_case_engine(strat, &cb);
    engine->bind_driver_thread(std::this_thread::get_id());  // 本线程 = driver

    std::thread other([&] { engine->on_target_weights("sid", kOneWeight); });
    other.join();

    EXPECT_EQ(engine->cross_thread_dropped(), 1u);
    EXPECT_EQ(engine->pending_sell_count(), 0u);
    EXPECT_EQ(engine->pending_buy_count(), 0u);

    // driver 线程（本线程）不受影响。
    engine->on_target_weights("sid", kOneWeight);
    EXPECT_EQ(engine->pending_buy_count(), 1u);
    EXPECT_EQ(engine->cross_thread_dropped(), 1u);
}

// 受守卫的其余六个入口同样 fail-closed（逐个各计一次）。
TEST(XtSelEngine_ThreadGuard, BoundOffThreadDropsAllGuardedEntries) {
    int cb = 0;
    RecordingStrategy strat;
    auto engine = make_guard_case_engine(strat, &cb);
    engine->on_target_weights("sid", kOneWeight);  // 先在 driver 侧留一条 pending
    ASSERT_EQ(engine->pending_buy_count(), 1u);
    engine->bind_driver_thread(std::this_thread::get_id());

    uint32_t task_id = 99;
    std::thread other([&] {
        engine->on_minute_tick(20260401, 1500);
        engine->flush_pending(XtSelEngine::Phase::All);
        engine->on_session_begin(20260401);
        engine->on_session_end(20260401);
        engine->update_params(3);
        SelTaskInfo info;
        info.strategy_id = "sid";
        info.period      = TaskPeriod::Daily;
        info.time        = 1500;
        task_id = engine->add_task(info);
        // 公开入口独立过闸（不只靠 on_minute_tick 外闸）：直调同样被拦。
        engine->on_schedule_fire(info, 20260401, 1500, 0);
    });
    other.join();

    EXPECT_EQ(engine->cross_thread_dropped(), 7u);
    EXPECT_EQ(strat.fire_count, 0) << "跨线程 on_schedule_fire 不得回调 strategy";
    EXPECT_EQ(task_id, 0u) << "add_task 跨线程返回未注册约定值";
    EXPECT_EQ(cb, 0) << "跨线程 flush_pending 不得派发";
    EXPECT_EQ(engine->pending_buy_count(), 1u) << "pending 不被跨线程 session/flush 动到";
}

// 必败钉：sid 无对应 ctx 时整次 target 丢弃。旧实现只在「找到 ctx 且 disabled」
// 时拒，未注册直接落 pending —— 装配期 on_init（早于 add_context）发的权重据此逃逸。
TEST(XtSelEngine, UnregisteredSidTargetWeightsDropped) {
    int cb = 0;
    RecordingStrategy strat;
    auto engine = make_guard_case_engine(strat, &cb);

    engine->on_target_weights("ghost_sid", kOneWeight);
    EXPECT_EQ(engine->unregistered_sid_weight_drops(), 1u);
    EXPECT_EQ(engine->pending_buy_count(), 0u);
    EXPECT_EQ(cb, 0);

    // 已注册 sid 不受影响（对照钉）。
    engine->on_target_weights("sid", kOneWeight);
    EXPECT_EQ(engine->unregistered_sid_weight_drops(), 1u);
    EXPECT_EQ(engine->pending_buy_count(), 1u);
}

// T2.STRAT-1 装配契约：registry 注册+freeze 后 add_context 内 ensure_sid_resolved
// 生效——resolved_sid_id() 返注册值而非 0（回报归因/init-failure kill latch 读点）。
TEST(XtSelEngine, AddContextResolvesSidWhenRegistryFrozen) {
    xt_identity::StrategySidRegistry reg;
    const uint32_t sid_id = reg.register_or_lookup("sel_sid_case");
    ASSERT_NE(sid_id, xt_identity::StrategySidRegistry::kNoSidId);
    reg.freeze();
    GlobalSidRegistryGuard guard(&reg);

    auto engine = make_engine();
    RecordingStrategy strat;
    auto ctx      = make_sel_context(strat, "sel_sid_case");
    auto* ctx_ptr = ctx.get();
    ASSERT_EQ(engine.add_context(std::move(ctx), &strat), 0u);

    EXPECT_EQ(ctx_ptr->resolved_sid_id(), sid_id);
    EXPECT_NE(ctx_ptr->resolved_sid_id(), 0u);
}

}  // namespace xt_engine_sel
