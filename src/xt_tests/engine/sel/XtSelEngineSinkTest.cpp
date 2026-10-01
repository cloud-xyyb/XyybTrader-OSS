// -----------------------------------------------------------------------------
// XtSelEngineSinkTest — per-fire pending 分组 + BT 观测 sink 契约
// -----------------------------------------------------------------------------
// 覆盖三条不变量：
//   ① 同一分钟多个 task 触发时，先触发那次 fire 的差额不得被后一次覆盖
//      （旧实现在 on_target_weights 开头 clear pending，先来的整组静默丢失）。
//   ② 每个 symbol 的规划期判定都必须可见，含 `continue` 早退路径（无价 / 无变动 /
//      min_lot 截零）；每次 on_target_weights 调用必须恰好一条 fire 记录，
//      含 equity<=0 的整体跳过路径。
//   ③ sink 未注入（live 形态）时，派发序列与注入时逐条一致。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "xt_core/XtFixedPoint.h"
#include "xt_engine/sel/SelTaskScheduler.h"
#include "xt_engine/sel/XtSelEngine.h"
#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/XtStrategyContext.h"

namespace xt_engine_sel {
namespace {

class NoHolidayProvider : public IHolidayProvider {
public:
    bool is_holiday(const std::string&, uint32_t, bool) const override { return false; }
    uint32_t next_trading_date(const std::string&, uint32_t cur) const override {
        return cur + 1;
    }
};

/// @brief 空策略：engine 只为已注册 sid 挂 target，harness 用它把测试 sid 注册进 ctx 表。
class NoopStrategy final : public XtStrategyBase {};

/// @brief 一次 handle_pos_change_fn 派发的可断言快照。
struct Dispatch {
    std::string code;
    int64_t     diff_pos_fp = 0;
    uint64_t    fire_seq = 0;   ///< 派发期 engine 自报的归属 fire
};

/// @brief 装配一个「真 diff 模式」engine：价格/持仓/权益 resolver 全注入。
///
/// 持仓表由测试直接维护（position_resolver 读之），模拟 runner 侧 account。
struct EngineHarness {
    std::unique_ptr<NoHolidayProvider>       holiday = std::make_unique<NoHolidayProvider>();
    std::unordered_map<std::string, int64_t> prices;      ///< code → close_fp（缺 = 无价）
    std::unordered_map<std::string, int64_t> positions;   ///< code → qty_fp
    int64_t                                  equity_fp = xt_fp::to_fp(1'000'000.0);
    int                                      min_lot = 1;
    /// true = 派发即落账（positions 立刻加上 diff），模拟 runner 的逐笔 fire+drain。
    bool                                     apply_fills = false;
    std::vector<Dispatch>                    dispatches;
    std::vector<SelFireRecord>               fires;
    std::vector<SelTargetDecision>           decisions;
    /// 非空 = 注入整数投影 hook（BT 形态）；测试据此模拟合规/违约的写回。
    XtSelEngine::Config::TargetProjectionFn  projection;
    std::unique_ptr<XtSelEngine>             engine;
    NoopStrategy                             strat_sid, strat_a, strat_b;  ///< 寿命罩住 engine

    void build(bool with_sinks) {
        XtSelEngine::Config cfg;
        cfg.scheduler = std::make_unique<SelTaskScheduler>(holiday.get());
        cfg.price_resolver_fn = [this](const std::string& code) -> int64_t {
            const auto it = prices.find(code);
            return it == prices.end() ? 0 : it->second;
        };
        cfg.position_resolver_fn = [this](const std::string& code) -> int64_t {
            const auto it = positions.find(code);
            return it == positions.end() ? 0 : it->second;
        };
        cfg.equity_resolver_fn = [this]() -> int64_t { return equity_fp; };
        cfg.min_lot_resolver_fn = [this](const std::string&) -> int { return min_lot; };
        cfg.handle_pos_change_fn =
            [this](const std::string&, const std::string& code, int64_t diff_pos_fp,
                   int64_t, XtTimeInForce, const std::string&) {
                dispatches.push_back(
                    Dispatch{code, diff_pos_fp, engine->current_fire_seq()});
                if (apply_fills) positions[code] += diff_pos_fp;
            };
        if (with_sinks) {
            cfg.fire_sink = [this](const SelFireRecord& r) { fires.push_back(r); };
            cfg.target_decision_sink =
                [this](const SelTargetDecision& d) { decisions.push_back(d); };
        }
        if (projection) cfg.target_projection_fn = projection;
        engine = std::make_unique<XtSelEngine>(std::move(cfg));
        register_ctx_(strat_sid, "sid");
        register_ctx_(strat_a, "sid_a");
        register_ctx_(strat_b, "sid_b");
    }

    /// @brief 注册 strategy_id 为 sid 的 ctx（未注册 sid 的 weights 会被 engine 丢弃）。
    void register_ctx_(XtStrategyBase& strategy, const std::string& sid) {
        XtStrategyContext::Config ctx_cfg;
        ctx_cfg.strategy      = &strategy;
        ctx_cfg.strategy_id   = sid;
        ctx_cfg.on_weights_fn = [](const std::string&,
                                   const std::unordered_map<std::string, double>&) {};
        ASSERT_NE(engine->add_context(std::make_unique<XtStrategyContext>(ctx_cfg), &strategy),
                  SIZE_MAX);
    }
};

/// @brief runner 主循环的 SELL→BUY 逐笔消费序（XtSelBacktestRunner run_sel_loop 同形）。
void drain_like_backtest(XtSelEngine& engine) {
    while (engine.pending_sell_count() > 0) {
        engine.fire_one_pending(XtSelEngine::Phase::SellOnly);
    }
    while (engine.pending_buy_count() > 0) {
        engine.fire_one_pending(XtSelEngine::Phase::BuyOnly);
    }
}

}  // namespace

// ── ① 同槽双 task 丢单钉 ───────────────────────────────────────────────────
//
// 两次 on_target_weights（同分钟两个 task）之后才 drain。两次各产一条腿，
// 都必须派发出去。旧实现在第二次调用开头清空 pending → A 的腿消失（本断言 FAIL）。
TEST(XtSelEngineSink, TwoFiresInSameSlotBothDispatched) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.prices["000001.SZ"] = xt_fp::to_fp(20.0);
    h.build(/*with_sinks=*/true);

    h.engine->on_target_weights("sid_a", {{"600000.SH", 0.5}});
    h.engine->on_target_weights("sid_b", {{"000001.SZ", 0.25}});

    EXPECT_EQ(h.engine->pending_buy_count(), 2u);  // 两组各一条 BUY，均未被吞
    drain_like_backtest(*h.engine);

    ASSERT_EQ(h.dispatches.size(), 2u);
    // 组间 FIFO：先触发的 task 先派发。
    EXPECT_EQ(h.dispatches[0].code, "600000.SH");
    EXPECT_EQ(h.dispatches[0].fire_seq, 1u);
    EXPECT_EQ(h.dispatches[1].code, "000001.SZ");
    EXPECT_EQ(h.dispatches[1].fire_seq, 2u);
    // 数量按各自 fire 的 weight×equity/price 计（50000 / 12500）。
    EXPECT_EQ(h.dispatches[0].diff_pos_fp, xt_fp::to_fp(50000.0));
    EXPECT_EQ(h.dispatches[1].diff_pos_fp, xt_fp::to_fp(12500.0));
}

// flush_pending 路径（live 分钟管线形态）同样不得丢组。
TEST(XtSelEngineSink, TwoFiresInSameSlotBothFlushed) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.prices["000001.SZ"] = xt_fp::to_fp(20.0);
    h.positions["000001.SZ"] = xt_fp::to_fp(30000.0);  // 让第二组产 SELL
    h.build(/*with_sinks=*/true);

    h.engine->on_target_weights("sid_a", {{"600000.SH", 0.5}});
    h.engine->on_target_weights("sid_b", {{"000001.SZ", 0.25}});

    h.engine->flush_pending(XtSelEngine::Phase::SellOnly);
    ASSERT_EQ(h.dispatches.size(), 1u);
    EXPECT_EQ(h.dispatches[0].code, "000001.SZ");
    EXPECT_LT(h.dispatches[0].diff_pos_fp, 0);
    h.engine->flush_pending(XtSelEngine::Phase::BuyOnly);
    ASSERT_EQ(h.dispatches.size(), 2u);
    EXPECT_EQ(h.dispatches[1].code, "600000.SH");
    EXPECT_EQ(h.engine->pending_sell_count(), 0u);
    EXPECT_EQ(h.engine->pending_buy_count(), 0u);
}

// ── ② 决策可见钉：三条早退路径 + 每次调用一条 fire 记录 ────────────────────
//
// 只在「产出非零 diff」时发 sink 的实现会漏掉全部三行（本断言 FAIL）。
TEST(XtSelEngineSink, EveryPlanningEarlyExitIsVisible) {
    EngineHarness h;
    h.min_lot = 100;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);      // ok（有价、有 diff）
    h.prices["600519.SH"] = xt_fp::to_fp(1'700.0);   // min_lot 截零
    h.prices["000001.SZ"] = xt_fp::to_fp(20.0);      // 与现仓一致
    // 000002.SZ 不给价 → no_price
    h.positions["000001.SZ"] = xt_fp::to_fp(5000.0);
    h.equity_fp = xt_fp::to_fp(1'000'000.0);
    h.build(/*with_sinks=*/true);

    h.engine->on_target_weights("sid", {
        {"600000.SH", 0.5},      // target 50000 vs 0 → ok
        {"600519.SH", 0.0001},   // raw 0.0588 → 截到 0，pre 0 → min_lot_zero
        {"000001.SZ", 0.1},      // target 5000 == pre 5000 → no_change
        {"000002.SZ", 0.2},      // 无价 → no_price
    });

    ASSERT_EQ(h.decisions.size(), 4u);
    std::unordered_map<std::string, SelPlanningStatus> by_code;
    for (const auto& d : h.decisions) by_code[d.code] = d.status;
    EXPECT_EQ(by_code["600000.SH"], SelPlanningStatus::kOk);
    EXPECT_EQ(by_code["600519.SH"], SelPlanningStatus::kMinLotZero);
    EXPECT_EQ(by_code["000001.SZ"], SelPlanningStatus::kNoChange);
    EXPECT_EQ(by_code["000002.SZ"], SelPlanningStatus::kNoPrice);

    // no_price 行的价格为 0（消费方据此判空，不是 sentinel 数量）。
    for (const auto& d : h.decisions) {
        if (d.code == "000002.SZ") {
            EXPECT_EQ(d.price_fp, 0);
            EXPECT_EQ(d.diff_qty_fp, 0);
        }
        if (d.code == "000001.SZ") {
            EXPECT_EQ(d.pre_qty_fp, xt_fp::to_fp(5000.0));
            EXPECT_EQ(d.diff_qty_fp, 0);
        }
    }
    // 只有 ok 那条进 pending。
    EXPECT_EQ(h.engine->pending_buy_count(), 1u);

    ASSERT_EQ(h.fires.size(), 1u);
    EXPECT_EQ(h.fires[0].fire_seq, 1u);
    EXPECT_EQ(h.fires[0].n_targets, 4u);
    EXPECT_FALSE(h.fires[0].equity_nonpositive);
    EXPECT_EQ(h.fires[0].equity_fp, xt_fp::to_fp(1'000'000.0));
}

// equity<=0 的整 fire 跳过路径也必须留一条 fire 记录（否则消费方分不清
// 「没触发」与「触发了但权益非正」）。
TEST(XtSelEngineSink, FireRecordEmittedOnEquityNonPositiveSkip) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.equity_fp = 0;
    h.build(/*with_sinks=*/true);

    h.engine->on_target_weights("sid", {{"600000.SH", 0.5}});

    ASSERT_EQ(h.fires.size(), 1u);
    EXPECT_TRUE(h.fires[0].equity_nonpositive);
    EXPECT_EQ(h.fires[0].n_targets, 1u);
    EXPECT_TRUE(h.decisions.empty());
    EXPECT_EQ(h.engine->pending_buy_count(), 0u);
}

// fire_seq 在 engine 内单调递增，是 E1↔E2 的关联键。
TEST(XtSelEngineSink, FireSeqIsMonotonicAcrossCalls) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.build(/*with_sinks=*/true);
    for (int i = 0; i < 3; ++i) {
        h.engine->on_target_weights("sid", {{"600000.SH", 0.1}});
        drain_like_backtest(*h.engine);
    }
    ASSERT_EQ(h.fires.size(), 3u);
    EXPECT_EQ(h.fires[0].fire_seq, 1u);
    EXPECT_EQ(h.fires[1].fire_seq, 2u);
    EXPECT_EQ(h.fires[2].fire_seq, 3u);
    // 派发期自报的 fire_seq 与产生它的那次 fire 对齐（harness 不模拟 fill →
    // 持仓恒 0，每次 fire 都产同一条 BUY 腿，正好把归属逐次对上）。
    ASSERT_EQ(h.dispatches.size(), 3u);
    EXPECT_EQ(h.dispatches[0].fire_seq, 1u);
    EXPECT_EQ(h.dispatches[1].fire_seq, 2u);
    EXPECT_EQ(h.dispatches[2].fire_seq, 3u);
}

// 非 fire 期 current_fire_seq() 归 0（不泄漏上一次派发的归属）。
TEST(XtSelEngineSink, CurrentFireSeqResetsOutsideDispatch) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.build(/*with_sinks=*/true);
    EXPECT_EQ(h.engine->current_fire_seq(), 0u);
    h.engine->on_target_weights("sid", {{"600000.SH", 0.5}});
    EXPECT_EQ(h.engine->current_fire_seq(), 0u);
    drain_like_backtest(*h.engine);
    EXPECT_EQ(h.engine->current_fire_seq(), 0u);
    ASSERT_EQ(h.dispatches.size(), 1u);
    EXPECT_EQ(h.dispatches[0].fire_seq, 1u);
}

// ── ③ 对照钉：sink 注入与否，派发序列逐条一致（live 零行为差异）──────────────
TEST(XtSelEngineSink, SinkInjectionDoesNotChangeDispatchSequence) {
    const std::unordered_map<std::string, double> w1 = {
        {"600000.SH", 0.4}, {"000001.SZ", 0.3}, {"600519.SH", 0.3}};
    const std::unordered_map<std::string, double> w2 = {
        {"600000.SH", 0.5}, {"000001.SZ", 0.2}, {"600519.SH", 0.3}};

    auto run = [&](bool with_sinks) {
        EngineHarness h;
        h.min_lot = 100;
        h.prices["600000.SH"] = xt_fp::to_fp(8.0);
        h.prices["000001.SZ"] = xt_fp::to_fp(12.0);
        h.prices["600519.SH"] = xt_fp::to_fp(1'700.0);
        h.build(with_sinks);
        h.engine->on_target_weights("sid", w1);
        drain_like_backtest(*h.engine);
        // 第二轮：持仓按第一轮 diff 更新（模拟 fill 落账）。
        for (const auto& d : h.dispatches) h.positions[d.code] += d.diff_pos_fp;
        h.engine->on_target_weights("sid", w2);
        drain_like_backtest(*h.engine);
        return h.dispatches;
    };

    const auto without_sinks = run(false);
    const auto with_sinks    = run(true);
    ASSERT_EQ(without_sinks.size(), with_sinks.size());
    ASSERT_FALSE(without_sinks.empty());  // 防两侧同空的假阳性
    for (size_t i = 0; i < with_sinks.size(); ++i) {
        EXPECT_EQ(without_sinks[i].code, with_sinks[i].code) << "i=" << i;
        EXPECT_EQ(without_sinks[i].diff_pos_fp, with_sinks[i].diff_pos_fp) << "i=" << i;
    }
}

// ── target 幂等替换钉（B0 fold P1）────────────────────────────────────────────
//
// 同槽同 sid 同 code 两次 target：pending 若冻结规划期差额，第二条会拿着「仓位仍是
// 0」的旧差额再买一遍 → 终仓翻倍。存绝对 target + dispatch 时重算才是 target
// weights 的语义。
//
// 本 harness 在派发回调里即时更新 positions（模拟 runner 的 drain+apply）。
TEST(XtSelEngineSink, SameSlotDuplicateTargetDoesNotDoubleUp) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.equity_fp = xt_fp::to_fp(1'000.0);   // target = 0.1×1000/10 = 10 股
    h.apply_fills = true;                  // 派发即落账（逐笔 fire + drain 形态）
    h.build(/*with_sinks=*/false);

    h.engine->on_target_weights("sid", {{"600000.SH", 0.1}});  // target 10
    h.engine->on_target_weights("sid", {{"600000.SH", 0.1}});  // 同 target → 就地覆盖
    EXPECT_EQ(h.engine->pending_buy_count(), 1u)
        << "同 (sid,code) 后发目标应就地覆盖旧 pending，不新建第二条";
    EXPECT_EQ(h.engine->same_slot_target_replacements(), 1u);
    drain_like_backtest(*h.engine);

    // 单条 pending 买 10 股。终仓必须是 10 而不是 20。
    ASSERT_EQ(h.dispatches.size(), 1u);
    EXPECT_EQ(h.dispatches[0].diff_pos_fp, xt_fp::to_fp(10.0));
    EXPECT_EQ(h.positions["600000.SH"], xt_fp::to_fp(10.0));
    EXPECT_EQ(h.engine->pending_dispatch_noops(), 0u)
        << "覆盖后不存在第二条去 noop";
}

// 同槽两个不同 target（100 后 50）：后发目标**就地覆盖**未 dispatch 的 pending，
// 终仓直接落 50（幂等替换）——不再先买 100 再卖 50（那会凭空造 150 换手）。
TEST(XtSelEngineSink, SameSlotLowerTargetWinsWithDirectionFlip) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.equity_fp = xt_fp::to_fp(1'000.0);
    h.apply_fills = true;
    h.build(/*with_sinks=*/false);

    h.engine->on_target_weights("sid", {{"600000.SH", 1.0}});  // target 100
    h.engine->on_target_weights("sid", {{"600000.SH", 0.5}});  // target 50 → 覆盖
    EXPECT_EQ(h.engine->same_slot_target_replacements(), 1u);
    drain_like_backtest(*h.engine);

    ASSERT_EQ(h.dispatches.size(), 1u)
        << "覆盖后只应有一笔成交（直接买到末 target）";
    EXPECT_EQ(h.dispatches[0].diff_pos_fp, xt_fp::to_fp(50.0));
    EXPECT_EQ(h.positions["600000.SH"], xt_fp::to_fp(50.0));
    EXPECT_EQ(h.engine->pending_dispatch_direction_flips(), 0u);
}

// 同槽后发「零差额目标」（kNoChange）必须同样覆盖旧 pending：只跳过 diff==0 会让
// 旧目标继续执行，后发「维持现状（仓位 0）」被静默吹掉，终仓停在旧目标的已成交
// 部分（窄审轮 2 P1 场景：100k→0，旧实现终仓永久停在当日额度 10k）。
TEST(XtSelEngineSink, SameSlotZeroTargetCancelsEarlierFire) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.equity_fp = xt_fp::to_fp(1'000.0);
    h.apply_fills = true;
    h.build(/*with_sinks=*/false);

    h.engine->on_target_weights("sid", {{"600000.SH", 1.0}});  // target 100
    h.engine->on_target_weights("sid", {{"600000.SH", 0.0}});  // target 0（kNoChange，初仓 0）
    EXPECT_EQ(h.engine->same_slot_target_replacements(), 1u)
        << "diff==0 的后发目标没有覆盖旧 pending（P1：旧目标的剩余部分会继续执行）";
    drain_like_backtest(*h.engine);

    EXPECT_TRUE(h.dispatches.empty())
        << "末 target=0 且初仓 0：不该有任何成交";
    EXPECT_EQ(h.positions["600000.SH"], 0);
    EXPECT_EQ(h.engine->pending_buy_count(), 0u)
        << "旧 pending 被摘除后不该有任何残留条目";
    // 摘空的旧组必须就地清掉：次日 session 边界不得把它误计成跨日残留。
    h.engine->on_session_begin(20260113);
    EXPECT_EQ(h.engine->stale_pending_groups_dropped(), 0u)
        << "幂等替换摘空的组活到了次日（观测假阳性）";
}

// SELL 桶重算后翻 BUY：必须推迟到 BUY phase 执行，不得在 SELL phase 插买单
// （先卖后买的现金序 = BUY 用的钱必须已由 SELL 释放）。
//
// 同 (sid,code) 已由就地覆盖收敛，翻转只剩**跨 sid** 同 code 可达（引擎刻意不跨
// sid 覆盖，两策略各自拥有自己的 target）：sid_a 先把仓卖到 20，sid_b 的 SELL
// 条目（target 50，规划期差额 −50）dispatch 重算成 +30 → 翻向。
TEST(XtSelEngineSink, SellBucketFlipToBuyDeferredToBuyPhase) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.equity_fp = xt_fp::to_fp(1'000.0);              // target = weight×1000/10
    h.positions["600000.SH"] = xt_fp::to_fp(100.0);
    h.apply_fills = true;                             // 派发即落账（逐笔 fire + drain 形态）
    h.build(/*with_sinks=*/false);

    h.engine->on_target_weights("sid_a", {{"600000.SH", 0.2}});  // target 20 → planned −80
    h.engine->on_target_weights("sid_b", {{"600000.SH", 0.5}});  // target 50 → planned −50（跨 sid 不覆盖）
    ASSERT_EQ(h.engine->pending_sell_count(), 2u);
    ASSERT_EQ(h.engine->pending_buy_count(), 0u);
    EXPECT_EQ(h.engine->same_slot_target_replacements(), 0u);

    while (h.engine->pending_sell_count() > 0) {
        h.engine->fire_one_pending(XtSelEngine::Phase::SellOnly);
    }
    ASSERT_EQ(h.dispatches.size(), 1u)
        << "SELL phase 内派发了翻向后的买单（先卖后买的现金序被破坏）";
    EXPECT_EQ(h.dispatches[0].diff_pos_fp, xt_fp::to_fp(-80.0));
    EXPECT_EQ(h.engine->pending_buy_count(), 1u) << "翻向条目没归位到 BUY 桶（整笔丢失）";

    while (h.engine->pending_buy_count() > 0) {
        h.engine->fire_one_pending(XtSelEngine::Phase::BuyOnly);
    }
    ASSERT_EQ(h.dispatches.size(), 2u);
    EXPECT_EQ(h.dispatches[1].diff_pos_fp, xt_fp::to_fp(30.0));
    EXPECT_EQ(h.positions["600000.SH"], xt_fp::to_fp(50.0)) << "终仓不是末 target";
    EXPECT_EQ(h.engine->pending_dispatch_direction_flips(), 1u);
}

// flush_pending 路径（live 分钟管线形态）同一语义：SellOnly 只出卖单，翻向条目落到
// 随后的 BuyOnly（同上，跨 sid 场景）。
TEST(XtSelEngineSink, SellBucketFlipToBuyDeferredAcrossFlushPhases) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.equity_fp = xt_fp::to_fp(1'000.0);
    h.positions["600000.SH"] = xt_fp::to_fp(100.0);
    h.apply_fills = true;
    h.build(/*with_sinks=*/false);

    h.engine->on_target_weights("sid_a", {{"600000.SH", 0.2}});
    h.engine->on_target_weights("sid_b", {{"600000.SH", 0.5}});

    h.engine->flush_pending(XtSelEngine::Phase::SellOnly);
    ASSERT_EQ(h.dispatches.size(), 1u) << "SellOnly flush 里出现买单";
    EXPECT_LT(h.dispatches[0].diff_pos_fp, 0);
    EXPECT_EQ(h.engine->pending_buy_count(), 1u);

    h.engine->flush_pending(XtSelEngine::Phase::BuyOnly);
    ASSERT_EQ(h.dispatches.size(), 2u);
    EXPECT_EQ(h.dispatches[1].diff_pos_fp, xt_fp::to_fp(30.0));
    EXPECT_EQ(h.positions["600000.SH"], xt_fp::to_fp(50.0));
    EXPECT_EQ(h.engine->pending_sell_count(), 0u);
    EXPECT_EQ(h.engine->pending_buy_count(), 0u);
}

// 跨 sid 同 code 挂 target：语义冲突，warn + 计数（不改行为）。
TEST(XtSelEngineSink, CrossSidSameCodePendingCounted) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.build(/*with_sinks=*/false);
    h.engine->on_target_weights("sid_a", {{"600000.SH", 0.1}});
    EXPECT_EQ(h.engine->cross_sid_same_code_pendings(), 0u);
    h.engine->on_target_weights("sid_b", {{"600000.SH", 0.2}});
    EXPECT_EQ(h.engine->cross_sid_same_code_pendings(), 1u);
}

// 跨日残留 pending 在 session 边界丢弃（昨日 target 不按今日价重新执行）。
TEST(XtSelEngineSink, StalePendingDroppedAtSessionBegin) {
    EngineHarness h;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.build(/*with_sinks=*/false);
    h.engine->on_target_weights("sid", {{"600000.SH", 0.1}});
    ASSERT_EQ(h.engine->pending_buy_count(), 1u);
    h.engine->on_session_begin(20260113);
    EXPECT_EQ(h.engine->pending_buy_count(), 0u);
    EXPECT_EQ(h.engine->stale_pending_groups_dropped(), 1u);
    EXPECT_TRUE(h.dispatches.empty());
}

// ── 对照钉：单 fire（live 形态）下 SELL 先于 BUY、组内按 code 字典序 ─────────
TEST(XtSelEngineSink, SingleFireKeepsSellFirstAndCodeOrder) {
    EngineHarness h;
    h.prices["000001.SZ"] = xt_fp::to_fp(20.0);
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);
    h.prices["600519.SH"] = xt_fp::to_fp(100.0);
    h.positions["600000.SH"] = xt_fp::to_fp(90000.0);  // → SELL
    h.positions["600519.SH"] = xt_fp::to_fp(9000.0);   // → SELL
    h.build(/*with_sinks=*/false);

    h.engine->on_target_weights(
        "sid", {{"000001.SZ", 0.3}, {"600000.SH", 0.1}, {"600519.SH", 0.1}});
    h.engine->flush_pending(XtSelEngine::Phase::All);

    ASSERT_EQ(h.dispatches.size(), 3u);
    EXPECT_EQ(h.dispatches[0].code, "600000.SH");  // SELL，字典序在前
    EXPECT_LT(h.dispatches[0].diff_pos_fp, 0);
    EXPECT_EQ(h.dispatches[1].code, "600519.SH");  // SELL
    EXPECT_LT(h.dispatches[1].diff_pos_fp, 0);
    EXPECT_EQ(h.dispatches[2].code, "000001.SZ");  // BUY 最后
    EXPECT_GT(h.dispatches[2].diff_pos_fp, 0);
}

// ── ④ 整数投影 hook 的契约闸 ───────────────────────────────────────────────
//
// 投影是注入方（回测 runner）写的市场语义；engine 只负责「批的组织 + 契约校验」。
// 契约破了必须 fail-closed 还原基线并计数 —— 投影没有能力破 long-only 与整手约束，
// 让违约值直接进撮合等于把两条硬约束交给一个可替换的外部函数。

// ④a 违约条目逐条还原、计数可见；同批的合规条目照常采纳。
TEST(XtSelEngineSink, ProjectionContractViolationRestoresBaselinePerEntry) {
    EngineHarness h;
    h.min_lot = 100;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);   // 基线 50000（0.5 × 1e6 / 10）
    h.prices["600519.SH"] = xt_fp::to_fp(20.0);   // 基线 12500（0.25 × 1e6 / 20）
    h.projection = [](const std::vector<SelTargetProjectionEntry>& plan,
                      std::vector<int64_t>& targets) {
        for (size_t i = 0; i < plan.size(); ++i) {
            if (plan[i].code == "600000.SH") {
                targets[i] += 37 * kPriceFpScale;      // 违约：差额不是 100 的整数倍
            } else {
                targets[i] += 100 * kPriceFpScale;     // 合规：整一手
            }
        }
    };
    h.build(/*with_sinks=*/true);

    h.engine->on_target_weights("sid", {{"600000.SH", 0.5}, {"600519.SH", 0.25}});
    drain_like_backtest(*h.engine);

    EXPECT_EQ(h.engine->projection_rejected_entries(), 1u);
    EXPECT_EQ(h.engine->projection_adjusted_targets(), 1u);
    ASSERT_EQ(h.dispatches.size(), 2u);
    // 违约腿回到基线，合规腿采纳投影值。
    EXPECT_EQ(h.dispatches[0].code, "600000.SH");
    EXPECT_EQ(h.dispatches[0].diff_pos_fp, xt_fp::to_fp(50000.0));
    EXPECT_EQ(h.dispatches[1].code, "600519.SH");
    EXPECT_EQ(h.dispatches[1].diff_pos_fp, xt_fp::to_fp(12600.0));
}

// ④b 负值写回同样违约（long-only）；抛异常与改批长度则整批还原。
TEST(XtSelEngineSink, ProjectionNegativeAndThrowingWritebacksAreRejected) {
    {   // 负值：单条违约
        EngineHarness h;
        h.min_lot = 100;
        h.prices["600000.SH"] = xt_fp::to_fp(10.0);
        h.projection = [](const std::vector<SelTargetProjectionEntry>&,
                          std::vector<int64_t>& targets) {
            targets[0] = -100 * kPriceFpScale;
        };
        h.build(/*with_sinks=*/true);
        h.engine->on_target_weights("sid", {{"600000.SH", 0.5}});
        drain_like_backtest(*h.engine);
        EXPECT_EQ(h.engine->projection_rejected_entries(), 1u);
        ASSERT_EQ(h.dispatches.size(), 1u);
        EXPECT_EQ(h.dispatches[0].diff_pos_fp, xt_fp::to_fp(50000.0));
    }
    {   // 抛异常：整批还原，计数 = 批长度
        EngineHarness h;
        h.min_lot = 100;
        h.prices["600000.SH"] = xt_fp::to_fp(10.0);
        h.prices["600519.SH"] = xt_fp::to_fp(20.0);
        h.projection = [](const std::vector<SelTargetProjectionEntry>&,
                          std::vector<int64_t>& targets) {
            targets[0] += 100 * kPriceFpScale;      // 改了一半才抛
            throw std::runtime_error("projection blew up");
        };
        h.build(/*with_sinks=*/true);
        h.engine->on_target_weights("sid", {{"600000.SH", 0.5}, {"600519.SH", 0.25}});
        drain_like_backtest(*h.engine);
        EXPECT_EQ(h.engine->projection_rejected_entries(), 2u);
        EXPECT_EQ(h.engine->projection_adjusted_targets(), 0u);
        ASSERT_EQ(h.dispatches.size(), 2u);
        EXPECT_EQ(h.dispatches[0].diff_pos_fp, xt_fp::to_fp(50000.0));
        EXPECT_EQ(h.dispatches[1].diff_pos_fp, xt_fp::to_fp(12500.0));
    }
    {   // 改批长度：整批还原
        EngineHarness h;
        h.min_lot = 100;
        h.prices["600000.SH"] = xt_fp::to_fp(10.0);
        h.projection = [](const std::vector<SelTargetProjectionEntry>&,
                          std::vector<int64_t>& targets) {
            targets.push_back(0);
        };
        h.build(/*with_sinks=*/true);
        h.engine->on_target_weights("sid", {{"600000.SH", 0.5}});
        drain_like_backtest(*h.engine);
        EXPECT_EQ(h.engine->projection_rejected_entries(), 1u);
        ASSERT_EQ(h.dispatches.size(), 1u);
        EXPECT_EQ(h.dispatches[0].diff_pos_fp, xt_fp::to_fp(50000.0));
    }
}

// ④c 决策行序 = code 字典序，早退条目不因「不进投影批」而前移。
//
// 落地序是产物消费方的契约（target_weights.csv / events_full.csv）。早退条目若在
// Pass 1 就地发射，行序会变成「先全部早退、后全部正常」（本断言在那种实现下 FAIL）。
TEST(XtSelEngineSink, DecisionEmitOrderIsCodeOrderRegardlessOfEarlyExits) {
    EngineHarness h;
    h.min_lot = 100;
    h.prices["600000.SH"] = xt_fp::to_fp(10.0);   // ok
    h.prices["600999.SH"] = xt_fp::to_fp(20.0);   // ok
    // 600519.SH 无价 → kNoPrice；600888.SH 负权重 → kNegativeWeight。两者字典序都
    // 夹在正常条目中间。
    h.build(/*with_sinks=*/true);

    h.engine->on_target_weights("sid", {{"600000.SH", 0.2},
                                        {"600519.SH", 0.2},
                                        {"600888.SH", -0.2},
                                        {"600999.SH", 0.2}});

    ASSERT_EQ(h.decisions.size(), 4u);
    EXPECT_EQ(h.decisions[0].code, "600000.SH");
    EXPECT_EQ(h.decisions[1].code, "600519.SH");
    EXPECT_EQ(h.decisions[1].status, SelPlanningStatus::kNoPrice);
    EXPECT_EQ(h.decisions[2].code, "600888.SH");
    EXPECT_EQ(h.decisions[2].status, SelPlanningStatus::kNegativeWeight);
    EXPECT_EQ(h.decisions[3].code, "600999.SH");
}

}  // namespace xt_engine_sel
