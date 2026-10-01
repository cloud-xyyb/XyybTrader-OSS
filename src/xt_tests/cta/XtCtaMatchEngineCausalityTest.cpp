// XtCtaMatchEngineCausalityTest.cpp
//
// View Causality 单元测试 — 验证 XtCtaMatchEngine 延迟队列契约。
//
// Step A 阶段以 GTEST_SKIP 占位,Step D 实现 fill_latency_ns / advance_clock 后转绿。

#include <gtest/gtest.h>

#include <cstdio>
#include <vector>

#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_engine/sim/XtCtaMatchEngine.h"

namespace {

TEST(XtCtaMatchEngine_Causality, FillLatencyDelaysVisibility) {
    XtCtaMatchEngine engine;
    engine.configure({/*fill_latency_ns=*/500'000'000, /*cancel_latency_ns=*/0});

    // 推进时钟到 09:30:00.000
    engine.advance_clock(20260406, 93000000);

    // 立即成交一笔(走 fill_immediate 绕过 pending)
    engine.fill_immediate("rb2510", true, 1'000'000, 5000'000'000,
                          "tag", XtOffset::Open, 20260406, 93000000);

    // 立即 drain → 应当为空(延迟 500ms 未到期)
    std::vector<XtTradeRecord> out;
    engine.drain_filled_trades(out);
    EXPECT_TRUE(out.empty()) << "fill_latency=500ms,立即 drain 不应看到";

    // 推进 250ms → 仍未到期
    engine.advance_clock(20260406, 93000250);
    engine.drain_filled_trades(out);
    EXPECT_TRUE(out.empty());

    // 推进到 500ms → 到期
    engine.advance_clock(20260406, 93000500);
    engine.drain_filled_trades(out);
    EXPECT_EQ(out.size(), 1u) << "500ms 后应当看到延迟释放的成交";
}

TEST(XtCtaMatchEngine_Causality, ZeroLatencyDefaultBackwardCompat) {
    XtCtaMatchEngine engine;
    // 不调 configure,默认 fill_latency_ns = 0

    engine.advance_clock(20260406, 93000000);
    engine.fill_immediate("rb2510", true, 1'000'000, 5000'000'000,
                          "tag", XtOffset::Open, 20260406, 93000000);

    std::vector<XtTradeRecord> out;
    engine.drain_filled_trades(out);
    EXPECT_EQ(out.size(), 1u);  // 延迟为 0,立即可见
}

TEST(XtCtaMatchEngine_Causality, AdvanceClockReleasesEarliestDueOnly) {
    XtCtaMatchEngine engine;
    engine.configure({/*fill_latency_ns=*/500'000'000, /*cancel_latency_ns=*/0});
    engine.advance_clock(20260406, 93000000);

    // 在 t=0 投递两笔(visible_at=500ms)
    engine.fill_immediate("rb2510", true, 1'000'000, 5000'000'000,
                          "t1", XtOffset::Open, 20260406, 93000000);
    engine.fill_immediate("rb2510", true, 1'000'000, 5001'000'000,
                          "t2", XtOffset::Open, 20260406, 93000000);

    // 推进 200ms 后再投递一笔(visible_at=200+500=700ms)
    engine.advance_clock(20260406, 93000200);
    engine.fill_immediate("rb2510", true, 1'000'000, 5002'000'000,
                          "t3", XtOffset::Open, 20260406, 93000200);

    // 推进到 500ms → 应该只看到前两笔
    engine.advance_clock(20260406, 93000500);
    std::vector<XtTradeRecord> out;
    engine.drain_filled_trades(out);
    EXPECT_EQ(out.size(), 2u);

    // 推进到 700ms → 看到第三笔
    // 注:drain_* 是 append 语义,需要 clear 才能独立检查本次释放数量
    out.clear();
    engine.advance_clock(20260406, 93000700);
    engine.drain_filled_trades(out);
    EXPECT_EQ(out.size(), 1u);
}


// T2.SIM-2：带 id 订单成交必须同步产出 Filled update（filled=qty, leaves=0），
// 驱动 account order-freeze 释放链；无 id 合成成交（fill_immediate）不发未知 id 终态。
TEST(XtCtaMatchEngine_Causality, FillEmitsFilledOrderUpdate) {
    XtCtaMatchEngine engine;
    engine.advance_clock(20260406, 93000000);

    XtCtaMatchEngine::PendingFill pf;
    std::snprintf(pf.code, sizeof(pf.code), "%s", "rb2510");
    pf.is_buy         = true;
    pf.qty_fp         = 2'000'000;
    pf.offset         = XtOffset::Open;
    pf.tif            = XtTimeInForce::GTC;
    pf.limit_price_fp = 5000'000'000;
    pf.order_id       = engine.alloc_order_id();
    engine.enqueue_pending(pf);

    ASSERT_EQ(engine.process_pending("rb2510", 4999'000'000, 20260406, 93000100), 1);

    std::vector<XtCtaMatchEngine::PendingOrderUpdate> ups;
    engine.drain_order_updates(ups);
    ASSERT_EQ(ups.size(), 1u);
    EXPECT_EQ(ups[0].order_id, pf.order_id);
    EXPECT_EQ(ups[0].status, XtOrderStatus::Filled);
    EXPECT_EQ(ups[0].filled_fp, pf.qty_fp);
    EXPECT_EQ(ups[0].leaves_fp, 0);

    // fill_immediate（无 id）→ trade 有、update 无
    engine.fill_immediate("rb2510", true, 1'000'000, 5000'000'000,
                          "tag", XtOffset::Open, 20260406, 93000200);
    ups.clear();
    engine.drain_order_updates(ups);
    EXPECT_TRUE(ups.empty());
}


// event 轴终态（P3，取代 T2.BT-4 绗带铉）：午夜前成交的延迟 fill 在自然日
// 递增的 event 轴上自然到期——不提前（旧绗带在回跳后首个 clock 提前释放）、
// 不卡死；同日回绕脏源（legacy 轴）降级为冻结，后续真推进正常释放。
TEST(XtCtaMatchEngine_Causality, NightCrossMidnightFillMaturesOnEventAxis) {
    XtCtaMatchEngine engine;
    engine.configure({/*fill_latency_ns=*/500'000'000, /*cancel_latency_ns=*/0});

    // 夜盘 23:59:59.900 成交；visible_at = 次自然日 00:00:00.400。
    engine.advance_clock(20260406, 235959900);
    engine.fill_immediate("ag2506", true, 1'000'000, 5000'000'000,
                          "tag", XtOffset::Open, 20260406, 235959900);

    std::vector<XtTradeRecord> out;
    engine.drain_filled_trades(out);
    ASSERT_TRUE(out.empty()) << "500ms 延迟未到期";

    // 脏源形态：同自然日 + 时间回绕 → 时钟冻结，不提前释放（旧绗带在这里
    // 重标为立即可见 = 错误地吞掉 400ms 剩余延迟）。
    engine.advance_clock(20260406, 100);
    engine.drain_filled_trades(out);
    EXPECT_TRUE(out.empty()) << "倒流冻结不得提前释放";

    // 真 event 轴：自然日递增 00:00:00.100 ——仍差 300ms，不可见。
    engine.advance_clock(20260407, 100);
    engine.drain_filled_trades(out);
    EXPECT_TRUE(out.empty()) << "跨午夜累计 200ms < 500ms，不得提前可见";

    // 00:00:00.500 ≥ visible_at 00:00:00.400 → 自然到期。
    engine.advance_clock(20260407, 500);
    engine.drain_filled_trades(out);
    EXPECT_EQ(out.size(), 1u) << "跨午夜延迟 fill 在 event 轴自然到期（不卡死不提前）";
}

}  // namespace

// N17：同期到期事件保序——同批到期的释放序必须等于入队序
// （同单 Partial→Filled 等序敏感对不得倒序；updates 队列同一 compaction 逻辑）。
// 旧 swap-and-pop 行为：4 笔同期释放序为 t0,t3,t2,t1。
TEST(XtCtaMatchEngine_Causality, SameDueBatchPreservesEnqueueOrder) {
    XtCtaMatchEngine engine;
    engine.configure({/*fill_latency_ns=*/500'000'000, /*cancel_latency_ns=*/0});
    engine.advance_clock(20260406, 93000000);

    for (int i = 0; i < 4; ++i) {
        engine.fill_immediate("rb2510", true, 1'000'000,
                              5000'000'000 + static_cast<int64_t>(i) * 1'000'000,
                              "seq", XtOffset::Open, 20260406, 93000000);
    }
    engine.advance_clock(20260406, 93000500);
    std::vector<XtTradeRecord> out;
    engine.drain_filled_trades(out);
    ASSERT_EQ(out.size(), 4u);
    for (int i = 0; i < 4; ++i) {
        EXPECT_DOUBLE_EQ(out[static_cast<size_t>(i)].exec_price, 5000.0 + i)
            << "同期释放序必须等于入队序（idx " << i << "）";
    }
}

// N12：fill 驱动的 Filled update 与 trade 同用 fill_latency——500ms 前
// drain_order_updates 不得先看到 Filled（旧行为走 cancel_latency=0 即时泄漏
// → 策略/账本在滞后窗口读到「已终态但仓位未变」分裂态）；cancel 类回报仍走
// cancel_latency（0 = 即时）。
TEST(XtCtaMatchEngine_Causality, FilledUpdateSharesFillLatency) {
    XtCtaMatchEngine engine;
    engine.configure({/*fill_latency_ns=*/500'000'000, /*cancel_latency_ns=*/0});
    engine.advance_clock(20260406, 93000000);

    XtCtaMatchEngine::PendingFill pf;
    std::snprintf(pf.code, sizeof(pf.code), "%s", "rb2510");
    pf.is_buy         = true;
    pf.qty_fp         = 1'000'000;
    pf.offset         = XtOffset::Open;
    pf.tif            = XtTimeInForce::GTC;
    pf.limit_price_fp = 0;
    pf.order_id       = engine.alloc_order_id();
    engine.enqueue_pending(pf);
    engine.process_pending_ignore_limit("rb2510", 5000'000'000, 20260406, 93000000);

    std::vector<XtCtaMatchEngine::PendingOrderUpdate> upds;
    engine.drain_order_updates(upds);
    EXPECT_TRUE(upds.empty())
        << "Filled update 必须与 trade 同 500ms 延迟（N12 回归钉）";

    engine.advance_clock(20260406, 93000500);
    engine.drain_order_updates(upds);
    ASSERT_EQ(upds.size(), 1u);
    EXPECT_EQ(upds[0].status, XtOrderStatus::Filled);

    // cancel 类回报仍走 cancel_latency=0：立即可见。
    XtCtaMatchEngine::PendingFill pf2 = pf;
    pf2.order_id = engine.alloc_order_id();
    engine.enqueue_pending(pf2);
    ASSERT_TRUE(engine.cancel(pf2.order_id));
    upds.clear();
    engine.drain_order_updates(upds);
    ASSERT_EQ(upds.size(), 1u);
    EXPECT_EQ(upds[0].status, XtOrderStatus::Canceled);
}
