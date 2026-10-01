// =============================================================================
// XtCtaMatchEngineIsolationTest — 撮合核心零业务依赖契约固化
// =============================================================================
//
// 目标（Goal）：验证 XtCtaMatchEngine 在完全不持有任何业务对象
//   （account / listener / gateway）的情况下完成 submit → match → drain 全流程，
//   防止未来回归引入业务语义泄漏（business-logic leakage）。
//
// 覆盖主题（Coverage）：零依赖生命周期 / drain 消费型语义 / session-end 结清契约
//   （含缺价 Canceled、延迟队列 force-drain 三形态）。
//
// 隔离性约定（Isolation）：
//   - 不 include XtBacktestOrderGateway / IBacktestReportListener / xt_account::AccountStateStore
//   - 不依赖任何 mock 框架
//   - 不创建任何 gateway / listener / account 对象

#include <gtest/gtest.h>

#include <cstring>
#include <set>
#include <vector>

#include "xt_engine/sim/XtCtaMatchEngine.h"

// XtCtaMatchEngine.h 传递引入：
//   xt_core/XtTradeRecord.h  → XtTradeRecord
//   xt_core/OrderTypes.h     → XtOrderStatus / order_exec_from_fp / kOrderExecFpScale
//   xt_core/XtOffset.h       → XtOffset
//   xt_core/XtFixedPoint.h   → xt_fp 工具

namespace {

// 测试用常量（固定合约码和 fixed-point 参数）
constexpr int64_t kFp  = kOrderExecFpScale;   // 1e6，fixed-point 单位
constexpr int64_t kQty = 2 * kFp;             // 2 手
constexpr uint32_t kDate   = 20260406;
constexpr uint32_t kTimeMs = 93000000;

// ─────────────────────────────────────────────────────────────────────────────

/// @brief Test 1: 零外部依赖完成完整生命周期（submit → match → drain）
///
/// 证明 XtCtaMatchEngine 可在零外部依赖下构造并完成全流程。
/// 不传入任何 account / listener / gateway 对象。
TEST(XtCtaMatchEngine_Isolation, ZeroDependencyLifecycle) {
    // 零参数构造——接口契约的核心断言
    XtCtaMatchEngine engine;

    // 构造一笔限价买单（limit_price_fp = 5000，确保价格低于 limit 时可成交）
    const int64_t limit_price_fp = 5000 * kFp;
    const int64_t cross_price_fp = 4999 * kFp;  // 买单：成交价 <= limit_price 才穿越

    XtCtaMatchEngine::PendingFill pf{};
    std::strncpy(pf.code, "IF2406", sizeof(pf.code) - 1);
    pf.is_buy         = true;
    pf.qty_fp         = kQty;
    pf.offset         = XtOffset::Open;
    pf.tif            = XtTimeInForce::GTC;
    pf.limit_price_fp = limit_price_fp;
    pf.order_id       = engine.alloc_order_id();

    engine.enqueue_pending(pf);

    // pending_count 应为 1（挂单已入队）
    ASSERT_EQ(engine.pending_count(), 1u);

    engine.update_price("IF2406", cross_price_fp);
    const int filled = engine.process_pending("IF2406", cross_price_fp, kDate, kTimeMs);

    // 应有 1 笔成交
    EXPECT_EQ(filled, 1);

    // 挂单应已被消耗
    EXPECT_EQ(engine.pending_count(), 0u);

    // drain_filled_trades：验证成交记录字段正确性
    std::vector<XtTradeRecord> trades;
    engine.drain_filled_trades(trades);

    ASSERT_EQ(trades.size(), 1u) << "应恰好一笔成交";
    EXPECT_EQ(trades[0].code, "IF2406");
    EXPECT_TRUE(trades[0].is_buy);
    EXPECT_EQ(trades[0].qty_fp, kQty);
    // exec_price 是 double，用 order_exec_from_fp 还原后比较
    EXPECT_DOUBLE_EQ(trades[0].exec_price, order_exec_from_fp(cross_price_fp));
    EXPECT_EQ(trades[0].order_id, pf.order_id);

    // drain_order_updates：T2.SIM-2 后成交路径同步产 Filled update（带 id 单），
    // 驱动 account order-freeze 释放链。
    std::vector<XtCtaMatchEngine::PendingOrderUpdate> updates;
    engine.drain_order_updates(updates);
    ASSERT_EQ(updates.size(), 1u) << "成交路径应产生 Filled update";
    EXPECT_EQ(updates[0].order_id, pf.order_id);
    EXPECT_EQ(updates[0].status, XtOrderStatus::Filled);
    EXPECT_EQ(updates[0].filled_fp, kQty);
    EXPECT_EQ(updates[0].leaves_fp, 0);
}

// ─────────────────────────────────────────────────────────────────────────────

/// @brief Test 2: drain 消费型语义 + 累积行为
///
/// 验证：
///   (a) 第一次 drain 取出全部累积 trades，第二次 drain 返回空（消费语义）
///   (b) drain 后新产生的成交独立累积（不与已消费数据混合）
///   (c) order update 的对称 drain 语义（cancel 路径）
TEST(XtCtaMatchEngine_Isolation, DrainSemanticsCorrectness) {
    XtCtaMatchEngine engine;

    // ── (a)(b) fill_immediate 路径：绕过 pending 队列直接产生成交 ──

    // 投递第一笔 fill_immediate
    const int64_t px1 = 3000 * kFp;
    ASSERT_TRUE(engine.fill_immediate("RB2406", true, kQty, px1, "t1",
                                      XtOffset::Open, kDate, kTimeMs));
    // 投递第二笔 fill_immediate
    const int64_t px2 = 3010 * kFp;
    ASSERT_TRUE(engine.fill_immediate("RB2406", false, kQty, px2, "t2",
                                      XtOffset::Close, kDate, kTimeMs));

    // 第一次 drain：应取出 2 笔成交
    std::vector<XtTradeRecord> out1;
    engine.drain_filled_trades(out1);
    EXPECT_EQ(out1.size(), 2u) << "两笔 fill_immediate 应累积到 buffer";

    // 第二次 drain：buffer 已清空，应为空
    std::vector<XtTradeRecord> out2;
    engine.drain_filled_trades(out2);
    EXPECT_TRUE(out2.empty()) << "drain 后 buffer 应清空（消费语义）";

    // 再投一笔，drain 应只取出这一笔（独立累积）
    ASSERT_TRUE(engine.fill_immediate("RB2406", true, kQty, px1, "t3",
                                      XtOffset::Open, kDate, kTimeMs));
    std::vector<XtTradeRecord> out3;
    engine.drain_filled_trades(out3);
    EXPECT_EQ(out3.size(), 1u) << "drain 后新成交应独立累积";

    // ── (c) order update 的对称 drain 语义（cancel 路径）──

    // 投入两笔限价挂单，价格故意不穿越
    const int64_t no_cross = 9999 * kFp;  // 买单 limit 极高，卖入时不会成交
    XtCtaMatchEngine::PendingFill pf1{};
    std::strncpy(pf1.code, "AU2406", sizeof(pf1.code) - 1);
    pf1.is_buy         = false;  // 卖单
    pf1.qty_fp         = kQty;
    pf1.offset         = XtOffset::Open;
    pf1.tif            = XtTimeInForce::GTC;
    pf1.limit_price_fp = no_cross;
    pf1.order_id       = engine.alloc_order_id();

    XtCtaMatchEngine::PendingFill pf2 = pf1;
    pf2.order_id = engine.alloc_order_id();

    engine.enqueue_pending(pf1);
    engine.enqueue_pending(pf2);
    ASSERT_EQ(engine.pending_count(), 2u);

    // 逐一 cancel：各产生一条 Canceled update
    EXPECT_TRUE(engine.cancel(pf1.order_id));
    EXPECT_TRUE(engine.cancel(pf2.order_id));

    // 第一次 drain order updates：应取出 2 条 Canceled
    std::vector<XtCtaMatchEngine::PendingOrderUpdate> upd1;
    engine.drain_order_updates(upd1);
    EXPECT_EQ(upd1.size(), 2u) << "两次 cancel 应产生 2 条 update";

    // 第二次 drain：消费语义，应为空
    std::vector<XtCtaMatchEngine::PendingOrderUpdate> upd2;
    engine.drain_order_updates(upd2);
    EXPECT_TRUE(upd2.empty()) << "drain 后 order update buffer 应清空";

    // 再 cancel 一个不存在的 id：不产生 update（cancel 返回 false）
    EXPECT_FALSE(engine.cancel(99999u));
    std::vector<XtCtaMatchEngine::PendingOrderUpdate> upd3;
    engine.drain_order_updates(upd3);
    EXPECT_TRUE(upd3.empty()) << "cancel 不存在的订单不应产生 update";
}

// ─────────────────────────────────────────────────────────────────────────────

/// @brief Test 3: 日结撤单（flush_session_end）产生 Canceled update 事件
///
/// 验证：
///   - flush_session_end 对限价单产生逐笔 Canceled update
///   - update 数量等于挂单数
///   - 每条 update 的 status 为 Canceled
///   - order_id 与投递时一一对应
///   - flush 后 pending_count 归零
TEST(XtCtaMatchEngine_Isolation, SessionEndFlushProducesCanceledUpdates) {
    XtCtaMatchEngine engine;

    constexpr int kOrderCount = 3;
    // 限价卖单：limit_price 设极低（1），确保不会被任何正常价格穿越成交
    const int64_t no_fill_limit = 1 * kFp;

    std::set<uint64_t> submitted_ids;
    for (int i = 0; i < kOrderCount; ++i) {
        XtCtaMatchEngine::PendingFill pf{};
        std::strncpy(pf.code, "SC2406", sizeof(pf.code) - 1);
        pf.is_buy         = false;
        pf.qty_fp         = kQty;
        pf.offset         = XtOffset::Open;
        pf.tif            = XtTimeInForce::GTC;
        pf.limit_price_fp = no_fill_limit;
        pf.order_id       = engine.alloc_order_id();
        submitted_ids.insert(pf.order_id);
        engine.enqueue_pending(pf);
    }

    // 确认全部挂单入队
    ASSERT_EQ(engine.pending_count(), static_cast<size_t>(kOrderCount));

    // 日结处理
    engine.flush_session_end(kDate, kTimeMs);

    // flush 后挂单清零
    EXPECT_EQ(engine.pending_count(), 0u) << "日结后挂单应全部清空";

    // drain order updates：应产生 kOrderCount 条 Canceled
    std::vector<XtCtaMatchEngine::PendingOrderUpdate> updates;
    engine.drain_order_updates(updates);

    ASSERT_EQ(updates.size(), static_cast<size_t>(kOrderCount))
        << "限价单数量 == Canceled update 数量";

    // 逐条验证 status == Canceled，收集 order_id 做集合比对
    std::set<uint64_t> canceled_ids;
    for (const auto& u : updates) {
        EXPECT_EQ(u.status, XtOrderStatus::Canceled)
            << "日结限价单应产生 Canceled update";
        canceled_ids.insert(u.order_id);
    }

    // order_id 集合应与投递时严格一致（允许任意顺序）
    EXPECT_EQ(canceled_ids, submitted_ids)
        << "canceled order_id 集合应与 submitted_id 集合一致";

    // 二次 drain 应为空（消费语义）
    std::vector<XtCtaMatchEngine::PendingOrderUpdate> updates2;
    engine.drain_order_updates(updates2);
    EXPECT_TRUE(updates2.empty());
}

/// 日结 market 单缺价契约（防静默吞单）：
///   - market 单（limit_price_fp==0）且 session 内无该合约 last price →
///     flush_session_end 必须产 Canceled 终态（filled=0 / leaves=全量），
///     不得静默丢弃（否则上层在途视图悬挂 + accept 侧冻结泄漏）。
///   - 有 last price 的 market 单仍按既有路径成交，不受影响。
/// 注：gateway 路径不可达（风控对无价 market 单拒单，见 CtaGatewayContract），
/// 此分支是引擎级防御兜底，在此钉住。
TEST(XtCtaMatchEngine_Isolation, SessionEndMarketOrderNoLastPriceEmitsCanceled) {
    XtCtaMatchEngine engine;

    // market 单：limit_price_fp = 0；"NOPX" 从未喂过 last price。
    XtCtaMatchEngine::PendingFill pf{};
    std::strncpy(pf.code, "NOPX", sizeof(pf.code) - 1);
    pf.is_buy         = true;
    pf.qty_fp         = kQty;
    pf.offset         = XtOffset::Open;
    pf.tif            = XtTimeInForce::GTC;
    pf.limit_price_fp = 0;
    pf.order_id       = engine.alloc_order_id();
    engine.enqueue_pending(pf);
    ASSERT_EQ(engine.pending_count(), 1u);

    const int fills = engine.flush_session_end(kDate, kTimeMs);
    EXPECT_EQ(fills, 0) << "无 last price 不得成交";
    EXPECT_EQ(engine.pending_count(), 0u);

    std::vector<XtCtaMatchEngine::PendingOrderUpdate> updates;
    engine.drain_order_updates(updates);
    ASSERT_EQ(updates.size(), 1u) << "缺价 market 单必须产 Canceled 终态，不得静默丢弃";
    EXPECT_EQ(updates[0].order_id, pf.order_id);
    EXPECT_EQ(updates[0].status, XtOrderStatus::Canceled);
    EXPECT_EQ(updates[0].filled_fp, 0);
    EXPECT_EQ(updates[0].leaves_fp, kQty);
}

// ───────────────────────────────────────────────────────────
// session-end 延迟事件结清契约（code_axis B2 / T2.ENG-6）：flush 必须 force-drain
// 双延迟队列——末日事件不丢、中间日事件不泄漏进下一 session。
// 用 tick-mode 默认量级 fill_latency=500ms（XtCtaRunnerConfig 默认）作红队形态。

/// @brief B2-(a): 末日 session-end 市价 fill 在 fill_latency>0 下不得丢失。
TEST(XtCtaMatchEngine_Isolation, SessionEndMarketFillNotLostUnderFillLatency) {
    XtCtaMatchEngine engine;
    XtCtaMatchEngine::Config cfg;
    cfg.fill_latency_ns = 500'000'000;  // tick-mode 默认 500ms
    engine.configure(cfg);
    engine.advance_clock(kDate, 145959000);

    XtCtaMatchEngine::PendingFill pf{};
    std::strncpy(pf.code, "AG2606", sizeof(pf.code) - 1);
    pf.is_buy         = true;
    pf.qty_fp         = kQty;
    pf.offset         = XtOffset::Open;
    pf.tif            = XtTimeInForce::GTC;
    pf.limit_price_fp = 0;  // 市价单：flush 时按 last price 成交
    pf.order_id       = engine.alloc_order_id();
    engine.enqueue_pending(pf);
    engine.update_price("AG2606", 5000 * kFp);

    const int fills = engine.flush_session_end(kDate, 150000000);
    EXPECT_EQ(fills, 1);

    // 旧码：fill 进 delay_trades_（visible_at = 15:00:00.500 > clock）且无后续 clock
    // → 永久丢失。新码：flush 末尾 force-drain → 立即可 drain。
    std::vector<XtTradeRecord> trades;
    engine.drain_filled_trades(trades);
    ASSERT_EQ(trades.size(), 1u) << "session-end 市价 fill 被延迟队列吞掉（末日丢失形态）";
    EXPECT_EQ(trades[0].order_id, pf.order_id);
    EXPECT_DOUBLE_EQ(trades[0].exec_price, order_exec_from_fp(5000 * kFp));
}

/// @brief B2-(b): 中间日已在途延迟 fill 不得泄漏进下一 session——派发必须发生在
/// flush 内而非下 session 首个 clock（旧码在后者也能派发 = PASS 陷阱，故断言点
/// 在 flush 后立即 drain）。
TEST(XtCtaMatchEngine_Isolation, SessionEndDrainsInFlightDelayedFillWithinSession) {
    XtCtaMatchEngine engine;
    XtCtaMatchEngine::Config cfg;
    cfg.fill_latency_ns = 500'000'000;
    engine.configure(cfg);
    engine.advance_clock(kDate, 145959900);

    // 收盘前 100ms 的穿越限价成交：visible_at = 15:00:00.400 落在 session 外。
    XtCtaMatchEngine::PendingFill pf{};
    std::strncpy(pf.code, "AG2606", sizeof(pf.code) - 1);
    pf.is_buy         = true;
    pf.qty_fp         = kQty;
    pf.offset         = XtOffset::Open;
    pf.tif            = XtTimeInForce::GTC;
    pf.limit_price_fp = 5000 * kFp;
    pf.order_id       = engine.alloc_order_id();
    engine.enqueue_pending(pf);
    engine.update_price("AG2606", 4999 * kFp);
    ASSERT_EQ(engine.process_pending("AG2606", 4999 * kFp, kDate, 145959900), 1);

    // 延迟窗口内：尚不可见（坑位前提验证，证明 fill 确在延迟队列里）。
    std::vector<XtTradeRecord> before;
    engine.drain_filled_trades(before);
    ASSERT_TRUE(before.empty()) << "前提失效：fill 未进延迟队列，测试形态无效";

    // flush 内必须把在途延迟 fill 结清落账（而非留给下 session 首 clock）。
    engine.flush_session_end(kDate, 150000000);
    std::vector<XtTradeRecord> trades;
    engine.drain_filled_trades(trades);
    ASSERT_EQ(trades.size(), 1u)
        << "在途延迟 fill 未在 flush 内结清（中间日会泄漏进 day-N+1，时间戳仍 day-N）";
    EXPECT_EQ(trades[0].order_id, pf.order_id);
}

/// @brief B2-(c): cancel_latency>0 下 session-end Canceled 终态同批落账（与 fill 对称）。
TEST(XtCtaMatchEngine_Isolation, SessionEndCanceledUpdateNotDelayedPastFlush) {
    XtCtaMatchEngine engine;
    XtCtaMatchEngine::Config cfg;
    cfg.cancel_latency_ns = 500'000'000;
    engine.configure(cfg);
    engine.advance_clock(kDate, 145959000);

    XtCtaMatchEngine::PendingFill pf{};
    std::strncpy(pf.code, "AG2606", sizeof(pf.code) - 1);
    pf.is_buy         = true;
    pf.qty_fp         = kQty;
    pf.offset         = XtOffset::Open;
    pf.tif            = XtTimeInForce::GTC;
    pf.limit_price_fp = 4000 * kFp;  // 不穿越：flush 时走 Canceled 分支
    pf.order_id       = engine.alloc_order_id();
    engine.enqueue_pending(pf);

    engine.flush_session_end(kDate, 150000000);
    std::vector<XtCtaMatchEngine::PendingOrderUpdate> updates;
    engine.drain_order_updates(updates);
    ASSERT_EQ(updates.size(), 1u)
        << "session-end Canceled 被 cancel_latency 延迟队列吞掉（在途视图跨 session 悬挂）";
    EXPECT_EQ(updates[0].order_id, pf.order_id);
    EXPECT_EQ(updates[0].status, XtOrderStatus::Canceled);
}

}  // namespace
