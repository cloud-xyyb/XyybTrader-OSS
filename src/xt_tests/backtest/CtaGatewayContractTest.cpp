// CtaGatewayContractTest.cpp
//
// XtCtaBacktestOrderGateway reject 路径契约测试（对称 HftGatewayContractTest）：
//   1. 风控拒单不在 order_strategy_ sidecar 残留 entry（泄漏回归）；
//   2. reject 路径 Rejected update 的 local_cb_us 与 drain 路径同编码
//      （date×1e9 + HHMMSSmmm×1000）；
//   3. CTA「风控前 fan-out on_order_submit」语义锚（被拒订单留 submit 痕迹）。

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "api/ExecFlags.h"
#include "api/IHftEngineObserver.h"
#include "xt_backtest/cta/XtCtaBacktestInfra.h"
#include "xt_core/XtFixedPoint.h"
#include "xt_account/ingest/AccountEventCodec.h"
#include "xt_identity/StrategySidRegistry.h"

namespace {

constexpr uint32_t kDate   = 20260611;
constexpr uint32_t kTimeMs = 93000000;  // 09:30:00.000 (HHMMSSmmm)

struct CapturingObserver final : xt_runtime::IHftEngineObserver {
    std::vector<XtOrderExecUpdate> updates;
    std::vector<uint64_t>          submits;

    void on_order_update(const XtOrderExecUpdate& u,
                         XtLatencyEligibility = XtLatencyEligibility::kEligible) override { updates.push_back(u); }
    void on_order_submit(int64_t /*ts*/, uint64_t id, const char* /*code*/,
                         bool /*is_buy*/, int64_t /*price_fp*/, int64_t /*qty_fp*/,
                         std::string_view /*user_tag*/, bool /*is_close*/,
                         bool /*is_today*/) override {
        submits.push_back(id);
    }
};

// 最小 gateway 环境：max_order_qty=1 手 → qty=2 必拒（MaxOrderQty）。
struct CtaGwEnv {
    BacktestInfra     infra;
    CapturingObserver obs;

    CtaGwEnv() {
        XtCtaBacktestOrderGateway::Config cfg;
        cfg.account.initial_cash = 1'000'000.0;
        cfg.account.futures_mode = true;
        cfg.risk.max_order_qty   = 1.0;
        auto& info = cfg.commodities.map["TEST"];
        info.vol_scale   = 1.0;
        info.price_tick  = 0.01;
        info.margin_rate = 0.1;
        // "NOPX"：catalog 内有品种信息但测试从不喂市场价（market 单缺价场景）。
        auto& nopx = cfg.commodities.map["NOPX"];
        nopx.vol_scale   = 1.0;
        nopx.price_tick  = 0.01;
        nopx.margin_rate = 0.1;
        infra = build_backtest_infra(cfg);
        infra.gw->bind(infra.account.get(), infra.risk_gateway.get(),
                       infra.risk_guard_ctx.get(), infra.cost_applier.get(),
                       infra.order_book.get(), infra.commodities.get());
        infra.gw->add_observer(&obs);
        // 喂一次市场价，设定 last_date_/last_time_ms_（reject 时间戳来源）。
        infra.gw->update_market_price("TEST", order_exec_to_fp(100.0), kDate, kTimeMs);
    }

    xt_runtime::SubmitResult submit(double qty, double px) {
        return infra.gw->submit("TEST", /*is_buy=*/true, order_exec_to_fp(px),
                                order_exec_to_fp(qty), XtTimeInForce::GTC,
                                "cta_gw_test", XtOffset::Open, exec_none(), "alpha");
    }
};

// 拒单不残留 sidecar；通过的单 insert → terminal erase 全生命周期归零。
TEST(CtaGatewayContract, RiskRejectErasesSidecar) {
    CtaGwEnv env;

    const auto rejected = env.submit(/*qty=*/2.0, /*px=*/100.0);
    EXPECT_EQ(rejected.order_id, 0u);
    EXPECT_EQ(env.infra.gw->order_strategy_size(), 0u)
        << "risk-rejected order must not leave order_strategy_ entry";

    // 远离市价的 GTC 限价单：通过风控、保持 pending → sidecar 持有 entry。
    const auto accepted = env.submit(/*qty=*/1.0, /*px=*/50.0);
    ASSERT_NE(accepted.order_id, 0u);
    EXPECT_EQ(env.infra.gw->order_strategy_size(), 1u);

    // cancel → Canceled 终态 → drain 路径 erase。
    ASSERT_TRUE(env.infra.gw->cancel(accepted.order_id));
    EXPECT_EQ(env.infra.gw->order_strategy_size(), 0u);
}

// reject 路径与 drain 路径的 local_cb_us 必须同编码（date×1e9 + HHMMSSmmm×1000）。
TEST(CtaGatewayContract, RejectUpdateLocalCbUsMatchesDrainEncoding) {
    CtaGwEnv env;

    // 被拒订单：Rejected update 同步 fan-out 给 observer。
    (void)env.submit(/*qty=*/2.0, /*px=*/100.0);
    ASSERT_EQ(env.obs.updates.size(), 1u);
    const auto& rej = env.obs.updates[0];
    EXPECT_EQ(rej.status, XtOrderStatus::Rejected);
    const uint64_t expect_rej_us =
        static_cast<uint64_t>(kDate) * 1'000'000'000ULL +
        static_cast<uint64_t>(kTimeMs) * 1000ULL;
    EXPECT_EQ(rej.local_cb_us, expect_rej_us)
        << "reject path must use drain-path encoding (date*1e9 + HHMMSSmmm*1000)";

    // 正常 drain 路径对照：日结 Canceled update 带真实时间戳
    // （fill 路径只产 trade 不发 update；手动 cancel 路径 date/time 填 0，均不能作锚）。
    const auto accepted = env.submit(/*qty=*/1.0, /*px=*/50.0);
    ASSERT_NE(accepted.order_id, 0u);
    constexpr uint32_t kTimeMs2 = 150000000;  // 15:00:00.000 日结
    env.infra.gw->flush_session_end(kDate, kTimeMs2);
    std::vector<XtOrderExecUpdate> drained;
    env.infra.gw->drain_order_updates(drained);
    bool saw_canceled = false;
    for (const auto& u : drained) {
        if (u.id == accepted.order_id && u.status == XtOrderStatus::Canceled) {
            saw_canceled = true;
            EXPECT_EQ(u.local_cb_us,
                      static_cast<uint64_t>(kDate) * 1'000'000'000ULL +
                      static_cast<uint64_t>(kTimeMs2) * 1000ULL);
        }
    }
    EXPECT_TRUE(saw_canceled);
}

// CTA 语义锚：on_order_submit 在风控前 fan-out（被拒订单留 submit 痕迹 + 终态）。
TEST(CtaGatewayContract, RejectedOrderEmitsSubmitThenTerminal) {
    CtaGwEnv env;

    const auto rejected = env.submit(/*qty=*/2.0, /*px=*/100.0);
    EXPECT_EQ(rejected.order_id, 0u);

    ASSERT_EQ(env.obs.submits.size(), 1u);
    ASSERT_EQ(env.obs.updates.size(), 1u);
    EXPECT_EQ(env.obs.updates[0].status, XtOrderStatus::Rejected);
    EXPECT_EQ(env.obs.updates[0].id, env.obs.submits[0])
        << "rejected order's terminal update must reference the submitted oid";
}

// CTA BT submit 必须带 sid 注册 book：portfolio_router 下 ctx 的 book 级
// sid 过滤（get_open_orders）才看得到本策略挂单（对称 HFT BT intern 路径）。
struct GlobalSidRegistryScope {
    explicit GlobalSidRegistryScope(xt_identity::StrategySidRegistry* r) {
        xt_identity::StrategySidRegistry::set_global(r);
    }
    ~GlobalSidRegistryScope() { xt_identity::StrategySidRegistry::set_global(nullptr); }
};

TEST(CtaGatewayContract, SubmitRegistersSidInBookForOpenOrderFilter) {
    xt_identity::StrategySidRegistry reg;
    const uint32_t alpha_id = reg.register_or_lookup("alpha");
    const uint32_t beta_id  = reg.register_or_lookup("beta");
    reg.freeze();
    GlobalSidRegistryScope scope(&reg);

    CtaGwEnv env;
    // GTC buy @50 < market 100 → resting，不成交。
    const auto r = env.submit(/*qty=*/1.0, /*px=*/50.0);
    ASSERT_NE(r.order_id, 0u);

    XtOrderExecUpdate out[4];
    EXPECT_EQ(env.infra.order_book->collect_open_orders(out, 4, nullptr, alpha_id), 1u)
        << "submit 必须以本策略 sid 注册 slot，sid 过滤下可见自己的挂单";
    EXPECT_EQ(env.infra.order_book->collect_open_orders(out, 4, nullptr, beta_id), 0u)
        << "他策略 sid 过滤不得看到 alpha 的挂单";
    EXPECT_EQ(env.infra.order_book->collect_open_orders(out, 4, nullptr, 0), 1u);
}

// market 单（price_fp=0）风控估价契约：gateway 用最近 update_market_price 替代
// 评估价；合约从未喂价时无价可替 → 风控输入守卫拒（InvalidPriceOrQty），不入
// pending、不留 sidecar。由此 sim 日结的 market-无价 Canceled 分支在 gateway 路径
// 不可达（纯防御兜底，引擎级契约见 XtCtaMatchEngineIsolationTest）。
TEST(CtaGatewayContract, MarketOrderWithoutAnyMarketPriceIsRejected) {
    CtaGwEnv env;

    const auto mkt = env.infra.gw->submit("NOPX", /*is_buy=*/true, /*price_fp=*/0,
                                          order_exec_to_fp(1.0), XtTimeInForce::GTC,
                                          "mkt_nolast", XtOffset::Open, exec_none(),
                                          "alpha");
    EXPECT_EQ(mkt.order_id, 0u) << "无价可估的 market 单必须被风控拒，不得静默入 pending";
    EXPECT_EQ(env.infra.gw->order_strategy_size(), 0u);
    ASSERT_FALSE(env.obs.updates.empty());
    EXPECT_EQ(env.obs.updates.back().status, XtOrderStatus::Rejected);
}

// Auto 提交入口解析钉：Auto 在 observer fan-out/风控前解析为 Open，
// 回报 payload 永不见 Auto（对称 HFT BT submit 入口）。
TEST(CtaGatewayContract, AutoOffsetResolvedToOpenBeforeFanout) {
    CtaGwEnv env;

    // GTC buy @50 < market 100 → resting；Auto 若未解析会以 close 语义进风控/撮合。
    const auto r = env.infra.gw->submit("TEST", /*is_buy=*/true,
                                        order_exec_to_fp(50.0),
                                        order_exec_to_fp(1.0), XtTimeInForce::GTC,
                                        "auto_offset", XtOffset::Auto, exec_none(),
                                        "alpha");
    ASSERT_NE(r.order_id, 0u);

    // 日结 Canceled 回报的 close_offset 必须是解析后的 Open，非 Auto。
    env.infra.gw->flush_session_end(kDate, /*time_ms=*/150000000);
    std::vector<XtOrderExecUpdate> drained;
    env.infra.gw->drain_order_updates(drained);
    bool saw = false;
    for (const auto& u : drained) {
        if (u.id == r.order_id) {
            saw = true;
            EXPECT_EQ(u.close_offset, XtOffset::Open)
                << "Auto 必须在提交入口解析，回报 payload 不得泄漏 sentinel";
        }
    }
    EXPECT_TRUE(saw);
}


// T2.SIM-2：futures 提交即冻 guard-margin（gate 翻开后）；sim 成交发 Filled
// update，经 account_event_build::order_event（引擎 drain 同款转换）释放 → 归零。
TEST(CtaGatewayContract, FuturesMarginFrozenOnSubmitReleasedOnFill) {
    CtaGwEnv env;
    ASSERT_EQ(env.infra.account->snapshot_account().frozen_cash, 0);

    auto res = env.submit(/*qty=*/1.0, /*px=*/100.0);
    ASSERT_NE(res.order_id, 0u);
    const int64_t frozen1 = env.infra.account->snapshot_account().frozen_cash;
    EXPECT_GT(frozen1, 0) << "futures submit must freeze guard-margin (order_accepted)";

    // 喂价触发撮合（GTC 买 @100，市场价 99 ≤ 限价 → 成交）
    env.infra.gw->update_market_price("TEST", order_exec_to_fp(99.0), kDate, kTimeMs + 1000);
    std::vector<XtOrderExecUpdate> ups;
    env.infra.gw->drain_order_updates(ups);
    ASSERT_FALSE(ups.empty());
    const auto* filled = &ups.back();
    ASSERT_EQ(filled->status, XtOrderStatus::Filled);

    // 引擎 drain 同款释放链（mirror XtCtaEngine::on_order_update 的 account apply）
    env.infra.account->apply(account_event_build::order_event(*filled));
    EXPECT_EQ(env.infra.account->snapshot_account().frozen_cash, 0)
        << "Filled update must release the full order freeze";
}

// N11（bt_audit_batch345）：生产装配（build_backtest_infra）必须把 order_book 接进
// guard——同一 tick 内连续 close 单要看到前序在途平仓量，第二笔 over-close 被拒。
TEST(CtaGatewayContract, SameTickSecondCloseSeesOutstandingCloseQty) {
    CtaGwEnv env;
    // 持仓：多头 1 手（recovery 灌入）
    std::vector<ITradeGateway::PositionSnapshot> snap(1);
    snap[0].code     = "TEST";
    snap[0].long_qty = 1.0;
    snap[0].long_avg = 100.0;
    env.infra.account->on_position_snapshot(snap, /*is_recovery=*/true,
                                            *env.infra.commodities);

    // 第一笔 close：GTC sell @150 > market 100 → resting，在途占满可平额度。
    const auto first = env.infra.gw->submit("TEST", /*is_buy=*/false,
                                            order_exec_to_fp(150.0),
                                            order_exec_to_fp(1.0), XtTimeInForce::GTC,
                                            "close1", XtOffset::Close, exec_none(),
                                            "alpha");
    ASSERT_NE(first.order_id, 0u);

    // 第二笔同 tick close：guard 经 order_book 扣减在途平仓量 → 拒。
    const auto second = env.infra.gw->submit("TEST", /*is_buy=*/false,
                                             order_exec_to_fp(150.0),
                                             order_exec_to_fp(1.0), XtTimeInForce::GTC,
                                             "close2", XtOffset::Close, exec_none(),
                                             "alpha");
    EXPECT_EQ(second.order_id, 0u)
        << "第二笔 close 必须看到在途平仓量被拒（guard 未接 order_book 时此钉失效）";
    ASSERT_FALSE(env.obs.updates.empty());
    EXPECT_EQ(env.obs.updates.back().status, XtOrderStatus::Rejected);
}



// T2.OFFSET-2：CloseFirst 品种 Open 意图路由——对侧可平 → 改写为 close 切片
// + open 余量；对侧在途 Open → defer 拒 OppositeOpenPending；默认 OpenAllowed
// 单切片直通（golden 结构性不动的语义锚）。
struct CtaGwCloseFirstEnv {
    BacktestInfra     infra;
    CapturingObserver obs;

    CtaGwCloseFirstEnv() {
        XtCtaBacktestOrderGateway::Config cfg;
        cfg.account.initial_cash = 1'000'000.0;
        cfg.account.futures_mode = true;
        cfg.risk.max_order_qty   = 100.0;
        auto& info = cfg.commodities.map["CF"];
        info.vol_scale     = 1.0;
        info.price_tick    = 0.01;
        info.margin_rate   = 0.1;
        info.offset_policy = XtOffsetPolicy::CloseFirst;
        infra = build_backtest_infra(cfg);
        infra.gw->bind(infra.account.get(), infra.risk_gateway.get(),
                       infra.risk_guard_ctx.get(), infra.cost_applier.get(),
                       infra.order_book.get(), infra.commodities.get());
        infra.gw->add_observer(&obs);
        infra.gw->update_market_price("CF", order_exec_to_fp(100.0), kDate, kTimeMs);
    }

    xt_runtime::SubmitResult submit_open(bool is_buy, double qty, double px) {
        return infra.gw->submit("CF", is_buy, order_exec_to_fp(px),
                                order_exec_to_fp(qty), XtTimeInForce::GTC,
                                "cf_test", XtOffset::Open, exec_none(), "alpha");
    }
};

TEST(CtaGatewayContract, CloseFirstRewritesOpenIntoCloseSlicesPlusRemainder) {
    CtaGwCloseFirstEnv env;
    // 对侧持仓：空头 2 手（昨仓）→ buy Open 3 手应改写为 close 2 + open 1
    std::vector<ITradeGateway::PositionSnapshot> snap(1);
    snap[0].code      = "CF";
    snap[0].short_qty = 2.0;
    snap[0].short_avg = 100.0;
    env.infra.account->on_position_snapshot(snap, /*is_recovery=*/true,
                                            *env.infra.commodities);

    const auto res = env.submit_open(/*is_buy=*/true, /*qty=*/3.0, /*px=*/100.0);
    EXPECT_NE(res.order_id, 0u);
    // rewrite → 多切片：observer submit fan-out ≥2（close 腿 + open 余量）
    EXPECT_GE(env.obs.submits.size(), 2u)
        << "CloseFirst rewrite must emit one submit per slice";
    EXPECT_EQ(env.infra.gw->pending_count(), env.obs.submits.size());
}

TEST(CtaGatewayContract, CloseFirstDefersOnOppositeOpenPending) {
    CtaGwCloseFirstEnv env;
    // 先挂一笔对侧（sell）Open 在途（GTC 高限价不成交）
    const auto sell = env.submit_open(/*is_buy=*/false, /*qty=*/1.0, /*px=*/200.0);
    ASSERT_NE(sell.order_id, 0u);

    // buy Open 撞对侧在途 Open → defer 拒 OppositeOpenPending
    const auto res = env.submit_open(/*is_buy=*/true, /*qty=*/1.0, /*px=*/100.0);
    EXPECT_EQ(res.order_id, 0u);
    EXPECT_EQ(res.reject_reason, xt_runtime::XtRejectCode::OppositeOpenPending);
}

TEST(CtaGatewayContract, OpenAllowedDefaultSingleSlicePassthrough) {
    CtaGwEnv env;  // TEST 品种未配 offset_policy → 默认 OpenAllowed
    const auto res = env.submit(/*qty=*/1.0, /*px=*/100.0);
    ASSERT_NE(res.order_id, 0u);
    EXPECT_EQ(env.obs.submits.size(), 1u) << "OpenAllowed must stay single-slice";
}

}  // namespace
