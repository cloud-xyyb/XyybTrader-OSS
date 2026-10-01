#include "XtBacktestOrderGateway.h"

#include <cstring>

#include "xt_core/XtFixedPoint.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_account/api/AccountSnapshot.h"
#include "api/IRejectSink.h"
#include "xt_backtest/common/XtBtTimeEncode.h"  // bt_risk_now_ms
#include "xt_core/order/XtOffsetRouter.h"
#include "xt_identity/StrategySidRegistry.h"              // sid_id intern via registry

// ── bind ─────────────────────────────────────────────────────────────────────

void XtCtaBacktestOrderGateway::bind(
    xt_account::AccountStateStore*                        account,
    XtRiskGateway*                             risk_gw,
    XtBacktestRiskGuardContext*                risk_guard_ctx,
    xt_fill_cost::FillCostApplier*             cost,
    xt_runtime::order::XtOrderRuntimeBook*     order_book,
    const XtCommodityCatalog*                  catalog) {
    account_ref_    = account;
    risk_gw_        = risk_gw;
    risk_guard_ctx_ = risk_guard_ctx;
    cost_           = cost;
    order_book_     = order_book;
    catalog_        = catalog;
    // observers_ / reject_sink_ 由 runner 分别绑定；match_engine_ 无外部依赖。
}

// ── IExecutionGateway ────────────────────────────────────────────────────────

xt_runtime::SubmitResult XtCtaBacktestOrderGateway::submit(
    const char* code, bool is_buy, int64_t price_fp,
    int64_t qty_fp, XtTimeInForce tif,
    std::string_view user_tag, XtOffset offset,
    ExecFlags /*exec_flags*/,
    std::string_view strategy_id) {
    // ExecFlags 仅影响实盘/低延迟路径，CTA 回测撮合显式忽略。
    // XtOffset::Auto 提交入口解析（BT 提交全为策略源）：Auto → Open 意图，
    // 与 HFT BT/live gateway 同点位 —— Auto 永不进 observer/风控/撮合/回报 payload。
    if (offset == XtOffset::Auto) {
        offset = xt_offset_router::resolve_submit_offset(
                     offset, xt_core::XtOrderSource::Strategy)
                     .value_or(XtOffset::Open);
    }

    // T2.OFFSET-2：close_first 品种的 Open 意图路由（对齐 HFT BT
    // build_final_slices_ 的 route 段；CTA 无 max_order_qty 切片预算，
    // 切片上限 = close 今昔 2 + open 余量 1）。catalog 未 bind 或 policy
    // 非 CloseFirst 时不进路由 → 单切片直通。
    struct Slice_ { XtOffset offset; int64_t qty_fp; };
    Slice_ slices[3] = {{offset, qty_fp}, {}, {}};
    int slice_count = 1;
    if (offset == XtOffset::Open && account_ref_ && catalog_ && order_book_ &&
        catalog_->effective_offset_policy(code) == XtOffsetPolicy::CloseFirst) {
        xt_offset_router::OffsetRouteInput in;
        in.code       = code;
        in.policy     = XtOffsetPolicy::CloseFirst;
        in.req_qty_fp = qty_fp;
        const auto cv = xt_account::closeable_view(
            account_ref_->snapshot_position(code ? code : ""), is_buy);
        in.opposite_closeable_today_fp     = cv.today_fp;
        in.opposite_closeable_yesterday_fp = cv.yesterday_fp;
        order_book_->accumulate_close_outstanding_fp(
            code, is_buy, in.opposite_close_outstanding_today_fp,
            in.opposite_close_outstanding_yesterday_fp);
        // pending 对侧 Open 占用可平窗口，close-first 路由需按在途量 defer。
        in.opposite_open_outstanding_fp =
            order_book_->open_no_trade_qty_fp(code, !is_buy);

        const auto d = xt_offset_router::route_open_intent(in);
        using Action = xt_offset_router::OffsetRouteDecision::Action;
        if (d.action == Action::kDefer) {
            // 对侧在途 Open 未决：本地拒 + 拒因返回策略（对齐 HFT BT）。
            return xt_runtime::SubmitResult{
                0, xt_runtime::XtRejectCode::OppositeOpenPending};
        }
        if (d.action == Action::kRewrite) {
            slice_count = 0;
            for (int i = 0; i < d.close_slice_count; ++i) {
                slices[slice_count++] = {d.close_slices[i].offset,
                                         d.close_slices[i].qty_fp};
            }
            if (d.open_remainder_fp > 0) {
                slices[slice_count++] = {XtOffset::Open, d.open_remainder_fp};
            }
        }
        // kPassOpen 保持单切片 {Open, qty}。
    }

    // 逐切片提交：默认路径 slice_count==1，循环体≡历史单单流程（bit-identical）。
    // 某切片被风控拒不回滚已接受切片（部分接受是真实语义）；全拒时返拒。
    uint64_t first_oid = 0;
    for (int si = 0; si < slice_count; ++si) {
        const XtOffset s_offset = slices[si].offset;
        const int64_t  s_qty_fp = slices[si].qty_fp;
        const uint64_t oid = match_engine_.alloc_order_id();

        // 提交意图先于风控 fan-out，报告层可看到被拒订单的初始请求。
        if (!observers_.empty()) {
            const bool is_close = (s_offset != XtOffset::Open);
            const bool is_today = (s_offset == XtOffset::CloseToday);
            const int64_t ts = current_ts();
            for (auto* obs : observers_) {
                obs->on_order_submit(ts, oid, code, is_buy,
                                     price_fp, s_qty_fp, user_tag,
                                     is_close, is_today);
            }
        }

        // 风控检查（submit 路径，使用最近一次 update_market_price 时间戳）
        const XtRiskResult risk_result =
            evaluate_risk_(code, is_buy, price_fp, s_qty_fp, tif, s_offset, oid);
        if (!risk_result.passed) {
            continue;
        }

        // sidecar 记 order_id → strategy_id；apply_fills_ 读出补齐 trade.strategy_id。
        // 风控通过后才写入：被拒订单无 fill 也无终态 drain，提前写入会永久残留。
        // 空 strategy_id 保持为空；严格归属由调用方与 trade-capture 契约校验。
        if (oid != 0) {
            order_strategy_[oid] = std::string(strategy_id);
        }

        XtCtaMatchEngine::PendingFill pf{};
        std::strncpy(pf.code, code, sizeof(pf.code) - 1);
        pf.is_buy         = is_buy;
        pf.qty_fp         = s_qty_fp;
        pf.offset         = s_offset;
        pf.tif            = tif;
        pf.order_id       = oid;
        std::strncpy(pf.tag, std::string(user_tag).c_str(), sizeof(pf.tag) - 1);
        // IOC/市价单用 0 表示无限价；GTC 限价单保留 price_fp。
        pf.limit_price_fp = (tif == XtTimeInForce::GTC && price_fp > 0) ? price_fp : 0;

        match_engine_.enqueue_pending(pf);

        // 冻结保证金（order_accepted 事件）：stock-mode capital reservation +
        // 期货 guard-margin 冻结（T2.SIM-2 后激活：CTA sim 成交现发 per-order
        // Filled update，order-freeze 经 AccountState::on_order_update 增量/终态
        // 释放，不再累积泄漏；对齐 HFT BT XtHftBacktestOrderGateway accept 即冻）。
        // stock-mode（SEL）冻结由 SelFrozenReleaseObserver 释放。
        if (account_ref_ && risk_result.required_margin_fp > 0) {
            account_ref_->apply(XtAccountEvent::order_accepted(
                oid, risk_result.required_margin_fp, risk_result.per_unit_margin_fp));
        }

        // order_book 只登记已通过风控的在途量。
        // sid 在 submit 期 lookup（只读查表）并直接写 slot（HFT BT 走 drain 期 upd.sid_id
        // 回填，机制不同、终态等价）：portfolio_router 下 slot.sid_id 与策略
        // resolved sid 同口径，get_open_orders 的 book 级过滤才不会全滤光。
        // 终态 tombstone 消费收在注册入口内部（返回 kSkippedTerminal）；CTA BT 的冻结
        // 由上方 order_accepted 独立发出，不按注册结果分派，故忽略返回值。
        if (order_book_) {
            uint32_t sid_id = xt_identity::StrategySidRegistry::kNoSidId;
            if (!strategy_id.empty()) {
                if (auto* reg = xt_identity::StrategySidRegistry::global()) {
                    sid_id = reg->lookup(std::string(strategy_id));
                }
            }
            const bool is_close = (s_offset != XtOffset::Open);
            (void)order_book_->on_order_submitted(
                oid, code, is_buy, s_qty_fp, price_fp, s_offset,
                (is_close && account_ref_)
                    ? xt_account::closeable_view(
                          account_ref_->snapshot_position(code ? code : ""), is_buy)
                    : xt_account::CloseableView{},
                StrategySid::from_raw(sid_id));
        }
        if (first_oid == 0) first_oid = oid;
    }
    if (first_oid == 0) {
        return {0, xt_runtime::XtRejectCode::Unknown};
    }
    return {first_oid, xt_runtime::XtRejectCode::NoError};
}

bool XtCtaBacktestOrderGateway::cancel(uint64_t order_id) {
    const bool ok = match_engine_.cancel(order_id);
    drain_and_dispatch_order_updates_();
    return ok;
}

// ── 回测专用接口 ─────────────────────────────────────────────────────────────

int XtCtaBacktestOrderGateway::update_market_price(
    const char* code, int64_t price_fp,
    uint32_t date, uint32_t time_ms) {
    last_date_    = date;
    last_time_ms_ = time_ms;
    if (price_fp > 0) last_market_price_fp_[code] = price_fp;
    match_engine_.update_price(code, price_fp);
    const int fills = match_engine_.process_pending(code, price_fp, date, time_ms);
    apply_fills_();
    drain_and_dispatch_order_updates_();  // 保持所有推进入口的 drain 顺序一致。
    return fills;
}

int XtCtaBacktestOrderGateway::fill_pending_at_bar_open(
    const char* code, int64_t open_fp,
    uint32_t date, uint32_t time_ms) {
    last_date_    = date;
    last_time_ms_ = time_ms;
    if (open_fp > 0) last_market_price_fp_[code] = open_fp;
    match_engine_.update_price(code, open_fp);
    const int fills = match_engine_.process_pending_ignore_limit(
        code, open_fp, date, time_ms);
    apply_fills_();
    drain_and_dispatch_order_updates_();
    return fills;
}

bool XtCtaBacktestOrderGateway::fill_immediate(
    const char* code, bool is_buy, int64_t qty_fp,
    int64_t price_fp, const char* tag, XtOffset offset,
    uint32_t date, uint32_t time_ms,
    bool skip_risk) {
    if (!skip_risk) {
        const XtRiskResult risk_result =
            evaluate_risk_(code, is_buy, price_fp, qty_fp,
                           XtTimeInForce::GTC, offset, 0);
        if (!risk_result.passed) return false;
    }
    const bool filled = match_engine_.fill_immediate(
        code, is_buy, qty_fp, price_fp, tag, offset, date, time_ms);
    apply_fills_();
    drain_and_dispatch_order_updates_();  // 保持所有推进入口的 drain 顺序一致。
    return filled;
}

int XtCtaBacktestOrderGateway::flush_session(uint32_t date, uint32_t time_ms,
                                             bool allow_market_fill) {
    return flush_session_end(date, time_ms, allow_market_fill);
}

void XtCtaBacktestOrderGateway::advance_clock(uint32_t date, uint32_t time_ms) {
    last_date_    = date;
    last_time_ms_ = time_ms;
    match_engine_.advance_clock(date, time_ms);
    apply_fills_();                         // 释放本次推进产生的 visible trades
    drain_and_dispatch_order_updates_();    // 释放本次推进产生的 visible updates
}

int XtCtaBacktestOrderGateway::flush_session_end(uint32_t date, uint32_t time_ms,
                                                 bool allow_market_fill) {
    const int fills = match_engine_.flush_session_end(date, time_ms, allow_market_fill);
    apply_fills_();
    drain_and_dispatch_order_updates_();  // flush_session_end 可能产生限价单 Canceled update
    return fills;
}

// ── 配置接口 ─────────────────────────────────────────────────────────────────

void XtCtaBacktestOrderGateway::configure_match_engine(
    const XtCtaMatchEngine::Config& cfg) {
    match_engine_.configure(cfg);
}

// ── private: 风控检查 ────────────────────────────────────────────────────────

XtRiskResult XtCtaBacktestOrderGateway::evaluate_risk_(
    const char* code, bool is_buy, int64_t price_fp,
    int64_t qty_fp, XtTimeInForce tif, XtOffset offset,
    uint64_t order_id) {
    XtRiskResult ok;
    ok.passed = true;
    if (!risk_gw_ || !risk_guard_ctx_) return ok;

    // 市价单（price_fp == 0）：用最近一次 update_market_price 的价格作为风控估价。
    // XtRiskEngine 基础不变量要求 price_fp > 0，市价单提交时不含成交价，
    // 用最新市价替代保持语义等价（风控评估名义价值 price × qty）。
    int64_t eval_price_fp = price_fp;
    if (eval_price_fp <= 0 && code) {
        auto it = last_market_price_fp_.find(code);
        if (it != last_market_price_fp_.end()) eval_price_fp = it->second;
    }

    xt_core::XtOrder req;
    req.set_code(code);
    req.is_buy       = is_buy;
    req.price_fp     = eval_price_fp;
    req.qty_fp       = qty_fp;
    req.tif          = tif;
    req.close_offset = offset;

    XtRiskEvalContext eval_ctx;
    eval_ctx.now_ms = bt_risk_now_ms(last_date_, last_time_ms_);
    // 集合竞价 phase 只影响 A 股 venue；其他 venue 始终 Continuous。
    {
        const uint32_t hhmm = xt_core::extract_hhmm_from_packed(last_time_ms_);
        const auto phase = xt_core::classify_phase(venue_kind_, hhmm);
        eval_ctx.is_in_call_auction = xt_core::is_call_auction(phase);
    }

    const auto result = risk_gw_->evaluate(req, *risk_guard_ctx_, eval_ctx);
    if (result.passed) return result;

    ++risk_reject_count_;
    // reject 只通过 IRejectSink 落地，gateway 不依赖具体 reporter 类型。
    if (reject_sink_) {
        reject_sink_->on_reject(req, result);
    }
    // 风险点：拒单必须发 terminal update；否则策略侧和 order_book 可能
    // 长期认为订单仍有效，导致在途量悬挂。
    if (order_id != 0 && !observers_.empty()) {
        XtOrderExecUpdate upd;
        upd.id           = order_id;
        upd.is_buy        = is_buy;
        upd.close_offset = offset;
        upd.status       = XtOrderStatus::Rejected;
        upd.price_fp     = price_fp;
        upd.qty_fp       = qty_fp;
        upd.filled_fp    = 0;
        upd.leaves_fp    = qty_fp;
        upd.last_fill_fp = 0;
        // 与 drain 路径（drain_and_dispatch_order_updates_）同编码：
        // date×1e9 + HHMMSSmmm×1000，保证 events 流同列同源可比。
        upd.local_cb_us  = static_cast<uint64_t>(last_date_) * 1'000'000'000ULL
                         + static_cast<uint64_t>(last_time_ms_) * 1000ULL;
        // 拒单必须同步 fan-out 终态 update。
        for (auto* obs : observers_) {
            obs->on_order_update(upd);
        }
    }
    return result;
}

// ── private: order update drain ──────────────────────────────────────────────
//
// 此处只从 match_engine 抽取并入 updates_buf_；engine 通过 drain_order_updates()
// 完成 order_book / translator / exec sink 等 domain 处理。

void XtCtaBacktestOrderGateway::drain_and_dispatch_order_updates_() {
    match_engine_.drain_order_updates(order_update_scratch_);
    for (const auto& u : order_update_scratch_) {
        XtOrderExecUpdate upd;
        upd.id           = u.order_id;
        upd.is_buy        = u.is_buy;
        upd.close_offset = u.offset;
        upd.status       = u.status;
        upd.price_fp     = u.price_fp;
        upd.qty_fp       = u.qty_fp;
        upd.filled_fp    = u.filled_fp;
        upd.leaves_fp    = u.leaves_fp;
        upd.last_fill_fp = u.last_fill_fp;
        upd.local_cb_us  = static_cast<uint64_t>(u.date)    * 1'000'000'000ULL
                         + static_cast<uint64_t>(u.time_ms) * 1000ULL;
        updates_buf_.push_back(upd);
        // 终态 (Filled / Canceled / Rejected) 清理 sidecar，防止 map 无限增长。
        if (order_status_is_final(upd.status)) {
            order_strategy_.erase(upd.id);
        }
    }
    order_update_scratch_.clear();
}

// ── private: fill offset 推断 + 完整回报链 ──────────────────────────────

// CTA BT 的 Open 意图不走 XtOffsetRouter（close-first 路由仅 HFT BT/live 有）：
// 此处按 account 净仓读视图推断 is_open/is_close，是 CTA sim 净额对开模型的配套口径；
// 跨引擎对照 offset 路由行为时需知此分叉。
void XtCtaBacktestOrderGateway::finalize_fill_offsets_(
    std::vector<XtTradeRecord>& trades) {
    for (auto& tr : trades) {
        const int64_t pos_fp = account_ref_
            ? account_ref_->snapshot_position(tr.code).qty
            : 0;
        const bool is_open =
            (tr.close_offset == XtOffset::Open) ||
            (tr.close_offset != XtOffset::Close        &&
             tr.close_offset != XtOffset::CloseToday   &&
             tr.close_offset != XtOffset::CloseYesterday &&
             ((tr.is_buy && pos_fp >= 0) || (!tr.is_buy && pos_fp <= 0)));
        tr.is_close     = !is_open;
        tr.close_offset = is_open ? XtOffset::Open : tr.close_offset;
    }
}

// gateway 只做 cost enrichment 与 is_close/is_today 推断，然后入 fills_buf_。
// order_book/account/listener 的 domain 处理由 engine.on_trade + drain_fills 完成。
//
// 关键不变量：finalize_fill_offsets_ 基于 account snapshot 的读视图
// 判断 is_close/is_open。**必须先于 engine.on_trade 调 account.ingest
// 完成**——所以 finalize 在本 apply_fills_ 内对 drained 批次一次性完成，
// 保证批内所有 trade 读到同一个 pre-batch account 状态。
void XtCtaBacktestOrderGateway::apply_fills_() {
    trades_scratch_.clear();
    match_engine_.drain_filled_trades(trades_scratch_);
    finalize_fill_offsets_(trades_scratch_);
    for (auto& tr : trades_scratch_) {
        // 成本模型就地补齐 fee/slippage/exec_price。
        if (cost_) cost_->apply(tr);
        // listener 按 tr.strategy_id 分发；terminal order_update 负责清理 sidecar。
        auto it = order_strategy_.find(tr.order_id);
        if (it != order_strategy_.end()) {
            tr.strategy_id = it->second;
            // 部分成交时保留 entry，确保同一订单后续 trade 仍能归属到相同策略。
        }
        fills_buf_.push_back(tr);
    }
}

// ── ICtaGateway drain overrides ────────────────────────────────────────────

size_t XtCtaBacktestOrderGateway::drain_fills(uint32_t /*date*/, uint32_t /*time_ms*/,
                                               std::vector<XtTradeRecord>& out) {
    if (fills_buf_.empty()) return 0;
    const size_t before = out.size();
    out.insert(out.end(),
               std::make_move_iterator(fills_buf_.begin()),
               std::make_move_iterator(fills_buf_.end()));
    fills_buf_.clear();
    // observer 在事件真正 drain 给 engine 时收到回报。
    for (size_t i = before; i < out.size(); ++i) {
        for (auto* obs : observers_) {
            obs->on_trade(out[i]);
        }
    }
    return out.size() - before;
}

size_t XtCtaBacktestOrderGateway::drain_order_updates(std::vector<XtOrderExecUpdate>& out) {
    if (updates_buf_.empty()) return 0;
    const size_t before = out.size();
    out.insert(out.end(),
               std::make_move_iterator(updates_buf_.begin()),
               std::make_move_iterator(updates_buf_.end()));
    updates_buf_.clear();
    // observer 在事件真正 drain 给 engine 时收到回报。
    for (size_t i = before; i < out.size(); ++i) {
        for (auto* obs : observers_) {
            obs->on_order_update(out[i]);
        }
    }
    return out.size() - before;
}
