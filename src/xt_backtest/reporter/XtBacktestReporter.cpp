#include "XtBacktestReporter.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <utility>
#include <vector>

#include "XtLogger.h"
#include "xt_core/market/XtRawTick.h"
#include "XtFixedPoint.h"
#include "xt_backtest/common/XtBacktestReport.h"  // packed_ts_from_trade / packed_ts_from_tick
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_risk/engine/XtRiskEngine.h"   // XtRiskResult POD type (header-only access);
                                    // reporter 不调 risk_reason_text 函数,仅靠 include 拿 type 定义。

bool XtBacktestReporter::init(xt_report::IReportSink& report_sink,
                                   std::unique_ptr<XtEventStreamSink> event_stream_sink) {
    sink_ = &report_sink;
    event_writer_ = std::move(event_stream_sink);
    return true;
}

void XtBacktestReporter::on_tick_pre(int64_t ts, const XtRawTick& t) {
    // replayer 传入 packed ts (== packed_ts_from_tick(t)),直接 cache;
    // packed_ts_from_tick 仍可 derive 用作 cross-check (assert 跑测试用)。
    last_tick_ts_ = ts;
    event_writer_->on_tick(last_tick_ts_, t);
    // ── daily breakdown: track current trading_date + 累计 tick_count ─────
    // trading_date == 0 时 fallback 到 action_date。
    const uint32_t td = t.trading_date != 0 ? t.trading_date : t.action_date;
    if (td != 0) {
        // 日切边界采样上一交易日的日终账户：新交易日首 tick 尚未产生任何事件，
        // 此刻账户状态 == 上一日收盘状态。
        if (current_trading_date_ != 0 && td != current_trading_date_ && account_probe_) {
            const AccountEod eod = account_probe_();
            daily_breakdown_.on_day_account(current_trading_date_, eod.equity, eod.cash,
                                            eod.frozen);
        }
        current_trading_date_ = td;
        daily_breakdown_.on_tick(td);
    }
}

void XtBacktestReporter::on_tick_post(int64_t /*ts*/, size_t active_orders) {
    // ts 已在 on_tick_pre 缓存为 last_tick_ts_;event_writer 走 cached ts 保证
    // pre/post 一对 tick 用同一 ts (避免边界异常)。
    event_writer_->on_sys_stats(last_tick_ts_, active_orders);
}

bool XtBacktestReporter::flush() {
    // backtest 只写 fills.csv（经 IReportSink）+ daily_breakdown.json（finalize 落盘）
    //   + 可选 events_full.csv（trace_mode=full）。run_summary / kpi_1s /
    //   position_snapshot / account_truth 均为 live-only,backtest 不产出。

    // 最后一个交易日没有下一次日切，这里补采样其日终账户。
    if (current_trading_date_ != 0 && account_probe_) {
        const AccountEod eod = account_probe_();
        daily_breakdown_.on_day_account(current_trading_date_, eod.equity, eod.cash, eod.frozen);
    }

    // 6. write fills（on_fill 时已即时收集，与实盘路径统一）
    for (const auto& f : fills_) {
        if (!sink_->on_fill(f)) {
            XtLogger::warn_m("REPORT", "on_fill drop oid={} ts={}", f.order_id, f.ts_ms);
            ++drop_count_;
        }
    }

    // 7. flush — propagate sink fsync 错误
    bool ok = true;
    if (!sink_->sync_flush()) {
        XtLogger::error_m("REPORT", "sink_->sync_flush() failed (disk-full / failbit / fsync error)");
        ok = false;
    }
    event_writer_->flush();
    return ok;
}

void XtBacktestReporter::on_fill(const xt_report::XtFillRow& fill, const XtTradeRecord& raw) {
    ++trade_count_;
    // daily breakdown: XtFillRow 只含 trading_day 字段 (无 trade_date)，
    //   fallback 链: fill.trading_day > 0 → raw.trade_date > 0 → current_trading_date_。
    //   raw 提供 trade_date 给 fallback（实盘 trading_day 缺失时回测路径 fallback）。
    const uint32_t td =
        fill.trading_day != 0 ? fill.trading_day :
        (raw.trade_date != 0 ? raw.trade_date : current_trading_date_);
    daily_breakdown_.on_fill(td);

    fills_.push_back(fill);

    // 事件轴 stamp：优先取 gateway 盖的模型可见时刻（自然日 epoch ns，撮合 + 回程
    // 延迟）。它是这笔成交真正对策略可见的瞬间，与交付它的是市场 tick 还是 grid
    // clock / EOF 无关。
    //
    // 退回 last_tick_ts_ 的旧写法只在 sim_visible_at_ns 未盖章时保留：那个值是「最近
    // 一个市场 tick」，而 grid clock（HFT 强制 250ms 网格）与 EOF 交付都不经过
    // reporter 的 tick 回调，于是两个真实 tick 之间交付的成交会带上陈旧时刻。
    // trade_date/trade_time 是账务列，始终不动。
    {
        auto& row = fills_.back();
        if (raw.sim_visible_at_ns >= 0) {
            row.ts_event_ns = raw.sim_visible_at_ns;
            row.ts_recv_ns  = raw.sim_visible_at_ns;
            row.ts_ms       = raw.sim_visible_at_ns / 1'000'000LL;
        } else if (last_tick_ts_ >= 1'000'000'000LL) {  // date 位非零才重 stamp
            const auto ad = static_cast<uint32_t>(last_tick_ts_ / 1'000'000'000LL);
            const auto at = static_cast<uint32_t>(last_tick_ts_ % 1'000'000'000LL);
            row.ts_ms       = xt_time::to_ns(ad, at) / 1'000'000LL;
            row.ts_event_ns = row.ts_ms * 1'000'000LL;
            row.ts_recv_ns  = row.ts_event_ns;
        }
    }

    // event_writer：fill 行与 tick/update 行同走自然日 packed 轴（单一单调时钟）；
    // 无 tick 时退 trade 派生 packed ts（仅理论路径，fill 必由 tick 驱动 drain）。
    event_writer_->on_trade(
        last_tick_ts_ >= 1'000'000'000LL ? last_tick_ts_ : packed_ts_from_trade(raw), raw);

    // per-fill PnL 由 AccountStateStore 维护；reporter 只收集 fill 行与计数，不做累计。
}

void XtBacktestReporter::on_order_update(const XtOrderExecUpdate& u) {
    // 用 last_tick_ts_ 派生 event_writer ts (XtOrderExecUpdate 无内置 date/time
    // 字段,走 cached tick ts fallback)
    event_writer_->on_order_update(last_tick_ts_, u);
}

void XtBacktestReporter::on_order_submit(int64_t ts,
                                              uint64_t id,
                                              const char* code,
                                              bool is_buy,
                                              int64_t price_fp,
                                              int64_t qty_fp,
                                              std::string_view user_tag,
                                              bool is_close,
                                              bool is_today) {
    // daily breakdown: 每次 submit 计 1(包含被 risk reject 后的 submit;与 fill_rate 对账)
    daily_breakdown_.on_submit(current_trading_date_);
    event_writer_->on_order_submit(ts, id, code, is_buy, price_fp, qty_fp, user_tag, is_close, is_today);
}

void XtBacktestReporter::on_risk_reject(int64_t ts,
                                             const char* code,
                                             bool is_buy,
                                             int64_t price_fp,
                                             int64_t qty_fp,
                                             const std::string& reason) {
    (void)is_buy;
    const double price = xt_fp::from_fp(price_fp);
    const double qty = xt_fp::from_fp(qty_fp);
    (void)ts;
    int& cnt = risk_rejects_[reason];
    cnt += 1;
    // daily breakdown: risk_reject 按 current_trading_date_ 聚合，reason 进分桶——
    // 「策略为什么不交易」的第一现场，不再只有裸计数。
    daily_breakdown_.on_risk_reject(current_trading_date_, reason);

    if (cnt <= 5) {
        XtLogger::error_m("RISK", "reject {} {} @{} reason={}", code ? code : "", qty, price, reason);
    }
}

// ── IRejectSink override ──────────────────────────────────────────────────
// Gateway 走 IRejectSink::on_reject / on_margin_reject 直接调；本层解包
// 代码 / qty / price 后复用 on_risk_reject，更新 daily breakdown 并限量打 error log。
//
// caller (`risk_action::dispatch_reject` via `XtRiskEngine::make_reject`) **保证**
// `decision.reason_text` 非空 (内部已 `text.empty() ? risk_reason_text(code) : text`
// fallback)。reporter 不再调 risk_reason_text,xt_bt_reporter CMake 去掉 xt_risk
// link (api/XtRiskTypes.h 只接 POD 类型,跨 module forward decl 无需具体 lib)。

void XtBacktestReporter::on_margin_reject(const xt_core::XtOrder& req,
                                          const XtRiskResult& decision,
                                          int64_t /*now_ms*/,
                                          std::string_view /*strategy_id*/) {
    on_risk_reject(last_tick_ts_, req.code, req.is_buy, req.price_fp, req.qty_fp,
                   decision.reason_text);
}

void XtBacktestReporter::on_reject(const xt_core::XtOrder& req,
                                   const XtRiskResult& decision,
                                   std::string_view /*strategy_id*/) {
    on_risk_reject(last_tick_ts_, req.code, req.is_buy, req.price_fp, req.qty_fp,
                   decision.reason_text);
}

bool XtBacktestReporter::write_daily_breakdown(const std::string& out_path,
                                               uint32_t cfg_start_date,
                                               uint32_t cfg_end_date) const {
    // Delegate 到独立 XtBacktestDailyBreakdown 类 (SRP)。
    return daily_breakdown_.write_json(out_path, cfg_start_date, cfg_end_date);
}
