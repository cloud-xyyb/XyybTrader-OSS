#pragma once

/// @file
/// @brief Backtest 报告共享行类型与 CSV helper。
///
/// 本文件定义 CTA/SEL 共用的 report row schema 与写出 helper。CSV schema 受
/// regression golden 约束，新增或重排列必须显式评估兼容性。

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include "xt_backtest/core/BtRunnerUtil.h"
#include "api/XtBtFillTruth.h"
#include "xt_core/XtFixedPoint.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_logger/XtLogger.h"
#include "xt_util/path/PathUtil.h"
#include "xt_util/time/XtTime.h"

/// @brief CTA/SEL backtest `trades.csv` 的规范化成交行。
///
/// 由 `to_bar_trade_row(const XtBtFillTruth&)` 在真值点（account apply 之后）构造，
/// `equity_after` 随构造一次写定。本结构仍是公开聚合体，字段技术上可被后续赋值；
/// 保证的是「没有任何构造路径能产出缺值的行」，不是不可变性。`strategy_id` 仅在
/// 多策略或 SEL schema 中输出，CTA legacy schema 默认不写该列。
struct BarTradeRow {
    uint64_t order_id     = 0;
    std::string code;
    bool     is_buy       = true;
    bool     is_close     = false;
    double   qty          = 0.0;
    double   exec_price   = 0.0;
    double   fee          = 0.0;
    double   slippage     = 0.0;
    uint32_t trade_date   = 0;
    uint32_t trade_time   = 0;
    uint32_t trading_day  = 0;
    double   equity_after = 0.0;
    std::string strategy_id;
};

inline int64_t packed_ts_from_trade(const XtTradeRecord& tr) {
    return xt_time::packed_ts(tr.trade_date, tr.trade_time);
}

inline int64_t packed_ts_from_tick(const XtRawTick& t) {
    return xt_time::packed_ts(t.action_date, t.action_time);
}

inline BarTradeRow to_bar_trade_row(const XtBtFillTruth& t) {
    const XtTradeRecord& tr = t.tr;
    BarTradeRow row;
    row.order_id    = tr.order_id;
    row.code        = tr.code;
    row.is_buy      = tr.is_buy;
    row.is_close    = tr.is_close;
    row.qty         = order_exec_from_fp(tr.qty_fp);
    row.exec_price  = tr.exec_price;
    row.fee         = tr.fee;
    row.slippage    = tr.slippage;
    row.trade_date  = tr.trade_date;
    row.trade_time  = tr.trade_time;
    row.trading_day = t.trading_day != 0 ? t.trading_day : tr.trade_date;
    row.strategy_id = tr.strategy_id;
    row.equity_after = xt_fp::from_fp(t.equity_after_fp);
    return row;
}

inline BarTradeRow to_bar_trade_row(const XtTradeRecord&) = delete;

inline std::string csv_escape_field(std::string_view s) {
    const bool needs_quote = s.find_first_of(",\"\r\n") != std::string_view::npos;
    if (!needs_quote) return std::string(s);

    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('"');
    for (char ch : s) {
        if (ch == '"') out.push_back('"');
        out.push_back(ch);
    }
    out.push_back('"');
    return out;
}

[[nodiscard]] inline bool finish_report_stream(PathUtil::AtomicOfstream& out,
                                              const std::string& path,
                                              const char* tag,
                                              const char* what,
                                              size_t rows) {
    if (!out.stream()) {
        out.abort();
        XtLogger::error_m(tag, "{} write failed before commit: {}", what, path);
        return false;
    }
    if (!out.commit()) {
        XtLogger::error_m(tag, "{} commit failed: {} (disk full / fsync / rename error)",
                          what, path);
        return false;
    }
    XtLogger::info_m(tag, "{} written: {} rows={}", what, path, rows);
    return true;
}

[[nodiscard]] inline bool write_daily_equity_csv(
    const std::string& out_dir,
    const std::vector<std::pair<uint32_t, int64_t>>& rows) {
    const auto path = std::filesystem::path(out_dir) / "daily_equity.csv";
    PathUtil::AtomicOfstream af(path.string());
    if (!af.is_open()) {
        XtLogger::error_m("CTA_BT", "daily_equity.csv open failed: {}", path.string());
        return false;
    }
    auto& out = af.stream();
    out << "date,equity\n";
    char buf[64];
    for (const auto& [date, eq_fp] : rows) {
        int n = std::snprintf(buf, sizeof(buf), "%u,%.2f\n",
                              date, xt_fp::from_fp(eq_fp));
        if (n > 0) out.write(buf, n);
    }
    return finish_report_stream(af, path.string(), "CTA_BT", "daily_equity.csv", rows.size());
}

[[nodiscard]] inline bool write_trades_csv(const std::string& out_dir,
                                           const std::vector<BarTradeRow>& rows,
                                           bool include_strategy_id = false) {
    const auto path = std::filesystem::path(out_dir) / "trades.csv";
    PathUtil::AtomicOfstream af(path.string());
    if (!af.is_open()) {
        XtLogger::error_m("CTA_BT", "trades.csv open failed: {}", path.string());
        return false;
    }
    auto& out = af.stream();

    if (include_strategy_id) {
        out << "order_id,date,time,code,direction,offset,qty,exec_price,fee,slippage,"
               "strategy_id,equity_after\n";
    } else {
        out << "order_id,date,time,code,direction,offset,qty,exec_price,fee,slippage,equity_after\n";
    }

    char buf[320];
    for (const auto& r : rows) {
        const char* dir = r.is_buy   ? "BUY"   : "SELL";
        const char* off = r.is_close ? "CLOSE" : "OPEN";
        const std::string code = csv_escape_field(r.code);
        const std::string strategy_id = csv_escape_field(r.strategy_id);
        int n;
        if (include_strategy_id) {
            n = std::snprintf(buf, sizeof(buf),
                "%llu,%u,%u,",
                static_cast<unsigned long long>(r.order_id),
                r.trade_date, r.trade_time);
            if (n > 0) out.write(buf, n);
            out << code;
            n = std::snprintf(buf, sizeof(buf),
                ",%s,%s,%.4f,%.4f,%.4f,%.4f,",
                dir, off, r.qty, r.exec_price, r.fee, r.slippage);
            if (n > 0) out.write(buf, n);
            out << strategy_id;
            n = std::snprintf(buf, sizeof(buf), ",%.2f\n", r.equity_after);
        } else {
            n = std::snprintf(buf, sizeof(buf),
                "%llu,%u,%u,",
                static_cast<unsigned long long>(r.order_id),
                r.trade_date, r.trade_time);
            if (n > 0) out.write(buf, n);
            out << code;
            n = std::snprintf(buf, sizeof(buf),
                ",%s,%s,%.4f,%.4f,%.4f,%.4f,%.2f\n",
                dir, off, r.qty, r.exec_price, r.fee, r.slippage, r.equity_after);
        }
        if (n > 0) out.write(buf, n);
    }
    return finish_report_stream(af, path.string(), "CTA_BT", "trades.csv", rows.size());
}

/// @brief CTA bar/tick runner 共用的 `summary.json` 字段集。
struct BtSummaryJsonRow {
    std::string cfg_path;
    std::string strategy_id;
    std::string engine_type;
    std::string bar_period;
    double      initial_cash    = 0.0;
    size_t      total_bars      = 0;
    size_t      total_ticks     = 0;
    int64_t     final_equity_fp = 0;
    int         risk_rejects    = 0;
    uint64_t    strategy_callbacks = 0;
    bool        shared_account     = false;
};

[[nodiscard]] inline bool write_summary_json(const std::string& out_dir,
                                             const BtSummaryJsonRow& row) {
    const auto path = std::filesystem::path(out_dir) / "summary.json";
    PathUtil::AtomicOfstream af(path.string());
    if (!af.is_open()) {
        XtLogger::error_m("CTA_BT", "summary.json open failed: {}", path.string());
        return false;
    }
    auto& out = af.stream();
    const double final_eq  = xt_fp::from_fp(row.final_equity_fp);
    const double total_pnl = final_eq - row.initial_cash;
    const double pnl_pct   = row.initial_cash > 0.0 ? (total_pnl / row.initial_cash * 100.0) : 0.0;
    char buf[64];
    auto fmt2 = [&buf](double v) -> const char* {
        std::snprintf(buf, sizeof(buf), "%.2f", v);
        return buf;
    };
    out << "{\n";
    out << "  \"config_path\": \"" << xt_bt::json_escape(row.cfg_path) << "\",\n";
    out << "  \"strategy_id\": \"" << xt_bt::json_escape(row.strategy_id) << "\",\n";
    out << "  \"engine\": \"" << xt_bt::json_escape(row.engine_type) << "\",\n";
    out << "  \"bar_period\": \"" << xt_bt::json_escape(row.bar_period) << "\",\n";
    out << "  \"total_bars\": " << row.total_bars << ",\n";
    out << "  \"total_ticks\": " << row.total_ticks << ",\n";
    out << "  \"initial_cash\": " << fmt2(row.initial_cash) << ",\n";
    out << "  \"final_equity\": " << fmt2(final_eq) << ",\n";
    out << "  \"total_pnl\": " << fmt2(total_pnl) << ",\n";
    char pct_buf[32];
    std::snprintf(pct_buf, sizeof(pct_buf), "%.6f", pnl_pct);
    out << "  \"pnl_pct\": " << pct_buf << ",\n";
    out << "  \"risk_rejects\": " << row.risk_rejects << ",\n";
    out << "  \"strategy_callbacks\": " << row.strategy_callbacks;

    if (row.shared_account) out << ",\n  \"shared_account\": true";
    out << "\n}\n";
    return finish_report_stream(af, path.string(), "CTA_BT", "summary.json", 1);
}
