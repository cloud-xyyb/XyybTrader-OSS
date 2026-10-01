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

#include "xt_backtest/core/BtRunnerUtil.h"  // xt_bt::json_escape
#include "api/XtBtFillTruth.h"
#include "xt_core/XtFixedPoint.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_logger/XtLogger.h"
#include "xt_util/path/PathUtil.h"   // AtomicOfstream：写出即完整
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
    double   qty          = 0.0;  ///< 由 fixed-point 成交量转换为手数。
    double   exec_price   = 0.0;  ///< cost model 后的最终成交价。
    double   fee          = 0.0;
    double   slippage     = 0.0;
    uint32_t trade_date   = 0;    ///< YYYYMMDD 自然日（事件轴）。
    uint32_t trade_time   = 0;    ///< HHMMSSmmm。
    uint32_t trading_day  = 0;    ///< YYYYMMDD 账务归属交易日；夜盘与 trade_date 分叉。
    double   equity_after = 0.0;  ///< 本笔成交落账后的账户权益（逐笔 running equity）。
    std::string strategy_id;
};

/// @brief 从 XtTradeRecord 派生 packed timestamp（YYYYMMDD*1e9 + HHMMSSmmm）。
inline int64_t packed_ts_from_trade(const XtTradeRecord& tr) {
    return xt_time::packed_ts(tr.trade_date, tr.trade_time);
}

/// @brief 从 XtRawTick 派生 packed timestamp。
inline int64_t packed_ts_from_tick(const XtRawTick& t) {
    return xt_time::packed_ts(t.action_date, t.action_time);
}

/// @brief 在真值点把成交转换为 CSV 行。
///
/// 唯一构造入口：`XtBtFillTruth` 只能在 account apply 完成后的同一帧构造，因而
/// `equity_after` / `trading_day` 必然携带真值；旧的「先造空行、后按区间回填」
/// 路径已随 `XtTradeRecord` 重载的删除而无法表达。
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

/// @brief 墓碑重载：禁止绕过真值点由裸成交记录直接造行（那是 BR-01 的成因）。
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

/// @brief 提交一次报告写出，并判定是否真的完整落盘。
///
/// 走 `PathUtil::AtomicOfstream`：先写临时文件，commit 时 fsync + rename。因此
/// 「文件存在」即蕴含「内容完整」——消费者不会读到半截文件。这一点比事后再写一个
/// 完成标记更有用：目录不可写时标记本身也写不出去，而下游（viewer normalize）是
/// 容忍缺文件、却会在截断行上抛异常的。
///
/// 只判流状态，不看调用方返回值——「写函数自己恒返回 true」正是要避免的判据。
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

/// @brief 写 `daily_equity.csv` 到指定目录。
/// @return false = 未能完整落盘（open/write/close 任一失败）；调用方必须处理。
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

/// @brief 写 `trades.csv` 到指定目录。
///
/// `include_strategy_id=false` 保持 CTA legacy schema；SEL 和多策略路径应显式传 true。
/// @return false = 未能完整落盘；trades.csv 是回归与 viewer ingest 的必需产物，
///         调用方不得吞掉该失败。
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
    std::string engine_type;          ///< "bar" / "tick"。
    std::string bar_period;
    double      initial_cash    = 0.0;
    size_t      total_bars      = 0;  ///< bar 模式有效；tick 模式传 0 保持 schema 稳定。
    size_t      total_ticks     = 0;  ///< tick 模式有效；bar 模式传 0。
    int64_t     final_equity_fp = 0;
    int         risk_rejects    = 0;
    uint64_t    strategy_callbacks = 0;  ///< 策略 on_tick/on_bar 分发累计（防静默 0-callback）。
    bool        shared_account     = false;  ///< true = 本目录的资金/权益类字段是共享账户口径。
};

/// @brief 写 per-strategy `summary.json` 到指定目录。
/// @note numeric 字段固定 %.2f（pnl_pct %.6f）精度，避免平台默认格式造成报告漂移。
/// @return false = 未能完整落盘；调用方必须处理。
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
    // 共享账户披露：bar 模式多策略共用一个 account/engine，本目录里的 final_equity /
    // total_pnl / risk_rejects / total_bars / daily_equity / trades.equity_after 全是
    // portfolio 口径，只有 trades 行本身是本策略的。仅为 true 时写出 → 单策略与
    // tick/SEL 产物逐字节不变（沿用 shared_execution_counters 先例）。
    if (row.shared_account) out << ",\n  \"shared_account\": true";
    out << "\n}\n";
    return finish_report_stream(af, path.string(), "CTA_BT", "summary.json", 1);
}
