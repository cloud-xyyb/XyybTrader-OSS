#include "xt_live/hft/reconcile/XtHftAccountReconcile.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <sstream>
#include <string>
#include <system_error>

#include "xt_live/hft/control/HftRunnerControl.h"
#include "XtRunnerControlCfg.h"
#include "AccountStateStore.h"
#include "api/ITradeGateway.h"
#include "api/XtRunnerSignals.h"
#include "XtLogger.h"
#include "XtFixedPoint.h"
#include "xt_util/path/PathUtil.h"

namespace xt_live::control {

namespace {

int64_t now_ms_system() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool nearly_equal(double a, double b, double abs_tol, double rel_tol) {
    const double diff = std::fabs(a - b);
    const double scale = std::max(std::fabs(a), std::fabs(b));
    return diff <= (abs_tol + rel_tol * scale);
}

std::string sanitize_file_token(const std::string& in, const std::string& fallback) {
    if (in.empty()) return fallback;
    std::string out;
    out.reserve(in.size());
    for (unsigned char ch : in) {
        if ((ch >= 'a' && ch <= 'z') ||
            (ch >= 'A' && ch <= 'Z') ||
            (ch >= '0' && ch <= '9') ||
            ch == '_' ||
            ch == '-') {
            out.push_back(static_cast<char>(ch));
        } else {
            out.push_back('_');
        }
    }
    return out.empty() ? fallback : out;
}

std::string account_reconcile_report_path(const XtRunnerControlConfig& cfg) {
    const std::filesystem::path dir = cfg.unified_report_dir.empty()
                                          ? std::filesystem::path("var/live_metrics/report_contract")
                                          : std::filesystem::path(cfg.unified_report_dir);
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    const std::string run_id = sanitize_file_token(cfg.unified_run_id, "default");
    return (dir / ("account_reconcile_" + run_id + ".log")).string();
}

bool write_text_file(const std::string& path, const std::string& content) {
    return PathUtil::write_atomic(path, content);
}

struct ReconcileDiff {
    bool pass = true;
    double lhs = 0.0;
    double rhs = 0.0;
    double diff = 0.0;
};

ReconcileDiff make_reconcile_diff(double lhs, double rhs, double abs_tol, double rel_tol) {
    ReconcileDiff out;
    out.lhs = lhs;
    out.rhs = rhs;
    out.diff = std::fabs(lhs - rhs);
    out.pass = nearly_equal(lhs, rhs, abs_tol, rel_tol);
    return out;
}

// 停机时刻 runtime 账本视图；fp → double 仅用于报告比较。
struct RuntimeView {
    double cash = 0.0;
    double available = 0.0;
    double equity = 0.0;
    double unrealized = 0.0;
    double realized = 0.0;
    double fees = 0.0;
    account_audit::XtFrozenAuditReport frozen_audit;
};

RuntimeView fetch_runtime_view(const XtHftRunnerControlContext& ctx) {
    RuntimeView out;
    if (!ctx.account_store) return out;
    const auto m = ctx.account_store->snapshot_account();
    out.cash       = xt_fp::from_fp(m.cash);
    out.available  = xt_fp::from_fp(m.available_cash);
    out.equity     = xt_fp::from_fp(m.equity);
    out.unrealized = xt_fp::from_fp(m.unrealized_pnl);
    out.realized   = xt_fp::from_fp(m.realized_pnl);
    out.fees       = xt_fp::from_fp(m.fees);
    ctx.account_store->frozen_audit_report(out.frozen_audit);
    return out;
}

struct BrokerView {
    bool   has_snapshot = false;
    bool   fresh = false;
    double balance = 0.0;
    uint64_t snapshot_ms = 0;
    ReconcileDiff balance_d;
};

BrokerView fetch_broker_view(const XtHftRunnerControlContext& ctx,
                             const RuntimeView& rt,
                             double abs_tol, double rel_tol) {
    const bool cached_present =
        ctx.runtime_shared_state.has_account_snapshot.load(std::memory_order_acquire);
    const double cached_balance =
        xt_fp::from_fp(ctx.runtime_shared_state.latest_balance_fp.load(std::memory_order_acquire));
    const uint64_t cached_ms =
        ctx.runtime_shared_state.latest_account_snapshot_ms.load(std::memory_order_acquire);
    const BrokerBalancePick pick = pick_broker_balance(
        ctx.td_gateway, cached_present, cached_balance, cached_ms,
        static_cast<uint64_t>(now_ms_system()));

    BrokerView out;
    out.has_snapshot = pick.has;
    out.fresh = pick.fresh;
    if (!out.has_snapshot) return out;
    out.balance = pick.balance;
    out.snapshot_ms = pick.snapshot_ms;
    out.balance_d = make_reconcile_diff(rt.cash, out.balance, abs_tol, rel_tol);
    return out;
}

void write_reconcile_report(const RuntimeView& rt,
                            const BrokerView& bd,
                            double abs_tol, double rel_tol,
                            bool all_pass,
                            const std::string& report_path) {
    std::ostringstream report;
    report << "ts_ms=" << now_ms_system() << "\n";
    report << "status=" << (all_pass ? "PASS" : "FAIL") << "\n";
    report << "runtime_cash=" << rt.cash << "\n";
    report << "runtime_available=" << rt.available << "\n";
    report << "runtime_equity=" << rt.equity << "\n";
    report << "runtime_unrealized=" << rt.unrealized << "\n";
    report << "runtime_realized=" << rt.realized << "\n";
    report << "runtime_fees=" << rt.fees << "\n";
    report << "frozen_audit_pass=" << (rt.frozen_audit.ok ? 1 : 0) << "\n";
    report << "frozen_cash=" << rt.frozen_audit.frozen_cash << "\n";
    report << "frozen_cash_by_orders=" << rt.frozen_audit.frozen_cash_by_orders << "\n";
    if (!rt.frozen_audit.ok) {
        report << "frozen_audit_reason=" << rt.frozen_audit.reason << "\n";
    }
    report << "broker_snapshot_present=" << (bd.has_snapshot ? 1 : 0) << "\n";
    if (bd.has_snapshot) {
        report << "broker_source=" << (bd.fresh ? "fresh_rest" : "anchor_cache") << "\n";

        report << "broker_snapshot_ms=" << bd.snapshot_ms << "\n";
        report << "broker_balance=" << bd.balance << "\n";
        report << "broker_balance_diff=" << bd.balance_d.diff << "\n";
        report << "broker_pass=" << (bd.balance_d.pass ? 1 : 0) << "\n";
    } else {
        report << "broker_pass=1\n";
    }
    report << "tolerance_abs=" << abs_tol << "\n";
    report << "tolerance_rel=" << rel_tol << "\n";

    if (!write_text_file(report_path, report.str())) {
        XtLogger::warn_m("RT", "account reconcile report write failed path={}", report_path);
    }
}

}

BrokerBalancePick pick_broker_balance(ITradeGateway* gw,
                                      bool cached_present,
                                      double cached_balance,
                                      uint64_t cached_ms,
                                      uint64_t now_ms) {
    BrokerBalancePick out;
    double fresh_balance = 0.0;
    if (gw && gw->query_wallet_balance_sync(fresh_balance)) {
        out.has = true;
        out.fresh = true;
        out.balance = fresh_balance;
        out.snapshot_ms = now_ms;
        return out;
    }
    out.has = cached_present;
    out.balance = cached_balance;
    out.snapshot_ms = cached_ms;
    return out;
}

void run_account_reconcile_on_stop(const XtRunnerControlConfig& cfg,
                                   const XtHftRunnerControlContext& ctx) {
    if (!cfg.account_reconcile_on_stop) return;
    if (!ctx.account_store) {
        XtLogger::warn_m("RT", "account reconcile skipped: account_store is null");
        return;
    }

    const double abs_tol = std::max(0.0, cfg.account_reconcile_abs_tol);
    const double rel_tol = std::max(0.0, cfg.account_reconcile_rel_tol);

    const RuntimeView rt = fetch_runtime_view(ctx);
    const BrokerView bd = fetch_broker_view(ctx, rt, abs_tol, rel_tol);

    const bool broker_pass = !bd.has_snapshot || bd.balance_d.pass;
    const bool frozen_pass = rt.frozen_audit.ok;
    const bool all_pass = broker_pass && frozen_pass;

    const std::string report_path = account_reconcile_report_path(cfg);
    write_reconcile_report(rt, bd, abs_tol, rel_tol, all_pass, report_path);

    if (all_pass) {
        XtLogger::info_m("RT",
                         "account reconcile PASS cash={} broker_present={} broker_source={} report={}",
                         rt.cash, bd.has_snapshot ? 1 : 0,
                         bd.has_snapshot ? (bd.fresh ? "fresh_rest" : "anchor_cache") : "none",
                         report_path);
    } else {
        XtLogger::error_m("RT",
                          "account reconcile FAIL broker_pass={} frozen_pass={} broker_source={} report={}",
                          broker_pass ? 1 : 0, frozen_pass ? 1 : 0,
                          bd.has_snapshot ? (bd.fresh ? "fresh_rest" : "anchor_cache") : "none",
                          report_path);
    }
}

}
