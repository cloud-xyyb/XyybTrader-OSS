#include "xt_live/hft/reconcile/XtHftAccountReconcile.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

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

// gateway 交付投影 open 集合（差集观测输入）；@pre 同 run_account_reconcile_on_stop
// —— gateway stop() 已完成，投影读取为无并发写者的冷路径。
struct LedgerView {
    bool supported = false;
    std::vector<uint64_t> open_ids;
};

LedgerView fetch_ledger_view(const XtHftRunnerControlContext& ctx) {
    LedgerView out;
    if (!ctx.td_gateway) return out;
    out.supported = ctx.td_gateway->collect_open_delivery_ids(out.open_ids);
    return out;
}

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
                            const LedgerView& lv,
                            const LedgerOpenObservation& obs,
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

    report << "exec_ledger_present=" << (obs.present ? 1 : 0) << "\n";
    if (obs.present) {
        report << "exec_ledger_open_count=" << obs.open_count << "\n";
        if (!lv.open_ids.empty()) {
            constexpr std::size_t kIdDumpCap = 32;
            report << "exec_ledger_open_ids=";
            const std::size_t dump = std::min(lv.open_ids.size(), kIdDumpCap);
            for (std::size_t i = 0; i < dump; ++i) {
                if (i > 0) report << ",";
                report << lv.open_ids[i];
            }
            if (lv.open_ids.size() > kIdDumpCap) report << ",...";
            report << "\n";
        }
        report << "exec_ledger_stale_frozen_suspect="
               << (obs.stale_frozen_suspect ? 1 : 0) << "\n";
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

LedgerOpenObservation make_ledger_open_observation(bool supported,
                                                   std::size_t open_count,
                                                   double frozen_cash_by_orders) {
    LedgerOpenObservation out;
    out.present = supported;
    out.open_count = supported ? open_count : 0;
    out.stale_frozen_suspect =
        supported && open_count == 0 && frozen_cash_by_orders > 0.0;
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
    const LedgerView lv = fetch_ledger_view(ctx);
    const LedgerOpenObservation obs = make_ledger_open_observation(
        lv.supported, lv.open_ids.size(), rt.frozen_audit.frozen_cash_by_orders);

    const bool broker_pass = !bd.has_snapshot || bd.balance_d.pass;
    const bool frozen_pass = rt.frozen_audit.ok;
    const bool all_pass = broker_pass && frozen_pass;

    const std::string report_path = account_reconcile_report_path(cfg);
    write_reconcile_report(rt, bd, lv, obs, abs_tol, rel_tol, all_pass, report_path);

    if (obs.stale_frozen_suspect) {
        XtLogger::warn_m("RT",
                         "account reconcile: stale frozen suspect — 交付投影 open=0 但 "
                         "frozen_by_orders={}（trade-only 终态漏收尾候选）",
                         rt.frozen_audit.frozen_cash_by_orders);
    }
    if (all_pass) {
        XtLogger::info_m("RT",
                         "account reconcile PASS cash={} broker_present={} broker_source={} "
                         "ledger_open={} report={}",
                         rt.cash, bd.has_snapshot ? 1 : 0,
                         bd.has_snapshot ? (bd.fresh ? "fresh_rest" : "anchor_cache") : "none",
                         obs.present ? static_cast<long long>(obs.open_count) : -1LL,
                         report_path);
    } else {
        XtLogger::error_m("RT",
                          "account reconcile FAIL broker_pass={} frozen_pass={} broker_source={} "
                          "ledger_open={} report={}",
                          broker_pass ? 1 : 0, frozen_pass ? 1 : 0,
                          bd.has_snapshot ? (bd.fresh ? "fresh_rest" : "anchor_cache") : "none",
                          obs.present ? static_cast<long long>(obs.open_count) : -1LL,
                          report_path);
    }
}

}
