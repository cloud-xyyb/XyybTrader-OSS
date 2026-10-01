#include "xt_live/hft/telemetry/XtHftTelemetryWiring.h"

#include <string>
#include <filesystem>
#include <system_error>
#include <utility>

#include "xt_engine/hft/XtHftEngine.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"
#include "xt_metrics/XtVenueLabel.h"
#include "xt_risk/config/XtRiskCfg.h"
#include "XtFixedPoint.h"
#include "XtMetricRegistry.h"
#include "XtLiveConfig.h"
#include "XtLogger.h"

namespace xt_live::hft {

namespace {

// aggr state 文件路径的唯一推导处：load（启动）与 save（shutdown）共用。instance_id
// 单一身份派生（resolved_instance_id/instance_state_dir，与 safety / baseline 同子目录）。
std::string aggregator_state_path(const XtLiveConfig& cfg) {
    return instance_state_dir(cfg.parsed.md_provider, cfg.runtime.control_cfg.instance_id) +
           "/aggregator_state.json";
}

std::string lifecycle_state_path(const XtLiveConfig& cfg) {
    // open_ts 与 recorder 同目录持久化；recording 未启用时跳过，避免生成
    // 无 lifecycle event 可关联的孤立状态文件。
    if (cfg.bootstrap.recording_cfg.file_path.empty()) return {};
    const std::string& rec_path = cfg.bootstrap.recording_cfg.file_path;
    const std::size_t slash = rec_path.find_last_of('/');
    const std::string dir = (slash == std::string::npos)
                            ? "."
                            : rec_path.substr(0, slash);
    return dir + "/lifecycle_open_ts.json";
}

// engine 只保存 observer raw ptr；Runner 用 unique_ptr 持有到 stop 路径结束。
// engine 析构期不得再回调 observer。本 observer 只把 fee 计入 PnL aggregator。
class XtHftFeeObserver final : public xt_runtime::IHftEngineObserver {
public:
    explicit XtHftFeeObserver(ct::metrics::XtStrategyPnlAggregator* a)
        : aggr_(a) {}
    void on_trade(const XtTradeRecord& tr, XtLatencyEligibility /*elig*/ = XtLatencyEligibility::kEligible,
                  uint64_t /*logical_order_id*/ = 0) override {
        // fee 记账是业务观测（每笔成交计 fee，与 latency eligibility 无关）；忽略 elig/logical。
        if (aggr_) {
            aggr_->on_fill(tr.strategy_id, tr.code, tr.fee);
        }
        // maker/taker 指标在 recorder drain 路径统一 emit；本 observer
        // 只把 fee 计入 PnL aggregator，避免重复计数。
    }
private:
    ct::metrics::XtStrategyPnlAggregator* aggr_;
};

}  // namespace

bool wire_hft_telemetry(
    std::unique_ptr<ct::metrics::XtStrategyPnlAggregator>& aggr_slot,
    std::unique_ptr<ct::metrics::XtStrategyPnlBaseline>& baseline_slot,
    std::unique_ptr<xt_runtime::IHftEngineObserver>& fee_observer_slot,
    XtHftEngine::Hooks& hooks,
    const XtLiveConfig& cfg) {
    // per-(sid, code) PNL 聚合器由 HftEngineRunResources 持有，HftRunnerControl 只读 raw ptr。
    aggr_slot = std::make_unique<ct::metrics::XtStrategyPnlAggregator>();

    // 跨重启恢复 realized/fees 累计值；否则 today_pnl_v2 只覆盖重启后的 fill，
    // 与 wallet truth 出现同日漂移。文件路径与 baseline 使用同一 instance_id。
    {
        using LR = ct::metrics::XtStrategyPnlAggregator::LoadResult;
        const std::string aggr_path = aggregator_state_path(cfg);
        const LR res = aggr_slot->load_state(aggr_path);
        if (res == LR::kLoaded) {
            XtLogger::info_m("control", "PNL aggregator state reloaded from {}", aggr_path);
        } else if (res == LR::kMissing) {
            XtLogger::info_m("control", "PNL aggregator first-init (no prior state at {})", aggr_path);
        } else {
            // kInvalid：原文件已 quarantine、rows_ 未覆盖。若本 runner 启用 per-strategy
            // daily-loss 风控，损坏的 PnL 输入会让该风控静默失效 → fail-closed 拒绝启动。
            if (has_active_per_strategy_daily_loss(cfg.bootstrap.order_gateway_cfg.risk)) {
                XtLogger::error_m("control",
                    "PNL aggregator state corrupt and per-strategy daily-loss enabled: "
                    "refusing to start with corrupt risk input (path={})", aggr_path);
                return false;
            }
            XtLogger::warn_m("control",
                "PNL aggregator state corrupt (quarantined), no per-strategy daily-loss "
                "configured: cold-starting from empty state (path={})", aggr_path);
        }
    }

    // baseline 缺失或损坏时 fail-closed；ready 前 HftRunnerControl 不 emit today_pnl_v2。
    // 跨重启 load 保持 today_pnl，跨 day rotation 由 HftRunnerControl save+rotate。
    {
        // baseline instance_id 与 aggregator 共用 resolved_instance_id（override 或 md_provider），
        // 保证同子目录、同 per-book 隔离；base_dir 不变 → 路径自然成 /state/<id>/pnl_baseline_<day>.json。
        // tz_offset_h=0 维持 state 文件名兼容口径。
        const std::string instance_id =
            resolved_instance_id(cfg.parsed.md_provider, cfg.runtime.control_cfg.instance_id);
        const int tz_offset_h = 0;
        baseline_slot = std::make_unique<ct::metrics::XtStrategyPnlBaseline>(instance_id);
        const std::string base_dir   = state_root();
        const int trading_day_id     = ct::metrics::XtStrategyPnlBaseline::compute_trading_day_id(tz_offset_h);
        const bool loaded = baseline_slot->load(base_dir, trading_day_id);
        if (!loaded) {
            const auto status = baseline_slot->last_load_status();
            ct::metrics::XtMetricRegistry::instance().inc_strategy_baseline_load_failed_total(
                xt_util::canonical_venue_label(cfg.parsed.md_provider),
                ct::metrics::XtStrategyPnlBaseline::load_status_name(status));
            // 兼容同日 baseline 仍落在 venue 目录的状态文件：若 per-instance 目录缺失、
            // venue 目录存在同名文件，则复制后 reload，保留真实 day-open，避免
            // baseline 缺失导致 today PnL 整日不 emit。
            if (status == ct::metrics::XtStrategyPnlBaseline::LoadStatus::Missing &&
                !cfg.runtime.control_cfg.instance_id.empty()) {
                namespace fs = std::filesystem;
                const std::string fname =
                    "pnl_baseline_" + std::to_string(trading_day_id) + ".json";
                const fs::path inst_path =
                    fs::path(instance_state_dir(cfg.parsed.md_provider,
                                                cfg.runtime.control_cfg.instance_id)) / fname;
                const fs::path legacy_path =
                    fs::path(instance_state_dir(cfg.parsed.md_provider, std::string())) / fname;
                std::error_code ec;
                if (legacy_path != inst_path &&
                    fs::exists(legacy_path, ec) && !fs::exists(inst_path, ec)) {
                    fs::create_directories(inst_path.parent_path(), ec);
                    fs::copy_file(legacy_path, inst_path, fs::copy_options::skip_existing, ec);
                    if (!ec && baseline_slot->load(base_dir, trading_day_id)) {
                        XtLogger::info_m("control",
                            "PNL baseline recovered: migrated legacy {} -> per-instance {} and reloaded",
                            legacy_path.string(), inst_path.string());
                    } else {
                        XtLogger::info_m("control",
                            "PNL baseline legacy->per-instance recovery skipped/failed (ec={}): {}",
                            ec.message(), inst_path.string());
                    }
                }
            }
        }
        // Baseline 文件可能只覆盖部分 book；按聚合器中的活跃 strategy 补齐 opening。
        // 已存在 sid 保留真实 day-open；缺失 sid 以当前 cumulative 作为 opening，
        // 使 today_pnl 从 0 开始，而不是等于全量 cumulative。
        if (aggr_slot) {
            const auto aggr_rows = aggr_slot->snapshot();
            if (!aggr_rows.empty()) {
                auto base_rows = baseline_slot->snapshot();
                int seeded = 0;
                for (const auto& ar : aggr_rows) {
                    bool is_new = false;
                    (void)baseline_slot->get(ar.sid, ar.code, &is_new);
                    if (is_new) {
                        ct::metrics::XtStrategyPnlBaseline::BaselineRow br;
                        br.sid      = ar.sid;
                        br.code     = ar.code;
                        br.realized = ar.realized;
                        br.fees     = ar.fees;
                        br.fills    = ar.fills;
                        base_rows.push_back(br);
                        ++seeded;
                    }
                }
                if (seeded > 0) {
                    baseline_slot->rotate_day(base_rows);
                    baseline_slot->save(base_dir, trading_day_id);
                    XtLogger::info_m("control",
                        "PNL baseline backfilled {} missing strategy sid(s) from aggregator (today=0 for those)",
                        seeded);
                }
            }
        }
    }
    {
        fee_observer_slot = std::make_unique<XtHftFeeObserver>(aggr_slot.get());
        hooks.observers.push_back(fee_observer_slot.get());
    }

    // 持仓 lifecycle 与 per-position metric 走 engine 构造期一次性注入。
    {
        XtHftEngine::LifecycleTelemetryInit init;
        init.state_path = lifecycle_state_path(cfg);
        // global() lazy lookup 允许 recorder 在 start() 后再 set_global。
        init.lifecycle_sink = [](const XtHftEngine::PositionLifecycleEvent& ev) {
            auto* rec = XtLiveRecorder::global();
            if (!rec) return;
            XtLiveRecorder::PositionLifecycleParams p;
            p.ts_local_ns      = ev.ts_local_ns;
            p.ts_exch_ns       = ev.ts_exch_ns;
            p.strat_id         = ev.strategy_id.c_str();
            p.book_id          = ev.strategy_id.c_str();   // 单 book 策略下 book = strategy_id
            p.account_id       = nullptr;
            p.code             = ev.code.c_str();
            p.event_type       = ev.event_type;
            p.qty_fp           = ev.qty_fp;
            p.avg_price_fp     = ev.avg_price_fp;
            // lifecycle realized 取 engine read-delta；权威 per-fill 口径仍以
            // TRADE_RTN.realized_pnl / fill_realized callback 为准。
            p.realized_pnl_fp  = ev.realized_pnl_fp;
            p.hold_duration_ms = ev.hold_duration_ms;
            p.cause_trade_id   = ev.cause_trade_id;
            rec->emit_position_lifecycle(p);
        };

        const std::string venue_label = cfg.parsed.md_provider;  // "binance_u" / "ctp" / "xtp"
        const std::string account_label = cfg.runtime.control_cfg.account_label.empty()
            ? "_default"
            : cfg.runtime.control_cfg.account_label;
        init.metric_sink =
            [venue_label, account_label](const std::string& sid, const char* code, int64_t qty_fp) {
                // 单 book 策略约束下，book label 与 sid 保持一致。
                auto& reg = ct::metrics::XtMetricRegistry::instance();
                reg.set_position_qty_fp(sid, sid, venue_label, code ? code : "", qty_fp);
                reg.set_strategy_position_qty_fp(
                    venue_label, account_label, sid, sid, code ? code : "", qty_fp);
            };
        hooks.lifecycle = std::move(init);
    }

    // per-fill realized 覆盖 partial close 和 vol_scale，避免 lifecycle close/sign-flip
    // 事件口径重复累加。
    {
        ct::metrics::XtStrategyPnlAggregator* aggr_raw = aggr_slot.get();
        hooks.fill_realized =
            [aggr_raw](const std::string& sid, const std::string& code, int64_t realized_fp) {
                if (aggr_raw) aggr_raw->on_close(sid, code, realized_fp);
            };
    }

    // CTP set_cash 重锚 divergence telemetry：|raw−derived| 每 account_refresh_ms 被
    // set_cash 重锚归零，XtVenueEquityDivergence 的 for 窗口撑不过 → 重锚前采样才是
    // 真漂移信号。CTP-scoped：binance account_store 漂移由 reconcile loop +
    // state_divergence（per_symbol broker truth 自比）独立覆盖，不接此探针。
    if (cfg.parsed.td_provider == "ctp") {
        const std::string venue_label{
            xt_util::canonical_venue_label(cfg.parsed.md_provider)};
        const std::string account_label = cfg.runtime.control_cfg.account_label.empty()
            ? "_default"
            : cfg.runtime.control_cfg.account_label;
        hooks.cash_reanchor =
            [venue_label, account_label](int64_t divergence_fp) {
                ct::metrics::XtMetricRegistry::instance().observe_cash_reanchor(
                    venue_label, account_label, xt_fp::from_fp(divergence_fp));
            };
    }
    return true;
}

void save_hft_telemetry(const ct::metrics::XtStrategyPnlAggregator* aggr,
                        const XtLiveConfig& cfg) {
    // 与启动期 load_state 配对；在 logger shutdown 前保存累计 realized/fees，
    // 让 today_pnl_v2 跨重启连续。
    if (!aggr) return;
    const std::string aggr_path = aggregator_state_path(cfg);
    const bool saved = aggr->save_state(aggr_path);
    XtLogger::info_m("control", "PNL aggregator state save ok={} path={}",
                      saved, aggr_path);
}

}  // namespace xt_live::hft
