/// @file
/// @brief SEL backtest runner 的装配、调度与报告写出实现。
///
/// 本文件把 trading calendar、close price、strategy factory、CTA backtest gateway
/// 和 SEL engine 装配到一次性回测流程中；策略对象由 factory owning wrapper 管理。
///
/// 产物：`trades.csv` / `daily_equity.csv` / `summary.json` / `consumed_manifest.json`
/// + `tradability_events.csv`（配 tradability_csv 时）+ `schedule_fires.csv` /
/// `target_weights.csv` / `daily_positions.csv` / `fill_costs.csv`
/// + `corporate_action_events.csv`（配 corporate_actions_csv 时）
/// + `events_full.csv`（`backtest.trace_mode: full` 时，见 XtSelTraceSink）。
///
/// @note 权益口径唯一：`portfolio_equity = account equity + 现金应收 + 在途红股市值`
///       （见 `portfolio_equity_fp`）。daily_equity / trades.csv `equity_after` /
///       summary `final_equity` / engine sizing 分母 / daily_positions 五处同源；
///       买入可用资金另算（只看 account 可用现金，应收不可动用）。
///
/// @note 执行真实度层（`execution:` 节 + close_prices 的可选 `volume` 列）：整数投影
///       口径（floor / largest_remainder）+ 单 code 单日参与率上限 + 未成量跨日
///       carry；两者都严格 gated——volume 列缺席且未写 `execution:` 时整段惰性、
///       summary 无 `execution` 块、产物与该层落地前逐位一致。
///
/// @note `trades.csv` 的 `time` 列是占位值 `fire_time × 100000`（HHMM → HHMMSSmmm），
///       不是真实成交时刻：SEL 的成交口径是「T 日收盘价一次性成交」，日内没有真时刻
///       可报。需要事件级时序的消费方看 `date`（交易日）即可，不要把 `time` 当延迟或
///       排序依据。

#include "XtSelBacktestRunner.h"
#include "XtSelCorporateAction.h"
#include "XtSelDataLoader.h"
#include "XtSelTraceSink.h"
#include "XtSelTradability.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "XtCtaBacktestInfra.h"  // build_backtest_infra + BacktestInfra
#include "../common/XtBacktestReport.h"           // shared BarTradeRow / write_trades_csv
#include "api/XtRejectCode.h"                     // SubmitResult 拒因落列
#include "api/IRejectSink.h"                      // 真风控拒因捕获（reject_code 列）
#include "api/XtRiskTypes.h"                      // XtRiskResult / XtRiskReasonCode
#include "xt_cost/XtFillCostModel.h"              // breakdown_fee（费用拆分旁车）
#include "xt_account/api/AccountMarking.h"        // position_market_value（E3 会计恒等）
#include "xt_backtest/core/BtRunnerUtil.h"        // ConsumedManifest + write_consumed_manifest
#include "../common/XtBacktestTradeCapture.h"   // 真值点成交采集器
#include "api/IHftEngineObserver.h"               // SelFrozenReleaseObserver base
#include "xt_account/state/AccountState.h"        // XtAccountEvent
#include "xt_account/state/AccountStateStore.h"   // SelFrozenReleaseObserver account ref
#include "xt_core/order/XtOrderLifecycle.h"           // order_status_is_final
#include "xt_engine/sel/XtSelEngine.h"
#include "xt_engine/sel/SelTaskScheduler.h"
#include "xt_strategy_engine/XtStrategyContext.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"
#include "xt_strategy_engine/loader/XtStrategyFactory.h"
#include "xt_config/yaml/YAMLPathUtil.h"
#include "xt_logger/XtLogger.h"
#include "xt_core/XtFixedPoint.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_account/state/AccountEvent.h"
#include "xt_core/order/XtOrderTypes.h"   // XtTimeInForce, ExecFlags
#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtTradeRecord.h"

namespace xt_bt_sel {

namespace {

/// @brief 解析 ENV XT_DATALAKE_ROOT；空时返回 yaml 配置值。
std::string resolve_datalake_root(const std::string& yaml_root) {
    if (const char* env = std::getenv("XT_DATALAKE_ROOT")) {
        if (env[0] != '\0') return env;
    }
    return yaml_root;
}

/// @brief close_prices.csv 的读入结果（`volume` 是 canonical 可选列族）。
struct ClosePriceTable {
    std::unordered_map<uint32_t, std::unordered_map<std::string, double>> closes;
    /// (date → code → 当日成交量，**股**，fp 口径)；`has_volume==false` 时恒空。
    std::unordered_map<uint32_t, std::unordered_map<std::string, int64_t>> volumes_fp;
    /// 表头带 `volume` 列。判据是表头而非「有没有解析出值」——一份带列但整列为 0
    /// 的数据集是合法形态（停牌/无成交），不能因为没有非零值就当成无量数据。
    bool has_volume = false;
    /// 非空 = 遇到不可解释的 volume 单元格；调用方按 rc=1 fail-fast。
    /// 静默丢弃这类行会让「参与率上限没生效」看起来像引擎没实现，而不是数据坏了。
    std::string error;
};

/// @brief 严格解析成交量单元格：纯十进制数字、全串消费、非负、不超 canonical 上界。
///
/// 宽松解析（`stoll`）会把 `1e6` 读成 1、`123.5` 读成 123、`-5` 读成 -5，都是把
/// 「数据坏了」静默变成「流动性极低」——恰好与参与率上限的作用方向一致，肉眼不可辨。
/// @param s   已去掉首尾空白的单元格
/// @param out 成功时写入股数
/// @return false = 非法（调用方 fail-fast）
[[nodiscard]] bool parse_volume_shares(std::string_view s, int64_t& out) {
    /// volume 的 canonical 上界（股，见 ASTOCK_DATA_FORMAT §4）；×1e6 定点 = 1e18
    /// 仍在 int64 内。
    constexpr int64_t kMaxVolumeShares = 1'000'000'000'000LL;
    if (s.empty()) return false;
    for (const char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    }
    int64_t v = 0;
    const auto res = std::from_chars(s.data(), s.data() + s.size(), v);
    if (res.ec != std::errc() || res.ptr != s.data() + s.size()) return false;
    if (v < 0 || v > kMaxVolumeShares) return false;
    out = v;
    return true;
}

/// @brief 去掉单元格首尾空白（CSV 里 `close, 100` 这种写法很常见）。
[[nodiscard]] std::string_view trim_cell(std::string_view s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))  s.remove_suffix(1);
    return s;
}

/// @brief close_prices.csv schema: `date,code,close[,volume]`。读到 by-date map。
///
/// @note volume 列的判据是**第四个字段精确等于 `volume`**：前缀匹配会把
///       `volume_usd`（另一口径的量，单位是钱不是股）当成股数吃进参与率分母。
[[nodiscard]] ClosePriceTable read_close_prices_csv(const std::string& path) {
    ClosePriceTable out;
    if (path.empty()) return out;
    std::ifstream ifs(path);
    if (!ifs) return out;

    std::string line;
    bool header_seen = false;
    size_t line_no = 0;
    while (std::getline(ifs, line)) {
        ++line_no;
        // trim CR/LF
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) line.pop_back();
        if (line.empty()) continue;

        const auto comma1 = line.find(',');
        if (comma1 == std::string::npos) continue;
        const auto comma2 = line.find(',', comma1 + 1);
        if (comma2 == std::string::npos) continue;
        const auto comma3 = line.find(',', comma2 + 1);
        const auto comma4 = comma3 == std::string::npos
            ? std::string::npos : line.find(',', comma3 + 1);

        const std::string date_str = line.substr(0, comma1);
        const std::string code     = line.substr(comma1 + 1, comma2 - comma1 - 1);
        const std::string close_s  = comma3 == std::string::npos
            ? line.substr(comma2 + 1)
            : line.substr(comma2 + 1, comma3 - comma2 - 1);
        // 第四个字段（不含其后的任何扩展列）。
        const std::string_view vol_cell = comma3 == std::string::npos
            ? std::string_view{}
            : trim_cell(comma4 == std::string::npos
                            ? std::string_view(line).substr(comma3 + 1)
                            : std::string_view(line).substr(comma3 + 1,
                                                            comma4 - comma3 - 1));

        if (!header_seen) {
            header_seen = true;
            // 检测 header（首列非数字）；带 volume 列的形态在此定形。
            if (!std::isdigit(static_cast<unsigned char>(date_str[0]))) {
                out.has_volume = vol_cell == "volume";
                continue;
            }
        }
        try {
            const uint32_t date = static_cast<uint32_t>(std::stoul(date_str));
            const double close  = std::stod(close_s);
            if (date == 0 || !(close > 0.0) || code.empty()) continue;
            out.closes[date][code] = close;
            if (!out.has_volume) continue;
            // 表头声明了 volume 列，此行却没有该列 → schema 不自洽，不当缺量放行。
            int64_t vol = 0;
            if (comma3 == std::string::npos || !parse_volume_shares(vol_cell, vol)) {
                out.error = "close_prices.csv line " + std::to_string(line_no) +
                            ": bad volume cell '" + std::string(vol_cell) +
                            "' (需非负整数股数，上界 1e12)";
                return out;
            }
            out.volumes_fp[date][code] = vol * kPriceFpScale;
        } catch (...) { continue; }
    }
    return out;
}

// write_trades_csv 走共享 XtBacktestReport.h (BarTradeRow + write_trades_csv
// with include_strategy_id=true 给 SEL 路径)。
//
/// @brief SEL 回测的 frozen cash 释放 + order book 清账 observer。
///
/// SEL match engine 的成交路径不发布 order_update event；本 observer 在
/// `on_trade` / `on_order_update` 中补齐 account frozen 释放，并把终态回灌
/// XtOrderRuntimeBook 清 slot——guard 的在途平仓量扣减读 book，残留 slot
/// 会永久占用可平额度（后续 SELL 被误拒）。trade capture 仍保持
/// read-only，只负责写 CSV row。
class SelFrozenReleaseObserver final : public xt_runtime::IHftEngineObserver {
public:
    SelFrozenReleaseObserver(xt_account::AccountStateStore* account,
                             xt_runtime::order::XtOrderRuntimeBook* order_book) noexcept
        : account_(account), order_book_(order_book) {}

    void on_trade(const XtTradeRecord& tr,
                  XtLatencyEligibility /*elig*/ = XtLatencyEligibility::kEligible,
                  uint64_t /*logical_order_id*/ = 0) override {
        // SEL match_engine 的成交路径不发布 order_update；这里按整笔 fill
        // 以终态释放 frozen。CTA 路径走 order_update，不挂本 observer。
        // BT observer 不喂 latency tracker；忽略 elig/logical。
        if (tr.order_id == 0) return;
        if (account_) {
            account_->apply(XtAccountEvent::order_update(tr.order_id, tr.qty_fp,
                                                         /*is_final=*/true));
        }
        if (order_book_) {
            // 合成 Filled 终态清 book slot（整笔 fill 语义与上方 frozen 释放同源）。
            XtOrderExecUpdate upd{};
            upd.id        = tr.order_id;
            upd.status    = XtOrderStatus::Filled;
            upd.filled_fp = tr.qty_fp;
            upd.leaves_fp = 0;
            order_book_->on_order_update(upd, /*final_status=*/true,
                                         /*effective_remaining_fp=*/0);
        }
    }

    void on_order_update(const XtOrderExecUpdate& upd,
                         XtLatencyEligibility /*elig*/ = XtLatencyEligibility::kEligible) override {
        // cancel 路径释放 frozen (Filled 已在 on_trade 释放,不重复)。
        if (order_status_is_final(upd.status) && upd.status != XtOrderStatus::Filled) {
            if (account_) {
                account_->apply(XtAccountEvent::order_update(upd.id, upd.filled_fp,
                                                             /*is_final=*/true));
            }
            if (order_book_) {
                order_book_->on_order_update(
                    upd, /*final_status=*/true,
                    order_lifecycle::effective_remaining_fp(upd));
            }
        }
    }

private:
    xt_account::AccountStateStore* account_;
    xt_runtime::order::XtOrderRuntimeBook* order_book_;
};

// ── B0 产物：流式 CSV writer + 调度观测 + 决策行缓冲 ─────────────────────────

/// @brief 流式 CSV 产物：run 起始开 .tmp、逐事件 append、收尾 fsync+rename。
///
/// 走 `PathUtil::AtomicOfstream`，所以「文件存在」蕴含「内容完整」；run 中途
/// fail-fast（rc≠0，不进 write_reports）时析构自动删 tmp，不留半截产物。
class StreamCsvWriter {
public:
    /// @return false = 打不开（调用方按 rc=9 处理）。
    [[nodiscard]] bool open(const std::string& dir, const char* name,
                            const std::string& header) {
        path_ = (std::filesystem::path(dir) / name).string();
        name_ = name;
        af_ = std::make_unique<PathUtil::AtomicOfstream>(path_);
        if (!af_->is_open()) {
            XtLogger::error_m("SEL_BT", "{} open failed: {}", name_, path_);
            af_.reset();
            return false;
        }
        af_->stream() << header;
        return true;
    }

    [[nodiscard]] bool is_open() const noexcept { return af_ != nullptr; }
    std::ostream& out() { return af_->stream(); }
    void add_row() noexcept { ++rows_; }

    /// @return false = 未完整落盘（调用方按 rc=9 处理）。
    [[nodiscard]] bool commit() {
        if (!af_) return false;
        const bool ok = finish_report_stream(*af_, path_, "SEL_BT", name_, rows_);
        af_.reset();
        return ok;
    }

private:
    std::unique_ptr<PathUtil::AtomicOfstream> af_;
    std::string path_;
    const char* name_ = "";
    size_t      rows_ = 0;
};

/// @brief 记录 task 实际 fire 的 scheduler 装饰器（schedule_fires.csv 的 `fired` 列源）。
///
/// engine 只在 fire 时回调策略，runner 无从判断「没触发」与「触发了但没设权重」；
/// 本装饰器在 fire 回调外层打点，纯观测、转发语义与内层 scheduler 逐位一致。
class RecordingSelScheduler final : public xt_engine_sel::ISelTaskScheduler {
public:
    explicit RecordingSelScheduler(std::unique_ptr<xt_engine_sel::ISelTaskScheduler> inner)
        : inner_(std::move(inner)) {}

    void on_minute_end(uint32_t curDate, uint32_t curTime, FireCallback cb) override {
        if (!inner_) return;
        // cb 按值捕获：内层 scheduler 可能把 lambda 存起来延后调（本装饰器不能依赖
        // 调用方栈帧还活着）。FireCallback 是 std::function，拷贝成本在调度粒度上可忽略。
        inner_->on_minute_end(curDate, curTime,
            [this, cb](const xt_engine_sel::SelTaskInfo& task, uint32_t cd,
                       uint32_t ct, int64_t ft_ms) {
                fired_.insert(key_(task.id, cd));
                if (cb) cb(task, cd, ct, ft_ms);
            });
    }

    uint32_t add_task(xt_engine_sel::SelTaskInfo info) override {
        return inner_ ? inner_->add_task(std::move(info)) : 0;
    }

    [[nodiscard]] std::vector<xt_engine_sel::SelTaskInfo> list_tasks() const override {
        return inner_ ? inner_->list_tasks() : std::vector<xt_engine_sel::SelTaskInfo>{};
    }

    [[nodiscard]] bool fired(uint32_t task_id, uint32_t date) const {
        return fired_.count(key_(task_id, date)) != 0;
    }

private:
    [[nodiscard]] static uint64_t key_(uint32_t task_id, uint32_t date) noexcept {
        return (static_cast<uint64_t>(task_id) << 32) | date;
    }

    std::unique_ptr<xt_engine_sel::ISelTaskScheduler> inner_;
    std::unordered_set<uint64_t> fired_;
};

/// @brief 单次 fire 的汇总（schedule_fires.csv 一行的聚合量）。
struct SelFireAgg {
    uint64_t    fire_seq = 0;
    std::string strategy_id;
    uint32_t    task_id = 0;     ///< 触发该 fire 的 scheduler task；0 = 非 schedule 路径
    int64_t     equity_fp = 0;
    bool        equity_nonpositive = false;
    size_t      n_targets = 0;
    size_t      n_buys    = 0;   ///< 规划期 diff>0 的腿数（gate 之前）
    size_t      n_sells   = 0;   ///< 规划期 diff<0 的腿数（gate 之前）
    int64_t     turnover_fp = 0; ///< Σ |diff_qty| × price（规划期口径）
    double      gross_weight = 0.0;
};

/// @brief target_weights.csv 一行：规划期字段由 engine sink 填，执行列由 gate/
///        submit/drain 三处回填。
struct SelDecisionRow {
    uint64_t    fire_seq = 0;
    std::string code;
    double      weight = 0.0;
    int64_t     price_fp = 0;
    int64_t     target_qty_raw_fp = 0;
    int64_t     target_qty_fp = 0;
    int64_t     pre_qty_fp = 0;
    int64_t     diff_qty_fp = 0;
    xt_engine_sel::SelPlanningStatus planning =
        xt_engine_sel::SelPlanningStatus::kOk;
    // ── 执行列（entered_execution=false 时全部留空）────────────────────────
    bool        entered_execution = false;  ///< 进了 handle_pos_change_fn
    const char* gate = "";                  ///< tradability 判定
    bool        is_buy = false;
    bool        submitted = false;          ///< 真调了 gateway.submit
    uint64_t    order_id = 0;
    int64_t     submitted_qty_fp = 0;
    bool        accepted = false;
    xt_runtime::XtRejectCode reject_code = xt_runtime::XtRejectCode::NoError;
    int64_t     filled_qty_fp = 0;
    int64_t     post_qty_fp = 0;
};

/// @brief `XtRejectCode` 的稳定字符串名（产物列值；新增枚举须同步补分支）。
[[nodiscard]] const char* reject_code_name(xt_runtime::XtRejectCode c) {
    using RC = xt_runtime::XtRejectCode;
    switch (c) {
        case RC::NoError:                      return "NoError";
        case RC::Unknown:                      return "Unknown";
        case RC::InvalidInput:                 return "InvalidInput";
        case RC::UnknownStrategySid:           return "UnknownStrategySid";
        case RC::InvalidFlag:                  return "InvalidFlag";
        case RC::PriceTickMismatch:            return "PriceTickMismatch";
        case RC::TradingNotReady:              return "TradingNotReady";
        case RC::Cooldown:                     return "Cooldown";
        case RC::RateLimited:                  return "RateLimited";
        case RC::CapabilityUnsupported:        return "CapabilityUnsupported";
        case RC::CloseoutNoOpen:               return "CloseoutNoOpen";
        case RC::MaxOrderQty:                  return "MaxOrderQty";
        case RC::MaxOrderNotional:             return "MaxOrderNotional";
        case RC::MinOrderNotional:             return "MinOrderNotional";
        case RC::PositionLimit:                return "PositionLimit";
        case RC::ShortNotAllowed:              return "ShortNotAllowed";
        case RC::InsufficientMargin:           return "InsufficientMargin";
        case RC::InsufficientCloseToday:       return "InsufficientCloseToday";
        case RC::InsufficientCloseYesterday:   return "InsufficientCloseYesterday";
        case RC::InCallAuctionLimitNotAllowed: return "InCallAuctionLimitNotAllowed";
        case RC::PositionSyncStale:            return "PositionSyncStale";
        case RC::CloseSyncPending:             return "CloseSyncPending";
        case RC::KillSwitchActive:             return "KillSwitchActive";
        case RC::HardCircuitBreakerTripped:    return "HardCircuitBreakerTripped";
        case RC::SelfTradeDetected:            return "SelfTradeDetected";
        case RC::VelocityLimitExceeded:        return "VelocityLimitExceeded";
        case RC::DailyLossExceeded:            return "DailyLossExceeded";
        case RC::Gated:                        return "Gated";
        case RC::OpenHalted:                   return "OpenHalted";
        case RC::OppositeOpenPending:          return "OppositeOpenPending";
        case RC::SymbolBlacklisted:            return "SymbolBlacklisted";
        case RC::OrderCountExceeded:           return "OrderCountExceeded";
        case RC::OrderRateExceeded:            return "OrderRateExceeded";
        case RC::SelfMatchPostTrade:           return "SelfMatchPostTrade";
        case RC::AnnualLossExceeded:           return "AnnualLossExceeded";
        case RC::StrategyMaxPosQtyExceeded:    return "StrategyMaxPosQtyExceeded";
        case RC::StrategyDailyLossExceeded:    return "StrategyDailyLossExceeded";
        case RC::StrategyKillSwitchActive:     return "StrategyKillSwitchActive";
        case RC::StrategyBlockedByWatchdog:    return "StrategyBlockedByWatchdog";
        case RC::PriceExceedsLimit:            return "PriceExceedsLimit";
        case RC::MarginCallLiquidation:        return "MarginCallLiquidation";
        case RC::AutoOffsetUnresolved:         return "AutoOffsetUnresolved";
        case RC::BacktestEofClosing:           return "BacktestEofClosing";
    }
    return "Unknown";
}

/// @brief 风控 reason code → 通用 reject code（target_weights.csv 的 reject_code 列）。
///
/// CTA/SEL 回测 gateway 的 `SubmitResult.reject_reason` 在整单被拒时恒为
/// `Unknown`（提交入口只知道「没有切片被受理」，不透出风控 reason）；真拒因只经
/// `IRejectSink` 出现一次。本映射与 live 侧 `live_reject_reason::from_risk` 同形，
/// 让产物的 reject_code 列保持单一枚举命名空间（XtRejectCode 名）。
/// 无对应通用码的 reason（session/td-capability/cancel 类）落 Unknown —— 这些在
/// SEL 回测的提交路径上结构性不可达。
[[nodiscard]] xt_runtime::XtRejectCode reject_code_from_risk_reason(XtRiskReasonCode r) {
    using RC = xt_runtime::XtRejectCode;
    switch (r) {
        case XtRiskReasonCode::EmptyCode:
        case XtRiskReasonCode::InvalidPriceOrQty:          return RC::InvalidInput;
        case XtRiskReasonCode::UnknownStrategySid:         return RC::UnknownStrategySid;
        case XtRiskReasonCode::Cooldown:                   return RC::Cooldown;
        case XtRiskReasonCode::RateLimited:                return RC::RateLimited;
        case XtRiskReasonCode::PriceTickMismatch:          return RC::PriceTickMismatch;
        case XtRiskReasonCode::TradingNotReady:
        case XtRiskReasonCode::TdRecovering:
        case XtRiskReasonCode::TdNotInitialized:           return RC::TradingNotReady;
        case XtRiskReasonCode::CloseoutNoOpen:             return RC::CloseoutNoOpen;
        case XtRiskReasonCode::KillSwitchActive:           return RC::KillSwitchActive;
        case XtRiskReasonCode::SelfTradeDetected:          return RC::SelfTradeDetected;
        case XtRiskReasonCode::VelocityLimitExceeded:      return RC::VelocityLimitExceeded;
        case XtRiskReasonCode::HardCircuitBreakerTripped:  return RC::HardCircuitBreakerTripped;
        case XtRiskReasonCode::TdCapCloseTodayUnsupported:
        case XtRiskReasonCode::TdCapL2OrderFlowUnsupported:
        case XtRiskReasonCode::TdCapIocFokUnsupported:     return RC::CapabilityUnsupported;
        case XtRiskReasonCode::MaxOrderQty:                return RC::MaxOrderQty;
        case XtRiskReasonCode::MaxOrderNotional:           return RC::MaxOrderNotional;
        case XtRiskReasonCode::MinOrderNotional:           return RC::MinOrderNotional;
        case XtRiskReasonCode::MaxPosQty:
        case XtRiskReasonCode::InsufficientAvailablePosition:
        case XtRiskReasonCode::PositionStateUnknown:       return RC::PositionLimit;
        case XtRiskReasonCode::ShortNotAllowed:            return RC::ShortNotAllowed;
        case XtRiskReasonCode::InsufficientMargin:         return RC::InsufficientMargin;
        case XtRiskReasonCode::DailyLossExceeded:          return RC::DailyLossExceeded;
        case XtRiskReasonCode::AnnualLossExceeded:         return RC::AnnualLossExceeded;
        case XtRiskReasonCode::StrategyMaxPosQtyExceeded:  return RC::StrategyMaxPosQtyExceeded;
        case XtRiskReasonCode::StrategyDailyLossExceeded:  return RC::StrategyDailyLossExceeded;
        case XtRiskReasonCode::StrategyKillSwitchActive:   return RC::StrategyKillSwitchActive;
        case XtRiskReasonCode::StrategyBlockedByWatchdog:  return RC::StrategyBlockedByWatchdog;
        case XtRiskReasonCode::PositionSyncStale:          return RC::PositionSyncStale;
        case XtRiskReasonCode::CloseSyncPending:           return RC::CloseSyncPending;
        case XtRiskReasonCode::InsufficientCloseToday:     return RC::InsufficientCloseToday;
        case XtRiskReasonCode::InsufficientCloseYesterday: return RC::InsufficientCloseYesterday;
        case XtRiskReasonCode::InCallAuctionLimitNotAllowed:
            return RC::InCallAuctionLimitNotAllowed;
        case XtRiskReasonCode::PriceOutOfDailyLimit:       return RC::PriceExceedsLimit;
        case XtRiskReasonCode::SymbolBlacklisted:          return RC::SymbolBlacklisted;
        case XtRiskReasonCode::OrderCountExceeded:         return RC::OrderCountExceeded;
        case XtRiskReasonCode::OrderRateExceeded:          return RC::OrderRateExceeded;
        case XtRiskReasonCode::SelfMatchPostTrade:         return RC::SelfMatchPostTrade;
        case XtRiskReasonCode::None:
        case XtRiskReasonCode::SessionClosed:
        case XtRiskReasonCode::CancelRateLimited:
        case XtRiskReasonCode::StrategyExposureExceeded:
        case XtRiskReasonCode::StrategyMarginPctExceeded:
        case XtRiskReasonCode::SiblingForceFlattenInProgress:
        case XtRiskReasonCode::Count:                      break;
    }
    return RC::Unknown;
}

/// @brief 单次 submit 窗口内的真风控拒因捕获（BT-only）。
///
/// gateway 在 `evaluate_risk_` 内同步调用本 sink，故「submit 前 reset()、submit 后
/// 读」即可把拒因归到该笔提交上。同一 submit 多切片时保留第一个拒因（订单失败的
/// 首因）。
class SelRejectCapture final : public IRejectSink {
public:
    void on_reject(const xt_core::XtOrder& /*req*/, const XtRiskResult& decision,
                   std::string_view /*strategy_id*/) override {
        record_(decision);
    }

    void on_margin_reject(const xt_core::XtOrder& /*req*/, const XtRiskResult& decision,
                          int64_t /*now_ms*/, std::string_view /*strategy_id*/) override {
        record_(decision);
    }

    void reset() noexcept { has_ = false; }
    [[nodiscard]] bool has_reject() const noexcept { return has_; }
    [[nodiscard]] XtRiskReasonCode reason() const noexcept { return reason_; }

private:
    void record_(const XtRiskResult& decision) {
        if (has_) return;  // 首因优先
        has_    = true;
        reason_ = decision.reason_code;
    }

    XtRiskReasonCode reason_ = XtRiskReasonCode::None;
    bool             has_    = false;
};

/// @brief BUY 的可负担数量上限（含手续费），向下对齐到 min_lot。
///
/// SEL 的成交是「按 T 日 close 强制成交」，撮合侧不做资金校验；不设上限时
/// 名义额 + 费用可以超过现金，账本直接扣成负现金（无声加杠杆，等价于免费融资）。
/// 现实约束是券商在执行时拒/部分成交，故在提交口按可负担量截断。
///
/// 成本对数量单调不减（费率线性 + min_commission 抬升 + 印花税/过户费线性），
/// 故用二分求最大可负担 lot 数，避免逐 lot 递减的不定迭代。
/// @param cash_fp   可用现金（fp）
/// @param price_fp  成交价（fp，= T 日 close）
/// @param want_fp   期望数量（fp，已是整 lot 倍数）
/// @param min_lot   最小交易单位（<=0 视为 1）
/// @param fee_of    (qty_fp) → 手续费（fp）
/// @return 可负担数量（fp，整 lot 倍数，可能为 0）；>= want_fp 时原样返回 want_fp。
[[nodiscard]] int64_t affordable_qty_fp(
    int64_t cash_fp, int64_t price_fp, int64_t want_fp, int min_lot,
    const std::function<int64_t(int64_t)>& fee_of) {
    if (want_fp <= 0 || price_fp <= 0) return want_fp;
    const int64_t lot_fp = static_cast<int64_t>(min_lot > 0 ? min_lot : 1) * kPriceFpScale;
    const auto cost_of = [&](int64_t qty_fp) {
        return xt_fp::mul(price_fp, qty_fp) + fee_of(qty_fp);
    };
    if (cost_of(want_fp) <= cash_fp) return want_fp;
    if (cash_fp <= 0) return 0;

    // lot 数上界：名义额单独就撑不住的 lot 数一定不可负担。
    int64_t hi = want_fp / lot_fp;                       // want 是整 lot 倍数
    int64_t lo = 0;                                      // 0 lot 恒可负担
    while (lo < hi) {
        const int64_t mid = lo + (hi - lo + 1) / 2;      // 上取整，收敛到最大可行值
        if (cost_of(mid * lot_fp) <= cash_fp) lo = mid;
        else                                  hi = mid - 1;
    }
    return lo * lot_fp;
}

/// @brief 单条 tradability gate 事件(events CSV 行的内存形态)。
struct TradabilityEventRow {
    uint32_t            date = 0;      ///< YYYYMMDD
    std::string         code;
    bool                is_buy = true;
    TradabilityVerdict  verdict = TradabilityVerdict::Ok;
    int64_t             qty_fp = 0;    ///< |diff_pos_fp|
};

/// @brief verdict → events CSV reason 列(D5 契约: suspended/limit_up/limit_down)。
[[nodiscard]] const char* tradability_reason_str(TradabilityVerdict v) {
    switch (v) {
        case TradabilityVerdict::Suspended:     return "suspended";
        case TradabilityVerdict::LimitUpBuy:    return "limit_up";
        case TradabilityVerdict::LimitDownSell: return "limit_down";
        case TradabilityVerdict::Ok:            break;
    }
    return "ok";
}

/// @brief JSON string 最小转义(仅 `\` 与 `"`);tradability.source 路径字段用。
[[nodiscard]] std::string json_escape_min(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (const char c : s) {
        if (c == '\\' || c == '"') out.push_back('\\');
        out.push_back(c);
    }
    return out;
}

/// @brief summary.json 的追加观测量（B0 + 深挖 fold F03/F05/F09）。
struct SelObsCounters {
    size_t  affordability_caps        = 0;  ///< BUY 因现金不足被截断的腿数
    double  affordability_shortfall   = 0.0;///< 被截掉的名义额合计
    size_t  negative_cash_events      = 0;  ///< post-fill 现金为负次数（应恒 0）
    size_t  min_lot_zero_targets      = 0;  ///< 整 lot 截零的目标数
    uint64_t negative_weight_rejects  = 0;  ///< 负目标权重被拒数
    double  residual_cash             = 0.0;///< 收尾现金（未投出的部分）
    size_t  equity_identity_violations = 0; ///< 权益恒等（fp 精确）不成立的日数（应恒 0）
    size_t  sell_fee_unaffordable_rejects = 0;  ///< SELL 费用超「现金+所得」被拒的腿数
    /// gateway 未受理（order_id==0）的提交腿数：这些腿没有订单存在过（trace 侧走
    /// `sel_reject` 而非 `order_submit`）。与 target_weights 的 accepted=0 行同源。
    size_t  local_submit_rejects      = 0;
    // pending 语义观测（engine 侧计数；同槽多 target 复合的可见面）。
    uint64_t pending_dispatch_noops = 0;           ///< 重算差额归零、未派发
    uint64_t pending_dispatch_direction_flips = 0; ///< 重算后方向与分桶相反
    uint64_t cross_sid_same_code_pendings = 0;     ///< 同 code 跨 sid 同时挂 target
    uint64_t stale_pending_groups_dropped = 0;     ///< 日界丢弃的跨日残留组（应恒 0）
};

/// @brief summary.json 可选 corporate_actions 块（仅配置 corporate_actions_csv 时写出）。
struct CorporateActionSummary {
    std::string source;                          ///< CA CSV 路径（config 原串）
    size_t applied                    = 0;       ///< 真施加的事件数（跳过/忽略不计）
    double cash_dividends_total       = 0.0;     ///< 现金分红净额合计（税后，含未到账）
    size_t delist_liquidations        = 0;
    size_t rights_skipped             = 0;
    size_t out_of_window_ignored      = 0;       ///< 窗外事件整条忽略（禁止归日 → 防 future leak）
    size_t rebased_to_trading_date    = 0;       ///< 窗内非交易日事件归到前一交易日
    /// 确权基数退化为「ex 日初持仓」的事件数 = 下面三项之和（拆因披露，见各项）。
    size_t record_qty_fallbacks       = 0;
    /// CSV 未给 record_date：ex 日初仓 ≡ 前一交易日收盘仓（A 股 record = ex−1 惯例），
    /// 口径等价、无风险。
    size_t record_qty_fallback_absent = 0;
    /// `record_date == ex_date`：确权本该按 ex 日**收盘**仓，退化用 ex 日初仓 →
    /// 误差 = 该 code 当日的调仓量（方向不定）。
    size_t record_qty_fallback_ex_day = 0;
    /// record_date 早于窗首交易日：窗首后建的仓会拿到不该拿的分红（真过度计提）。
    size_t record_qty_fallback_before_window = 0;
    size_t delist_fee_capped          = 0;       ///< 清算费用超清算所得被压到所得（不产生负现金）
    size_t delist_pending_bonus_liquidated = 0;  ///< 退市时按清算 ref 一并了结的在途红股笔数
    size_t delist_buy_rejects         = 0;       ///< 退市后仍下 BUY 被闩锁拒的腿数
    size_t delisted_price_reappearances = 0;     ///< 退市后该 code 又出现 close 行的日数
    double receivable_outstanding_cash = 0.0;    ///< 收尾未到账现金分红
    double receivable_outstanding_bonus = 0.0;   ///< 收尾在途红股市值
    /// 本跑生效的退市清算折价（`risk.delist_haircut_pct`）。清算所得直接由它定，
    /// 而配置值在产物里此前没有可见面 —— 换了折价重跑，结果差异无从归因。
    double delist_haircut_pct         = 0.0;
    /// 现金分红当日直接入现金（无应收窗口）的笔数：pay_date 缺省或不晚于 ex 日。
    /// 与 receivable_created 互补，两者之和 = 有股权的现金分红笔数；非零表示
    /// 这些笔的 ex→pay 资金占用被当零处理（真实 A 股 pay 日滞后 1-2 周）。
    size_t pay_date_absent_immediate_cash = 0;
    /// 新股（送转 / 配股）当日直接入仓（无在途窗口）的笔数：CSV 显式给了 ex 日当天，
    /// 或窗末已无下一交易日。非零表示这些笔的 ex→list 不可卖窗口被当零处理。
    size_t shares_list_date_absent_immediate = 0;
    /// CSV 未给 shares_list_date、到账日取缺省（ex 日下一交易日）的笔数：披露这些笔
    /// 的不可卖窗口是模型给的近似（真实上市日可能更晚），不是数据里的事实。
    size_t shares_list_date_defaulted = 0;
    /// 到账日落在回测窗之外的笔数：这些新股整跑不到可卖仓，只在应收里估值
    /// （与 `receivable_outstanding_bonus` 对应）。
    size_t shares_list_date_beyond_window = 0;
    /// CSV 说「ex 日即到账」但 ex 被归日前移，到账实际变滞后的笔数：滞后是归日的
    /// 副作用而非数据事实，与 `rebased_to_trading_date` 配套解读。
    size_t shares_list_date_lagged_by_rebase = 0;
    // ── 配股（rights）──────────────────────────────────────────────────────
    /// 本跑生效的配股口径（`selector.rights_policy`）；与下面三项计数配套解读。
    std::string rights_policy;
    size_t rights_subscribed  = 0;       ///< 缴款认购成功的笔数（含被现金截断的）
    size_t rights_forfeited   = 0;       ///< 弃权笔数（policy=forfeit）
    /// 现金不足、按可缴整股截断的笔数：非零 = 这些笔的实际获配少于应配，缺口已在
    /// 参考价上体现（不另记损失），但仓位与真实历史分叉。
    size_t rights_cash_capped = 0;
    double rights_cash_paid   = 0.0;     ///< 配股缴款总额
    /// 同交易日第 2 笔起的配股缴款腿数（先到先占现金）：非零 = 截断结果依赖施加序。
    size_t rights_same_day_cash_contention = 0;
    // ── 换股（security conversion）─────────────────────────────────────────
    size_t conversions            = 0;   ///< 真施加的换股笔数
    size_t conversion_buy_rejects = 0;   ///< 换股后旧 code 仍下 BUY 被闩锁拒的腿数
    double conversion_cash_in_lieu = 0.0;///< 阀下零股现金替代所得合计
    /// 换股闩锁后仍落在同一 code 上、被整条拒绝的后续 CA 事件数（防死 code 复活）。
    size_t post_conversion_events_rejected = 0;
    /// SELL 腿被截到可交割量的腿数（差额 = 尚未到账的在途新股，下期顺延）。
    size_t pending_shares_sell_deferrals = 0;
};

// ── B7e 执行真实度：参与率上限 + 跨日 carry ────────────────────────────────

/// ADV 窗口长度（交易日）：summary 的 `adv20_*` 口径。
constexpr size_t kAdvWindow = 20;

/// @brief 被当日执行约束截下、等待跨日复投的一腿调仓意图。
///
/// carry 只由**当日执行侧的量约束**产生：参与率上限，以及参与率缩量后 SELL 侧
/// 「所得不够付费用」的整腿顺延。资金可负担截断与在途新股不可交割截断另有既定语义
/// （缺口由下期 rebalance 以 target−actual 顺延），再挂一份 carry 会让同一份缺口
/// 被追两遍。
///
/// @note 存的是**绝对目标**而非未成增量（与 engine `PendingDiff::target_qty_fp`
///       同形，遵循 target weights 的幂等替换契约）。复投前用
///       `target − position_resolver(code)` 重算差额：存增量的写法在持仓被 CA
///       改动（十送十把仓翻倍）或被别的通道成交后会盲投，把仓位推过 target。
struct SelCarryLeg {
    std::string strategy_id;
    std::string code;
    std::string tag;
    bool        is_buy = true;
    int64_t     target_qty_fp = 0; ///< 绝对目标持仓（fp）；复投差额由它与实时仓重算
    int64_t     remaining_fp = 0;  ///< 观测口径：尚未达成的股数（fp）
    uint64_t    fire_seq = 0;      ///< 产生该意图的那次 fire（trace/归因用）
    uint32_t    created_date = 0;  ///< 生成日：当日不复投（当日份额已用尽）
    uint32_t    days = 0;          ///< 已跨的交易日数；>= carry_max_days 即不再复投
    /// 已按 noop / 到限丢弃结账：日末只负责把它清出，不再计 `carry_completed`
    /// （否则同一条腿会同时出现在两个互斥的终局计数里）。
    bool        retired = false;
};

/// @brief summary.json 可选 execution 块（volume 列在场 或 配了 `execution:` 时写出）。
struct ExecutionSummary {
    std::string integer_projection;      ///< floor | largest_remainder（口径回显）
    bool   volume_available   = false;   ///< close_prices.csv 带 volume 列
    double max_participation  = 0.0;
    uint32_t carry_max_days   = 0;
    size_t capped_fills       = 0;
    double capped_shortfall_qty = 0.0;
    /// 提交腿命中「当日该 code volume == 0」的 (交易日 × code) 数：零流动性，
    /// 当日一股不成、整腿转 carry。与下面的 missing 分列——importer 若把缺失写成 0，
    /// 两个数会此消彼长，是数据口径冲突的可见面。
    size_t volume_zero_days   = 0;
    /// 提交腿命中「当日该 code 无 volume 行」的 (交易日 × code) 数：无流动性证据
    /// → fail-open 不 cap（与 tradability 缺行同规则）。
    size_t volume_missing_days = 0;
    size_t carry_created      = 0;
    size_t carry_resubmits    = 0;
    size_t carry_completed    = 0;
    size_t carry_superseded   = 0;
    /// 复投前重算差额已归零或反向、按 noop 摘除的腿数（持仓被 CA / 别的通道改动）。
    size_t carry_noop_removed = 0;
    /// 新 fire 的决策集里没有该 code、按「整体重估」显式丢弃的**已建**腿数。
    size_t carry_dropped_stale = 0;
    double carry_dropped_stale_qty = 0.0;
    /// 建腿点即被同日更晚 fire 过滤、**未建腿**（不计 created）的未成量笔数。
    size_t carry_stale_before_create = 0;
    double carry_stale_before_create_qty = 0.0;
    size_t carry_dropped      = 0;
    double carry_dropped_qty  = 0.0;
    size_t carry_outstanding  = 0;       ///< 收尾仍未成的 carry 腿数
    double carry_outstanding_qty = 0.0;
    /// carry 复投的 SELL 腿被截到账上可交割量的腿数（与 CA 在途新股通道分列：
    /// 成因是持仓在两次提交之间变了，不是新股没到账）。
    size_t carry_sell_clamped = 0;
    /// 参与率缩量后 SELL「所得 < 费用」、整腿顺延（转 carry）的笔数。
    size_t sell_fee_deferrals = 0;
    size_t adv20_samples      = 0;
    double adv20_ratio_max    = 0.0;
    double adv20_ratio_mean   = 0.0;
    size_t legs_over_participation = 0;
    size_t projection_fires   = 0;
    size_t projection_lots_added = 0;
    size_t projection_lots_removed = 0;
    size_t projection_infeasible_fires = 0;
    uint64_t projection_adjusted_targets = 0;
    uint64_t projection_rejected_entries = 0;
};

/// @brief summary.json 可选 tradability 块(仅配置 tradability_csv 时写出)。
struct TradabilitySummary {
    std::string source;                   ///< tradability CSV 路径
    size_t suspended_skips         = 0;
    size_t limit_up_buy_rejects    = 0;
    size_t limit_down_sell_rejects = 0;
};

/// @note tradability=nullptr 时不写 tradability 块（未配置时 schema 稳定）；
///       initial_cash/final_equity 走 %.2f 全精度（默认 operator<<(double) 只保 6 位
///       有效数字，亿级权益会丢到百元级）。
/// @return false = 未能完整落盘；调用方必须处理。
///
/// @note SEL 保留自己的 writer（字段集与 CTA 完全不同），但落盘契约必须与共享
///       writer 一致：AtomicOfstream + 检查返回值。注意本函数在匿名 namespace 内，
///       名字遮蔽了 XtBacktestReport.h 的同名全局重载——`[[nodiscard]]` 够不到这里，
///       所以契约要手动对齐，不能指望编译器逼出来。
[[nodiscard]] bool write_summary_json(const std::string& dir,
                          const std::string& source,
                          size_t total_dates,
                          size_t trade_rows,
                          double initial_cash,
                          double final_equity,
                          double realized_pnl,
                          int risk_rejects,
                          uint64_t sparse_weights_events,
                          size_t undrained_reports,
                          int64_t frozen_cash_at_exit_fp,
                          size_t stale_mark_carry_events,
                          size_t n_fires,
                          size_t n_skips,
                          const SelObsCounters& obs,
                          const TradabilitySummary* tradability,
                          const CorporateActionSummary* corporate_actions,
                          const ExecutionSummary* execution) {
    std::filesystem::create_directories(dir);
    const std::string path = (std::filesystem::path(dir) / "summary.json").string();
    PathUtil::AtomicOfstream af(path);
    if (!af.is_open()) {
        XtLogger::error_m("SEL_BT", "summary.json open failed: {}", path);
        return false;
    }
    auto& out = af.stream();
    char buf[64];
    auto fmt2 = [&buf](double v) -> const char* {
        std::snprintf(buf, sizeof(buf), "%.2f", v);
        return buf;
    };
    out << "{\n";
    out << "  \"engine\": \"sel\",\n";
    out << "  \"data_source\": \"" << source << "\",\n";
    out << "  \"total_trading_dates\": " << total_dates << ",\n";
    out << "  \"trade_rows\": " << trade_rows << ",\n";
    out << "  \"initial_cash\": " << fmt2(initial_cash) << ",\n";
    out << "  \"final_equity\": " << fmt2(final_equity) << ",\n";
    // 累计已实现盈亏：平仓成交按 (成交价 − 持仓均价) × 量 结账。除权除息会调整
    // 均价（送转 ÷(1+b)），归因正确性只能在这里观测——equity 是市值口径，均价在
    // 其中互相抵消，产物里没有别的均价可见面。
    out << "  \"realized_pnl\": " << fmt2(realized_pnl) << ",\n";
    out << "  \"risk_rejects\": " << risk_rejects << ",\n";
    out << "  \"sparse_weights_events\": " << sparse_weights_events << ",\n";
    // 回报消费完整性（0 = 全部 fill/order update 都已送达 observer）。
    out << "  \"undrained_reports\": " << undrained_reports << ",\n";
    // 收尾残留冻结（0 = 所有终态订单的冻结都已释放）。
    out << "  \"frozen_cash_at_exit\": " << fmt2(xt_fp::from_fp(frozen_cash_at_exit_fp)) << ",\n";
    // 持仓 symbol 当日无 close → carry 旧 mark 的次数（估值降级披露）。
    out << "  \"stale_mark_carry_events\": " << stale_mark_carry_events << ",\n";
    // 调度统计：n_fires = 无 skip_reason 的 (date × task) 行数（真触发、equity 正、
    // 且策略设了权重；权重全为「无变动」时仍计入 fire）；n_skips = 有 skip_reason
    // 的行数。两者之和 = schedule_fires.csv 行数。
    out << "  \"n_fires\": " << n_fires << ",\n";
    out << "  \"n_skips\": " << n_skips << ",\n";
    // 资金可负担截断（BUY 名义+费用超现金 → 截到可负担整 lot 量）。
    out << "  \"affordability_caps\": " << obs.affordability_caps << ",\n";
    out << "  \"affordability_shortfall\": " << fmt2(obs.affordability_shortfall) << ",\n";
    // 恒应为 0：非零 = 截断口径与撮合成本脱节，账本被透支。
    out << "  \"negative_cash_events\": " << obs.negative_cash_events << ",\n";
    // 整 lot 截零的目标数（组合被 min_lot 吃掉的暴露）+ 收尾未投出现金。
    out << "  \"min_lot_zero_targets\": " << obs.min_lot_zero_targets << ",\n";
    out << "  \"negative_weight_rejects\": " << obs.negative_weight_rejects << ",\n";
    // 恒应为 0：Σ持仓市值 + 现金 + 应收 ≠ portfolio equity（fp 精确口径）。
    out << "  \"equity_identity_violations\": " << obs.equity_identity_violations << ",\n";
    // SELL 侧费用超「现金 + 卖出所得」→ 该腿不提交（不 cap 真实费用，也不透支）。
    out << "  \"sell_fee_unaffordable_rejects\": "
        << obs.sell_fee_unaffordable_rejects << ",\n";
    // gateway 未受理的提交腿数（order_id==0 → 订单从未存在）。
    out << "  \"local_submit_rejects\": " << obs.local_submit_rejects << ",\n";
    // pending 语义可见面（同槽多 target 复合）：noop = 已被更早成交达成；
    // direction_flip = 重算后与分桶反向（SELL 桶翻 BUY 推迟到 BUY phase）；
    // cross_sid = 同 code 跨策略挂 target；stale_dropped 恒应为 0。
    out << "  \"pending_dispatch_noops\": " << obs.pending_dispatch_noops << ",\n";
    out << "  \"pending_dispatch_direction_flips\": "
        << obs.pending_dispatch_direction_flips << ",\n";
    out << "  \"cross_sid_same_code_pendings\": "
        << obs.cross_sid_same_code_pendings << ",\n";
    out << "  \"stale_pending_groups_dropped\": "
        << obs.stale_pending_groups_dropped << ",\n";
    out << "  \"residual_cash\": " << fmt2(obs.residual_cash);
    if (corporate_actions) {
        const auto& ca = *corporate_actions;
        out << ",\n";
        out << "  \"corporate_actions\": {\n";
        out << "    \"source\": \"" << json_escape_min(ca.source) << "\",\n";
        out << "    \"applied\": " << ca.applied << ",\n";
        out << "    \"cash_dividends_total\": " << fmt2(ca.cash_dividends_total) << ",\n";
        out << "    \"delist_liquidations\": " << ca.delist_liquidations << ",\n";
        out << "    \"rights_skipped\": " << ca.rights_skipped << ",\n";
        out << "    \"out_of_window_ignored\": " << ca.out_of_window_ignored << ",\n";
        out << "    \"rebased_to_trading_date\": " << ca.rebased_to_trading_date << ",\n";
        // 确权基数退化：总数 + 三个拆因（absent 无害 / ex_day 误差=当日调仓 /
        // before_window 真过度计提）。
        out << "    \"record_qty_fallbacks\": " << ca.record_qty_fallbacks << ",\n";
        out << "    \"record_qty_fallback_absent\": "
            << ca.record_qty_fallback_absent << ",\n";
        out << "    \"record_qty_fallback_ex_day\": "
            << ca.record_qty_fallback_ex_day << ",\n";
        out << "    \"record_qty_fallback_before_window\": "
            << ca.record_qty_fallback_before_window << ",\n";
        out << "    \"delist_fee_capped\": " << ca.delist_fee_capped << ",\n";
        out << "    \"delist_pending_bonus_liquidated\": "
            << ca.delist_pending_bonus_liquidated << ",\n";
        out << "    \"delist_buy_rejects\": " << ca.delist_buy_rejects << ",\n";
        out << "    \"delisted_price_reappearances\": "
            << ca.delisted_price_reappearances << ",\n";
        out << "    \"receivable_outstanding_cash\": "
            << fmt2(ca.receivable_outstanding_cash) << ",\n";
        out << "    \"receivable_outstanding_bonus\": "
            << fmt2(ca.receivable_outstanding_bonus) << ",\n";
        // 生效的清算折价（配置回显）+ 两个「滞后日期缺省 → 当日即时生效」计数：
        // 缺省是合法写法，但把 ex→pay 资金占用 / ex→list 不可卖窗口按零处理，
        // 结果偏乐观，需在产物里可见（而非只在 CSV 输入里）。
        out << "    \"delist_haircut_pct\": " << ca.delist_haircut_pct << ",\n";
        out << "    \"pay_date_absent_immediate_cash\": "
            << ca.pay_date_absent_immediate_cash << ",\n";
        out << "    \"shares_list_date_absent_immediate\": "
            << ca.shares_list_date_absent_immediate << ",\n";
        // 到账日缺省（ex+1 交易日）是模型近似而非数据事实，与上一项互补披露。
        out << "    \"shares_list_date_defaulted\": "
            << ca.shares_list_date_defaulted << ",\n";
        // 到账日落在窗外（整跑不到账，只在应收里估值）+ 归日把「ex 日即到账」变成
        // 滞后的笔数：两者都是模型边界，不看这两个数会把在途余额误读成漏账。
        out << "    \"shares_list_date_beyond_window\": "
            << ca.shares_list_date_beyond_window << ",\n";
        out << "    \"shares_list_date_lagged_by_rebase\": "
            << ca.shares_list_date_lagged_by_rebase << ",\n";
        // 配股：口径回显 + 认购/弃权/截断三态计数 + 缴款额。
        out << "    \"rights_policy\": \"" << json_escape_min(ca.rights_policy) << "\",\n";
        out << "    \"rights_subscribed\": " << ca.rights_subscribed << ",\n";
        out << "    \"rights_forfeited\": " << ca.rights_forfeited << ",\n";
        out << "    \"rights_cash_capped\": " << ca.rights_cash_capped << ",\n";
        out << "    \"rights_cash_paid\": " << fmt2(ca.rights_cash_paid) << ",\n";
        // 同日多笔配股抢现金：非零 = 截断结果依赖施加序（序按 raw ex 日，不改）。
        out << "    \"rights_same_day_cash_contention\": "
            << ca.rights_same_day_cash_contention << ",\n";
        // 换股：笔数 + 零股现金替代所得 + 旧 code 闩锁拒买腿数 + 闩锁后被拒的后续事件。
        out << "    \"conversions\": " << ca.conversions << ",\n";
        out << "    \"conversion_buy_rejects\": " << ca.conversion_buy_rejects << ",\n";
        out << "    \"conversion_cash_in_lieu\": "
            << fmt2(ca.conversion_cash_in_lieu) << ",\n";
        out << "    \"post_conversion_events_rejected\": "
            << ca.post_conversion_events_rejected << ",\n";
        // SELL 腿被截到可交割量的腿数：在途新股计进 pre_qty 但不可交割，差额顺延。
        out << "    \"pending_shares_sell_deferrals\": "
            << ca.pending_shares_sell_deferrals << "\n";
        out << "  }";
    }
    if (tradability) {
        out << ",\n";
        out << "  \"tradability\": {\n";
        out << "    \"source\": \"" << json_escape_min(tradability->source) << "\",\n";
        out << "    \"suspended_skips\": " << tradability->suspended_skips << ",\n";
        out << "    \"limit_up_buy_rejects\": " << tradability->limit_up_buy_rejects << ",\n";
        out << "    \"limit_down_sell_rejects\": " << tradability->limit_down_sell_rejects << "\n";
        out << "  }";
    }
    if (execution) {
        const auto& ex = *execution;
        out << ",\n";
        out << "  \"execution\": {\n";
        // 口径回显：投影模式 + 参与率上限 + carry 上限（研究门禁按这三项判执行假设）。
        out << "    \"integer_projection\": \"" << json_escape_min(ex.integer_projection)
            << "\",\n";
        out << "    \"volume_available\": " << (ex.volume_available ? "true" : "false")
            << ",\n";
        out << "    \"max_participation\": " << ex.max_participation << ",\n";
        out << "    \"carry_max_days\": " << ex.carry_max_days << ",\n";
        // 参与率截断：笔数 + 被截股数。volume_available=false 时恒 0（无量可依）。
        out << "    \"capped_fills\": " << ex.capped_fills << ",\n";
        out << "    \"capped_shortfall_qty\": " << fmt2(ex.capped_shortfall_qty) << ",\n";
        // volume 的两种「没有量」：显式 0（零流动性，cap 到 0）与缺行（fail-open）。
        // 分列即 importer 口径的对账面 —— 把缺失写成 0 会让前者吃掉后者。
        out << "    \"volume_zero_days\": " << ex.volume_zero_days << ",\n";
        out << "    \"volume_missing_days\": " << ex.volume_missing_days << ",\n";
        // carry 生命周期：created → resubmits →（completed | superseded |
        // noop_removed | dropped_stale | dropped | outstanding）。六个终局互斥且与
        // created 守恒。`carry_stale_before_create` 在守恒式**之外**：那些腿在建腿点
        // 即被同日更晚 fire 过滤，从未计入 created。
        out << "    \"carry_created\": " << ex.carry_created << ",\n";
        out << "    \"carry_resubmits\": " << ex.carry_resubmits << ",\n";
        out << "    \"carry_completed\": " << ex.carry_completed << ",\n";
        out << "    \"carry_superseded\": " << ex.carry_superseded << ",\n";
        out << "    \"carry_noop_removed\": " << ex.carry_noop_removed << ",\n";
        out << "    \"carry_dropped_stale\": " << ex.carry_dropped_stale << ",\n";
        out << "    \"carry_dropped_stale_qty\": "
            << fmt2(ex.carry_dropped_stale_qty) << ",\n";
        out << "    \"carry_stale_before_create\": "
            << ex.carry_stale_before_create << ",\n";
        out << "    \"carry_stale_before_create_qty\": "
            << fmt2(ex.carry_stale_before_create_qty) << ",\n";
        out << "    \"carry_sell_clamped\": " << ex.carry_sell_clamped << ",\n";
        out << "    \"sell_fee_deferrals\": " << ex.sell_fee_deferrals << ",\n";
        out << "    \"carry_dropped\": " << ex.carry_dropped << ",\n";
        out << "    \"carry_dropped_qty\": " << fmt2(ex.carry_dropped_qty) << ",\n";
        out << "    \"carry_outstanding\": " << ex.carry_outstanding << ",\n";
        out << "    \"carry_outstanding_qty\": " << fmt2(ex.carry_outstanding_qty) << ",\n";
        // 规模 vs 流动性：提交量 / 该 code 近 20 个有量交易日的平均成交量。
        // ADV20 是**前视无关**的（只取提交日之前的成交量），与参与率 cap 用当日
        // volume 的口径刻意不同——前者是研究判据，后者是执行约束。
        out << "    \"adv20_samples\": " << ex.adv20_samples << ",\n";
        out << "    \"adv20_ratio_max\": " << ex.adv20_ratio_max << ",\n";
        out << "    \"adv20_ratio_mean\": " << ex.adv20_ratio_mean << ",\n";
        out << "    \"legs_over_participation\": " << ex.legs_over_participation << ",\n";
        // 整数投影：补/削手数 + 削完仍不可负担的 fire 数（后者非零 = 组合目标超出
        // 可用现金，剩余缺口仍由落账口的可负担截断兜）。
        out << "    \"projection_fires\": " << ex.projection_fires << ",\n";
        out << "    \"projection_lots_added\": " << ex.projection_lots_added << ",\n";
        out << "    \"projection_lots_removed\": " << ex.projection_lots_removed << ",\n";
        out << "    \"projection_infeasible_fires\": "
            << ex.projection_infeasible_fires << ",\n";
        out << "    \"projection_adjusted_targets\": "
            << ex.projection_adjusted_targets << ",\n";
        // 恒应为 0：非零 = 投影写回值破了「非负 + 整手」契约，engine 已还原基线。
        out << "    \"projection_rejected_entries\": "
            << ex.projection_rejected_entries << "\n";
        out << "  }";
    }
    out << "\n}\n";
    return finish_report_stream(af, path, "SEL_BT", "summary.json", 1);
}

/// @brief SEL backtest 单次运行的共享状态，跨 load/assemble/loop/report 四段传递。
///
/// 成员声明顺序即析构逆序：engine 持有的 ctx 终生读 init_cfgs 裸指针，故
/// init_cfgs 必须早于 engine 声明；observer(listener/frozen_release) 引用 infra
/// 与 captured_trades，故 infra/captured_trades 早于 observer 声明；resolver
/// lambda 捕获 &current_closes_fp，handle_pos_change_fn 捕获 SelRunState*
/// (tradability gate 状态)，故这些成员均早于 engine 声明。
struct SelRunState {
    XtSelDataLoader::LoadResult loaded;
    std::unordered_map<uint32_t, std::unordered_map<std::string, double>> closes_by_date;
    /// 真风控拒因捕获；gateway 持其裸指针 → 必须早于 infra 声明（析构后于 gateway）。
    SelRejectCapture reject_capture;
    BacktestInfra infra;
    std::vector<BarTradeRow> captured_trades;
    std::vector<std::pair<uint32_t, int64_t>> daily_equity_rows;  ///< (YYYYMMDD, equity_eod_fp) 逐日收盘权益
    std::unordered_set<std::string> traded_codes;  ///< 曾成交过的 code（sparse-weights held 计数用）
    std::unique_ptr<XtBacktestTradeCapture> listener;
    std::unique_ptr<SelFrozenReleaseObserver> frozen_release;
    std::unordered_map<std::string, int64_t> current_closes_fp;
    // ── tradability gate 状态(未配置 tradability_csv 时全部保持初值,gate 全放行)──
    TradabilityCalendar tradability;
    const TradabilityCalendar::CodeFlagsMap* current_tradability = nullptr;  ///< 当日 flags;缺日/未配置 = nullptr
    uint32_t current_date = 0;                        ///< 主循环当前 trading date(事件行用)
    std::vector<TradabilityEventRow> tradability_events;
    size_t suspended_skips         = 0;
    size_t limit_up_buy_rejects    = 0;
    size_t limit_down_sell_rejects = 0;
    // ── consumed manifest 归因 ───────────────────────────────────────────────
    size_t submit_attempts = 0;   ///< 通过 gate 且 diff!=0 的 submit 尝试数 = rows_total。
    size_t no_change_skips = 0;   ///< diff_pos_fp==0 无变动 skip（other 分类）。
    // ── 首日 T-1 探针 + mark carry 观测 ──────────────────────────────────────
    /// 首日无 T-1 close 时激活：price_resolver 落空即计 miss，fire 后 fail-fast。
    bool   first_day_sizing_probe_active = false;
    size_t first_day_sizing_misses       = 0;  ///< 探针窗口内 sizing price 查询落空次数
    size_t undrained_reports             = 0;  ///< run 收尾时仍未 drain 的回报数（应为 0）
    int64_t frozen_cash_at_exit_fp       = 0;  ///< run 收尾时账户残留冻结（应为 0）
    bool report_write_failed             = false;  ///< 任一被调用的 writer 失败
    size_t stale_mark_carry_events       = 0;  ///< 持仓 symbol 当日无 close → carry 旧 mark 次数
    // ── B0 产物（schedule_fires / target_weights / daily_positions / fill_costs）──
    StreamCsvWriter fires_csv;
    StreamCsvWriter targets_csv;
    StreamCsvWriter positions_csv;
    StreamCsvWriter costs_csv;
    /// SEL 原生门禁链事件流（`backtest.trace_mode: full` 时才 open；off 时全部
    /// emit 点走 `is_open()==false` 早退，产物集合与未配置时逐位一致）。
    XtSelTraceSink  trace;
    RecordingSelScheduler* scheduler_probe = nullptr;  ///< non-owning；engine 持有
    /// non-owning 交易日历（所有权随 Config move 进 engine）；skip_reason 判据用。
    const xt_engine_sel::IHolidayProvider* holiday_probe = nullptr;
    std::vector<uint32_t> task_ids;                    ///< 与 cfg.tasks 平行的 scheduler id
    bool t1_enabled = false;                           ///< risk.enforce_long_t_plus_1 落到 account
    /// 本交易日的 fire 汇总（按 fire_seq 升序 append；日末落盘后清空）。
    std::vector<SelFireAgg> day_fires;
    /// 本交易日的 (fire, code) 决策行（日末落盘后清空）。
    std::vector<SelDecisionRow> day_decisions;
    /// (fire_seq, code) → day_decisions 下标；执行列回填用。
    std::unordered_map<std::string, size_t> decision_index;
    /// order_id → 累计成交量（fp）；日末清空。
    std::unordered_map<uint64_t, int64_t> filled_by_order;
    /// order_id → 产生它的 fire_seq；trace 的 `trade` 行靠它接回规划段（六段链的
    /// order 侧只有 order_id，`sel_target`/`sel_gate` 只有 fire_seq）。日末清空。
    std::unordered_map<uint64_t, uint64_t> fire_seq_by_order;
    /// order_id → 提交它的 strategy_id；参与率截断挂 carry 腿时要沿用同一 sid。
    /// 日末与 fire_seq_by_order 同批清空。
    std::unordered_map<uint64_t, std::string> submit_sid_by_order;
    /// order_id → 该腿瞄准的**绝对目标持仓**（fp）：carry 腿存目标而非未成增量，
    /// 复投前据此与实时持仓重算差额。日末与上面两张表同批清空。
    std::unordered_map<uint64_t, int64_t> submit_target_by_order;
    /// 最近一次 handle_pos_change_fn 命中的决策行下标；-1 = 无。
    long last_decision_idx = -1;
    size_t n_fires = 0;   ///< 真触发且产出调仓的 (date × task) 数
    size_t n_skips = 0;   ///< 有 skip_reason 的 (date × task) 数
    size_t  affordability_caps      = 0;  ///< BUY 因现金不足被截断的腿数（含截到 0）
    int64_t affordability_shortfall_fp = 0;  ///< 累计被截掉的名义额（fp）
    size_t  negative_cash_events    = 0;  ///< post-fill 现金为负的次数（应恒 0）
    size_t  min_lot_zero_targets    = 0;  ///< 整 lot 截零的目标数（F09 观测）
    size_t  equity_identity_violations = 0;  ///< 权益恒等 fp 精确失败的日数（应恒 0）
    /// gateway 未受理（order_id==0）的提交腿数：这些腿没有订单存在过，trace 里
    /// 走 `sel_reject` 而非 `order_submit`。与 target_weights 的 accepted=0 行同源。
    size_t  local_submit_rejects    = 0;
    // ── B0b corporate action（未配置 corporate_actions_csv 时全部保持初值，
    //    ca_enabled=false → 施加点整段短路，产物与 summary 块都不出现）─────────
    bool ca_enabled = false;
    CorporateActionCalendar corporate_actions;
    CaReceivableLedger      ca_ledger;
    StreamCsvWriter         ca_events_csv;
    double  ca_dividend_tax_rate = 0.0;
    double  ca_delist_haircut    = 0.0;
    RightsPolicy ca_rights_policy = RightsPolicy::kSubscribe;
    /// 合成清算单的 run 内 order_id 序列；高位基址与撮合分配的 id 域不重叠
    /// （match engine 从小到大分配，回测单跑不会撞到 4G）。
    uint64_t next_ca_order_id = 0xD000'0000ULL;
    /// record 日收盘仓位快照需求：date → 该日需要留仓位的 code 列表。
    /// @note 只排「真会被读」的 (date, code)：record_date 缺省或 == ex_date 时施加点
    ///       走 ex 日初仓，不排快照（排了也永不读 = 死排程）。
    std::unordered_map<uint32_t, std::vector<std::string>> ca_record_needs;
    /// "date|code" → 该 record 日收盘 long_qty（fp）；确权股权数锁定用。
    std::unordered_map<std::string, int64_t> ca_record_qty;
    /// 已退市 code 闩锁：清算后拒新 BUY，且该 code 再现 close 行时告警计数。
    std::unordered_set<std::string> ca_delisted_codes;
    /// 已换股出清的旧 code 闩锁：该证券已不存在，后续 BUY 一律拒（数据源若仍留着
    /// 旧 code 的 close 行，按它建仓等于在一只已经换掉的票上开仓）。与退市闩锁分开
    /// 记，两者的成因与披露口径不同。
    std::unordered_set<std::string> ca_converted_codes;
    /// 已就该 (date, code) 报过「退市后再现价」的键（同日同 code 只报一次）。
    std::unordered_set<std::string> ca_delist_price_warned;
    size_t   ca_applied              = 0;
    int64_t  ca_cash_dividends_fp    = 0;
    size_t   ca_delist_liquidations  = 0;
    size_t   ca_delist_fee_capped    = 0;
    size_t   ca_delist_pending_bonus_liquidated = 0;
    size_t   ca_delist_buy_rejects   = 0;
    size_t   ca_delisted_price_reappearances = 0;
    size_t   ca_record_qty_fallback_absent = 0;
    size_t   ca_record_qty_fallback_ex_day = 0;
    size_t   ca_record_qty_fallback_before_window = 0;
    /// 现金分红「无应收窗口」（pay_date 缺省或 ≤ ex 日）笔数 —— summary 披露用。
    size_t   ca_pay_date_absent_immediate_cash = 0;
    /// 新股「无在途窗口」（到账日不晚于 ex 日：CSV 显式给了 ex 日，或窗末已无次日）
    /// 笔数 —— 同上。
    size_t   ca_shares_list_date_absent_immediate = 0;
    /// CSV 未给 shares_list_date、到账日取缺省（ex 日下一交易日）的笔数。
    size_t   ca_shares_list_date_defaulted = 0;
    /// 新股到账日落在回测窗之外（窗末 ex 已无次日，或 CSV 显式给了窗后日期）的笔数：
    /// 这些股整跑不到账，只在应收里估值。
    size_t   ca_shares_list_date_beyond_window = 0;
    /// CSV 显式给的到账日等于原始 ex 日、但 ex 被归日前移 → 到账实际变滞后的笔数。
    size_t   ca_shares_list_date_lagged_by_rebase = 0;
    // ── 配股（rights）──────────────────────────────────────────────────────
    size_t   ca_rights_subscribed  = 0;  ///< 认购成功（含被现金截断的）笔数
    size_t   ca_rights_forfeited   = 0;  ///< 弃权（policy=forfeit）笔数
    size_t   ca_rights_cash_capped = 0;  ///< 现金不足按可缴整股截断的笔数
    int64_t  ca_rights_cash_paid_fp = 0; ///< 缴款总额（fp）
    /// 同一交易日的第 2 笔起的配股缴款腿数：施加序 = raw ex 序，先到者先占现金，
    /// 后到者可能被截断。序不改（原时间序是唯一有依据的序），只披露竞争发生过。
    size_t   ca_rights_same_day_cash_contention = 0;
    // ── 换股（security conversion）─────────────────────────────────────────
    size_t   ca_conversions            = 0;  ///< 真施加的换股笔数（有仓才算）
    size_t   ca_conversion_buy_rejects = 0;  ///< 换股后旧 code 仍下 BUY 被闩锁拒的腿数
    int64_t  ca_conversion_cash_in_lieu_fp = 0;  ///< 零股现金替代所得合计（fp）
    /// 换股闩锁置位后、仍落在同一 code 上的后续 CA 事件数（整条拒绝）：这些事件会
    /// 把新股挂到一只已经不存在的证券上，到账日凭空复活它。
    size_t   ca_post_conversion_events_rejected = 0;
    /// SELL 腿因「pre_qty 含在途新股、但在途股不可交割」被截到可交割量的腿数。
    size_t   ca_pending_shares_sell_deferrals = 0;
    size_t   sell_fee_unaffordable_rejects = 0;
    // ── B7e 执行真实度（exec_enabled=false 时全部保持初值，投影 hook 不注入、
    //    参与率 cap 早退、summary 无 execution 块）─────────────────────────────
    bool     exec_enabled    = false;   ///< volume 列在场 或 yaml 配了 `execution:`
    bool     exec_has_volume = false;   ///< close_prices.csv 带 volume 列
    int64_t  exec_max_participation_fp = 0;  ///< 参与率上限（fp）
    uint32_t exec_carry_max_days = 5;
    /// (date → code → 当日成交量 fp)；无 volume 列时恒空。
    std::unordered_map<uint32_t, std::unordered_map<std::string, int64_t>> exec_volumes_fp;
    /// 当日 volume 视图（non-owning 指针指向 exec_volumes_fp 的当日槽）；缺日 = nullptr。
    const std::unordered_map<std::string, int64_t>* exec_day_volume = nullptr;
    /// 当日各 code 已成交量（fp）：参与率是**当日累计**口径，一天内多腿共用同一份额度。
    std::unordered_map<std::string, int64_t> exec_participation_used_fp;
    /// code → 近 kAdvWindow 个「有量交易日」的成交量（fp，队尾最新）；ADV20 分母。
    std::unordered_map<std::string, std::vector<int64_t>> exec_adv_window;
    /// 当日属于 carry 复投的 order_id：其未成量记回既有 carry 腿，不新建腿（防双计）。
    std::unordered_set<uint64_t> exec_carry_orders;
    /// (sid,code) → 当日最新「算出了目标」的 fire_seq：carry 建腿点据此过滤已被
    /// 同日更晚 fire 取代的腿——planning 期的 supersede/stale 收口跑在 carry 创建
    /// 之前，罩不住「稍后才因 cap 而生」的旧 fire 腿。日末与决策缓冲同批清空。
    std::map<std::pair<std::string, std::string>, uint64_t> latest_decided_fire;
    std::vector<SelCarryLeg> exec_carry;
    size_t   exec_capped_fills      = 0;  ///< 被参与率截断的成交笔数（含截到 0）
    /// 累计被截掉的股数（fp）。跨全跑累加，单日单腿上界即 risk.max_order_qty，
    /// 用 __int128 免去「多少天 × 多大单才溢出」的推导。
    __int128 exec_capped_shortfall_fp = 0;
    /// 已计过 volume 缺行 / 零量的 "date|code"（同日同 code 只记一次）。
    std::unordered_set<std::string> exec_volume_zero_seen;
    std::unordered_set<std::string> exec_volume_missing_seen;
    size_t   exec_carry_created     = 0;
    size_t   exec_carry_resubmits   = 0;
    size_t   exec_carry_completed   = 0;  ///< 复投补齐后摘除的腿数
    size_t   exec_carry_superseded  = 0;  ///< 被新 fire 的同 code 目标取代而摘除的腿数
    size_t   exec_carry_noop_removed = 0; ///< 复投前重算差额归零/反向而摘除的腿数
    /// 新 fire 决策集缺席该 code → 对**已建**腿的显式丢弃（计在 created 内）。
    size_t   exec_carry_dropped_stale = 0;
    __int128 exec_carry_dropped_stale_qty_fp = 0;
    /// 建腿点即被过滤（未建腿、**不计** created）的旧 fire 未成量：与上面一对
    /// 分列，否则 summary 的 created 守恒式对任意运行不可审计。
    size_t   exec_carry_stale_before_create = 0;
    __int128 exec_carry_stale_before_create_qty_fp = 0;
    size_t   exec_carry_sell_clamped = 0; ///< carry SELL 腿被截到可交割量的腿数
    size_t   exec_sell_fee_deferrals = 0; ///< 参与率缩量后「所得 < 费用」整腿顺延的笔数
    size_t   exec_carry_dropped     = 0;  ///< 到 carry_max_days 仍未成、丢弃的腿数
    __int128 exec_carry_dropped_qty_fp = 0;
    size_t   exec_adv20_samples     = 0;  ///< 有 ADV20 分母可比的提交腿数
    double   exec_adv20_ratio_max   = 0.0;
    double   exec_adv20_ratio_sum   = 0.0;
    size_t   exec_legs_over_participation = 0;  ///< 提交量 / ADV20 超参与率上限的腿数
    // 投影观测（execution.integer_projection: largest_remainder 时才非零）。
    size_t   exec_projection_fires       = 0;
    size_t   exec_projection_lots_added  = 0;
    size_t   exec_projection_lots_removed = 0;
    size_t   exec_projection_infeasible_fires = 0;  ///< 削完净买入腿仍现金为负的 fire 数
    CorporateActionCalendar::AlignStats ca_align{};
    std::vector<std::unique_ptr<XtStrategyInitConfig>> init_cfgs;
    std::unique_ptr<xt_engine_sel::XtSelEngine> engine;
    XtStrategyFactory factory;
    std::vector<XtStrategyFactory::UniqueStra> strategies;
    std::unordered_set<std::string> strategy_ids;
};

/// @brief (fire_seq, code) → decision_index 的复合 key。
[[nodiscard]] std::string decision_key(uint64_t fire_seq, const std::string& code) {
    return std::to_string(fire_seq) + '|' + code;
}

/// @brief 取（或新建）本交易日的 fire 汇总槽。
/// @note 行序 = fire_seq 首次出现序（engine 内单调递增，即时间序）。
SelFireAgg& day_fire_agg(SelRunState& st, uint64_t fire_seq, const std::string& sid) {
    for (auto& a : st.day_fires) {
        if (a.fire_seq == fire_seq) return a;
    }
    SelFireAgg fresh;
    fresh.fire_seq    = fire_seq;
    fresh.strategy_id = sid;
    st.day_fires.push_back(std::move(fresh));
    return st.day_fires.back();
}

// ── B0b portfolio truth（应收口径的唯一定义）────────────────────────────────
//
// SEL 的账本（account）没有 receivable 原语：现金分红在 ex→pay 窗口、送转股在
// ex→list 窗口都已确权但未到账。若权益只看 account，这两个窗口会出现「先凹后跳」
// 的假回撤。故定义 portfolio_equity = account equity + 应收现金 + 在途红股市值，
// 并在**全部五个消费点**统一消费：daily_equity 写点 / trades.csv 逐笔
// equity_after / summary final_equity / engine sizing 分母（equity_resolver_fn）/
// daily_positions。
// 买入可用资金仍只看 account 可用现金——应收不可用于买入。
// 未配置 CA 时两项恒 0，portfolio_equity ≡ account equity（历史输出逐位不变）。

/// @brief 某 code 的在途新股股数合计（fp）；无在途返 0。
[[nodiscard]] int64_t pending_shares_qty_fp(const SelRunState& st, const std::string& code) {
    int64_t total = 0;
    for (const auto& pending : st.ca_ledger.pending_shares()) {
        if (pending.code == code) total += pending.qty_fp;
    }
    return total;
}

/// @brief 在途红股市值（按各 code 当前 mark 逐笔重估；ex 日即修正后 ref）。
[[nodiscard]] int64_t bonus_receivable_value_fp(const SelRunState& st) {
    int64_t total = 0;
    for (const auto& pending : st.ca_ledger.pending_shares()) {
        const int64_t mark = st.infra.account->snapshot_position(pending.code).mark_price;
        if (mark > 0) total += xt_fp::mul(mark, pending.qty_fp);
    }
    return total;
}

/// @brief 应收合计（现金应收 + 在途红股市值）。
[[nodiscard]] int64_t receivable_total_fp(const SelRunState& st) {
    return st.ca_ledger.outstanding_cash_fp() + bonus_receivable_value_fp(st);
}

/// @brief portfolio 权益（account equity + 应收合计）。
[[nodiscard]] int64_t portfolio_equity_fp(const SelRunState& st) {
    return st.infra.account->snapshot_account().equity + receivable_total_fp(st);
}

// ── B6 trace：SEL 原生门禁链事件发射点 ──────────────────────────────────────
//
// 六段链 `sel_fire → sel_target → sel_gate → order_submit → trade → sel_settle`
// 各有一个发射函数；trace 未开启时全部在首行早退（零字符串构造）。
// `order_submit` / `trade` 的 payload key 与 HFT `XtEventStreamSink` 同名同义——
// producer 的 events_ingest 靠它们重建 per-order 因果链，改 key 即破跨语言契约。

/// @brief task 身份串（与 schedule_fires.csv 的 `task_uid` 列同规则）。
[[nodiscard]] std::string trace_task_uid(uint32_t task_id, const std::string& sid) {
    return task_id != 0 ? "task:" + std::to_string(task_id) : "legacy:" + sid;
}

/// @brief snprintf 结果 → payload 串（截断安全）。
///
/// `n` 是 snprintf 的**本应写入**长度：payload 超过栈缓冲区时它大于容量，直接拿去
/// 构造 std::string 会从 buffer 尾部越界读（长 strategy_id / task_uid / code 可达）。
/// clamp 到 `cap-1` 即 snprintf 真正写入的字节数，与 HFT 侧
/// `XtEventStreamSink::append_ring_event` 同一规则：宁可这一行被截断，不越界。
[[nodiscard]] std::string trace_payload(const char* buf, int n, size_t cap) {
    size_t len = n > 0 ? static_cast<size_t>(n) : 0;
    if (len >= cap) len = cap - 1;
    return std::string(buf, len);
}

/// @brief 一次 fire 的规划入口（含未产出调仓的早退 fire）。
void emit_trace_fire(SelRunState& st, const xt_engine_sel::SelFireRecord& r) {
    if (!st.trace.is_open()) return;
    char buf[320];
    const int n = std::snprintf(buf, sizeof(buf),
        "fire_seq=%llu,strategy_id=%s,task_uid=%s,equity=%.2f,n_targets=%zu,"
        "equity_nonpositive=%d",
        static_cast<unsigned long long>(r.fire_seq), r.strategy_id.c_str(),
        trace_task_uid(r.task_id, r.strategy_id).c_str(),
        xt_fp::from_fp(r.equity_fp), r.n_targets, r.equity_nonpositive ? 1 : 0);
    st.trace.emit("sel_fire", trace_payload(buf, n, sizeof(buf)));
}

/// @brief 单个 (fire, code) 的规划期目标（含 no-price / min-lot 截零等早退）。
void emit_trace_target(SelRunState& st, const xt_engine_sel::SelTargetDecision& d) {
    if (!st.trace.is_open()) return;
    char buf[384];
    const int n = std::snprintf(buf, sizeof(buf),
        "fire_seq=%llu,code=%s,weight=%.6f,price=%.4f,target_qty=%.4f,pre_qty=%.4f,"
        "diff_qty=%.4f,status=%s",
        static_cast<unsigned long long>(d.fire_seq), d.code.c_str(), d.weight,
        xt_fp::from_fp(d.price_fp), order_exec_from_fp(d.target_qty_fp),
        order_exec_from_fp(d.pre_qty_fp), order_exec_from_fp(d.diff_qty_fp),
        xt_engine_sel::sel_planning_status_name(d.status));
    st.trace.emit("sel_target", trace_payload(buf, n, sizeof(buf)));
}

/// @brief 执行前门禁裁决（tradability / 退市闩锁 / 换股闩锁 / 卖出费用不可负担）。
/// @param fire_seq 归因的 fire 身份；carry 复投沿用原 fire（engine 此刻并未派发）。
/// @param reject_reason nullptr = 放行（decision=pass）；非空 = 拒因串。
void emit_trace_gate(SelRunState& st, uint64_t fire_seq, const std::string& code,
                     bool is_buy, int64_t qty_fp, const char* reject_reason) {
    if (!st.trace.is_open()) return;
    char buf[320];
    const int n = std::snprintf(buf, sizeof(buf),
        "fire_seq=%llu,code=%s,side=%s,qty=%.4f,decision=%s,reason=%s",
        static_cast<unsigned long long>(fire_seq),
        code.c_str(), is_buy ? "BUY" : "SELL", order_exec_from_fp(qty_fp),
        reject_reason == nullptr ? "pass" : "reject",
        reject_reason == nullptr ? "" : reject_reason);
    st.trace.emit("sel_gate", trace_payload(buf, n, sizeof(buf)));
}

/// @brief 提交到 gateway 的一腿（key 集与 HFT order_submit 事件一致 + `fire_seq`）。
///
/// @note `fire_seq` 是六段链在 order 侧的唯一桥：`sel_target` / `sel_gate` 只有
///       fire_seq，`order_submit` / `trade` 只有 order_id，缺这个字段就无法把同一
///       code 的两次 fire 各自的提交/成交归回其规划行。列形态不变（仍是单个
///       payload 字段），只是多一个 `k=v`。
/// @note `carry` / `origin_date`：跨日复投腿的两个身份字段。fire_seq 沿用原 fire，
///       所以单看 fire_seq 无法把「当日派发」与「几天前的意图今天才补上」分开——
///       消费方按 `carry=1` 区分，`origin_date` 给出该意图的生成交易日。
///       非复投腿恒 `carry=0,origin_date=0`；列形态不变（仍是单个 payload 字段）。
/// @pre `order_id != 0`：gateway 未受理的腿没有订单存在过，走 `emit_trace_reject`。
void emit_trace_submit(SelRunState& st, uint64_t order_id, uint64_t fire_seq,
                       const std::string& code, bool is_buy, int64_t price_fp,
                       int64_t qty_fp, const std::string& tag, bool is_close,
                       const SelCarryLeg* carry) {
    if (!st.trace.is_open()) return;
    char buf[512];
    const int n = std::snprintf(buf, sizeof(buf),
        "order_id=%llu,fire_seq=%llu,code=%s,side=%s,status=Submitted,price=%.6f,"
        "qty=%.6f,is_close=%d,is_today=%d,user_tag=%s,carry=%d,origin_date=%u",
        static_cast<unsigned long long>(order_id),
        static_cast<unsigned long long>(fire_seq), code.c_str(),
        is_buy ? "BUY" : "SELL", xt_fp::from_fp(price_fp), order_exec_from_fp(qty_fp),
        is_close ? 1 : 0, 0, tag.c_str(),
        carry != nullptr ? 1 : 0, carry != nullptr ? carry->created_date : 0u);
    st.trace.emit("order_submit", trace_payload(buf, n, sizeof(buf)));
}

/// @brief gateway 未受理的一腿（`order_id == 0`）。
///
/// 独立事件型而不是 `order_submit`：order_id 是 producer 重建 per-order 因果链的
/// 主键，把未受理腿也发成 Submitted 会让一天里的多次拒绝合并成同一个从未存在过的
/// 订单 #0（它的 qty/price 互相覆盖）。本行只带 fire_seq + 拒因，不进 order 域。
void emit_trace_reject(SelRunState& st, uint64_t fire_seq, const std::string& code,
                       bool is_buy, int64_t price_fp, int64_t qty_fp,
                       const char* reject_code) {
    if (!st.trace.is_open()) return;
    char buf[384];
    const int n = std::snprintf(buf, sizeof(buf),
        "fire_seq=%llu,code=%s,side=%s,price=%.6f,qty=%.6f,reason=%s",
        static_cast<unsigned long long>(fire_seq), code.c_str(),
        is_buy ? "BUY" : "SELL", xt_fp::from_fp(price_fp), order_exec_from_fp(qty_fp),
        reject_code != nullptr ? reject_code : "");
    st.trace.emit("sel_reject", trace_payload(buf, n, sizeof(buf)));
}

/// @brief 落账的一笔成交（key 集与 HFT trade 事件一致 + `fire_seq` 桥）。
/// @note `visible_at=0`：SEL 是「T 日收盘价一次性成交」，没有延迟模型可见时刻可报；
///       0 与 HFT 侧「未盖章」同义，producer 据此不派生交付滞后。
/// @note `fire_seq=0` = 该成交不属于任何一次 fire（退市清算这类合成单）。
void emit_trace_fill(SelRunState& st, const XtTradeRecord& tr) {
    if (!st.trace.is_open()) return;
    const double qty_d     = order_exec_from_fp(tr.qty_fp);
    const double slip_cost = (tr.exec_price - tr.price) * (tr.is_buy ? 1.0 : -1.0) * qty_d;
    uint64_t fire_seq = 0;
    if (const auto it = st.fire_seq_by_order.find(tr.order_id);
        it != st.fire_seq_by_order.end()) {
        fire_seq = it->second;
    }
    char buf[512];
    const int n = std::snprintf(buf, sizeof(buf),
        "order_id=%llu,fire_seq=%llu,code=%s,side=%s,qty=%.6f,price=%.6f,"
        "exec_price=%.6f,fee=%.6f,slippage=%.6f,slippage_cost=%.6f,visible_at=0",
        static_cast<unsigned long long>(tr.order_id),
        static_cast<unsigned long long>(fire_seq), tr.code.c_str(),
        tr.is_buy ? "BUY" : "SELL", qty_d, tr.price, tr.exec_price, tr.fee,
        tr.slippage, slip_cost);
    st.trace.emit("trade", trace_payload(buf, n, sizeof(buf)));
}

/// @brief 日终结算（链的收口段：当日全部提交/成交/CA 之后的账户终态）。
void emit_trace_settle(SelRunState& st, uint32_t date, size_t n_positions) {
    if (!st.trace.is_open()) return;
    const auto acct = st.infra.account->snapshot_account();
    char buf[320];
    const int n = std::snprintf(buf, sizeof(buf),
        "date=%u,equity=%.2f,cash=%.2f,receivable=%.2f,positions=%zu,fills_total=%zu",
        date, xt_fp::from_fp(portfolio_equity_fp(st)), xt_fp::from_fp(acct.cash),
        xt_fp::from_fp(receivable_total_fp(st)), n_positions, st.captured_trades.size());
    st.trace.emit("sel_settle", trace_payload(buf, n, sizeof(buf)));
}

// ── B7e 执行真实度：整数投影 / 参与率 / carry 的共享算子 ────────────────────

/// @brief `__int128` 定点累加器 → 报告用 double（数量口径，1e6 scale）。
///
/// 全跑累加器用 `__int128` 存（单笔上界 × 天数的乘积推导不出稳定上界），落报告时
/// 才降到 double —— 报告本来就是 %.2f 精度，这一步不引入新的失真面。
[[nodiscard]] double order_exec_from_fp128(__int128 v) noexcept {
    return static_cast<double>(v) / static_cast<double>(kPriceFpScale);
}

/// @brief qty_fp 向零截断到 min_lot 整数倍（与 engine 的 round-target 同规则）。
[[nodiscard]] int64_t floor_to_min_lot_fp(int64_t qty_fp, int min_lot) noexcept {
    if (min_lot <= 1 || qty_fp <= 0) return qty_fp > 0 ? qty_fp : 0;
    const int64_t lot_fp = static_cast<int64_t>(min_lot) * kPriceFpScale;
    return (qty_fp / lot_fp) * lot_fp;
}

/// @brief 该 code 的 min_lot（无 catalog 或未声明时按 1）。
[[nodiscard]] int min_lot_of(const SelRunState& st, const std::string& code) {
    if (!st.infra.commodities) return 1;
    const auto* info = st.infra.commodities->find(code.c_str());
    return (info && info->min_lot > 0) ? info->min_lot : 1;
}

/// @brief 单腿费用试算（fp）：与落账口同一条 `breakdown_fee` 路径。
/// @param price_fp 计价用价格；投影期是规划价（T-1 close），落账期是成交价。
[[nodiscard]] int64_t leg_fee_fp(const SelRunState& st, const std::string& code,
                                 int64_t price_fp, int64_t qty_fp,
                                 bool is_buy, bool is_close) {
    if (qty_fp <= 0 || price_fp <= 0) return 0;
    const XtCommodityInfo* comm = st.infra.commodities
        ? st.infra.commodities->find(code.c_str()) : nullptr;
    XtTradeRecord probe;
    probe.code     = code;
    probe.is_buy   = is_buy;
    probe.qty_fp   = qty_fp;
    probe.is_close = is_close;
    probe.is_today = false;
    return to_fp_round(xt_fill_cost::breakdown_fee(
        xt_fp::from_fp(price_fp), probe, st.infra.fee_book.get(), comm).total);
}

/// @brief 摘除某 sid 在某 code 上尚未成交的 carry 腿（新 fire 的目标已重算过它）。
///
/// 不摘会双计：carry 说「还差 N 股没买到」，而新 fire 的 diff = target − actual
/// 里已经含着这 N 股，两者各提交一次就买成两份。
/// @note 只摘同一 sid 的腿：别的 sid 的调仓意图不因本 sid 重估而作废。
void supersede_carry_for_code(SelRunState& st, const std::string& sid,
                              const std::string& code) {
    if (st.exec_carry.empty()) return;
    const size_t before = st.exec_carry.size();
    st.exec_carry.erase(
        std::remove_if(st.exec_carry.begin(), st.exec_carry.end(),
                       [&](const SelCarryLeg& l) {
                           return l.code == code && l.strategy_id == sid;
                       }),
        st.exec_carry.end());
    st.exec_carry_superseded += before - st.exec_carry.size();
}

/// @brief 一次 fire 结束后，丢弃该 sid 在「本次决策集之外」的 code 上的残留 carry。
///
/// 新 fire 是对整个组合的重新评估：这一轮没给某个 code 出决策，就是不再要它了。
/// 让零成交的旧意图继续跨日执行，等于让一份已作废的判断在几天后才落地。
/// `supersede` 管「本轮出了决策的 code」，本函数管「本轮缺席的 code」，两者互补，
/// 合起来使一次 fire 后该 sid 不留任何上一轮的 carry。
void drop_stale_carry_for_sid(SelRunState& st, const std::string& sid,
                              const std::unordered_set<std::string>& decided_codes) {
    if (st.exec_carry.empty()) return;
    st.exec_carry.erase(
        std::remove_if(st.exec_carry.begin(), st.exec_carry.end(),
                       [&](const SelCarryLeg& l) {
                           if (l.strategy_id != sid) return false;
                           if (decided_codes.count(l.code) != 0) return false;
                           ++st.exec_carry_dropped_stale;
                           st.exec_carry_dropped_stale_qty_fp += l.remaining_fp;
                           XtLogger::warn_m("SEL_BT",
                               "date {}: carry '{}' {} 剩余 {} 被新 fire 丢弃 —— "
                               "sid='{}' 本轮决策集不含该 code（整体重估）",
                               st.current_date, l.code, l.is_buy ? "BUY" : "SELL",
                               order_exec_from_fp(l.remaining_fp), sid);
                           return true;
                       }),
        st.exec_carry.end());
}

/// @brief sizing 口径的当前持仓（fp）—— 必须与 engine `position_resolver_fn` 同式，
///        否则 carry 复投重算出的差额与 engine 的 target 语义对不上。
[[nodiscard]] int64_t sizing_position_fp(const SelRunState& st, const std::string& code) {
    const auto pos = st.infra.account->snapshot_position(code);
    return pos.long_qty - pos.short_qty + pending_shares_qty_fp(st, code);
}

/// @brief carry 腿当下仍缺的差额（fp，符号 = 方向）：`target − 实时持仓`。
[[nodiscard]] int64_t carry_pending_diff_fp(const SelRunState& st, const SelCarryLeg& leg) {
    return leg.target_qty_fp - sizing_position_fp(st, leg.code);
}

/// @brief 记一条「提交量 vs 该 code 近 20 个有量交易日均量」的样本（研究门禁口径）。
///
/// 分母只用**提交日之前**的成交量：ADV20 是判「这个规模的单子在这只票上现不现实」
/// 的研究判据，掺进当日量就成了事后诸葛。窗口为空（首次交易该 code）不记样本。
/// @note 求和走 `__int128`：单日 volume 上界 1e12 股 = 1e18 fp，20 个就越过 int64。
void record_adv20_sample(SelRunState& st, const std::string& code, int64_t qty_fp) {
    if (!st.exec_enabled || qty_fp <= 0) return;
    const auto it = st.exec_adv_window.find(code);
    if (it == st.exec_adv_window.end() || it->second.empty()) return;
    __int128 sum = 0;
    for (const int64_t v : it->second) sum += v;
    const int64_t adv_fp =
        static_cast<int64_t>(sum / static_cast<__int128>(it->second.size()));
    if (adv_fp <= 0) return;
    const double ratio = xt_fp::from_fp(qty_fp) / xt_fp::from_fp(adv_fp);
    ++st.exec_adv20_samples;
    st.exec_adv20_ratio_sum += ratio;
    if (ratio > st.exec_adv20_ratio_max) st.exec_adv20_ratio_max = ratio;
    if (st.exec_has_volume &&
        ratio > xt_fp::from_fp(st.exec_max_participation_fp)) {
        ++st.exec_legs_over_participation;
    }
}

/// @brief 提交一腿目标差额到 gateway —— fire 派发与 carry 复投共用的唯一提交口。
///
/// 门禁序（任一拒即整腿不进撮合，缺口由下期 rebalance 以 target−actual 顺延）：
/// 在途新股不可交割截断 → tradability → 退市/换股闩锁 → SELL 费用可负担 → submit。
///
/// @param diff_pos_fp 目标差额（>0 买 / <0 卖）；0 计 no_change_skip 并直接返回。
/// @param price_fp    提交价（回测口径 = T-1 close，lookahead-free）。
/// @param fire_seq    归因的 fire 身份；carry 复投沿用原 fire（engine 未在派发中）。
/// @param carry       非空 = 本腿是该 carry 腿的跨日复投（影响截量计数口径与 trace
///                    的 `carry` / `origin_date` 字段）。
/// @return gateway 受理的 order_id；0 = 未受理或未提交。
uint64_t submit_target_leg(SelRunState& st, const std::string& sid,
                           const std::string& code, int64_t diff_pos_fp,
                           int64_t price_fp, XtTimeInForce tif,
                           const std::string& tag, uint64_t fire_seq,
                           const SelCarryLeg* carry = nullptr) {
    if (diff_pos_fp == 0) { ++st.no_change_skips; return 0; }  // 无变动 skip（other）。

    auto* gw_p      = st.infra.gw.get();
    auto* account_q = st.infra.account.get();
    const bool is_buy = (diff_pos_fp > 0);
    int64_t    qty_fp = is_buy ? diff_pos_fp : -diff_pos_fp;
    // 本腿瞄准的绝对目标：`diff_pos_fp` 由 engine 在此刻用同一个 sizing 口径算出
    // （`target − position_resolver`），故加回当下持仓即还原 target。carry 腿存它
    // 而不存增量，跨日复投前才能与实时持仓重算（CA 改仓 / 别的通道成交都会让
    // 增量口径盲投）。
    const int64_t leg_target_fp = sizing_position_fp(st, code) + diff_pos_fp;

    // 在途新股计进了 sizing 的 pre_qty（与权益口径同源，见 position_resolver_fn），
    // 但它们尚未交割 —— SELL 腿能落地的只有账上实际持仓。整腿按 pre_qty 提交会被
    // 风控整单拒，连能卖的那部分也卖不掉（退出意图被在途股卡死）。故在提交口截到
    // 可交割量，缺口由下期 rebalance 以 target−actual 顺延（与 BUY 侧可负担截断
    // 同形）。SEL v1 long-only，`qty > long_qty` 的唯一成因就是在途股。
    if (!is_buy) {
        const int64_t deliverable_fp = account_q->snapshot_position(code).long_qty;
        if (qty_fp > deliverable_fp) {
            // 两条成因分开计数与措辞：fire 腿的差额来自 sizing 口径里的在途新股
            // （CA 通道），carry 复投腿的差额来自「两次提交之间持仓变了」——把后者
            // 也说成「在途新股」会把 CA 无关的跑引向错误的排查方向。
            if (carry != nullptr) {
                ++st.exec_carry_sell_clamped;
                XtLogger::warn_m("SEL_BT",
                    "date {}: carry SELL '{}' 截到可交割量 {} → {}（复投前持仓已"
                    "变动，差额下期 rebalance 顺延）", st.current_date, code,
                    order_exec_from_fp(qty_fp), order_exec_from_fp(deliverable_fp));
            } else {
                ++st.ca_pending_shares_sell_deferrals;
                XtLogger::warn_m("SEL_BT",
                    "date {}: SELL '{}' 截到可交割量 {} → {}（差额是尚未到账的在途"
                    "新股，缺口下期 rebalance 顺延）", st.current_date, code,
                    order_exec_from_fp(qty_fp), order_exec_from_fp(deliverable_fp));
            }
            qty_fp = deliverable_fp > 0 ? deliverable_fp : 0;
            if (qty_fp == 0) return 0;  // 一股都不可交割：整腿顺延
        }
    }

    // 执行列回填锚：本次 fire 的 (fire_seq, code) 决策行。carry 复投的原决策行属于
    // 更早的交易日、当日缓冲已清空 → 查不到，执行列不回填（该腿的成交在
    // trades.csv / fill_costs.csv 上仍完整可见）。
    st.last_decision_idx = -1;
    SelDecisionRow* drow = nullptr;
    {
        const auto it = st.decision_index.find(decision_key(fire_seq, code));
        if (it != st.decision_index.end() && it->second < st.day_decisions.size()) {
            drow = &st.day_decisions[it->second];
            st.last_decision_idx = static_cast<long>(it->second);
            drow->entered_execution = true;
            drow->is_buy            = is_buy;
        }
    }

    // tradability gate(D3/D4): 停牌 skip 全部、涨停拒 BUY、跌停拒 SELL;
    // 目标权重模型下一次 rebalance 以 target−actual 重算 diff,天然顺延。
    // 缺行(nullptr flags)= 完全可交易(fail-open)。
    const TradabilityFlags* fl = nullptr;
    if (st.current_tradability != nullptr) {
        const auto fit = st.current_tradability->find(code);
        if (fit != st.current_tradability->end()) fl = &fit->second;
    }
    const TradabilityVerdict verdict = check_tradability(fl, is_buy);
    if (drow) drow->gate = tradability_reason_str(verdict);
    if (verdict != TradabilityVerdict::Ok) {
        TradabilityEventRow ev;
        ev.date    = st.current_date;
        ev.code    = code;
        ev.is_buy  = is_buy;
        ev.verdict = verdict;
        ev.qty_fp  = qty_fp;
        st.tradability_events.push_back(std::move(ev));
        switch (verdict) {
            case TradabilityVerdict::Suspended:     ++st.suspended_skips;         break;
            case TradabilityVerdict::LimitUpBuy:    ++st.limit_up_buy_rejects;    break;
            case TradabilityVerdict::LimitDownSell: ++st.limit_down_sell_rejects; break;
            case TradabilityVerdict::Ok:            break;
        }
        emit_trace_gate(st, fire_seq, code, is_buy, qty_fp,
                        tradability_reason_str(verdict));
        return 0;  // 不进撮合;engine pending 消费不依赖 callback 结果(fire_front 无条件 pop)
    }

    // 退市闩锁：清算过的 code 不再接受买入（数据源若还留着后续 close 行，
    // 按它建仓等于在已终止上市的票上开仓）。卖出不拦——仓位理论上已清空，
    // 真有残仓（无价可清算的 stale carry）时仍应允许平掉。
    if (is_buy && st.ca_delisted_codes.count(code) != 0) {
        ++st.ca_delist_buy_rejects;
        if (drow) drow->gate = "delisted";
        emit_trace_gate(st, fire_seq, code, is_buy, qty_fp, "delisted");
        XtLogger::warn_m("SEL_BT",
            "date {}: BUY '{}' rejected — code 已退市清算（闩锁）",
            st.current_date, code);
        return 0;
    }

    // 换股闩锁：旧 code 换股后已不存在，同理拒买入（卖出不拦——仓位理论上
    // 已换空，真有残仓时仍应允许平掉）。
    if (is_buy && st.ca_converted_codes.count(code) != 0) {
        ++st.ca_conversion_buy_rejects;
        if (drow) drow->gate = "converted";
        emit_trace_gate(st, fire_seq, code, is_buy, qty_fp, "converted");
        XtLogger::warn_m("SEL_BT",
            "date {}: BUY '{}' rejected — code 已换股出清（闩锁）",
            st.current_date, code);
        return 0;
    }

    // Infer open/close offset from current signed position instead of
    // hardcoding ``is_buy ? Open : Close``. The naive hardcode tags SELLs
    // as Close even when there is no long to close — blocking sell-to-open
    // shorts. Matches CTA gateway's ``finalize_fill_offsets_`` logic exactly.
    // offset 推断用 `infer_offset_from_pos` (XtOffset.h)：独立函数便于
    // 隔离 gtest（见 XtSelOffsetInferenceTest.cpp）。
    const auto pos = account_q->snapshot_position(code);
    const XtOffset offset = infer_offset_from_pos(pos.qty, is_buy);

    // SELL 侧费用可负担闸：卖出费用（佣金下限 + 印花税 + 过户费）是真实成本，
    // **不 cap**——cap 掉就等于账本少扣钱。但「现金 + 卖出所得 < 费用」时提交
    // 这一单必然把现金扣成负（低价小额清仓 + 佣金下限的组合可确定性构造），
    // 故在提交口拒该腿并计数：缺口由下期 rebalance 以 target−actual 顺延。
    // 判据用提交价（T-1 close，lookahead-free）；成交价是 T 日 close，二者
    // 的价差窗口由落账口的现金不变量硬闸（rc=10）兜底。
    if (!is_buy && price_fp > 0) {
        const XtCommodityInfo* fee_comm = st.infra.commodities
            ? st.infra.commodities->find(code.c_str()) : nullptr;
        XtTradeRecord probe;
        probe.code     = code;
        probe.is_buy   = false;
        probe.qty_fp   = qty_fp;
        probe.is_close = (offset != XtOffset::Open);
        probe.is_today = false;
        const int64_t fee_fp = to_fp_round(xt_fill_cost::breakdown_fee(
            xt_fp::from_fp(price_fp), probe,
            st.infra.fee_book.get(), fee_comm).total);
        const int64_t proceeds_fp = xt_fp::mul(price_fp, qty_fp);
        const int64_t cash_fp = account_q->snapshot_account().available_cash;
        if (cash_fp + proceeds_fp < fee_fp) {
            ++st.sell_fee_unaffordable_rejects;
            if (drow) drow->gate = "sell_fee_unaffordable";
            emit_trace_gate(st, fire_seq, code, is_buy, qty_fp, "sell_fee_unaffordable");
            XtLogger::warn_m("SEL_BT",
                "date {}: SELL '{}' rejected — 费用 {:.2f} 超「可用现金 {:.2f} "
                "+ 卖出所得 {:.2f}」，提交即透支",
                st.current_date, code, xt_fp::from_fp(fee_fp),
                xt_fp::from_fp(cash_fp), xt_fp::from_fp(proceeds_fp));
            return 0;
        }
    }
    ExecFlags exec_flags{};
    emit_trace_gate(st, fire_seq, code, is_buy, qty_fp, /*reject_reason=*/nullptr);
    ++st.submit_attempts;  // rows_total：通过 gate 且 diff!=0 的提交尝试。
    record_adv20_sample(st, code, qty_fp);
    // SubmitResult 落列：本地未受理（order_id==0）在 target_weights.csv
    // 上必须可见，否则「目标未达成」只能靠事后 diff 猜。
    st.reject_capture.reset();  // 本笔 submit 的拒因窗口（gateway 同步回调）
    const auto res = gw_p->submit(code.c_str(), is_buy, price_fp, qty_fp, tif,
                                  tag, offset, exec_flags, sid);
    // 真风控拒因优先：SubmitResult 在整单被拒时只给 Unknown，
    // 「为什么没受理」全靠 sink 里的 reason code。
    const xt_runtime::XtRejectCode reject_code =
        st.reject_capture.has_reject()
            ? reject_code_from_risk_reason(st.reject_capture.reason())
            : res.reject_reason;
    if (res.order_id != 0) {
        st.fire_seq_by_order[res.order_id] = fire_seq;
        if (st.exec_enabled) {
            st.submit_sid_by_order[res.order_id]    = sid;
            st.submit_target_by_order[res.order_id] = leg_target_fp;
        }
        emit_trace_submit(st, res.order_id, fire_seq, code, is_buy, price_fp,
                          qty_fp, tag, offset != XtOffset::Open, carry);
    } else {
        // 本地未受理：没有订单存在过。发 order_submit 会让同一天的多次拒绝
        // 在 producer 侧合并成一个虚假订单 #0（order_id 是其因果链主键）。
        ++st.local_submit_rejects;
        emit_trace_reject(st, fire_seq, code, is_buy, price_fp, qty_fp,
                          reject_code_name(reject_code));
    }
    if (drow) {
        drow->submitted        = true;
        drow->submitted_qty_fp = qty_fp;
        drow->order_id         = res.order_id;
        drow->accepted         = res.accepted();
        drow->reject_code      = reject_code;
    }
    return res.order_id;
}

/// @brief F09 费用感知整数投影（largest-remainder + 残余现金约束）。
///
/// 基线（engine 侧 floor-to-lot）有两个系统性偏差：
///   ① 每个 code 各自丢掉不足一手的部分 → 组合整体欠配，残余现金堆积、小权重
///      直接被整手规则吃成 0（`min_lot_zero_targets` 就是它的可见面）；
///   ② 不看费用也不看现金 → Σ名义额 + 费用可超可用现金，超出部分在**落账口**
///      按 code 字典序被截（affordability cap），谁排在后面谁挨饿——这个偏差与
///      权重无关，纯粹是执行顺序的伪影。
/// 本投影按整手化的最大余数法重投影：先按余数从大到小给负担得起的 code 各补一手
/// （up-round），若基线本身就买不起则按余数从小到大削减**净买入腿**（down-round）
/// 到现金终值 ≥ 0。削减只碰净买入腿——削卖出腿等于卖得比权重要求的还多，那是另一
/// 种失真。
///
/// 现金终值口径：`cash0 + Σ 本日真能到手的卖出净额 − Σ 买入名义额与费用`，其中
/// cash0 = 提交前的可用现金（应收不可动用）。
///
/// **卖出回款按「今天真能卖掉多少」计**，不是按整条 SELL 腿的名义额：pre_qty 含在途
/// 新股（不可交割）、停牌/跌停腿今天一股卖不出、参与率上限还会再截一刀。把这些都当
/// 现金算，投影就会拿卖不掉的钱去给 BUY 腿补手数，落账口再被可负担截断打回来——
/// 补出来的那一手在报告上表现为「投影加了仓、实际没买到」。
/// 买入侧则按整腿名义额计（不因参与率少买而放宽），保持判据单向保守。
///
/// 费用走与落账同一条 `breakdown_fee` 路径。前提到此收窄为：**投影认为买得起 ⇒
/// 落账口买得起**（反向不保证——投影可能比落账更保守）；T-1→T 的价差窗口仍由落账口
/// 的可负担截断兜底。
///
/// @note `min_lot == 1` 时 up-round 整体惰性：engine 的 `round_to_min_lot_fp` 对
///       min_lot ≤ 1 是直通，基线 ≡ 原始目标 ⇒ 全部余数为 0 ⇒ 没有可补的手。
///       这是量纲规则的推论（见「数量规则模型」章的整手截断定义），不是现金约束
///       把补手拒掉了。down-round 仍可能触发（基线本身就超现金时）。
void project_targets_fee_aware(SelRunState& st,
                               const std::vector<xt_engine_sel::SelTargetProjectionEntry>& plan,
                               std::vector<int64_t>& targets) {
    if (plan.empty() || targets.size() != plan.size()) return;
    // 计数先落局部：engine 对抛出的投影整批还原基线，计数若已提交就会描述一个
    // 没有发生过的投影（`projection_rejected_entries` 与 `lots_added` 同时非零）。
    size_t local_lots_added = 0;
    size_t local_lots_removed = 0;
    size_t local_infeasible = 0;

    const int64_t cash0 = st.infra.account->snapshot_account().available_cash;

    // 本日该 code 真能卖出的股数上界（在途不可交割 → 停牌/跌停 → 参与率 → 整手）。
    // 末尾的 min_lot 截断必须与落账口（cap_fill_to_participation 后的
    // floor_to_min_lot_fp）同规则：参与率额度不足一手时落账侧一股不成，投影若按
    // 零整卖量计回款，就是把收不到的钱分给了 BUY 腿（字典序饥饿从后门回来）。
    const auto sell_cap_fp = [&st](const std::string& code, int64_t pre_qty_fp,
                                   int min_lot) -> int64_t {
        int64_t cap = pre_qty_fp - pending_shares_qty_fp(st, code);
        if (cap <= 0) return 0;
        if (st.current_tradability != nullptr) {
            const auto fit = st.current_tradability->find(code);
            if (fit != st.current_tradability->end() &&
                check_tradability(&fit->second, /*is_buy=*/false) !=
                    TradabilityVerdict::Ok) {
                return 0;  // 停牌 / 跌停：今天这腿一股换不成现金
            }
        }
        if (st.exec_has_volume && st.exec_day_volume != nullptr) {
            const auto vit = st.exec_day_volume->find(code);
            if (vit != st.exec_day_volume->end()) {
                const int64_t allowance =
                    xt_fp::mul(vit->second, st.exec_max_participation_fp);
                const auto uit = st.exec_participation_used_fp.find(code);
                const int64_t used = uit != st.exec_participation_used_fp.end()
                    ? uit->second : 0;
                const int64_t left = allowance > used ? allowance - used : 0;
                if (left < cap) cap = left;
            }
        }
        return floor_to_min_lot_fp(cap, min_lot);
    };

    // 单腿对现金终值的贡献：卖出记 min(|diff|, 本日可卖) 的净额，买入记整腿支出。
    const auto term_of = [&st, &plan, &sell_cap_fp](size_t i, int64_t target_fp) -> int64_t {
        const auto& e = plan[i];
        const int64_t diff = target_fp - e.pre_qty_fp;
        if (diff == 0) return 0;
        if (diff > 0) {
            return -xt_fp::mul(e.price_fp, diff) -
                   leg_fee_fp(st, e.code, e.price_fp, diff, /*is_buy=*/true,
                              /*is_close=*/false);
        }
        int64_t sell_fp = -diff;
        const int64_t cap = sell_cap_fp(e.code, e.pre_qty_fp, e.min_lot);
        if (sell_fp > cap) sell_fp = cap;
        if (sell_fp <= 0) return 0;
        return xt_fp::mul(e.price_fp, sell_fp) -
               leg_fee_fp(st, e.code, e.price_fp, sell_fp, /*is_buy=*/false,
                          /*is_close=*/e.pre_qty_fp > 0);
    };

    std::vector<int64_t> terms(plan.size(), 0);
    int64_t cash = cash0;
    for (size_t i = 0; i < plan.size(); ++i) {
        terms[i] = term_of(i, targets[i]);
        cash += terms[i];
    }

    // 一手的 fp 步长（min_lot ≤ 1 时 = 1 股）。
    const auto lot_fp_of = [&plan](size_t i) {
        return static_cast<int64_t>(plan[i].min_lot > 1 ? plan[i].min_lot : 1) *
               kPriceFpScale;
    };
    // 余数 = 原始目标 − 整手基线，恒在 [0, lot)：最大余数法的排序键。
    const auto remainder_of = [&plan](size_t i) {
        const int64_t r = plan[i].target_qty_raw_fp - plan[i].target_qty_fp;
        return r > 0 ? r : 0;
    };

    // 一步提交：改写 targets[i] 并同步现金终值。
    const auto apply_step = [&](size_t i, int64_t new_target) {
        const int64_t new_term = term_of(i, new_target);
        cash += new_term - terms[i];
        terms[i]   = new_term;
        targets[i] = new_target;
    };

    std::vector<size_t> order(plan.size());
    for (size_t i = 0; i < plan.size(); ++i) order[i] = i;

    // up-round：余数降序（并列按 code 升序 —— plan 已是字典序，用下标即可定序）。
    std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return remainder_of(a) > remainder_of(b);
    });
    for (const size_t i : order) {
        if (remainder_of(i) <= 0) continue;   // 余数已排序，但零余数条目仍在尾部
        const int64_t cand = targets[i] + lot_fp_of(i);
        const int64_t delta = term_of(i, cand) - terms[i];
        if (cash + delta < 0) continue;       // 这一手买不起，换下一个
        apply_step(i, cand);
        ++local_lots_added;
    }

    // down-round：基线本身就超现金时按余数升序削净买入腿（削到 pre 为止，
    // 不把买腿削成卖腿）。
    if (cash < 0) {
        std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            return remainder_of(a) < remainder_of(b);
        });
        for (const size_t i : order) {
            if (cash >= 0) break;
            const int64_t lot_fp = lot_fp_of(i);
            while (cash < 0 && targets[i] - lot_fp >= plan[i].pre_qty_fp) {
                apply_step(i, targets[i] - lot_fp);
                ++local_lots_removed;
            }
        }
        if (cash < 0) {
            ++local_infeasible;
            XtLogger::warn_m("SEL_BT",
                "date {}: 整数投影削完全部净买入腿仍现金为负（{:.2f}）—— 组合目标"
                "超出可用现金，剩余缺口由落账口的可负担截断兜",
                st.current_date, xt_fp::from_fp(cash));
        }
    }

    // 走到这里 = 投影整批完成、engine 会采纳写回值；此刻才提交计数。
    ++st.exec_projection_fires;
    st.exec_projection_lots_added        += local_lots_added;
    st.exec_projection_lots_removed      += local_lots_removed;
    st.exec_projection_infeasible_fires  += local_infeasible;
}

/// @brief 解析数据湖根 + 加载 trading dates/holiday provider + close_prices.csv
///        + tradability csv + corporate actions csv（后两者可选，配置了路径才加载）。
/// @return 0 成功；1 = 无可用 trading dates / tradability / CA csv 加载失败（fail-fast）。
int load_data(const XtSelRunnerConfig& cfg, SelRunState& st) {
    // ── 1) 解析数据湖根 + 加载 trading dates + holiday provider ──────────────
    const std::string datalake_root = resolve_datalake_root(cfg.datalake_root);
    st.loaded = XtSelDataLoader::load(cfg.trading_dates_csv,
                                        datalake_root,
                                        cfg.date_from, cfg.date_to);
    if (st.loaded.trading_dates.empty()) {
        XtLogger::error_m("SEL_BT",
                           "no trading dates resolved (csv='{}', datalake='{}')",
                           cfg.trading_dates_csv, datalake_root);
        return 1;
    }
    XtLogger::info_m("SEL_BT",
                      "trading dates: {} ({} → {}, source={})",
                      st.loaded.trading_dates.size(),
                      st.loaded.trading_dates.front(), st.loaded.trading_dates.back(),
                      st.loaded.source);

    // ── 2) 加载 close_prices.csv（sim 撮合定价用；可选 volume 列 → 参与率上限）──
    {
        auto table = read_close_prices_csv(cfg.close_prices_csv);
        if (!table.error.empty()) {
            // 坏 volume 行 fail-fast（与 CA 坏行同惯例）：静默丢弃会把「数据坏了」
            // 伪装成「这只票流动性极低」，方向恰与参与率上限一致、肉眼不可辨。
            XtLogger::error_m("SEL_BT",
                               "close_prices_csv 解析失败 (fail-fast): {}", table.error);
            return 1;
        }
        st.closes_by_date = std::move(table.closes);
        st.exec_volumes_fp = std::move(table.volumes_fp);
        st.exec_has_volume = table.has_volume;
    }
    if (st.closes_by_date.empty()) {
        XtLogger::warn_m("SEL_BT",
                          "close_prices_csv 空或未配置 ({}); sim 撮合将无法定价 → diff 计算 skip",
                          cfg.close_prices_csv);
    } else {
        XtLogger::info_m("SEL_BT",
                          "close_prices loaded: {} dates (volume column: {})",
                          st.closes_by_date.size(), st.exec_has_volume ? "yes" : "no");
    }
    // 执行真实度面开启判据：volume 列在场（参与率上限有据可依）**或** yaml 写了
    // `execution:` 节（投影口径/参数需要在产物里回显）。两者皆无 → 整段惰性，
    // 产物集合与本特性落地前逐位一致。
    st.exec_enabled = st.exec_has_volume || cfg.execution_configured;
    st.exec_max_participation_fp = xt_fp::to_fp(cfg.max_participation);
    st.exec_carry_max_days = cfg.carry_max_days;
    if (cfg.execution_configured && !st.exec_has_volume) {
        XtLogger::warn_m("SEL_BT",
            "execution: 已配置但 close_prices.csv 无 volume 列 —— 参与率上限"
            "（max_participation={}）无数据可依，本跑不 cap 成交量；整数投影口径不受影响",
            cfg.max_participation);
    }

    // ── 3) tradability CSV(可选)。close_prices 缺失只 warn,tradability 是正确性
    //    输入——配置了路径但打不开/0 行 → fail-fast,静默失真不可接受。
    if (!cfg.tradability_csv.empty()) {
        std::string terr;
        if (!st.tradability.load(cfg.tradability_csv, terr)) {
            XtLogger::error_m("SEL_BT",
                               "tradability_csv load failed (fail-fast): {}", terr);
            return 1;
        }
        // 覆盖度观测:多数行情源停牌日直接缺行而非 is_open=0,覆盖度低说明
        // panel 生成不全、gate 在静默失效(fail-open 语义下仅观测不改行为)。
        size_t covered = 0;
        for (const uint32_t d : st.loaded.trading_dates) {
            if (st.tradability.find_date(d) != nullptr) ++covered;
        }
        XtLogger::info_m("SEL_BT",
                          "tradability loaded: {} rows / {} dates; coverage {}/{} trading dates",
                          st.tradability.size(), st.tradability.date_count(),
                          covered, st.loaded.trading_dates.size());
    }

    // ── 4) corporate actions CSV（可选）。与 tradability 同为正确性输入，但坏行
    //    一律 fail-fast（CA 直接改钱与持仓，静默跳过一笔分红即账本失真）。
    if (!cfg.corporate_actions_csv.empty()) {
        std::string cerr_msg;
        const CaLoadStatus status = st.corporate_actions.load(
            cfg.corporate_actions_csv, cfg.allow_skip_rights, cerr_msg);
        if (status != CaLoadStatus::kOk) {
            XtLogger::error_m("SEL_BT", "corporate_actions_csv load failed: {}", cerr_msg);
            return 1;
        }
        st.corporate_actions.align_to_trading_dates(st.loaded.trading_dates, st.ca_align);
        st.ca_enabled            = true;
        st.ca_dividend_tax_rate  = cfg.dividend_tax_rate;
        st.ca_delist_haircut     = cfg.delist_haircut_pct;
        st.ca_rights_policy      = cfg.rights_policy;

        // record 日收盘仓位快照排程：确权股权数（现金分红 / 送转 / 配股同一口径，见
        // `ca_record_qty_fp`）在 record 日收盘锁定，不能读 ex 日仓位（ex 日已含当日
        // 调仓，会多记/少记）。
        //   record_date == 0（未提供 / 早于窗首）→ 不排：施加点退化取 ex 日初仓。
        //   record_date == ex_date → 不排：该日快照在 EOD 才生成，而施加点在日初，
        //     排了永不读（死排程）；同样退化取 ex 日初仓 + 计数披露。
        for (const auto& ev : st.corporate_actions.rows()) {
            if (ev.is_delist || ev.rights_skipped || !ev.conversion_to.empty()) continue;
            if (ev.cash_per_share <= 0.0 && ev.bonus_transfer_per_share <= 0.0 &&
                ev.rights_per_share <= 0.0) {
                continue;
            }
            if (ev.record_date == 0 || ev.record_date >= ev.ex_date) continue;
            st.ca_record_needs[ev.record_date].push_back(ev.code);
        }
        XtLogger::info_m("SEL_BT",
                          "corporate actions loaded: {} in-window events (rebased {}, "
                          "ignored out-of-window {}, rights skipped {}); dividend_tax_rate={} "
                          "delist_haircut_pct={} rights_policy={}",
                          st.corporate_actions.size(),
                          st.ca_align.rebased_to_prev_trading_date,
                          st.ca_align.ignored_before_window + st.ca_align.ignored_after_window,
                          st.corporate_actions.rights_skipped_count(),
                          cfg.dividend_tax_rate, cfg.delist_haircut_pct,
                          rights_policy_name(cfg.rights_policy));
    }
    return 0;
}

/// @brief external/plugin 两分支共用的单策略装配段：init_cfg 构建 → strat->init
///        → ctx_cfg(含 on_weights_fn) → on_init → engine.add_context →
///        strategy_ids 登记。
/// @param strat 已就绪的策略实例；所有权由 caller 处理（external 归注入方，
///        plugin 由 caller push 进 st.strategies）。
/// @return add_context 返回的 idx；SIZE_MAX = add_context 失败（caller 返 rc=5）。
size_t assemble_one_strategy(SelRunState& st, const SelStrategyCfg& scfg,
                             XtStrategyBase* strat,
                             xt_engine_sel::XtSelEngine* engine_p) {
    auto init_cfg = std::make_unique<XtStrategyInitConfig>();
    if (scfg.params && scfg.params.IsMap()) {
        XtConfigYamlUtil::flatten_yaml_node_to_map(scfg.params, "", init_cfg->values);
    }
    strat->init(*init_cfg);
    st.init_cfgs.push_back(std::move(init_cfg));

    XtStrategyContext::Config ctx_cfg;
    ctx_cfg.strategy    = strat;
    ctx_cfg.init_config = st.init_cfgs.back().get();
    ctx_cfg.strategy_id = scfg.strategy_id;
    ctx_cfg.on_weights_fn =
        [engine_p](const std::string& sid,
                   const std::unordered_map<std::string, double>& weights) {
            engine_p->on_target_weights(sid, weights);
        };

    auto ctx = std::make_unique<XtStrategyContext>(ctx_cfg);
    // fail-closed：backtest 任一策略 on_init 失败 → 不装配、不产出报表产物，run 失败退出。
    try {
        strat->on_init(ctx.get());
    } catch (const std::exception& e) {
        XtLogger::error_m("SEL_BT",
            "strategy '{}' on_init failed (fail-closed): {} — aborting run",
            scfg.strategy_id, e.what());
        return SIZE_MAX;
    } catch (...) {
        XtLogger::error_m("SEL_BT",
            "strategy '{}' on_init failed (fail-closed, non-std exception) — aborting run",
            scfg.strategy_id);
        return SIZE_MAX;
    }

    const size_t idx = st.engine->add_context(std::move(ctx), strat);
    if (idx == SIZE_MAX) return SIZE_MAX;
    st.strategy_ids.insert(scfg.strategy_id);
    return idx;
}

/// @brief 装配 infra/gateway/observer/engine/resolver/task/strategy 全链路。
/// @param external_strategy 非空时 cfg.strategies[0] 直接用该实例（跳过 plugin
///        factory；所有权不接管，caller 保生命周期跨 run）。
/// @return 0 成功；3/4/5/6 对应 strategy 装配(register 失败/so 空且无 external/
///         external 多 strategy)/create/add_context/task 匹配失败。
int assemble_sel_runtime(const XtSelRunnerConfig& cfg, SelRunState& st,
                         XtStrategyBase* external_strategy) {
    // ── 3) 装配 BacktestInfra（account / risk / fee / commodity / order_book / cost）
    XtCtaBacktestOrderGateway::Config gw_cfg;
    gw_cfg.commodities          = cfg.commodities_cfg;
    gw_cfg.risk                 = cfg.risk_cfg;
    gw_cfg.fees                 = cfg.fee_cfg;
    gw_cfg.cost                 = cfg.trade_cost_cfg;
    gw_cfg.account.initial_cash = cfg.initial_cash;
    gw_cfg.account.futures_mode = false;  // A 股股票模式（margin_rate=0 全额）
    // A 股 T+1：风控的 enforce_long_t_plus_1 需要账本真维护今昨仓拆分才有效
    // （否则 long_today 恒 0 → yesterday 口径恒等全仓 → 检查恒放行）。
    // 主循环每日 session_begin 事件负责滚动 today 计数。
    gw_cfg.account.t1_enabled   = cfg.risk_cfg.enforce_long_t_plus_1;
    st.t1_enabled               = cfg.risk_cfg.enforce_long_t_plus_1;
    st.infra = build_backtest_infra(gw_cfg);
    // SEL backtest 默认 A 股 venue → evaluate_risk_ 推断
    // eval_ctx.is_in_call_auction 真填，集合竞价时段 IOC/FOK 单 reject。GTC 不影响。
    st.infra.gw->set_venue_kind(xt_core::XtVenueKind::AShare);

    // trade_capture 不再挂 gateway fan：fan 在 account apply 之前触发，据此采集只能
    // 事后回填权益。改由 runner 在 apply_fill 内（真值点）直接构造报告行。
    // frozen_release 仍挂 fan —— SEL match engine 的成交路径不 publish order_update，
    // 该 observer 在 on_trade/on_order_update 内 account.apply 释放 frozen，是成交路径上
    // 唯一的 frozen 释放与 book slot 清理点，不可摘除。
    st.listener = std::make_unique<XtBacktestTradeCapture>(st.captured_trades);
    st.frozen_release = std::make_unique<SelFrozenReleaseObserver>(
        st.infra.account.get(), st.infra.order_book.get());

    st.infra.gw->bind(st.infra.account.get(),
                    st.infra.risk_gateway.get(),
                    st.infra.risk_guard_ctx.get(),
                    st.infra.cost_applier.get(),
                    st.infra.order_book.get());
    st.infra.gw->add_observer(st.frozen_release.get());
    // 真风控拒因只经 IRejectSink 出现一次：SubmitResult 在整单被拒时恒返 Unknown
    // （提交入口不透出 reason），产物的 reject_code 列不接这里就只能写 Unknown。
    st.infra.gw->bind_reject_sink(&st.reject_capture);

    // ── 5) 装配 SelTaskScheduler + XtSelEngine + 三 resolver ──────────────────
    auto scheduler = std::make_unique<xt_engine_sel::SelTaskScheduler>(st.loaded.holiday_provider.get());
    // 观测装饰器：记录 (task, date) 实际 fire，供 schedule_fires.csv 区分
    // 「未触发」与「触发但未设权重」。
    auto recording = std::make_unique<RecordingSelScheduler>(std::move(scheduler));
    st.scheduler_probe = recording.get();

    xt_engine_sel::XtSelEngine::Config engine_cfg;
    engine_cfg.scheduler        = std::move(recording);
    st.holiday_probe            = st.loaded.holiday_provider.get();  // move 前留观测指针
    engine_cfg.holiday_provider = std::move(st.loaded.holiday_provider);

    // resolvers — engine 用 close 价 + account 真做 diff。
    auto* st_res    = &st;
    auto* account_p = st.infra.account.get();
    engine_cfg.price_resolver_fn =
        [st_res](const std::string& code) -> int64_t {
            auto it = st_res->current_closes_fp.find(code);
            if (it == st_res->current_closes_fp.end()) {
                // 首日无 T-1 探针：策略在无 T-1 价的首日请求 sizing → 记 miss，
                // run_sel_loop 在 fire 后 fail-fast（拒绝退回 T 日 close lookahead）。
                if (st_res->first_day_sizing_probe_active) {
                    ++st_res->first_day_sizing_misses;
                }
                return 0;
            }
            return it->second;
        };
    // sizing 的持仓口径必须与权益口径同源：equity_resolver 走 portfolio_equity，
    // 其中含在途新股市值；持仓若只读已到账仓，ex 日调仓的 target 分子含在途股价值、
    // pre_qty 不含其股数 → diff 把「已经持有、只是还没交割」的那部分再买一遍
    //（十送十 + 目标 50% 会买成 75%）。在途新股就是同一敞口，故并入 pre_qty。
    // 现金应收不并——它不是任何 symbol 的敞口（「钱在路上不缩目标」只对现金成立）。
    engine_cfg.position_resolver_fn =
        [account_p, st_res](const std::string& code) -> int64_t {
            const auto pos = account_p->snapshot_position(code);
            return pos.long_qty - pos.short_qty + pending_shares_qty_fp(*st_res, code);
        };
    // sizing 分母 = portfolio 权益（含应收）：ex→pay / ex→list 窗口内不该因「钱在
    // 路上」缩减目标（缩了下期又要买回，凭空造换手）。可负担性由落账口的现金截断守。
    engine_cfg.equity_resolver_fn =
        [st_res]() -> int64_t {
            return portfolio_equity_fp(*st_res);
        };
    // min_lot resolver 注入 → engine 进入 round-target 模式。
    // 无 catalog 时 fall back min_lot=1，engine 退化为 round-delta。
    {
        const auto* commodities_q = st.infra.commodities.get();
        engine_cfg.min_lot_resolver_fn =
            [commodities_q](const std::string& code) -> int {
                if (!commodities_q) return 1;
                const auto* info = commodities_q->find(code.c_str());
                return (info && info->min_lot > 0) ? info->min_lot : 1;
            };
    }

    // ── 5b) 产物 sink（BT-only；live 不注入 → engine 侧零调用）─────────────────
    //
    // fire_sink：每次 on_target_weights 一条，schedule_fires.csv 的行源。
    // target_decision_sink：每个 (fire, code) 决策点一条（含早退），
    //   target_weights.csv 的规划期列源；执行列由下方 handle_pos_change_fn 回填。
    {
        auto* st_sink = &st;
        engine_cfg.fire_sink = [st_sink](const xt_engine_sel::SelFireRecord& r) {
            // 决策 sink 先于本回调到达（fire 记录在 on_target_weights 退出时才发），
            // 故这里补全 fire 级字段而不是新建行。
            SelFireAgg& agg = day_fire_agg(*st_sink, r.fire_seq, r.strategy_id);
            agg.task_id            = r.task_id;
            agg.equity_fp          = r.equity_fp;
            agg.equity_nonpositive = r.equity_nonpositive;
            agg.n_targets          = r.n_targets;
            // 一次 fire 是对该 sid 整个组合的重估：本轮决策集之外的 code 上残留的
            // carry 就此作废（`supersede` 只管本轮出了决策的 code）。fire_sink 在
            // on_target_weights 退出时才到，此刻当日缓冲已收齐本次 fire 的全部决策行。
            // 判据用「本次 fire 真发出过决策行」——equity<=0 / 策略 disabled 这类
            // 早退 fire 压根没重估过组合，不该据此作废在途意图。
            if (st_sink->exec_enabled) {
                std::unordered_set<std::string> decided;
                for (const auto& row : st_sink->day_decisions) {
                    if (row.fire_seq == r.fire_seq) decided.insert(row.code);
                }
                if (!decided.empty()) {
                    drop_stale_carry_for_sid(*st_sink, r.strategy_id, decided);
                }
            }
            emit_trace_fire(*st_sink, r);
        };
        engine_cfg.target_decision_sink =
            [st_sink](const xt_engine_sel::SelTargetDecision& d) {
                SelFireAgg& agg = day_fire_agg(*st_sink, d.fire_seq, d.strategy_id);
                agg.gross_weight += std::fabs(d.weight);
                if (d.diff_qty_fp > 0) ++agg.n_buys;
                if (d.diff_qty_fp < 0) ++agg.n_sells;
                if (d.diff_qty_fp != 0 && d.price_fp > 0) {
                    const int64_t abs_diff =
                        d.diff_qty_fp < 0 ? -d.diff_qty_fp : d.diff_qty_fp;
                    agg.turnover_fp += xt_fp::mul(abs_diff, d.price_fp);
                }

                if (d.status == xt_engine_sel::SelPlanningStatus::kMinLotZero) {
                    ++st_sink->min_lot_zero_targets;
                }
                // 新 fire 的目标覆盖该 code 上尚未成交的 carry 意图（diff =
                // target − actual 已含未成量，两边各提交一次就买成两份）。
                // 只认**算出了目标**的决策：kOk / kNoChange / kMinLotZero。
                // kNoPrice（停牌无价）与 kNegativeWeight 既没算 target 也不会有替代
                // 订单，把 carry 记成「被取代」等于让意图凭空蒸发还谎报了去向；
                // 这两种早退的 carry 归属由 fire 收口时的 stale 丢弃统一裁决。
                if (st_sink->exec_enabled &&
                    (d.status == xt_engine_sel::SelPlanningStatus::kOk ||
                     d.status == xt_engine_sel::SelPlanningStatus::kNoChange ||
                     d.status == xt_engine_sel::SelPlanningStatus::kMinLotZero)) {
                    supersede_carry_for_code(*st_sink, d.strategy_id, d.code);
                    // carry 建腿点的过滤锚：同 (sid,code) 取当日最大已定目标 fire_seq。
                    auto& latest = st_sink->latest_decided_fire[
                        {d.strategy_id, d.code}];
                    if (d.fire_seq > latest) latest = d.fire_seq;
                }

                SelDecisionRow row;
                row.fire_seq          = d.fire_seq;
                row.code              = d.code;
                row.weight            = d.weight;
                row.price_fp          = d.price_fp;
                row.target_qty_raw_fp = d.target_qty_raw_fp;
                row.target_qty_fp     = d.target_qty_fp;
                row.pre_qty_fp        = d.pre_qty_fp;
                row.diff_qty_fp       = d.diff_qty_fp;
                row.planning          = d.status;
                st_sink->decision_index.emplace(
                    decision_key(d.fire_seq, d.code), st_sink->day_decisions.size());
                st_sink->day_decisions.push_back(std::move(row));
                emit_trace_target(*st_sink, d);
            };
    }

    // ── 6) handle_pos_change_fn → gateway.submit (整笔提交目标 delta)
    //
    // Engine 已把 target round 到 min_lot 整数倍并算出 diff_pos_fp,直接整笔提交。
    // 真提交口是 `submit_target_leg`（fire 派发与 carry 复投共用），本回调只补
    // 「当前正在派发哪次 fire」这一个 engine 侧才知道的身份。
    auto* st_p = &st;   // tradability gate 状态(SelRunState 外存活期覆盖 engine)
    engine_cfg.handle_pos_change_fn =
        [st_p](const std::string& sid,
               const std::string& code,
               int64_t diff_pos_fp,
               int64_t price_fp,
               XtTimeInForce tif,
               const std::string& tag) {
            const uint64_t fire_seq =
                st_p->engine ? st_p->engine->current_fire_seq() : 0;
            (void)submit_target_leg(*st_p, sid, code, diff_pos_fp, price_fp, tif, tag,
                                    fire_seq);
        };

    // ── 6b) 整数投影 hook（B7e F09）：仅 `execution.integer_projection:
    //     largest_remainder` 时注入；未注入 = engine 的 floor-to-lot 基线即终值。
    if (cfg.projection_largest_remainder) {
        engine_cfg.target_projection_fn =
            [st_p](const std::vector<xt_engine_sel::SelTargetProjectionEntry>& plan,
                   std::vector<int64_t>& targets) {
                project_targets_fee_aware(*st_p, plan, targets);
            };
    }

    // sparse-weights 观测护栏（T1.135）：当前持仓 symbol 数 = traded_codes 中净仓非零者。
    // on_target_weights 在 on_minute_tick 内（当日 fill 之前）调用，读到的是上一交易日
    // 收盘持仓，正是与本次 weights 覆盖度对比的口径。
    // 集合口径 resolver：护栏可检出「等数量换集合」型稀疏（数量口径漏检）。
    engine_cfg.held_symbols_resolver_fn =
        [account_p, st_p]() -> std::vector<std::string> {
            std::vector<std::string> held;
            for (const auto& code : st_p->traded_codes) {
                if (account_p->snapshot_position(code).qty != 0) held.push_back(code);
            }
            return held;
        };

    st.engine = std::make_unique<xt_engine_sel::XtSelEngine>(std::move(engine_cfg));

    // ── 7) 注册 SEL tasks ────────────────────────────────────────────────────
    for (const auto& tcfg : cfg.tasks) {
        xt_engine_sel::SelTaskInfo info;
        info.name        = tcfg.strategy_id + "_task";
        info.strategy_id = tcfg.strategy_id;
        info.period      = tcfg.period;
        info.day         = tcfg.day;
        info.time        = tcfg.time;
        info.trdtpl      = tcfg.trdtpl;
        info.session     = tcfg.session;
        info.strict_time = tcfg.strict_time;
        const uint32_t task_id = st.engine->add_task(info);
        st.task_ids.push_back(task_id);  // 与 cfg.tasks 平行；schedule_fires 归因用
        XtLogger::info_m("SEL_BT",
                          "registered task: id={} sid={} period={} day={} time={}",
                          task_id, tcfg.strategy_id,
                          static_cast<int>(tcfg.period), tcfg.day, tcfg.time);
    }

    // ── 8) strategy 装配：external 注入 或 plugin (.so) 加载 ──────────────────
    if (cfg.strategies.empty()) {
        // run() 可被直接调用（测试/嵌入方手构 cfg），parse 层校验兜不住这里。
        XtLogger::error_m("SEL_BT", "cfg.strategies is empty, nothing to assemble");
        return 3;
    }
    {
        const auto base = std::filesystem::path(cfg.cfg_path).parent_path().string();
        if (!base.empty()) st.factory.set_base_path(base);
    }
    // init_cfgs（unique_ptr 稳指针，声明见 SelRunState）：直接放 vector<value>
    // 会在 push_back 扩容时悬空先前所有 ctx 的 init_config_ 指针。

    auto* engine_p = st.engine.get();
    if (external_strategy != nullptr) {
        // external 注入：跳过 register_plugin/create_managed；装配段与 plugin
        // 路径共用 assemble_one_strategy（缺 init_config 会让 ctx.get_param_*
        // 恒走默认值，策略参数静默丢失）。
        if (cfg.strategies.size() > 1) {
            XtLogger::error_m("SEL_BT",
                               "external strategy supports a single cfg.strategies entry, got {}",
                               cfg.strategies.size());
            return 3;
        }
        const auto& scfg = cfg.strategies[0];
        if (!scfg.lib_path.empty()) {
            XtLogger::warn_m("SEL_BT",
                              "external strategy 优先, 忽略 so='{}' (id={})",
                              scfg.lib_path, scfg.strategy_id);
        }
        const size_t idx = assemble_one_strategy(st, scfg, external_strategy, engine_p);
        if (idx == SIZE_MAX) {
            XtLogger::error_m("SEL_BT", "engine.add_context failed (external)");
            return 5;
        }
        // st.strategies 不入：external 实例所有权归 caller。
        XtLogger::info_m("SEL_BT", "loaded external strategy: id={} idx={}",
                          scfg.strategy_id, idx);
    } else {
        for (const auto& scfg : cfg.strategies) {
            if (scfg.lib_path.empty()) {
                // so 允许空仅服务 external 注入（run(cfg, strat)）；无 external 时
                // 此处具名 fail-fast，不落 factory 通用 register 错误。
                XtLogger::error_m("SEL_BT",
                                   "strategy.so empty and no external strategy: id='{}'",
                                   scfg.strategy_id);
                return 3;
            }
            if (!st.factory.register_plugin(scfg.strategy_id, scfg.lib_path)) {
                XtLogger::error_m("SEL_BT",
                                   "register_plugin failed: id='{}' so='{}' err={}",
                                   scfg.strategy_id, scfg.lib_path, st.factory.last_error());
                return 3;
            }
            auto strat = st.factory.create_managed(scfg.strategy_id);
            if (!strat) {
                XtLogger::error_m("SEL_BT", "create_managed failed: id='{}' err={}",
                                   scfg.strategy_id, st.factory.last_error());
                return 4;
            }
            const size_t idx = assemble_one_strategy(st, scfg, strat.get(), engine_p);
            if (idx == SIZE_MAX) {
                XtLogger::error_m("SEL_BT", "engine.add_context failed");
                return 5;
            }
            st.strategies.push_back(std::move(strat));
            XtLogger::info_m("SEL_BT", "loaded strategy: id={} so={} idx={}",
                              scfg.strategy_id, scfg.lib_path, idx);
        }
    }

    for (size_t i = 0; i < cfg.tasks.size(); ++i) {
        if (st.strategy_ids.find(cfg.tasks[i].strategy_id) == st.strategy_ids.end()) {
            XtLogger::error_m(
                "SEL_BT",
                "task[{}].strategy_id='{}' does not match any loaded strategy",
                i, cfg.tasks[i].strategy_id);
            return 6;
        }
    }
    return 0;
}

// ── B0 产物写出 ────────────────────────────────────────────────────────────

/// @brief 打开四个流式产物 writer（.tmp）；任一失败返 false（调用方 rc=9）。
/// @note 列集是 viewer 侧消费契约，改列即破 schema。
[[nodiscard]] bool open_artifact_writers(const XtSelRunnerConfig& cfg, SelRunState& st) {
    if (!st.fires_csv.open(cfg.report_dir, "schedule_fires.csv",
            "exec_date,planned_date,planned_time,strategy_id,task_uid,revision,"
            "fire_seq,fired,skip_reason,n_targets,n_buys,n_sells,turnover_value,"
            "gross_weight\n")) {
        return false;
    }
    // gate 列取值：`ok` | tradability 三态（`suspended` / `limit_up` / `limit_down`）
    // | `delisted`（退市闩锁拒买） | `sell_fee_unaffordable`（卖出费用超现金+所得）。
    // 后四者都在到 gateway 之前拒，故 order_id/submitted_qty/accepted/reject_code 留空。
    if (!st.targets_csv.open(cfg.report_dir, "target_weights.csv",
            "exec_date,fire_seq,code,weight,target_qty_raw,target_qty,pre_qty,"
            "diff_qty,planning_status,gate,phase,order_id,submitted_qty,accepted,"
            // reject_code 列语义：受理成功写 NoError；本地被拒时写真风控拒因的
            // XtRejectCode 名（gateway 的 SubmitResult 本身只给 Unknown，真拒因经
            // IRejectSink 捕获后映射，见 reject_code_from_risk_reason）。tradability
            // gate 拒的腿没到 gateway，该列留空。
            "reject_code,filled_qty,post_qty\n")) {
        return false;
    }
    // 单一 schema：long_today / available_sell 两列恒在，T+1 未启用时留空（账本不
    // 维护今昨仓拆分，写 0 会被读成「今仓真为 0」）。列集随 config 二选一会让
    // 消费方要按 header 分支，viewer/producer 侧无从预期。
    if (!st.positions_csv.open(cfg.report_dir, "daily_positions.csv",
            "date,code,qty,close,mark_is_stale,market_value,weight_actual,"
            "long_today,available_sell\n")) {
        return false;
    }
    if (!st.costs_csv.open(cfg.report_dir, "fill_costs.csv",
            "order_id,exec_date,code,direction,qty,exec_price,commission,"
            "stamp_duty,transfer_fee,total_fee\n")) {
        return false;
    }
    // CA events 严格 gated：未配置 corporate_actions_csv 时文件不创建（未配置时
    // 产物集合与 B0 逐位一致）。
    if (st.ca_enabled &&
        !st.ca_events_csv.open(cfg.report_dir, "corporate_action_events.csv",
            "ex_date,code,type,qty_before,qty_after,cash_delta,"
            "ref_price_before,ref_price_after\n")) {
        return false;
    }
    // events_full.csv 同为严格 gated（trace_mode: off 时文件不创建）。日内基准时刻
    // 取 run 的 fire 时刻，事件 ts 在其上按序号 +1ms。
    // @note `tasks.front().time` 是**单 task** 前提下的 run 级 fire 时刻（当前 yaml
    //       只解析一个 task，见 XtSelRunnerConfig）。多 task 落地后这里要按事件所属
    //       task 取各自时刻，否则不同 task 的事件会挤在同一条日内时间轴上。
    if (cfg.trace_full &&
        !st.trace.open(cfg.report_dir, cfg.tasks.empty() ? 1500 : cfg.tasks.front().time)) {
        return false;
    }
    return true;
}

/// @brief 追加一行 corporate_action_events.csv。
///
/// `type` ∈ `cash_div`（ex 日现金分红即时入账）| `receivable_created`（ex 日现金分红
/// 转应收，pay 日才入现金）| `pay_cash`（pay 日应收转现金）| `bonus_transfer`（ex 日
/// 送转：除权 + 均价调整，qty 变动仅在无到账滞后时同点发生）|
/// `rights_subscribed`（ex 日配股缴款认购，`cash_delta` 为负 = 缴款额）|
/// `rights_cash_capped`（现金一股都缴不起 = 事实全额弃权，qty/cash_delta 皆 0）|
/// `rights_forfeited`（弃权：只除权不获配）| `rights_skipped`（整行跳过）|
/// `shares_listed`（新股到账日在途股转真实持仓）| `pending_bonus_liquidated`（退市/
/// 换股时在途新股并入持仓）| `conversion_out`（换股：旧 code 清零，`cash_delta` =
/// 零股现金替代）| `conversion_in`（换股：新 code 增仓）| `delist_liquidation`。
/// @note `conversion_out` / `conversion_in` 恒**成对**出现，即使旧仓为 0（此时两行
///       qty 全 0）：目标 code 只在 in 行的 code 列上，单发 out 的话消费方不知道
///       换成了什么。
/// @note `cash_delta` 是**本行**导致的资产变动额，不可跨 type 求和：
///       {cash_div + receivable_created} 之和 = 分红确权净额；
///       {cash_div + pay_cash} 之和 = 实际入账现金。
/// @note `qty_before` 按 type 分两种语义，不是统一的「变动前持仓」：
///       - `cash_div` / `receivable_created` 写**确权股数**（record 日收盘仓，可能
///         ≠ 当前持仓——这正是该列的用处：分红基数可审计）；
///       - 其余 type 写变动前的真实 long_qty（`pay_cash` 前后同值，只动现金）。
void emit_ca_event_row(SelRunState& st, uint32_t date, const std::string& code,
                       const char* type, int64_t qty_before_fp, int64_t qty_after_fp,
                       int64_t cash_delta_fp, int64_t ref_before_fp, int64_t ref_after_fp) {
    if (!st.ca_events_csv.is_open()) return;
    auto& out = st.ca_events_csv.out();
    char buf[256];
    int n = std::snprintf(buf, sizeof(buf), "%u,", date);
    if (n > 0) out.write(buf, n);
    out << csv_escape_field(code) << ',' << type << ',';
    n = std::snprintf(buf, sizeof(buf), "%.4f,%.4f,%.2f,%.4f,%.4f\n",
                      order_exec_from_fp(qty_before_fp),
                      order_exec_from_fp(qty_after_fp),
                      xt_fp::from_fp(cash_delta_fp),
                      xt_fp::from_fp(ref_before_fp),
                      xt_fp::from_fp(ref_after_fp));
    if (n > 0) out.write(buf, n);
    st.ca_events_csv.add_row();
}

/// @brief 追加一行 fill_costs.csv（真值点：fee 已由 cost model 写进成交记录）。
///
/// @note `total_fee` 恒等于 trades.csv 的 fee（同一笔的落账值），不是「按公式再算
///       一次」的结果：退市清算把 fee 压到清算所得时（`delist_fee_capped`），公式值
///       会高于落账值，此时分项按同一比例缩到落账 total 上——否则 fill_costs 与
///       trades/账本对不上账，成本审计面出现只在这一支上成立的分叉。
void emit_fill_cost_row(SelRunState& st, const XtTradeRecord& tr) {
    if (!st.costs_csv.is_open()) return;
    const XtCommodityInfo* comm = st.infra.commodities
        ? st.infra.commodities->find(tr.code.c_str()) : nullptr;
    // 基准价用已落账的 exec_price（含滑点）→ 与 gateway 侧同一算术路径，
    // total_fee 与 trades.csv 的 fee 逐笔相等。
    auto bd = xt_fill_cost::breakdown_fee(tr.exec_price, tr,
                                          st.infra.fee_book.get(), comm);
    if (bd.total != tr.fee) {
        // 落账 fee 被调用点改写过（唯一形态 = 清算 fee cap）：分项等比缩，total 取真值。
        const double scale = bd.total != 0.0 ? tr.fee / bd.total : 0.0;
        bd.commission   *= scale;
        bd.stamp_duty   *= scale;
        bd.transfer_fee *= scale;
        bd.total         = tr.fee;
    }
    auto& out = st.costs_csv.out();
    char buf[256];
    int n = std::snprintf(buf, sizeof(buf), "%llu,%u,",
                          static_cast<unsigned long long>(tr.order_id), tr.trade_date);
    if (n > 0) out.write(buf, n);
    out << csv_escape_field(tr.code);
    n = std::snprintf(buf, sizeof(buf), ",%s,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f\n",
                      tr.is_buy ? "BUY" : "SELL",
                      order_exec_from_fp(tr.qty_fp), tr.exec_price,
                      bd.commission, bd.stamp_duty, bd.transfer_fee, bd.total);
    if (n > 0) out.write(buf, n);
    st.costs_csv.add_row();
}

/// @brief 单笔成交落账：account apply → traded_code → fill_costs 行 → 现金不变量
///        → 报告行（真值点构造，equity_after 即本笔落账后的 portfolio 权益）。
///
/// @note 报告行集合 ==「被 apply 的 fill」集合，是承重不变量：报告只描述真落账的
///       成交。撮合侧成交与本函数的落差只有资金可负担截断一处（见调用点）。
/// @note 合成清算单（退市）与撮合成交共用本函数——两者的账务/产物语义必须一致，
///       不允许清算走旁路绕开 fill_costs / 现金不变量 / trades.csv。
void apply_ledger_fill(SelRunState& st, const XtTradeRecord& tr, uint32_t date) {
    st.infra.account->apply(XtAccountEvent::trade_ex(
        tr.code.c_str(), tr.is_buy, tr.qty_fp,
        to_fp_round(tr.exec_price),
        to_fp_round(tr.fee),
        tr.is_close, tr.is_today));
    st.traded_codes.insert(tr.code);
    st.filled_by_order[tr.order_id] += tr.qty_fp;
    emit_fill_cost_row(st, tr);
    emit_trace_fill(st, tr);
    // cash ≥ 0 不变量（F03）：提交口的可负担截断应让透支结构上不可达；
    // 触发即说明截断口径与撮合实际成本脱节，必须留证据而不是静默透支。
    if (const int64_t cash_after = st.infra.account->snapshot_account().cash;
        cash_after < 0) {
        ++st.negative_cash_events;
        XtLogger::error_m("SEL_BT",
            "date {}: cash 落负 ({}) after fill order_id={} code='{}' "
            "qty={} price={} —— 可负担截断未拦住透支",
            date, xt_fp::from_fp(cash_after), tr.order_id, tr.code,
            order_exec_from_fp(tr.qty_fp), tr.exec_price);
    }
    (*st.listener)(XtBtFillTruth{
        tr,
        portfolio_equity_fp(st),
        /*realized_fp=*/0,   // SEL 引擎不逐笔算 realized，报告行也不消费该字段
        tr.trading_day != 0 ? tr.trading_day : date,
        tr.sim_visible_at_ns,
    });
}

// ── B0b corporate action 施加点 ─────────────────────────────────────────────

/// @brief 整股向下取整（fp）。
///
/// A 股送转对整数股保持整数；零碎股现实按现金补足，本模型不建模（单笔 < 1 股，
/// 量级可忽略），结果仍过 min_lot 卖出规则（零股可卖不可买）。
[[nodiscard]] int64_t floor_to_whole_shares_fp(int64_t v_fp) {
    if (v_fp <= 0) return 0;
    return (v_fp / kPriceFpScale) * kPriceFpScale;
}

/// @brief T-1 参考价（fp）：优先 price_date 的 close，缺失时退该 code 当前 mark
///        （= 最后可得 close）。两者皆无返回 0（无参考价，跳过价格修正）。
/// @param out_from_close_table 出参：true = 取自 `closes_by_date[price_date]` 真行；
///        false = 取自 carry 的 mark（该日**没有**这只票的 close 行）。修正后的参考价
///        只允许回写到本来就存在的行——凭空补一行会让 sizing 读到一个当日不存在的
///        价格（合成 T-1 价），把「当日无价跳过」变成「按合成价下单」。
[[nodiscard]] int64_t ca_prev_close_fp(const SelRunState& st, uint32_t price_date,
                                       const std::string& code,
                                       bool* out_from_close_table = nullptr) {
    if (out_from_close_table) *out_from_close_table = false;
    const auto dit = st.closes_by_date.find(price_date);
    if (dit != st.closes_by_date.end()) {
        const auto cit = dit->second.find(code);
        if (cit != dit->second.end() && cit->second > 0.0) {
            if (out_from_close_table) *out_from_close_table = true;
            return xt_fp::to_fp(cit->second);
        }
    }
    const int64_t mark = st.infra.account->snapshot_position(code).mark_price;
    return mark > 0 ? mark : 0;
}

/// @brief 确权股权数（现金分红与送转的**同一**口径）。
///
/// 按 record 日收盘持仓计；读 ex 日仓位会把 ex 日当天的调仓也算进去（多记/少记）。
/// 无快照可读时退 ex 日初（CA 施加前）持仓，并按成因分三路计数披露：
///   - `record_date` 未提供 → ex 日初仓 ≡ 前一交易日收盘仓，口径等价无风险；
///   - `record_date == ex_date` → 确权本该按 ex 日收盘仓（快照 EOD 才有，施加点在
///     日初），误差 = 该 code 当日调仓量；
///   - `record_date` 早于窗首 → 窗首后建的仓会拿到不该拿的分红（真过度计提）。
[[nodiscard]] int64_t ca_record_qty_fp(SelRunState& st, const SelCorporateAction& ev,
                                       int64_t ex_open_qty_fp) {
    if (ev.record_date != 0 && ev.record_date < ev.ex_date) {
        const auto it = st.ca_record_qty.find(
            std::to_string(ev.record_date) + '|' + ev.code);
        if (it != st.ca_record_qty.end()) return it->second;
    }
    if (ev.record_before_window)          ++st.ca_record_qty_fallback_before_window;
    else if (ev.record_date == ev.ex_date) ++st.ca_record_qty_fallback_ex_day;
    else                                   ++st.ca_record_qty_fallback_absent;
    return ex_open_qty_fp;
}

/// @brief 换股（吸收合并 / 证券变更）：ex 日旧仓按比例换成新 code。
///
/// 施加序：先把该 code 的在途新股并入真实持仓（否则 list 日会往一个已经消失的
/// symbol 上加仓），再整仓按 `conversion_ratio` 换算——阀下零股（不足整股的余数）
/// 按 `cash_in_lieu` 折现进现金。成本基准按「换到的整股 ÷ 应换股数」**按比例**迁到
/// 新股：零股腿视同处置（现金替代已入账），其份额的成本随它离场。换完旧 code 进
/// `ca_converted_codes` 闩锁：该证券已不存在，数据源若还留着它的 close 行，按它
/// 建仓就是在一只换掉的票上开仓；闩锁后落在同一 code 上的后续 CA 事件也一并拒绝
/// （见 `apply_corporate_actions`），否则它们会给一只死 code 挂上到账日复活的新股。
///
/// 新股 mark 取「新 code 的 T-1 close」；数据里没有该行时按价格平价推导
/// （旧 mark ÷ ratio——换股是等值交换）。**不往 close 表里补行**：补出来的合成价会
/// 让 sizing 拿它下单（与 `ca_prev_close_fp` 的回写规则同理），此处只修估值。
void apply_security_conversion(SelRunState& st, const SelCorporateAction& ev,
                               uint32_t date, uint32_t price_date) {
    st.ca_converted_codes.insert(ev.code);

    // 在途新股先入仓（与退市清算同一公式），随整仓一起换。
    {
        const auto taken = st.ca_ledger.take_pending_shares(ev.code);
        int64_t pending_qty_fp  = 0;
        int64_t pending_cost_fp = 0;
        for (const auto& b : taken) {
            pending_qty_fp  += b.qty_fp;
            pending_cost_fp += b.cost_fp;
        }
        if (pending_qty_fp > 0) {
            const auto before = st.infra.account->snapshot_position(ev.code);
            const int64_t after = before.long_qty + pending_qty_fp;
            const int64_t new_avg_fp =
                after > 0 ? xt_fp::div(
                    xt_fp::mul(before.long_qty, before.avg_price) + pending_cost_fp, after)
                          : 0;
            st.infra.account->apply(XtAccountEvent::set_position(
                ev.code.c_str(), after, new_avg_fp,
                /*short_qty=*/0, /*short_avg_price=*/0, /*last_price=*/0,
                /*long_today=*/before.long_today_qty, /*short_today=*/0));
            emit_ca_event_row(st, date, ev.code, "pending_bonus_liquidated",
                              before.long_qty, after, /*cash_delta=*/0,
                              before.mark_price, before.mark_price);
        }
    }

    const auto old_pos = st.infra.account->snapshot_position(ev.code);
    if (old_pos.long_qty <= 0) {
        // 无仓也要发行：消费方据此知道该 code 自此日起不再可交易（闩锁已置）。
        // out/in 成对发（qty 全 0）：单发 out 时消费方拿不到目标 code —— 换股的
        // 「换成了什么」只在 in 行的 code 列上，缺它这条事件无法解读。
        const auto prev_new = st.infra.account->snapshot_position(ev.conversion_to);
        emit_ca_event_row(st, date, ev.code, "conversion_out", old_pos.long_qty,
                          0, /*cash_delta=*/0, old_pos.mark_price, 0);
        emit_ca_event_row(st, date, ev.conversion_to, "conversion_in",
                          prev_new.long_qty, prev_new.long_qty, /*cash_delta=*/0,
                          prev_new.mark_price, prev_new.mark_price);
        return;
    }

    const int64_t ratio_fp   = xt_fp::to_fp(ev.conversion_ratio);
    const int64_t raw_new_fp = xt_fp::mul(old_pos.long_qty, ratio_fp);
    const int64_t new_qty_fp = floor_to_whole_shares_fp(raw_new_fp);
    const int64_t odd_fp     = raw_new_fp - new_qty_fp;
    const int64_t cash_fp    = xt_fp::mul(odd_fp, xt_fp::to_fp(ev.cash_in_lieu));
    const int64_t total_cost_fp = xt_fp::mul(old_pos.long_qty, old_pos.avg_price);
    // 迁移的成本按「换到的整股 ÷ 应换股数」拆：零股腿视同处置（已收 cash_in_lieu），
    // 其对应比例的成本随它离场，差额自然落在权益上。全额迁移在 ratio 小到整股为 0
    // 时最刺眼——新 code 已有仓的话，它的 avg 会被灌进整笔旧成本（B 的 avg 20→30），
    // 后续卖出结出一笔凭空的 realized；且零股既收了现金、成本又进了新仓 = 双计。
    // 逐股拆（先算每新股成本再乘整股数）而不是先乘后除：后者的中间积会溢出 int64。
    const int64_t cost_per_new_share_fp =
        raw_new_fp > 0 ? xt_fp::div(total_cost_fp, raw_new_fp) : 0;
    const int64_t migrated_cost_fp = xt_fp::mul(cost_per_new_share_fp, new_qty_fp);

    // 新 code 的估值基准：T-1 close 优先，缺则按平价从旧 mark 推。
    int64_t new_mark_fp = 0;
    if (const auto dit = st.closes_by_date.find(price_date); dit != st.closes_by_date.end()) {
        const auto cit = dit->second.find(ev.conversion_to);
        if (cit != dit->second.end() && cit->second > 0.0) new_mark_fp = xt_fp::to_fp(cit->second);
    }
    if (new_mark_fp <= 0 && old_pos.mark_price > 0 && ratio_fp > 0) {
        new_mark_fp = xt_fp::div(old_pos.mark_price, ratio_fp);
    }

    // 旧仓清零。
    st.infra.account->apply(XtAccountEvent::set_position(
        ev.code.c_str(), 0, 0, /*short_qty=*/0, /*short_avg_price=*/0, /*last_price=*/0,
        /*long_today=*/0, /*short_today=*/0));

    const auto prev_new = st.infra.account->snapshot_position(ev.conversion_to);
    const int64_t after_fp = prev_new.long_qty + new_qty_fp;
    const int64_t new_avg_fp =
        after_fp > 0
            ? xt_fp::div(
                  xt_fp::mul(prev_new.long_qty, prev_new.avg_price) + migrated_cost_fp,
                  after_fp)
            : 0;
    st.infra.account->apply(XtAccountEvent::set_position(
        ev.conversion_to.c_str(), after_fp, new_avg_fp,
        /*short_qty=*/0, /*short_avg_price=*/0, /*last_price=*/0,
        /*long_today=*/prev_new.long_today_qty, /*short_today=*/0));
    if (new_mark_fp > 0) {
        st.infra.account->apply(
            XtAccountEvent::market_tick(ev.conversion_to.c_str(), new_mark_fp));
    }
    // 新 code 未必成交过：不登记就不进 daily_positions 的 code 集，其市值会从会计
    // 恒等的 Σ 里整块缺席（rc=11）。
    st.traded_codes.insert(ev.conversion_to);

    if (cash_fp != 0) {
        st.infra.account->apply(XtAccountEvent::cash_transfer(cash_fp));
        st.ca_conversion_cash_in_lieu_fp += cash_fp;
    }
    ++st.ca_conversions;

    // 两行（out/in）而不是一行带 code_to：events CSV 的列集是 viewer 消费契约，
    // 换股用既有列就能完整表达（旧 code 清零 + 新 code 增仓 + 零股现金）。
    emit_ca_event_row(st, date, ev.code, "conversion_out", old_pos.long_qty, 0,
                      cash_fp, old_pos.mark_price, 0);
    emit_ca_event_row(st, date, ev.conversion_to, "conversion_in", prev_new.long_qty,
                      after_fp, /*cash_delta=*/0, prev_new.mark_price, new_mark_fp);
    XtLogger::info_m("SEL_BT",
        "date {}: security conversion '{}' → '{}' ratio={} qty {} → {}（零股现金 {}）",
        date, ev.code, ev.conversion_to, ev.conversion_ratio,
        order_exec_from_fp(old_pos.long_qty), order_exec_from_fp(new_qty_fp),
        xt_fp::from_fp(cash_fp));
}

/// @brief ex_date 当日的 CA 施加点：五元同点原子更新。
///
/// 调用位置在逐日循环内、**填 T-1 closes（current_closes_fp）与刷 account mark
/// 之前**：① 持仓 qty/avg（set_position）② 现金/应收 ③ T-1 参考价修正写回
/// `closes_by_date[price_date]`（current_closes_fp 的源，随后的填充 + mark 循环自然
/// 带上修正值）④ account mark 同步 ⑤ events 行。五者在同一函数内完成，中途无
/// sizing/风控读点，故不存在「按未修正价 sizing、按修正价估值」的错位。
///
/// 退市清算**不在**本函数：它必须晚于当日全部 rebalance 提交与 drain（清算所得
/// 不得资助当日早先的买入决策），见 `apply_delist_liquidations`。
///
/// @return false = 输入数据错误（每股现金分红 ≥ T-1 参考价，除息后参考价非正），
///         调用方整跑 fail-fast（rc=1）。clamp 到 0 会让该 code 估值失真且同 fire
///         的其他 symbol 按虚增权益放大目标，比整跑失败危险。
[[nodiscard]] bool apply_corporate_actions(SelRunState& st, uint32_t date,
                                          uint32_t price_date) {
    if (!st.ca_enabled) return true;

    // (1) 到期现金分红应收 → 现金（pay_date ≤ 今日；非交易日 pay_date 自然落到
    //     其后第一个交易日）。
    {
        const auto due = st.ca_ledger.take_due_cash(date);
        int64_t total = 0;
        for (const auto& p : due) total += p.amount_fp;
        if (total != 0) {
            st.infra.account->apply(XtAccountEvent::cash_transfer(total));
            for (const auto& p : due) {
                const int64_t qty = st.infra.account->snapshot_position(p.code).long_qty;
                emit_ca_event_row(st, date, p.code, "pay_cash", qty, qty, p.amount_fp, 0, 0);
            }
        }
    }

    // (2) 到期在途新股（送转股 / 配股认购股）→ 真实持仓（昨仓，可卖）+ 成本基础在
    //     此重算。ex 日刻意**不摊** avg（qty 没变，摊了总成本就凭空缩水，窗口内卖出
    //     的 realized 会错），到账时按「总成本 = 旧总成本 + 本笔取得成本」摊到到账后
    //     的股数：
    //       avg = (旧 qty × 旧 avg + cost) / (旧 qty + new)
    //     送转 cost=0 → 退化为「总成本不变」；配股 cost = ex 日已扣的缴款额。
    //     窗口内已清仓（旧 qty=0）时旧总成本已随那笔卖出结清，新股只带自身 cost——
    //     逐笔归因偏早，但累计 realized 正确（见 PendingBonus 契约注释）。
    for (const auto& b : st.ca_ledger.take_due_shares(date)) {
        const auto pos = st.infra.account->snapshot_position(b.code);
        const int64_t after = pos.long_qty + b.qty_fp;
        const int64_t new_avg_fp =
            after > 0
                ? xt_fp::div(xt_fp::mul(pos.long_qty, pos.avg_price) + b.cost_fp, after)
                : 0;
        st.infra.account->apply(XtAccountEvent::set_position(
            b.code.c_str(), after, new_avg_fp,
            /*short_qty=*/0, /*short_avg_price=*/0, /*last_price=*/0,
            /*long_today=*/pos.long_today_qty, /*short_today=*/0));
        emit_ca_event_row(st, date, b.code, "shares_listed", pos.long_qty, after,
                          /*cash_delta=*/0, pos.mark_price, pos.mark_price);
    }

    // (3) 本日 ex_date 事件。
    const auto* events = st.corporate_actions.find_ex_date(date);
    if (events == nullptr) return true;
    // 本日已执行的配股缴款腿数：同日多笔按 raw ex 序先到先占现金，第 2 笔起披露。
    size_t rights_legs_today = 0;
    for (const auto& ev : *events) {
        if (ev.is_delist) continue;  // 清算在 EOD（见 apply_delist_liquidations）
        // 换股闩锁后仍落在同一 code 上的事件（同日更晚的 raw ex 序，或后续交易日）：
        // 该证券已经换掉了。放行会按 record 日快照给它挂送转/配股新股，到账日
        // 在一只不存在的 symbol 上凭空复活一笔仓；现金分红同理（钱不该再从它来）。
        if (st.ca_converted_codes.count(ev.code) != 0) {
            ++st.ca_post_conversion_events_rejected;
            XtLogger::warn_m("SEL_BT",
                "date {}: corporate action on '{}' rejected — code 已换股出清（闩锁），"
                "后续事件不再施加", date, ev.code);
            continue;
        }
        if (!ev.conversion_to.empty()) {
            apply_security_conversion(st, ev, date, price_date);
            ++st.ca_applied;
            continue;
        }
        const auto pos = st.infra.account->snapshot_position(ev.code);
        if (ev.rights_skipped) {
            emit_ca_event_row(st, date, ev.code, "rights_skipped", pos.long_qty,
                              pos.long_qty, 0, pos.mark_price, pos.mark_price);
            continue;
        }
        // SEL v1 long-only（parser 拒 allow_short、engine 拒负权重）→ 空头结构性
        // 不可达；出现即模型前提被破坏，不静默按多头公式改仓。
        if (pos.short_qty != 0) {
            XtLogger::error_m("SEL_BT",
                "date {}: corporate action on '{}' skipped — short position present "
                "({}), SEL v1 long-only CA model does not cover it",
                date, ev.code, order_exec_from_fp(pos.short_qty));
            continue;
        }

        bool ref_from_close_table = false;
        const int64_t prev_close_fp =
            ca_prev_close_fp(st, price_date, ev.code, &ref_from_close_table);
        const int64_t bonus_fp      = xt_fp::to_fp(ev.bonus_transfer_per_share);
        const int64_t rights_fp     = xt_fp::to_fp(ev.rights_per_share);
        const int64_t rights_px_fp  = xt_fp::to_fp(ev.rights_price);
        // 除权因子 (1 + b + r)：送转与配股都摊薄股本，参考价同一个分母。
        const int64_t factor_fp     = kPriceFpScale + bonus_fp + rights_fp;
        const int64_t cash_ps_fp    = xt_fp::to_fp(ev.cash_per_share);

        // 确权股权数：现金分红 / 送转 / 配股**共用**同一口径（record 日收盘仓；无
        // 快照时退 ex 日初仓 + 分因计数）。各腿各读一个基数会让同一笔事件按不同持仓
        // 确权。每事件只解析一次，避免退化计数重复累加。
        const bool needs_record_qty = (cash_ps_fp != 0 || bonus_fp != 0 || rights_fp != 0);
        const int64_t record_qty_fp =
            needs_record_qty ? ca_record_qty_fp(st, ev, pos.long_qty) : 0;

        // ③ T-1 参考价修正（与交易所除权除息参考价公式一致）：
        //      ref = (prev − d + r × p_r) / (1 + b + r)
        //    配股缴款抬高分子（认购款进了公司），配股比例摊薄分母。**是否认购不改
        //    这个价**——它是市场的除权基准；弃权的损失正是「按摊薄价估值、却没拿到
        //    便宜的新股」这一差额。持仓为 0 也要修正：今日想买入该 code 的 sizing
        //    用的就是这个价。
        int64_t ref_after_fp = prev_close_fp;
        if (prev_close_fp > 0 && needs_record_qty) {
            const int64_t numerator_fp =
                prev_close_fp - cash_ps_fp + xt_fp::mul(rights_fp, rights_px_fp);
            if (numerator_fp <= 0) {
                // 除权后参考价 ≤ 0：输入数据错误（每股分红不可能吃掉整个股价）。
                // clamp 到 0 会让 mark 停在除权前的价（权益虚增），同 fire 的其他
                // symbol 按虚增权益放大目标 → 整跑 fail-fast，不静默失真。
                XtLogger::error_m("SEL_BT",
                    "date {}: corporate action on '{}' has cash_per_share {:.4f} vs T-1 "
                    "reference price {:.4f} (rights {:.4f}@{:.4f}) — 除权后参考价非正，"
                    "输入数据错误 (fail-fast, rc=1)", date, ev.code, ev.cash_per_share,
                    xt_fp::from_fp(prev_close_fp), ev.rights_per_share, ev.rights_price);
                return false;
            }
            ref_after_fp = xt_fp::div(numerator_fp, factor_fp);
            // 回写只允许覆盖**本来就存在**的 close 行：参考价取自 carry 的 mark 时
            // 凭空补一行 = 合成 T-1 价，会把「当日该 code 无价 → kNoPrice 跳过」变成
            // 「按合成价下单」。估值修正走 mark 分支即可（不进价源）。
            if (ref_from_close_table && price_date != 0) {
                st.closes_by_date[price_date][ev.code] = xt_fp::from_fp(ref_after_fp);
            }
            // ④ mark 同步：不依赖调用点之后的填充循环（该 code 可能不在 T-1 close
            //    集合里，例如参考价来自 carry 的 mark）。
            if (ref_after_fp > 0) {
                st.infra.account->apply(
                    XtAccountEvent::market_tick(ev.code.c_str(), ref_after_fp));
            }
        }

        // ① 新股腿（送转 + 配股认购）。两者的股份都不在 ex 日即到账：到账日
        //    `effective_list_date`（CSV 给了用它，未给取 ex 日的下一交易日）之前进
        //    在途账，**持仓与 avg 一概不动**（qty 没变就摊 avg = 总成本凭空缩水），
        //    成本基础到账日重算（见 take_due_shares 处）。参考价与市场同步在 ex 日
        //    修正（上面 ③），与股份何时到账无关。
        //
        //    配股缴款在 ex 日即扣现金（认购款当日划走），股份走同一条在途通道；
        //    现金不足时按**可缴整股**截断（缴不起的部分自然弃权）并计数披露。
        int64_t qty_after_fp = pos.long_qty;
        // 到账日 ≤ 当日 = 无在途窗口（CSV 显式给了 ex 日当天或更早）。窗末已无次日
        // 的缺省走 `kListDateBeyondWindow` 哨兵 → 恒不即时，整跑留在在途账。
        const bool list_immediate = (ev.effective_list_date <= date);

        // 本事件是否真产生了新股腿（送转/配股）；披露计数按事件计，不按腿计。
        bool placed_new_shares = false;

        /// 把一笔新股按到账日归位：滞后进在途账，否则 ex 日即入可卖仓（含 avg 重算）。
        auto place_new_shares = [&](int64_t add_fp, int64_t cost_fp) {
            if (add_fp <= 0) return;
            placed_new_shares = true;
            if (!list_immediate) {
                st.ca_ledger.accrue_shares(ev.code, ev.effective_list_date, add_fp, cost_fp);
                return;
            }
            const auto cur = st.infra.account->snapshot_position(ev.code);
            const int64_t target_qty = cur.long_qty + add_fp;
            const int64_t new_avg_fp = xt_fp::div(
                xt_fp::mul(cur.long_qty, cur.avg_price) + cost_fp, target_qty);
            // long_today 按调整前今昨比例继承。**该值是死算**：本函数在
            // session_begin 之前执行，T+1 reducer 随后把今仓整体迁昨仓，
            // long_today 必被覆盖为 0；保留只为 set_position 的字段完整性
            // （非 T+1 模式下 long_today 本身无消费者）。
            const int64_t new_today = (cur.long_today_qty > 0 && cur.long_qty > 0)
                ? std::min(floor_to_whole_shares_fp(
                      xt_fp::div(xt_fp::mul(cur.long_today_qty, target_qty), cur.long_qty)),
                      target_qty)
                : 0;
            st.infra.account->apply(XtAccountEvent::set_position(
                ev.code.c_str(), target_qty, new_avg_fp,
                /*short_qty=*/0, /*short_avg_price=*/0, /*last_price=*/0,
                /*long_today=*/new_today, /*short_today=*/0));
            qty_after_fp = target_qty;
        };

        if (bonus_fp != 0 && record_qty_fp > 0) {
            place_new_shares(floor_to_whole_shares_fp(xt_fp::mul(record_qty_fp, bonus_fp)),
                             /*cost_fp=*/0);
            emit_ca_event_row(st, date, ev.code, "bonus_transfer", pos.long_qty,
                              qty_after_fp, 0, prev_close_fp, ref_after_fp);
        }

        // ①b 配股：subscribe 缴款认购、forfeit 弃权。两者的参考价已按同一全式修正
        //     （上面 ③），弃权的除权损失因此如实进权益，不需要额外记账。
        if (rights_fp != 0 && record_qty_fp > 0) {
            if (st.ca_rights_policy == RightsPolicy::kForfeit) {
                ++st.ca_rights_forfeited;
                emit_ca_event_row(st, date, ev.code, "rights_forfeited", pos.long_qty,
                                  qty_after_fp, /*cash_delta=*/0, prev_close_fp,
                                  ref_after_fp);
            } else {
                int64_t want_fp = floor_to_whole_shares_fp(
                    xt_fp::mul(record_qty_fp, rights_fp));
                if (want_fp <= 0) {
                    // 应配不足 1 整股（record_qty×ratio 取整为零，非现金原因）：无缴款、
                    // 无入股、无弃权，零动作不发事件——发 `rights_cash_capped` 会与未
                    // ++ 的计数矛盾（两个消费面得出相反结论）。与 bonus 分支对
                    // record_qty==0 的静默跳过同形。
                    XtLogger::warn_m("SEL_BT",
                        "date {}: rights on '{}' entitlement < 1 share "
                        "(record_qty×ratio) — no-op", date, ev.code);
                } else {
                    // 同日多笔配股共用同一份现金，先到者先占（序 = raw ex 序，不改）。
                    // 凡有应配量的腿都算竞争参与者——被前面腿耗尽现金而截到零的
                    // 第二腿，恰是截断结果最依赖施加序的那种，不可漏计。
                    if (++rights_legs_today > 1) {
                        ++st.ca_rights_same_day_cash_contention;
                    }
                    // 缴款受可用现金约束：缴不起就按能缴的整股数认购（余下自动弃权），
                    // 而不是让账户透支。缺口在参考价上已经体现，无需再补记损失。
                    const int64_t cash_fp =
                        st.infra.account->snapshot_account().available_cash;
                    if (rights_px_fp > 0 && xt_fp::mul(want_fp, rights_px_fp) > cash_fp) {
                        // 整股数用**截断**整除，不走 xt_fp::div——后者是 half-away-from-zero
                        // 舍入，真实商差一点点到整数时会被 round 上去，落成「多缴一股」的
                        // 微幅透支。整除后再放大回 fp，结果天然是整股。
                        const int64_t affordable_fp =
                            cash_fp > 0 ? (cash_fp / rights_px_fp) * kPriceFpScale : 0;
                        ++st.ca_rights_cash_capped;
                        XtLogger::warn_m("SEL_BT",
                            "date {}: rights subscription on '{}' capped by cash — {} → {} 股"
                            "（可用现金 {:.2f}，配股价 {:.4f}）", date, ev.code,
                            order_exec_from_fp(want_fp), order_exec_from_fp(affordable_fp),
                            xt_fp::from_fp(cash_fp), ev.rights_price);
                        want_fp = affordable_fp;
                    }
                    const int64_t cost_fp = xt_fp::mul(want_fp, rights_px_fp);
                    if (want_fp > 0) {
                        st.infra.account->apply(XtAccountEvent::cash_transfer(-cost_fp));
                        st.ca_rights_cash_paid_fp += cost_fp;
                        ++st.ca_rights_subscribed;
                        place_new_shares(want_fp, cost_fp);
                    }
                    // 现金一股都缴不起 → 事实上是全额弃权，行类型必须说这个：发
                    // `rights_subscribed` 零量行会与 `rights_subscribed` 计数（未 ++）
                    // 对不上，两个消费面得出相反结论。进到这里恒有 cash_capped==true
                    //（entitlement 为零的腿已在上方 early 分支排除）。
                    emit_ca_event_row(st, date, ev.code,
                                      want_fp > 0 ? "rights_subscribed" : "rights_cash_capped",
                                      pos.long_qty, qty_after_fp, -cost_fp, prev_close_fp,
                                      ref_after_fp);
                }
            }
        }

        // 新股到账口径披露（按事件计一次）：缺省取 ex+1 交易日的笔数、退化成
        // 「ex 日即到账」（不可卖窗口被当零）的笔数、到账落在窗外（整跑不到账）的
        // 笔数，以及归日把「ex 日即到账」变成滞后的笔数。
        if (placed_new_shares) {
            if (ev.list_date_defaulted)  ++st.ca_shares_list_date_defaulted;
            if (ev.list_date_lagged_by_rebase) {
                ++st.ca_shares_list_date_lagged_by_rebase;
            }
            if (list_immediate) {
                ++st.ca_shares_list_date_absent_immediate;
            } else if (ev.effective_list_date > st.loaded.trading_dates.back()) {
                ++st.ca_shares_list_date_beyond_window;
            }
        }

        // ② 现金分红：股权数与送转同口径（record 日收盘锁定）；pay_date 滞后时进应收。
        if (cash_ps_fp != 0) {
            if (record_qty_fp > 0) {
                const int64_t gross_fp = xt_fp::mul(record_qty_fp, cash_ps_fp);
                const int64_t net_fp   = gross_fp - xt_fp::mul(
                    gross_fp, xt_fp::to_fp(st.ca_dividend_tax_rate));
                st.ca_cash_dividends_fp += net_fp;
                if (ev.pay_date > date) {
                    st.ca_ledger.accrue_cash(ev.code, ev.pay_date, net_fp);
                    emit_ca_event_row(st, date, ev.code, "receivable_created",
                                      record_qty_fp, qty_after_fp, net_fp,
                                      prev_close_fp, ref_after_fp);
                } else {
                    // pay_date 缺省或不晚于 ex 日 → 当日直接入现金（披露：无应收窗口）。
                    ++st.ca_pay_date_absent_immediate_cash;
                    st.infra.account->apply(XtAccountEvent::cash_transfer(net_fp));
                    emit_ca_event_row(st, date, ev.code, "cash_div", record_qty_fp,
                                      qty_after_fp, net_fp, prev_close_fp, ref_after_fp);
                }
            }
        }
        ++st.ca_applied;
    }
    return true;
}

/// @brief EOD 退市清算：持仓以最后可得 close ×(1−haircut) 合成 SELL 平掉。
///
/// 时点必须晚于当日全部 rebalance 提交与 drain：清算所得是「当日收盘后才拿到的
/// 钱」，若在开盘前入账就会资助当日早先的买入决策（未来资金 leak）。
/// 语义：豁免 T+1 与 tradability gate（强制清算不受可交易性约束）、费用按正常卖出
/// 计（含印花税）、fee 超清算所得时压到所得（强制清算不产生负现金）。
///
/// 在途红股（ex→list 窗口未到账）在此**一并了结**：先按「总成本不变」并入真实持仓，
/// 再随整仓以清算价卖出。留着不管会让其市值一直计在权益里，list 日又加回已退市
/// symbol（退市后复活仓）。清算后该 code 进 `ca_delisted_codes` 闩锁（拒后续买入）。
void apply_delist_liquidations(SelRunState& st, uint32_t date, uint32_t price_date,
                               uint32_t fill_time_ms) {
    if (!st.ca_enabled) return;
    const auto* events = st.corporate_actions.find_ex_date(date);
    if (events == nullptr) return;

    for (const auto& ev : *events) {
        if (!ev.is_delist) continue;
        st.ca_delisted_codes.insert(ev.code);
        // 在途红股先入仓（成本基础按总成本不变摊）：与 list 日到账走同一公式，
        // 清算所得/费用/realized 因此自然覆盖这部分股数。
        {
            const auto taken = st.ca_ledger.take_pending_shares(ev.code);
            int64_t bonus_fp = 0;
            int64_t cost_fp  = 0;
            for (const auto& b : taken) {
                bonus_fp += b.qty_fp;
                cost_fp  += b.cost_fp;
            }
            if (bonus_fp > 0) {
                const auto before = st.infra.account->snapshot_position(ev.code);
                const int64_t after = before.long_qty + bonus_fp;
                const int64_t new_avg_fp = after > 0
                    ? xt_fp::div(
                          xt_fp::mul(before.long_qty, before.avg_price) + cost_fp, after)
                    : 0;
                st.infra.account->apply(XtAccountEvent::set_position(
                    ev.code.c_str(), after, new_avg_fp,
                    /*short_qty=*/0, /*short_avg_price=*/0, /*last_price=*/0,
                    /*long_today=*/before.long_today_qty, /*short_today=*/0));
                st.ca_delist_pending_bonus_liquidated += taken.size();
                XtLogger::warn_m("SEL_BT",
                    "date {}: delist '{}' 在途红股 {} 股随清算了结（未到 list_date）",
                    date, ev.code, order_exec_from_fp(bonus_fp));
                emit_ca_event_row(st, date, ev.code, "pending_bonus_liquidated",
                                  before.long_qty, after, /*cash_delta=*/0,
                                  before.mark_price, before.mark_price);
            }
        }
        const auto pos = st.infra.account->snapshot_position(ev.code);
        const int64_t last_close_fp = ca_prev_close_fp(st, price_date, ev.code);
        // mark 优先（EOD 已刷 T 日 close = 退市前最后收盘）；两者皆无则无价可清算。
        const int64_t base_fp = pos.mark_price > 0 ? pos.mark_price : last_close_fp;
        if (pos.long_qty <= 0 || base_fp <= 0) {
            emit_ca_event_row(st, date, ev.code, "delist_liquidation", pos.long_qty,
                              pos.long_qty, 0, base_fp, base_fp);
            if (pos.long_qty > 0) {
                XtLogger::error_m("SEL_BT",
                    "date {}: delist '{}' has {} shares but no usable close — position "
                    "left in place (stale carry)", date, ev.code,
                    order_exec_from_fp(pos.long_qty));
            }
            continue;
        }
        const int64_t liq_px_fp = xt_fp::mul(
            base_fp, kPriceFpScale - xt_fp::to_fp(st.ca_delist_haircut));

        XtTradeRecord tr;
        tr.order_id     = st.next_ca_order_id++;
        tr.code         = ev.code;
        tr.is_buy       = false;
        tr.qty_fp       = pos.long_qty;
        tr.price        = xt_fp::from_fp(liq_px_fp);
        tr.exec_price   = tr.price;
        tr.trade_date   = date;
        tr.trade_time   = fill_time_ms;
        tr.trading_day  = date;
        tr.is_close     = true;
        tr.is_today     = false;   // 强制清算按昨仓平（豁免 T+1）
        tr.close_offset = XtOffset::Close;
        tr.user_tag     = "delist_liquidation";
        // 合成单不属于任何策略：trades.csv 的 strategy_id 列写固定 sentinel，消费方
        // 才能把「模型强制清算」与策略自己的卖出分开（空串会被当成归因缺失）。
        tr.strategy_id  = "ca_delist";
        const XtCommodityInfo* comm = st.infra.commodities
            ? st.infra.commodities->find(ev.code.c_str()) : nullptr;
        tr.fee = xt_fill_cost::breakdown_fee(tr.exec_price, tr,
                                             st.infra.fee_book.get(), comm).total;
        const double proceeds = xt_fp::from_fp(xt_fp::mul(liq_px_fp, tr.qty_fp));
        if (tr.fee > proceeds) {
            // 强制清算不该把账户扣成负现金：费用压到清算所得并披露。
            XtLogger::warn_m("SEL_BT",
                "date {}: delist '{}' fee {:.2f} exceeds proceeds {:.2f} — fee capped",
                date, ev.code, tr.fee, proceeds);
            tr.fee = proceeds;
            ++st.ca_delist_fee_capped;
        }

        apply_ledger_fill(st, tr, date);
        ++st.ca_delist_liquidations;
        emit_ca_event_row(st, date, ev.code, "delist_liquidation", pos.long_qty,
                          st.infra.account->snapshot_position(ev.code).long_qty,
                          to_fp_round(proceeds - tr.fee), base_fp, liq_px_fp);
    }
}

/// @brief 日末落盘本日 target_weights.csv 行（执行列已回填）。
void flush_day_decisions(SelRunState& st, uint32_t date) {
    // 日界清空的表集在两个分支必须一致：order 索引跨日残留会让次日的 carry/trace
    // 归因读到上一日的 fire_seq / sid / target（order_id 由撮合从小到大分配，跨日
    // 复用同一个 id 不是理论问题）。
    const auto reset_day_buffers = [&st]() {
        st.day_decisions.clear();
        st.decision_index.clear();
        st.filled_by_order.clear();
        st.fire_seq_by_order.clear();
        st.submit_sid_by_order.clear();
        st.submit_target_by_order.clear();
        st.latest_decided_fire.clear();
    };
    if (!st.targets_csv.is_open()) {
        reset_day_buffers();
        return;
    }
    auto& out = st.targets_csv.out();
    char buf[320];
    for (const auto& r : st.day_decisions) {
        // 无价 / 负权重拒：target 与 diff 都没算出来 → 数值列留空（非 0 sentinel）。
        const bool has_target =
            r.planning != xt_engine_sel::SelPlanningStatus::kNoPrice &&
            r.planning != xt_engine_sel::SelPlanningStatus::kNegativeWeight;
        int n = std::snprintf(buf, sizeof(buf), "%u,%llu,",
                              date, static_cast<unsigned long long>(r.fire_seq));
        if (n > 0) out.write(buf, n);
        out << csv_escape_field(r.code);
        n = std::snprintf(buf, sizeof(buf), ",%.6f,", r.weight);
        if (n > 0) out.write(buf, n);
        // 无价行：target/pre/diff 全部未知 → 留空（非 0 sentinel）。
        if (has_target) {
            n = std::snprintf(buf, sizeof(buf), "%.4f,%.4f,%.4f,%.4f,",
                              order_exec_from_fp(r.target_qty_raw_fp),
                              order_exec_from_fp(r.target_qty_fp),
                              order_exec_from_fp(r.pre_qty_fp),
                              order_exec_from_fp(r.diff_qty_fp));
            if (n > 0) out.write(buf, n);
        } else {
            out << ",,,,";
        }
        out << xt_engine_sel::sel_planning_status_name(r.planning) << ',';
        if (!r.entered_execution) {
            // 未进执行路径 → 执行列（gate..post_qty，8 列）整段留空。
            out << ",,,,,,,\n";
        } else {
            out << r.gate << ',' << (r.is_buy ? "buy" : "sell") << ',';
            if (r.submitted) {
                n = std::snprintf(buf, sizeof(buf), "%llu,%.4f,%d,%s,%.4f,%.4f\n",
                                  static_cast<unsigned long long>(r.order_id),
                                  order_exec_from_fp(r.submitted_qty_fp),
                                  r.accepted ? 1 : 0,
                                  reject_code_name(r.reject_code),
                                  order_exec_from_fp(r.filled_qty_fp),
                                  order_exec_from_fp(r.post_qty_fp));
            } else {
                // gate 拒：没到 gateway，无 order_id / submitted_qty / accepted /
                // reject_code；成交与终仓仍可观测（分别为 0 与未变动）。
                n = std::snprintf(buf, sizeof(buf), ",,,,%.4f,%.4f\n",
                                  order_exec_from_fp(r.filled_qty_fp),
                                  order_exec_from_fp(r.post_qty_fp));
            }
            if (n > 0) out.write(buf, n);
        }
        st.targets_csv.add_row();
    }
    reset_day_buffers();
}

/// @brief 日末落盘本日 schedule_fires.csv 行（每 (trading_date × task) 一行）。
///
/// `fired` = scheduler 真触发了该 task 的 on_schedule；`skip_reason` 说明为何没
/// 产生调仓（空 = 正常调仓）。两者分列，因为「触发了但策略没设权重」与「压根
/// 没触发」是不同的诊断结论。
void flush_day_fires(const XtSelRunnerConfig& cfg, SelRunState& st, uint32_t date,
                     const xt_engine_sel::IHolidayProvider* holiday) {
    for (size_t i = 0; i < cfg.tasks.size(); ++i) {
        const auto& tcfg = cfg.tasks[i];
        const uint32_t task_id = i < st.task_ids.size() ? st.task_ids[i] : 0;
        const bool fired = st.scheduler_probe && st.scheduler_probe->fired(task_id, date);

        // 归因按 task_id 而非 strategy_id：同 sid 多 task 同日触发时按 sid 找「第一个」
        // 会把两条 E1 都写成同一个 fire_seq，另一个 fire 在 E2 上变孤儿。task_id==0
        // （非 schedule 路径产生的 fire）时才回退 sid 匹配。
        const SelFireAgg* agg = nullptr;
        for (const auto& a : st.day_fires) {
            if (a.task_id == task_id && task_id != 0) { agg = &a; break; }
        }
        if (agg == nullptr) {
            for (const auto& a : st.day_fires) {
                if (a.task_id == 0 && a.strategy_id == tcfg.strategy_id) { agg = &a; break; }
            }
        }

        const char* skip_reason = "";
        if (!fired) {
            // 节假日判据与 scheduler 同源：本日在该 trdtpl 上休市时 task 不触发
            // （scheduler 把它顺延到下一交易日）。
            skip_reason = (holiday && holiday->is_holiday(tcfg.trdtpl, date, true))
                              ? "holiday_deferred" : "not_due";
        } else if (agg == nullptr) {
            skip_reason = "no_weights";
        } else if (agg->equity_nonpositive) {
            skip_reason = "equity_nonpositive";
        }
        if (skip_reason[0] == '\0') ++st.n_fires; else ++st.n_skips;

        if (!st.fires_csv.is_open()) continue;
        auto& out = st.fires_csv.out();
        char buf[256];
        // planned_date：BT 逐交易日驱动，无跨日 defer 观测通道 → 恒等 exec_date。
        int n = std::snprintf(buf, sizeof(buf), "%u,%u,%u,", date, date, tcfg.time);
        if (n > 0) out.write(buf, n);
        // task_uid：优先真 task id（同 sid 多 task 可区分）；scheduler 未分配 id 时
        // 回退 legacy:<sid>。
        out << csv_escape_field(tcfg.strategy_id) << ','
            << csv_escape_field(task_id != 0 ? "task:" + std::to_string(task_id)
                                             : "legacy:" + tcfg.strategy_id)
            << ",1,";
        n = std::snprintf(buf, sizeof(buf), "%llu,%d,",
                          static_cast<unsigned long long>(agg ? agg->fire_seq : 0),
                          fired ? 1 : 0);
        if (n > 0) out.write(buf, n);
        out << skip_reason << ',';
        n = std::snprintf(buf, sizeof(buf), "%zu,%zu,%zu,%.4f,%.6f\n",
                          agg ? agg->n_targets : size_t{0},
                          agg ? agg->n_buys : size_t{0},
                          agg ? agg->n_sells : size_t{0},
                          agg ? xt_fp::from_fp(agg->turnover_fp) : 0.0,
                          agg ? agg->gross_weight : 0.0);
        if (n > 0) out.write(buf, n);
        st.fires_csv.add_row();
    }
    st.day_fires.clear();
}

/// @brief 日末落盘本日 daily_positions.csv 行（含 CASH / RECEIVABLE 行）。
///
/// 会计恒等：`Σ(持仓行 market_value) + cash + receivable_total == portfolio_equity`
/// （CASH / RECEIVABLE 两行只是展示，不重复计入左侧求和）。portfolio_equity 的定义与
/// 五个消费点见本文件「B0b portfolio truth」节（`portfolio_equity_fp`，口径 SSOT）；
/// 本函数与 AccountStateStore 用同一 fp 公式（vol_scale 在股票模式恒 1.0），故 fp
/// 内部是精确相等；CSV 两侧统一 2dp 落盘，消费方按一次打印舍入比较。恒等破裂计入
/// summary.equity_identity_violations 并让整跑 fail-fast（rc=11，与现金落负同级）。
void flush_day_positions(SelRunState& st, uint32_t date,
                         const std::unordered_map<std::string, int64_t>& t_day_closes) {
    if (!st.positions_csv.is_open()) return;
    auto& out = st.positions_csv.out();
    const auto acct = st.infra.account->snapshot_account();
    const int64_t receivable_cash_fp  = st.ca_ledger.outstanding_cash_fp();
    const int64_t receivable_bonus_fp = bonus_receivable_value_fp(st);
    const int64_t receivable_fp       = receivable_cash_fp + receivable_bonus_fp;
    const int64_t equity_fp           = acct.equity + receivable_fp;
    int64_t position_mv_sum_fp        = 0;
    char buf[320];

    auto weight_of = [equity_fp](int64_t mv_fp) -> double {
        return equity_fp != 0
                   ? (static_cast<double>(mv_fp) / static_cast<double>(equity_fp))
                   : 0.0;
    };

    // 持仓行按 code 字典序（确定性输出）；仓位只经 fill 变动 → traded_codes ⊇ 持仓集。
    std::vector<std::string> codes(st.traded_codes.begin(), st.traded_codes.end());
    std::sort(codes.begin(), codes.end());
    for (const auto& code : codes) {
        const auto pos = st.infra.account->snapshot_position(code);
        if (pos.qty == 0) continue;
        const int64_t mv_fp = xt_fp::mul(pos.mark_price, pos.qty);
        position_mv_sum_fp += mv_fp;
        int n = std::snprintf(buf, sizeof(buf), "%u,", date);
        if (n > 0) out.write(buf, n);
        out << csv_escape_field(code);
        n = std::snprintf(buf, sizeof(buf), ",%.4f,%.4f,%d,%.2f,%.6f",
                          order_exec_from_fp(pos.qty),
                          xt_fp::from_fp(pos.mark_price),
                          t_day_closes.count(code) == 0 ? 1 : 0,
                          xt_fp::from_fp(mv_fp),
                          weight_of(mv_fp));
        if (n > 0) out.write(buf, n);
        if (st.t1_enabled) {
            n = std::snprintf(buf, sizeof(buf), ",%.4f,%.4f",
                              order_exec_from_fp(pos.long_today_qty),
                              order_exec_from_fp(pos.available_sell_total_qty));
            if (n > 0) out.write(buf, n);
        } else {
            out << ",,";  // T+1 未启用 → 今昨仓拆分不可信，留空而非写 0
        }
        out << '\n';
        st.positions_csv.add_row();
    }

    // CASH 行：qty=1、close=现金额 → market_value=现金，让 Σ market_value 直接
    // 等于 equity（消费方不必单独取现金口径）。
    int n = std::snprintf(buf, sizeof(buf), "%u,CASH,1.0000,%.4f,0,%.2f,%.6f",
                          date, xt_fp::from_fp(acct.cash),
                          xt_fp::from_fp(acct.cash), weight_of(acct.cash));
    if (n > 0) out.write(buf, n);
    out << (st.t1_enabled ? ",0.0000,0.0000" : ",,");
    out << '\n';
    st.positions_csv.add_row();

    // RECEIVABLE 行：已确权未到账资产（现金分红应收 + 在途红股市值）。仅非零时写出
    // ——未配置 CA 时恒 0，行不出现（daily_positions 与 B0 逐位一致）。
    if (receivable_fp != 0) {
        n = std::snprintf(buf, sizeof(buf), "%u,RECEIVABLE,1.0000,%.4f,0,%.2f,%.6f",
                          date, xt_fp::from_fp(receivable_fp),
                          xt_fp::from_fp(receivable_fp), weight_of(receivable_fp));
        if (n > 0) out.write(buf, n);
        // 与持仓行 / CASH 行同一 9 列 schema：T+1 未启用时补空字段，不缩列
        // （少写两列 = 该行 7 列 vs header 9 列，按列位读的消费方会把 weight 读成
        // available_sell，整行错位）。
        out << (st.t1_enabled ? ",0.0000,0.0000" : ",,");
        out << '\n';
        st.positions_csv.add_row();
    }

    // 会计恒等（fp 精确）：持仓市值 + 现金 + 应收 == portfolio 权益。
    if (position_mv_sum_fp + acct.cash + receivable_fp != equity_fp) {
        ++st.equity_identity_violations;
        XtLogger::error_m("SEL_BT",
            "date {}: equity identity broken — Σmv({}) + cash({}) + receivable({}) != "
            "portfolio_equity({})", date, xt_fp::from_fp(position_mv_sum_fp),
            xt_fp::from_fp(acct.cash), xt_fp::from_fp(receivable_fp),
            xt_fp::from_fp(equity_fp));
    }
}

/// @brief 主循环：trading dates → 喂 mark → on_session → 分段 fire/drain → flush。
/// @return 0 成功；1 = CA 输入数据错误（每股分红 ≥ T-1 参考价，见
///         `apply_corporate_actions`）；8 = 首日无 T-1 close 且策略当日请求 sizing
///         （fail-fast，拒绝以 T 日 close 兜底的 lookahead；报告不写出）；
///         9 = 新产物 writer 打不开；10 = 现金落负、11 = 权益会计恒等破裂
///         （两者同级：账本不变量破裂，后续每一天都建立在错的账上 → 报告不写出）。
int run_sel_loop(const XtSelRunnerConfig& cfg, SelRunState& st) {
    // ── 9) 主循环：trading dates → 喂 mark → on_session → submit → drain → flush
    XtLogger::info_m("SEL_BT",
                      "main loop: {} trading dates, {} tasks, {} strategies",
                      st.loaded.trading_dates.size(), cfg.tasks.size(), cfg.strategies.size());

    // 9-pre) 新产物流式 writer 全部在此开 .tmp；run 中途 fail-fast 时析构删 tmp
    //   （与既有「rc≠0 不产报告」语义一致）。任一打不开 → 整跑按 rc=9 失败。
    if (!open_artifact_writers(cfg, st)) return 9;

    // 单 task 前提（当前 yaml 只解析一个 task，见 XtSelRunnerConfig）：全 run 一个
    // fire 时刻。多 task 落地后 on_minute_tick 必须按各 task 的 time 分别驱动，
    // 这里的单值也要随之拆开（否则第二个 task 的 fire 会落在错的钟面时刻上）。
    const uint32_t fire_time = cfg.tasks.empty() ? 1500 : cfg.tasks.front().time;
    const uint32_t fill_time_ms = fire_time * 100000;  // HHMM → HHMMSSmmm 占位

    // fire 时 price_resolver 返回 T-1 close（前一交易日），
    // 避免在 14:59 fire 时拿到当日 close (要 15:00 才知道) 这种 lookahead bias。
    // fill 时用 T 日 close，并在 fill 前把 T 日 close 刷进 account mark（执行价与
    // 估值价同日）。首日 (prev_date=0) 在 close 数据窗内找 date 之前最近的日期作
    // 真 T-1（warmup 数据）；数据窗无更早行 → 不喂 fire 价，策略当日请求 sizing
    // 即 fail-fast（不再回退 T 日 close —— 那是 lookahead）。
    uint32_t prev_date = 0;
    for (uint32_t date : st.loaded.trading_dates) {
        // trace 日内时间轴切日（未开启时 no-op）。
        st.trace.begin_day(date);

        // 9a) fire 时 price = T-1 close（lookahead-free）。
        //     首日 prev_date=0：在 close 数据窗内找 date 之前最近的日期作真 T-1；
        //     无更早行 → 不喂 fire 价 + 激活探针（策略当日 sizing 即 fail-fast），
        //     不回退 T 日 close。
        uint32_t price_date = prev_date;
        if (price_date == 0) {
            for (const auto& kv : st.closes_by_date) {
                if (kv.first < date && kv.first > price_date) price_date = kv.first;
            }
            if (price_date != 0) {
                XtLogger::info_m("SEL_BT",
                                  "first day {}: sizing/mark price from warmup close "
                                  "date {} (true T-1)", date, price_date);
            } else {
                st.first_day_sizing_probe_active = true;
                XtLogger::warn_m("SEL_BT",
                                  "first day {}: no close data before it — fire-time "
                                  "prices unavailable (fail-fast only if strategy "
                                  "sizes today)", date);
            }
        }

        // 9a.1) corporate action 施加点（未配置 CA 时整段短路）：应收到期 → 在途红股
        //   到账 → 本日 ex_date 除权除息。必须早于 T-1 closes 填充与 mark 刷新，
        //   sizing 才能读到修正后的参考价与调整后的持仓（钉②：先 CA 后 sizing）。
        //   返回 false = 输入数据错误（分红吃掉整个股价）→ 整跑 fail-fast（rc=1）。
        if (!apply_corporate_actions(st, date, price_date)) return 1;

        st.current_closes_fp.clear();
        auto it = st.closes_by_date.find(price_date);
        if (it != st.closes_by_date.end()) {
            for (const auto& kv : it->second) {
                const int64_t close_fp = xt_fp::to_fp(kv.second);
                st.current_closes_fp[kv.first] = close_fp;
            }
        }
        // mark_price 喂 account 用 T-1 close，作为 fire 时刻 portfolio 估值。
        for (const auto& kv : st.current_closes_fp) {
            st.infra.account->apply(XtAccountEvent::market_tick(kv.first.c_str(), kv.second));
        }

        // tradability 当日 flags 切换(未配置 tradability_csv 时恒 nullptr → gate 全放行)。
        st.current_date        = date;
        st.current_tradability = st.tradability.find_date(date);

        // 当日成交量视图 + 参与率份额归零（无 volume 列时 exec_day_volume 恒 nullptr
        // → cap 首行早退）。份额是**当日**口径，跨日不结转。
        st.exec_day_volume = nullptr;
        if (st.exec_has_volume) {
            if (const auto vd = st.exec_volumes_fp.find(date);
                vd != st.exec_volumes_fp.end()) {
                st.exec_day_volume = &vd->second;
            }
            st.exec_participation_used_fp.clear();
            st.exec_carry_orders.clear();
        }

        // 9b) on_session_begin
        //   账本先过日界：T+1 语义要求今仓计数每日归零，否则 long_today 跨日累积、
        //   可卖量恒为 0（enforce_long_t_plus_1 变成永久拒卖）。非 T+1 且非期货
        //   模式下 reducer 内部早退，是 no-op。
        st.infra.account->apply(XtAccountEvent::session_begin());
        st.engine->on_session_begin(date);

        // 9c) on_minute_tick → SelTaskScheduler.fire → strategy.on_schedule →
        //     ctx.set_target_weights → engine.on_target_weights (compute + cache)，
        //     不直接 fire；下方按 SELL→BUY 分段提交。
        st.engine->on_minute_tick(date, fire_time);

        // 首日探针判定：无 T-1 价且策略当日请求了 sizing → fail-fast（rc=8），
        // 拒绝以 T 日 close 兜底（lookahead）。首日无 sizing 请求则放行（等价于
        // sel_demo 形态：首日只是日历起点，首个 rebalance 在后面的日期）。
        if (st.first_day_sizing_probe_active) {
            st.first_day_sizing_probe_active = false;
            if (st.first_day_sizing_misses > 0) {
                XtLogger::error_m("SEL_BT",
                    "first day {}: strategy requested sizing but no T-1 close exists "
                    "({} price lookups missed) — 首日无 T-1 价格,请提供 warmup 数据"
                    "或前移数据起点 (fail-fast, rc=8)",
                    date, st.first_day_sizing_misses);
                return 8;
            }
        }

        // 9d) fill 阶段用 T 日 close (实际成交价；模拟 14:59 提交单子在 15:00 close 撮合)
        auto t_day_it = st.closes_by_date.find(date);
        std::unordered_map<std::string, int64_t> t_day_closes;
        if (t_day_it != st.closes_by_date.end()) {
            for (const auto& kv : t_day_it->second) {
                t_day_closes[kv.first] = xt_fp::to_fp(kv.second);
            }
        }

        // 9d.0) 退市后再现价披露：已清算的 code 在后续交易日仍有 close 行（数据源
        //   未剔除退市票）。持仓已清空、买入被闩锁拒，故只观测不改行为；每
        //   (date, code) 报一次，避免多腿重复计数。
        if (st.ca_enabled && !st.ca_delisted_codes.empty()) {
            for (const auto& code : st.ca_delisted_codes) {
                if (t_day_closes.count(code) == 0) continue;
                if (!st.ca_delist_price_warned.insert(std::to_string(date) + '|' + code)
                         .second) {
                    continue;
                }
                ++st.ca_delisted_price_reappearances;
                XtLogger::warn_m("SEL_BT",
                    "date {}: delisted code '{}' still has a close row — 数据源未剔除"
                    "退市票（仅观测：仓位已清算，买入被闩锁拒）", date, code);
            }
        }

        // 9d.1) T 日 close 刷 account mark（target 数量已按 T-1 在 9c 冻结）：
        //     此后成交、逐笔 equity_after、EOD daily_equity 全部落在 T close 估值，
        //     消除「执行价 = T close、估值价 = T-1 close」的一日错位（新建仓当笔
        //     equity_after 不再带 (T-1 − T) × qty 伪盈亏，EOD/final 含当日行情）。
        for (const auto& kv : t_day_closes) {
            st.infra.account->apply(XtAccountEvent::market_tick(kv.first.c_str(), kv.second));
        }
        // 持仓 symbol 当日无 close 行 → carry 旧 mark（T-1 或更早），计数披露。
        // traded_codes ⊇ 持仓集（本 runner 仓位只经 fill 变动）。
        for (const auto& code : st.traded_codes) {
            if (t_day_closes.count(code) != 0) continue;
            if (st.infra.account->snapshot_position(code).qty != 0) {
                ++st.stale_mark_carry_events;
                XtLogger::warn_m("SEL_BT",
                                  "date {}: held symbol '{}' has no T close — mark "
                                  "carried over (stale valuation)", date, code);
            }
        }

        // SELL→BUY 分段 fire：
        //   先 fire SELL 单 → fill → cash 释放 → 再 fire BUY 单，避免同一 fire
        //   内同时 submit 时 BUY 侧被 cash crunch reject。

        // SEL fill semantics: 用 fill_pending_at_bar_open
        //   (process_pending_ignore_limit) 强制 T 日 close 成交，绕开 update_market_price
        //   的限价检查。
        //
        // 背景: SEL 的 submit 用 T-1 close 作 price_fp（避免 lookahead bias）→ 进 match
        //   engine 时 limit_price_fp = T-1 close（GTC 限价单语义）。但 T 日开盘价相对
        //   T-1 close 上下移动会让 BUY 限价 < 市价 / SELL 限价 > 市价 → 限价单不成交，
        //   strategy 期望的 "rebalance to target" 意图被违背。
        //
        // portfolio rebalance 语义要求下个 session 强制成交。bar 模式
        // process_pending_ignore_limit 已提供 "signal at bar[t] fills at
        // bar[t+1].open" 的无条件成交口径，SEL 复用同一口径。
        //
        // 注: SELL 单也复用此 API 让对称（同一 fill 时刻 + 同一逻辑）, 避免 BUY/SELL
        //   语义不对称带来 P&L 漂移。
        // 单笔 fill 落账：apply 到 account → 记 traded_code → 就地构造报告行。
        // @note 报告行集合 == 「被 apply 的 fill」集合，是承重不变量：报告只描述真
        //       落账的成交，不得出现「报告里有、账本里没有」的行。唯一的 drain→apply
        //       落差是下方资金可负担截断（截到 0 时既不落账也不出行，仍保持等价），
        //       且该落差在 summary.affordability_caps 上可见。
        // apply 与采集同一语句序列，report 行的 equity_after 即本笔落账后的真值，
        // 不存在「先采集后回填」的错位窗口（原游标 + 锚点守卫因此一并退役）。
        // 资金可负担截断（F03）：撮合按 T 日 close 强制成交，撮合侧不校验资金；
        // 提交口的风控用的是 T-1 提交价，T close 更高时差额没人管，账本被扣成负
        // 现金（无声融资）。故在落账口按实际成交价重算可负担量并截断，缺口由下期
        // rebalance 以 target−actual 天然顺延。
        //
        // 只截断「落账 + 报告」两侧：撮合与 order book 已按整笔终态收口（frozen
        // 已释放、slot 已清），二者的数量口径在透支这一支上刻意分叉——账本是钱的
        // 真相，撮合的整笔成交在现实里对应「券商拒掉超出资金的部分」。
        // @return 落账数量（fp）；0 = 一手都买不起，不落账不出报告行。
        auto cap_fill_to_cash = [&](XtTradeRecord& tr) -> int64_t {
            if (!tr.is_buy || tr.qty_fp <= 0) return tr.qty_fp;
            const int64_t px_fp = to_fp_round(tr.exec_price);
            if (px_fp <= 0) return tr.qty_fp;
            const XtCommodityInfo* comm = st.infra.commodities
                ? st.infra.commodities->find(tr.code.c_str()) : nullptr;
            const int min_lot = (comm && comm->min_lot > 0) ? comm->min_lot : 1;
            const XtFeeRule* rule = st.infra.fee_book
                ? st.infra.fee_book->lookup_fee_rule(tr.code.c_str()) : nullptr;
            // 试算 fee 必须走与落账同一条权威舍入路径（breakdown_fee 的 2dp）：
            // estimate_fee 返回未舍入值，最多比落账值小 0.005，够让 cash 落负
            // （min_commission 抬升 + 亚分位过户费的临界组合可确定性复现）。
            const auto fee_of = [&](int64_t q_fp) -> int64_t {
                if (!rule || q_fp <= 0) return 0;
                XtTradeRecord probe = tr;
                probe.qty_fp = q_fp;
                return to_fp_round(xt_fill_cost::breakdown_fee(
                    tr.exec_price, probe, st.infra.fee_book.get(), comm).total);
            };
            const int64_t cash_fp = st.infra.account->snapshot_account().available_cash;
            const int64_t ok_fp =
                affordable_qty_fp(cash_fp, px_fp, tr.qty_fp, min_lot, fee_of);
            if (ok_fp >= tr.qty_fp) return tr.qty_fp;

            ++st.affordability_caps;
            st.affordability_shortfall_fp += xt_fp::mul(px_fp, tr.qty_fp - ok_fp);
            XtLogger::warn_m("SEL_BT",
                "date {}: BUY '{}' 成交成本超现金 — 落账量 {} → {}（可用现金 {}，"
                "成交价 {}）；缺口由下期 rebalance 顺延",
                date, tr.code, order_exec_from_fp(tr.qty_fp),
                order_exec_from_fp(ok_fp), xt_fp::from_fp(cash_fp), tr.exec_price);
            if (ok_fp <= 0) return 0;
            // 数量变了，费用必须按同一模型对新数量重算，否则 fee 与成交量脱节。
            tr.qty_fp = ok_fp;
            tr.fee = xt_fill_cost::breakdown_fee(tr.exec_price, tr,
                                                 st.infra.fee_book.get(), comm).total;
            return ok_fp;
        };

        // 参与率上限（B7e F15）：单 code 单日最多吃掉当日 volume 的
        // `execution.max_participation`。SEL 的撮合是「T 日 close 强制整笔成交」，
        // 不看盘口深度——一只日成交 10 万股的票上想调 100 万股也照样全成，回测收益
        // 里于是含着现实中根本拿不到的量。故在落账口按当日成交量截断，未成部分转
        // carry 跨日复投（见 `carry_from_cap`）。
        //
        // @note 判据用**当日** volume：提交时刻其实还不知道当日成交量，这是模型层面
        //       的已知近似（业界日频容量模型的通行口径）。它只收紧执行、不进策略
        //       可见面，构不成信号型未来函数；ADV20 那一路才是前视无关的研究判据。
        // @note 当日无 volume 行 = 无流动性证据 → 不 cap（与 tradability 缺行
        //       fail-open 同规则：缺数据不等于「不能成交」）。
        // @note 只罩撮合成交这一路。退市清算的合成 SELL 直接走 apply_ledger_fill，
        //       不过本 cap 也不占当日份额——强制清算是交易所行为，不与自主调仓抢
        //       流动性（同它豁免 T+1 / tradability 的理由）。
        // @return 允许落账的数量（fp）；调用方按差额挂 carry。
        auto cap_fill_to_participation = [&](XtTradeRecord& tr) -> int64_t {
            if (!st.exec_has_volume || tr.qty_fp <= 0) return tr.qty_fp;
            const auto* day_vol = st.exec_day_volume;
            int64_t day_qty_fp = -1;   // -1 = 当日该 code 无 volume 行
            if (day_vol != nullptr) {
                if (const auto vit = day_vol->find(tr.code); vit != day_vol->end()) {
                    day_qty_fp = vit->second;
                }
            }
            if (day_qty_fp < 0) {
                // 缺行 = 无流动性证据 → fail-open。计数披露：importer 若把缺失写成
                // 0，这一格会搬到 volume_zero_days，两个数此消彼长即口径冲突。
                if (st.exec_volume_missing_seen.insert(
                        std::to_string(date) + '|' + tr.code).second) {
                    XtLogger::warn_m("SEL_BT",
                        "date {}: '{}' 无 volume 行 —— 参与率上限本日不生效"
                        "（缺数据不等于不能成交）", date, tr.code);
                }
                return tr.qty_fp;
            }
            if (day_qty_fp == 0) {
                // 显式 0 = 零流动性（停牌 / 无成交）：当日一股不成，整腿转 carry。
                st.exec_volume_zero_seen.insert(std::to_string(date) + '|' + tr.code);
            }

            const int64_t allowance_fp =
                xt_fp::mul(day_qty_fp, st.exec_max_participation_fp);
            const int64_t used_fp = st.exec_participation_used_fp[tr.code];
            const int64_t left_fp = allowance_fp > used_fp ? allowance_fp - used_fp : 0;
            if (left_fp >= tr.qty_fp) return tr.qty_fp;
            // 截断后仍须是整手（A 股买入的硬约束；卖出侧一并对齐，避免制造
            // 永远凑不满一手的零股残量）。不足一手 → 本日一股不成，整腿转 carry。
            const int64_t ok_fp =
                floor_to_min_lot_fp(left_fp, min_lot_of(st, tr.code));
            ++st.exec_capped_fills;
            st.exec_capped_shortfall_fp += tr.qty_fp - ok_fp;
            XtLogger::warn_m("SEL_BT",
                "date {}: {} '{}' 触参与率上限 — 落账量 {} → {}（当日成交量 {}，"
                "上限 {:.4f}，本日已用 {}）；未成量转 carry",
                date, tr.is_buy ? "BUY" : "SELL", tr.code,
                order_exec_from_fp(tr.qty_fp), order_exec_from_fp(ok_fp),
                order_exec_from_fp(day_qty_fp),
                xt_fp::from_fp(st.exec_max_participation_fp),
                order_exec_from_fp(used_fp));
            if (ok_fp <= 0) return 0;
            tr.qty_fp = ok_fp;
            const XtCommodityInfo* comm = st.infra.commodities
                ? st.infra.commodities->find(tr.code.c_str()) : nullptr;
            // 数量变了，费用按同一模型对新数量重算（与 cap_fill_to_cash 同规则）。
            tr.fee = xt_fill_cost::breakdown_fee(tr.exec_price, tr,
                                                 st.infra.fee_book.get(), comm).total;
            return ok_fp;
        };

        // 参与率缩量后的 SELL 费用闸：缩量只按比例砍了卖出所得，佣金下限却按新数量
        // 原样重算（min_commission 是保底不是费率），低价小额清仓被截到零头时会出现
        // 「所得 < 费用」——落账即负现金、run 撞 rc=10。BUY 侧有 `cap_fill_to_cash`，
        // SELL 侧此前整段跳过，此闸补上。
        // 处置选整腿顺延（截到 0 转 carry）而不是继续缩量：卖出所得随数量线性、费用
        // 有下限，越缩越付不起，二分不收敛到一个「更小但可付」的量。
        // @return 允许落账的数量（fp）；0 = 本日不卖，整腿转 carry。
        auto cap_sell_to_fee_affordable = [&](const XtTradeRecord& tr) -> int64_t {
            if (tr.is_buy || tr.qty_fp <= 0) return tr.qty_fp;
            const int64_t px_fp = to_fp_round(tr.exec_price);
            if (px_fp <= 0) return tr.qty_fp;
            const int64_t proceeds_fp = xt_fp::mul(px_fp, tr.qty_fp);
            const int64_t fee_fp      = to_fp_round(tr.fee);
            const int64_t cash_fp = st.infra.account->snapshot_account().available_cash;
            if (cash_fp + proceeds_fp >= fee_fp) return tr.qty_fp;
            ++st.exec_sell_fee_deferrals;
            XtLogger::warn_m("SEL_BT",
                "date {}: SELL '{}' 参与率缩量后费用 {:.2f} 超「可用现金 {:.2f} + "
                "所得 {:.2f}」—— 本日整腿顺延（转 carry）",
                date, tr.code, xt_fp::from_fp(fee_fp), xt_fp::from_fp(cash_fp),
                xt_fp::from_fp(proceeds_fp));
            return 0;
        };

        // 当日执行约束截下的未成量入 carry 账：复投单（order 在 exec_carry_orders
        // 里）的未成量记回**既有**腿，不新建 —— 新建即同一份意图被追两遍。
        // carry 存**绝对目标**（提交口盖章的 `submit_target_by_order`），复投前用它与
        // 实时持仓重算差额。
        auto carry_from_cap = [&](const XtTradeRecord& tr, int64_t shortfall_fp) {
            if (shortfall_fp <= 0) return;
            if (st.exec_carry_orders.count(tr.order_id) != 0) return;
            const auto tit = st.submit_target_by_order.find(tr.order_id);
            if (tit == st.submit_target_by_order.end()) {
                // 提交口没盖过目标（合成清算单等非调仓成交）→ 不挂 carry：没有
                // 「目标持仓」可言的成交，跨日复投无从定义。
                return;
            }
            SelCarryLeg leg;
            const auto sit = st.submit_sid_by_order.find(tr.order_id);
            leg.strategy_id   = sit != st.submit_sid_by_order.end() ? sit->second : "";
            leg.code          = tr.code;
            leg.tag           = "sel_carry";
            leg.is_buy        = tr.is_buy;
            leg.target_qty_fp = tit->second;
            leg.remaining_fp  = shortfall_fp;
            if (const auto fit = st.fire_seq_by_order.find(tr.order_id);
                fit != st.fire_seq_by_order.end()) {
                leg.fire_seq = fit->second;
            }
            // 同日同槽后发 fire 已重定该 code 目标 → 本腿的目标是旧版，不建。
            // planning 期的 supersede/stale 收口跑在这里之前，管不到本腿：建了就是
            // 旧目标跨日继续推仓，latest-target 语义失效（同槽 100→50 两 fire，
            // 旧 100 腿把仓位从最新目标 50 继续往 100 推）。
            if (const auto lit = st.latest_decided_fire.find(
                    {leg.strategy_id, tr.code});
                lit != st.latest_decided_fire.end() && lit->second > leg.fire_seq) {
                ++st.exec_carry_stale_before_create;
                st.exec_carry_stale_before_create_qty_fp += shortfall_fp;
                XtLogger::info_m("SEL_BT",
                    "date {}: '{}' 的未成量 {} 属旧 fire#{}（同日最新已定目标 "
                    "fire#{}）→ 不建 carry，按 stale 丢弃计数", date, tr.code,
                    order_exec_from_fp(shortfall_fp), leg.fire_seq, lit->second);
                return;
            }
            leg.created_date = date;
            st.exec_carry.push_back(std::move(leg));
            ++st.exec_carry_created;
        };

        auto apply_fill = [&](const XtTradeRecord& tr) {
            apply_ledger_fill(st, tr, date);
        };

        // 回报消费的唯一顺序：先 order update、后 fill —— 与 CTA engine 的
        // drain_gateway_reports_ 同契约。gateway 只在 drain_order_updates() 里把
        // update 派发给 observer（drain_and_dispatch_order_updates_ 尽管名字带
        // dispatch，实际只填内部缓冲），漏掉这一步 = 撤单的 frozen 释放与 book slot
        // 清理永不执行。Filled 更新由 observer 自身跳过（其释放走 on_trade），
        // 故顺序不会造成重复释放。
        auto drain_reports = [&](){
            std::vector<XtOrderExecUpdate> upd_buf;
            (void)st.infra.gw->drain_order_updates(upd_buf);
            std::vector<XtTradeRecord> fills_buf;
            (void)st.infra.gw->drain_fills(date, fill_time_ms, fills_buf);
            for (auto& tr : fills_buf) {
                // 截断序 = 市场约束先于券商约束：参与率上限（当日能吃下多少量）→
                // 资金可负担（这些量买不买得起）。前者的未成量转 carry 跨日再试，
                // 后者沿用既有语义（缺口由下期 rebalance 顺延，不挂 carry —— 两条
                // 顺延通道叠加会把同一份缺口追两遍）。
                const int64_t want_fp = tr.qty_fp;
                int64_t booked_fp = cap_fill_to_participation(tr);
                // 缩量后的 SELL 才过费用闸：未缩量的腿在提交口已按同一判据放行过，
                // 重判会把 T-1→T 的价差窗口变成新的拒绝面（那一支由 rc=10 兜）。
                if (booked_fp > 0 && !tr.is_buy && booked_fp < want_fp) {
                    booked_fp = cap_sell_to_fee_affordable(tr);
                }
                carry_from_cap(tr, want_fp - booked_fp);
                if (booked_fp <= 0) continue;
                // 截断到可负担量（多数情况恒等）；截到 0 = 不落账不出报告行。
                if (cap_fill_to_cash(tr) <= 0) continue;
                if (st.exec_has_volume) {
                    st.exec_participation_used_fp[tr.code] += tr.qty_fp;
                }
                apply_fill(tr);
            }
        };

        auto drain_and_apply = [&](){
            // 强制 T 日 close 成交，绕开限价检查。
            for (const auto& kv : t_day_closes) {
                (void)st.infra.gw->fill_pending_at_bar_open(
                    kv.first.c_str(), kv.second, date, fill_time_ms);
            }
            drain_reports();
        };

        // 单腿 fire + drain 之后回填该腿的成交列：此刻该 order 的 fill 已全部落账，
        // 持仓即该腿执行后的终仓（同一 code 每次 fire 至多一腿）。
        auto backfill_last_leg = [&](){
            if (st.last_decision_idx < 0) return;
            const size_t idx = static_cast<size_t>(st.last_decision_idx);
            st.last_decision_idx = -1;
            if (idx >= st.day_decisions.size()) return;
            auto& row = st.day_decisions[idx];
            if (row.order_id != 0) {
                const auto it = st.filled_by_order.find(row.order_id);
                if (it != st.filled_by_order.end()) row.filled_qty_fp = it->second;
            }
            row.post_qty_fp = st.infra.account->snapshot_position(row.code).qty;
        };

        // carry 日界闸（每日一次，**先于**任何复投）：计龄到限即丢弃 + 对账。
        //
        // 计龄在复投之前，语义才等于「最多跨 N 个交易日复投」：反过来（先投后计龄）
        // 会让 `carry_max_days: 1` 复投两天、`0` 还复投一次 —— 配置写下的天数与实际
        // 执行天数差一。
        //
        // 对账：carry 存绝对目标，复投前用 `target − 实时持仓` 重算差额。差额归零或
        // 反向 = 这份意图已被别的通道达成（CA 送转把仓翻倍 / 新 fire 的成交），按
        // noop 摘除。盲投未成增量会把仓位推过 target —— 十送十期间持有 carry 的
        // 那只票会买成 110%，SELL 侧同形卖穿。
        auto expire_and_reconcile_carry = [&]() {
            if (!st.exec_enabled || st.exec_carry.empty()) return;
            st.exec_carry.erase(
                std::remove_if(st.exec_carry.begin(), st.exec_carry.end(),
                    [&](SelCarryLeg& l) {
                        if (l.created_date == date) return false;  // 当日生成，不计龄
                        if (l.days >= st.exec_carry_max_days) {
                            ++st.exec_carry_dropped;
                            st.exec_carry_dropped_qty_fp += l.remaining_fp;
                            XtLogger::warn_m("SEL_BT",
                                "date {}: carry '{}' {} 剩余 {} 已跨 {} 个交易日"
                                "（上限 {}）→ 丢弃", date, l.code,
                                l.is_buy ? "BUY" : "SELL",
                                order_exec_from_fp(l.remaining_fp), l.days,
                                st.exec_carry_max_days);
                            return true;
                        }
                        const int64_t diff = carry_pending_diff_fp(st, l);
                        if (diff == 0 || (diff > 0) != l.is_buy) {
                            ++st.exec_carry_noop_removed;
                            XtLogger::info_m("SEL_BT",
                                "date {}: carry '{}' {} 复投前重算差额={} → 按 noop "
                                "摘除（目标已由别的通道达成或反向）", date, l.code,
                                l.is_buy ? "BUY" : "SELL", order_exec_from_fp(diff));
                            return true;
                        }
                        l.remaining_fp = diff > 0 ? diff : -diff;
                        return false;
                    }),
                st.exec_carry.end());
        };

        // carry 复投（B7e F15）：跨日未达成的目标逐腿再提交，一腿一 drain，与 fire
        // 腿同一节奏。SELL 腿排在当日 fire 之前 —— 它是更早的退出意图，且先卖先释放
        // 现金，与「SELL→BUY 分段」的现金序同向。
        // 当日新生成的 carry 不当日复投（当日份额已用尽，再投必然 0 成交）。
        // 按下标遍历、每次重新索引：drain 里可能给**别的** code 追加 carry 腿，
        // 持 vector 引用跨 drain 会悬垂。n 取进入时的长度，本轮不复投新生成的腿。
        auto replay_carry = [&](bool want_buy) {
            for (size_t i = 0, n = st.exec_carry.size(); i < n; ++i) {
                if (st.exec_carry[i].is_buy != want_buy) continue;
                if (st.exec_carry[i].retired) continue;
                if (st.exec_carry[i].remaining_fp <= 0) continue;
                if (st.exec_carry[i].created_date == date) continue;
                const auto pit = st.current_closes_fp.find(st.exec_carry[i].code);
                // 无 T-1 价 → 本日不提交（价与 sizing 同源；日末照常计一天）。
                if (pit == st.current_closes_fp.end() || pit->second <= 0) continue;
                // 提交口再对一次账：日界闸之后到这一刻，SELL phase 的成交可能已把
                // 该 code 的持仓改到目标位（SELL 复投在 fire 之前、BUY 复投在之后）。
                const int64_t diff_fp = carry_pending_diff_fp(st, st.exec_carry[i]);
                if (diff_fp == 0 || (diff_fp > 0) != want_buy) {
                    ++st.exec_carry_noop_removed;
                    st.exec_carry[i].retired      = true;
                    st.exec_carry[i].remaining_fp = 0;
                    continue;
                }
                ++st.exec_carry_resubmits;
                const uint64_t oid = submit_target_leg(
                    st, st.exec_carry[i].strategy_id, st.exec_carry[i].code,
                    diff_fp, pit->second, XtTimeInForce::GTC, st.exec_carry[i].tag,
                    st.exec_carry[i].fire_seq, &st.exec_carry[i]);
                if (oid != 0) st.exec_carry_orders.insert(oid);
                drain_and_apply();
                backfill_last_leg();
                // 复投后按目标重新对账（复投单的未成量由本处记账，`carry_from_cap`
                // 对它整段早退，否则同一份缺口挂两条腿）。
                const int64_t left_fp = carry_pending_diff_fp(st, st.exec_carry[i]);
                st.exec_carry[i].remaining_fp =
                    (left_fp > 0) == want_buy && left_fp != 0
                        ? (left_fp > 0 ? left_fp : -left_fp)
                        : 0;
            }
        };

        expire_and_reconcile_carry();
        replay_carry(/*want_buy=*/false);

        // SELL 一笔一笔 fire + drain (避免 cash race; 与 BUY 对称)
        while (st.engine->pending_sell_count() > 0) {
            st.engine->fire_one_pending(xt_engine_sel::XtSelEngine::Phase::SellOnly);
            drain_and_apply();
            backfill_last_leg();
        }

        // BUY 一笔一笔 fire + drain，确保每笔 BUY 的 risk check 读取上一笔 fill
        // 后的 cash，避免 batch 内多笔 BUY 重复使用同一 pre-batch cash。
        while (st.engine->pending_buy_count() > 0) {
            st.engine->fire_one_pending(xt_engine_sel::XtSelEngine::Phase::BuyOnly);
            drain_and_apply();
            backfill_last_leg();
        }

        replay_carry(/*want_buy=*/true);

        // 9e) flush_session_end — 当日剩 pending 限价单 cancel + 市价单尾价成交
        st.infra.gw->flush_session_end(date, fill_time_ms);

        // 9f) 最终 drain：flush_session_end 既可能产 fill（市价单尾价成交），
        //     也必然产 Canceled update（当日剩余限价单）——后者正是 frozen 释放点。
        drain_reports();

        // 9f.1) 退市清算（EOD）：晚于当日全部提交与 drain —— 清算所得是收盘后才拿到
        //   的钱，早于决策入账就成了未来资金 leak。
        apply_delist_liquidations(st, date, price_date, fill_time_ms);

        // 9f.2) record 日收盘仓位快照：分红股权数按 record 日收盘锁定（读 ex 日仓位
        //   会把 ex 当日调仓也算进分红基数）。只留 CA 真需要的 (date, code)。
        if (const auto need = st.ca_record_needs.find(date);
            need != st.ca_record_needs.end()) {
            for (const auto& code : need->second) {
                st.ca_record_qty[std::to_string(date) + '|' + code] =
                    st.infra.account->snapshot_position(code).long_qty;
            }
        }

        // 9g) on_session_end
        st.engine->on_session_end(date);

        // session-end 账户快照用于排查 SEL backtest 仓位/资金收敛。
        const auto m = st.infra.account->snapshot_account();
        XtLogger::debug_m("SEL_BT",
                           "session-end {}: cash_fp={} equity_fp={} unrealized_fp={} "
                           "600000.long_fp={} 000001.long_fp={} 600519.long_fp={}",
                           date, m.cash, m.equity, m.unrealized_pnl,
                           st.infra.account->snapshot_position("600000.SH").long_qty,
                           st.infra.account->snapshot_position("000001.SZ").long_qty,
                           st.infra.account->snapshot_position("600519.SH").long_qty);

        // 逐日收盘权益（T1.131）：与 CTA daily_equity.csv 同 schema，viewer ingest 靠它
        // 产日粒度 equity（回撤/月度收益/Sharpe 等派生指标）。口径 = portfolio 权益
        // （含应收），与 trades.csv equity_after / summary final_equity / E3 同源。
        st.daily_equity_rows.emplace_back(date, portfolio_equity_fp(st));

        // 9h.0) trace 链收口：日终结算行（当日全部提交/成交/CA 之后的账户终态）。
        if (st.trace.is_open()) {
            size_t held = 0;
            for (const auto& code : st.traded_codes) {
                if (st.infra.account->snapshot_position(code).qty != 0) ++held;
            }
            emit_trace_settle(st, date, held);
        }

        // 9h) 本日新产物落盘（顺序无关；均在 EOD 账户终态之后，故 E3 与
        //     daily_equity 同源同刻，会计恒等成立）。
        flush_day_positions(st, date, t_day_closes);
        flush_day_decisions(st, date);
        flush_day_fires(cfg, st, date, st.holiday_probe);

        // 9h.1) 执行真实度日界簿记：carry 计龄/丢弃 + ADV 窗口推进。
        if (st.exec_enabled) {
            // 此刻 remaining==0 且未 retired 的腿恰是「复投补齐」的：被新 fire 取代
            // 的已在 fire 期摘除、noop/到限的已在日界闸摘除，各自单独计数，四个终局
            // （completed / superseded / dropped_stale / dropped / noop_removed）
            // 互不吞并。计龄放在最后 —— 判龄发生在**次日**的日界闸上。
            for (const auto& leg : st.exec_carry) {
                if (!leg.retired && leg.remaining_fp <= 0) ++st.exec_carry_completed;
            }
            for (auto& leg : st.exec_carry) {
                if (leg.retired || leg.remaining_fp <= 0 || leg.created_date == date) {
                    continue;
                }
                ++leg.days;
            }
            st.exec_carry.erase(
                std::remove_if(st.exec_carry.begin(), st.exec_carry.end(),
                               [](const SelCarryLeg& l) {
                                   return l.retired || l.remaining_fp <= 0;
                               }),
                st.exec_carry.end());
            // ADV 窗口按「有量交易日」推进（缺行的日子不进窗，否则空洞被当成 0 量
            // 拉低分母）。窗口只含**已过去**的交易日，故在日末推进。
            if (st.exec_day_volume != nullptr) {
                for (const auto& kv : *st.exec_day_volume) {
                    auto& w = st.exec_adv_window[kv.first];
                    w.push_back(kv.second);
                    if (w.size() > kAdvWindow) w.erase(w.begin());
                }
            }
        }

        // 9i) 账本不变量硬闸（同级两闸，都不能只计数继续跑——后续每一天的权益都
        //   建立在错的账上）：
        //   rc=10 现金不变量破坏（现金落负；成因不预判——BUY 可负担截断与撮合成本
        //     脱节、SELL 费用超所得、CA 现金腿等都可能，具体看逐笔 error 日志）；
        //   rc=11 权益会计恒等破坏（Σ持仓市值 + 现金 + 应收 ≠ portfolio equity）。
        if (st.negative_cash_events > 0) {
            XtLogger::error_m("SEL_BT",
                "date {}: cash invariant violated ({} negative-cash fill(s)) — "
                "现金不变量破坏，aborting run (rc=10, 产物不写出)",
                date, st.negative_cash_events);
            return 10;
        }
        if (st.equity_identity_violations > 0) {
            XtLogger::error_m("SEL_BT",
                "date {}: equity identity violated ({} day(s)) — 权益恒等破坏（口径"
                "分裂或应收漏计），aborting run (rc=11, 产物不写出)",
                date, st.equity_identity_violations);
            return 11;
        }

        prev_date = date;  // for next iteration: fire 用此日 close 作 T-1
    }

    // 回报消费完整性：run 收尾时 gateway 不应再留未 drain 的 fill / order update。
    // 非零 = 有回报从未送达 observer —— 撤单的 frozen 释放与 order-book slot 清理
    // 都挂在 order update 派发上，漏 drain 即静默泄漏（BR-03 的形态）。写进 summary
    // 供机器判读，而不只是日志。
    st.undrained_reports = st.infra.gw->pending_report_count();
    // 所有订单终态后不应有残留冻结：撤单若未释放 frozen，该值 > 0。与
    // undrained_reports 互补——前者管"回报有没有送达"，本项管"送达后有没有真释放"。
    st.frozen_cash_at_exit_fp = st.infra.account->snapshot_account().frozen_cash;
    if (st.frozen_cash_at_exit_fp != 0) {
        XtLogger::error_m("SEL_BT",
                          "frozen cash left at exit: {} (fp) —— 终态订单的冻结未释放",
                          st.frozen_cash_at_exit_fp);
    }
    if (st.undrained_reports > 0) {
        XtLogger::error_m("SEL_BT",
                          "undrained gateway reports at exit: {} —— 撤单 frozen/"
                          "book slot 释放可能未执行", st.undrained_reports);
    }

    // mark carry 披露汇总（持仓日无 T close 的估值降级次数；0 时无输出）。
    if (st.stale_mark_carry_events > 0) {
        XtLogger::warn_m("SEL_BT",
                          "mark carry disclosure: {} held-symbol-days valued at "
                          "stale (pre-T) close", st.stale_mark_carry_events);
    }
    return 0;
}

/// @brief 写 trades.csv / daily_equity.csv / summary.json（成交行已在真值点构造完毕）。
void write_reports(const XtSelRunnerConfig& cfg, SelRunState& st) {
    // ── 10) 写 trades.csv / daily_equity.csv / summary.json ───────────────────
    // equity_after 已在主循环真值点随行构造（真 running equity），此处不再统一覆盖。
    // final_equity 与 daily_equity 末行同口径（portfolio 权益，含未到账应收）。
    const double final_equity = xt_fp::from_fp(portfolio_equity_fp(st));

    std::filesystem::create_directories(cfg.report_dir);
    // 切共享 write_trades_csv (XtCtaBacktestReport.h)。
    // SEL 多策略路径 include_strategy_id=true，trades.csv schema 与原 SEL 输出一致。
    // 写失败累积后统一判：判据是「被调用且失败」，不是「文件缺失」。
    if (!write_trades_csv(cfg.report_dir, st.captured_trades, /*include_strategy_id=*/true))
        st.report_write_failed = true;
    // 逐日收盘权益（与 CTA 同 schema `date,equity`）。
    if (!write_daily_equity_csv(cfg.report_dir, st.daily_equity_rows))
        st.report_write_failed = true;
    const std::string trades_path =
        (std::filesystem::path(cfg.report_dir) / "trades.csv").string();

    // tradability 观测输出严格 gated:未配置时不创建 events 文件、summary 无新块
    // (与历史输出字节一致)。
    TradabilitySummary tradability_summary;
    const bool tradability_enabled = !cfg.tradability_csv.empty();
    if (tradability_enabled) {
        // 原子写 + 失败即 rc=9：events 是 gate 的唯一逐笔证据，半截或缺失文件
        // 会让「没有拒单」与「拒单记录丢了」不可区分。
        const std::string ev_path =
            (std::filesystem::path(cfg.report_dir) / "tradability_events.csv").string();
        PathUtil::AtomicOfstream ev_af(ev_path);
        if (!ev_af.is_open()) {
            XtLogger::error_m("SEL_BT", "tradability_events.csv open failed: {}", ev_path);
            st.report_write_failed = true;
        } else {
            auto& ev_out = ev_af.stream();
            ev_out << "date,code,side,reason,qty\n";
            char buf[64];
            for (const auto& ev : st.tradability_events) {
                ev_out << ev.date << ',' << csv_escape_field(ev.code) << ','
                       << (ev.is_buy ? "BUY" : "SELL") << ','
                       << tradability_reason_str(ev.verdict) << ',';
                // qty 数值风格对齐 trades.csv(%.4f)。
                const int n = std::snprintf(buf, sizeof(buf), "%.4f\n",
                                            xt_fp::from_fp(ev.qty_fp));
                if (n > 0) ev_out.write(buf, n);
            }
            if (!finish_report_stream(ev_af, ev_path, "SEL_BT",
                                      "tradability_events.csv",
                                      st.tradability_events.size())) {
                st.report_write_failed = true;
            }
        }

        // source 写 config 原串（绝对化只服务打开文件），否则同一 config 在不同
        // 目录/机器上跑出的 summary.json 字节不同。
        tradability_summary.source                  = cfg.display.tradability_csv;
        tradability_summary.suspended_skips         = st.suspended_skips;
        tradability_summary.limit_up_buy_rejects    = st.limit_up_buy_rejects;
        tradability_summary.limit_down_sell_rejects = st.limit_down_sell_rejects;
    }

    SelObsCounters obs;
    obs.affordability_caps       = st.affordability_caps;
    obs.affordability_shortfall  = xt_fp::from_fp(st.affordability_shortfall_fp);
    obs.negative_cash_events     = st.negative_cash_events;
    obs.min_lot_zero_targets     = st.min_lot_zero_targets;
    obs.negative_weight_rejects  = st.engine->negative_weight_rejects();
    obs.residual_cash            = xt_fp::from_fp(st.infra.account->snapshot_account().cash);
    obs.equity_identity_violations = st.equity_identity_violations;
    obs.sell_fee_unaffordable_rejects = st.sell_fee_unaffordable_rejects;
    obs.local_submit_rejects     = st.local_submit_rejects;
    // pending 语义四计数（engine 是唯一真值点）：同槽多 target 复合的可见面。
    obs.pending_dispatch_noops   = st.engine->pending_dispatch_noops();
    obs.pending_dispatch_direction_flips =
        st.engine->pending_dispatch_direction_flips();
    obs.cross_sid_same_code_pendings = st.engine->cross_sid_same_code_pendings();
    obs.stale_pending_groups_dropped = st.engine->stale_pending_groups_dropped();

    // corporate action 观测输出严格 gated：未配置时不写 events 文件、summary 无新块。
    CorporateActionSummary ca_summary;
    if (st.ca_enabled) {
        ca_summary.source                   = cfg.display.corporate_actions_csv;
        ca_summary.applied                  = st.ca_applied;
        ca_summary.cash_dividends_total     = xt_fp::from_fp(st.ca_cash_dividends_fp);
        ca_summary.delist_liquidations      = st.ca_delist_liquidations;
        ca_summary.rights_skipped           = st.corporate_actions.rights_skipped_count();
        ca_summary.out_of_window_ignored    = st.ca_align.ignored_before_window +
                                              st.ca_align.ignored_after_window;
        ca_summary.rebased_to_trading_date  = st.ca_align.rebased_to_prev_trading_date;
        ca_summary.record_qty_fallback_absent = st.ca_record_qty_fallback_absent;
        ca_summary.record_qty_fallback_ex_day = st.ca_record_qty_fallback_ex_day;
        ca_summary.record_qty_fallback_before_window =
            st.ca_record_qty_fallback_before_window;
        ca_summary.record_qty_fallbacks     = st.ca_record_qty_fallback_absent +
                                              st.ca_record_qty_fallback_ex_day +
                                              st.ca_record_qty_fallback_before_window;
        ca_summary.delist_fee_capped        = st.ca_delist_fee_capped;
        ca_summary.delist_pending_bonus_liquidated =
            st.ca_delist_pending_bonus_liquidated;
        ca_summary.delist_buy_rejects       = st.ca_delist_buy_rejects;
        ca_summary.delisted_price_reappearances = st.ca_delisted_price_reappearances;
        ca_summary.receivable_outstanding_cash =
            xt_fp::from_fp(st.ca_ledger.outstanding_cash_fp());
        ca_summary.receivable_outstanding_bonus =
            xt_fp::from_fp(bonus_receivable_value_fp(st));
        ca_summary.delist_haircut_pct       = st.ca_delist_haircut;
        ca_summary.pay_date_absent_immediate_cash =
            st.ca_pay_date_absent_immediate_cash;
        ca_summary.shares_list_date_absent_immediate =
            st.ca_shares_list_date_absent_immediate;
        ca_summary.shares_list_date_defaulted = st.ca_shares_list_date_defaulted;
        ca_summary.shares_list_date_beyond_window =
            st.ca_shares_list_date_beyond_window;
        ca_summary.shares_list_date_lagged_by_rebase =
            st.ca_shares_list_date_lagged_by_rebase;
        ca_summary.rights_policy       = rights_policy_name(st.ca_rights_policy);
        ca_summary.rights_subscribed   = st.ca_rights_subscribed;
        ca_summary.rights_forfeited    = st.ca_rights_forfeited;
        ca_summary.rights_cash_capped  = st.ca_rights_cash_capped;
        ca_summary.rights_cash_paid    = xt_fp::from_fp(st.ca_rights_cash_paid_fp);
        ca_summary.rights_same_day_cash_contention =
            st.ca_rights_same_day_cash_contention;
        ca_summary.conversions             = st.ca_conversions;
        ca_summary.conversion_buy_rejects  = st.ca_conversion_buy_rejects;
        ca_summary.conversion_cash_in_lieu =
            xt_fp::from_fp(st.ca_conversion_cash_in_lieu_fp);
        ca_summary.post_conversion_events_rejected =
            st.ca_post_conversion_events_rejected;
        ca_summary.pending_shares_sell_deferrals =
            st.ca_pending_shares_sell_deferrals;
        if (st.ca_ledger.outstanding_cash_fp() != 0 ||
            !st.ca_ledger.pending_shares().empty()) {
            XtLogger::warn_m("SEL_BT",
                "corporate action receivable outstanding at exit: cash={:.2f} "
                "bonus_value={:.2f} (pay/list date beyond backtest window)",
                ca_summary.receivable_outstanding_cash,
                ca_summary.receivable_outstanding_bonus);
        }
    }

    // 执行真实度块严格 gated：volume 列缺席且未配 `execution:` 时 summary 无此块
    // （产物与本特性落地前逐位一致）。
    ExecutionSummary exec_summary;
    if (st.exec_enabled) {
        exec_summary.integer_projection =
            cfg.projection_largest_remainder ? "largest_remainder" : "floor";
        exec_summary.volume_available    = st.exec_has_volume;
        exec_summary.max_participation   = cfg.max_participation;
        exec_summary.carry_max_days      = cfg.carry_max_days;
        exec_summary.capped_fills        = st.exec_capped_fills;
        exec_summary.capped_shortfall_qty =
            order_exec_from_fp128(st.exec_capped_shortfall_fp);
        exec_summary.volume_zero_days    = st.exec_volume_zero_seen.size();
        exec_summary.volume_missing_days = st.exec_volume_missing_seen.size();
        exec_summary.carry_created       = st.exec_carry_created;
        exec_summary.carry_resubmits     = st.exec_carry_resubmits;
        exec_summary.carry_completed     = st.exec_carry_completed;
        exec_summary.carry_superseded    = st.exec_carry_superseded;
        exec_summary.carry_noop_removed  = st.exec_carry_noop_removed;
        exec_summary.carry_dropped_stale = st.exec_carry_dropped_stale;
        exec_summary.carry_dropped_stale_qty =
            order_exec_from_fp128(st.exec_carry_dropped_stale_qty_fp);
        exec_summary.carry_stale_before_create = st.exec_carry_stale_before_create;
        exec_summary.carry_stale_before_create_qty =
            order_exec_from_fp128(st.exec_carry_stale_before_create_qty_fp);
        exec_summary.carry_sell_clamped  = st.exec_carry_sell_clamped;
        exec_summary.sell_fee_deferrals  = st.exec_sell_fee_deferrals;
        exec_summary.carry_dropped       = st.exec_carry_dropped;
        exec_summary.carry_dropped_qty   =
            order_exec_from_fp128(st.exec_carry_dropped_qty_fp);
        exec_summary.carry_outstanding   = st.exec_carry.size();
        __int128 outstanding_fp = 0;
        for (const auto& leg : st.exec_carry) outstanding_fp += leg.remaining_fp;
        exec_summary.carry_outstanding_qty = order_exec_from_fp128(outstanding_fp);
        exec_summary.adv20_samples       = st.exec_adv20_samples;
        exec_summary.adv20_ratio_max     = st.exec_adv20_ratio_max;
        exec_summary.adv20_ratio_mean    = st.exec_adv20_samples > 0
            ? st.exec_adv20_ratio_sum / static_cast<double>(st.exec_adv20_samples)
            : 0.0;
        exec_summary.legs_over_participation = st.exec_legs_over_participation;
        exec_summary.projection_fires    = st.exec_projection_fires;
        exec_summary.projection_lots_added   = st.exec_projection_lots_added;
        exec_summary.projection_lots_removed = st.exec_projection_lots_removed;
        exec_summary.projection_infeasible_fires = st.exec_projection_infeasible_fires;
        exec_summary.projection_adjusted_targets =
            st.engine->projection_adjusted_targets();
        exec_summary.projection_rejected_entries =
            st.engine->projection_rejected_entries();
    }

    // 流式产物收尾（fsync+rename）；任一失败 → rc=9（产物不完整不可信）。
    if (!st.fires_csv.commit())     st.report_write_failed = true;
    if (!st.targets_csv.commit())   st.report_write_failed = true;
    if (!st.positions_csv.commit()) st.report_write_failed = true;
    if (!st.costs_csv.commit())     st.report_write_failed = true;
    if (st.ca_enabled && !st.ca_events_csv.commit()) st.report_write_failed = true;
    if (!st.trace.commit())                          st.report_write_failed = true;

    // data_source 写 config 原串（F06 绝对化只服务打开文件）。
    std::string display_source = st.loaded.source;
    if (display_source == "csv:" + cfg.trading_dates_csv) {
        display_source = "csv:" + cfg.display.trading_dates_csv;
    }

    if (!write_summary_json(cfg.report_dir, display_source,
                            st.loaded.trading_dates.size(),
                            st.captured_trades.size(),
                            cfg.initial_cash, final_equity,
                            xt_fp::from_fp(
                                st.infra.account->snapshot_account().realized_pnl),
                            st.infra.gw->risk_reject_count(),
                            st.engine->sparse_weights_events(),
                            st.undrained_reports,
                            st.frozen_cash_at_exit_fp,
                            st.stale_mark_carry_events,
                            st.n_fires, st.n_skips,
                            obs,
                            tradability_enabled ? &tradability_summary : nullptr,
                            st.ca_enabled ? &ca_summary : nullptr,
                            st.exec_enabled ? &exec_summary : nullptr)) {
        st.report_write_failed = true;
    }

    XtLogger::info_m("SEL_BT",
                      "backtest done: dates={} trade_rows={} final_equity={:.2f} "
                      "risk_rejects={}",
                      st.loaded.trading_dates.size(), st.captured_trades.size(),
                      final_equity, st.infra.gw->risk_reject_count());
    XtLogger::info_m("SEL_BT", "trades.csv → {} ({} rows)",
                      trades_path, st.captured_trades.size());

    // consumed_manifest.json 旁写（自有 summary.json 字节不动）。SEL rows_loaded =
    // 日历 raw dates、rows_total = rebalance 提交尝试；单位不同（日 vs 提交）→
    // 结构上不可守恒 → reconciled=false。exclusions 映射既有计数。
    {
        xt_bt::ConsumedManifest man;
        if (!cfg.trading_dates_csv.empty()) man.files.push_back(cfg.trading_dates_csv);
        man.tick_source    = "sel_calendar";
        man.rows_total     = st.submit_attempts;
        man.delivery_stage = "rebalance";
        man.rows_loaded    = static_cast<int64_t>(st.loaded.raw_date_count);
        man.exclusions.symbol =
            static_cast<int64_t>(st.suspended_skips + st.limit_up_buy_rejects +
                                 st.limit_down_sell_rejects);
        man.exclusions.window =
            static_cast<int64_t>(st.loaded.raw_date_count) -
            static_cast<int64_t>(st.loaded.trading_dates.size());
        man.exclusions.bad_row = static_cast<int64_t>(st.loaded.calendar_bad_rows);
        man.exclusions.other   = static_cast<int64_t>(st.no_change_skips);
        man.reconciled = false;  // 日历日 ↔ 提交尝试单位不同，不可精确守恒。
        const std::string man_path =
            (std::filesystem::path(cfg.report_dir) / "consumed_manifest.json").string();
        if (!xt_bt::write_consumed_manifest(man_path, man)) {
            XtLogger::warn_m("SEL_BT", "consumed_manifest.json write failed: {}", man_path);
        }
    }
}

}  // namespace

int XtSelBacktestRunner::run(const XtSelRunnerConfig& cfg,
                             XtStrategyBase* external_strategy) {
    XtLogger::info_m("SEL_BT", "XtSelBacktestRunner.run starts: cfg='{}'", cfg.cfg_path);

    SelRunState st;
    // rc=1：日历/tradability/CA 输入加载失败（fail-fast）。
    if (const int rc = load_data(cfg, st); rc != 0) return rc;
    if (const int rc = assemble_sel_runtime(cfg, st, external_strategy); rc != 0) return rc;
    // rc=8：首日无 T-1 close 且策略当日请求 sizing（fail-fast，报告不写出）。
    // rc=10/11：现金落负 / 权益恒等破裂（账本不变量破裂，报告不写出）。
    // rc=1：CA 输入数据错误（除权后参考价非正）。
    if (const int rc = run_sel_loop(cfg, st); rc != 0) return rc;
    write_reports(cfg, st);
    // 报告写出失败 → 整跑失败：回测本身跑完了，但产物不可信/不完整。
    if (st.report_write_failed) {
        XtLogger::error_m("SEL_BT", "report write failed — run reported as failure (rc=9)");
        return 9;
    }
    return 0;
}

}  // namespace xt_bt_sel
