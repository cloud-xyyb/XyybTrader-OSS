#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "xt_report/core/XtReportTypes.h"
#include "xt_report/core/IReportSink.h"
#include "api/IRejectSink.h"
#include "XtBacktestDailyBreakdown.h"
#include "XtEventStreamSink.h"
#include "xt_backtest/infra/XtReplayer.h"

namespace xt_core {
struct XtOrder;
}
struct XtRiskResult;
struct XtRawTick;
struct XtTradeRecord;

/// @brief 回测报告聚合器。
///
/// 由 replay 线程串行调用，汇总 tick、成交、订单与 reject 事件。
/// `IReportSink` 承担批量表写出；`XtEventStreamSink` 承担事件流写出。
class XtBacktestReporter : public IReplayReporter, public IRejectSink {
public:
    XtBacktestReporter() = default;

    /// @brief 注入输出依赖。
    ///
    /// `report_sink` 为 non-owning，调用方保证生命周期覆盖 reporter；`event_stream_sink`
    /// 必须非空并由 reporter 接管。本函数只保存依赖，不执行 I/O；输出目录等配置
    /// 由两个 sink 各自携带，reporter 不消费 XtReportCfg。
    bool init(xt_report::IReportSink& report_sink,
              std::unique_ptr<XtEventStreamSink> event_stream_sink);

    // ── Replay hooks ─────────────────────────────────────────────────
    /// @brief tick 派发给 runtime 前调用。
    ///
    /// `ts` 为 packed timestamp；本函数缓存该时间戳，写 tick event stream，并用
    /// tick 的 trading_date/action_date 更新当前交易日与 daily breakdown。
    void on_tick_pre(int64_t ts, const XtRawTick& t) override;

    /// @brief tick 派发给 runtime 后调用。
    ///
    /// 写系统状态事件；`active_orders` 是 runtime 在该 tick 之后观察到的活动订单数。
    void on_tick_post(int64_t ts, size_t active_orders) override;

    /// @brief 写出缓存 fill，flush sink 和 event stream。
    ///
    /// 调用前必须完成 `init()`。返回 false 仅表示 `IReportSink::sync_flush()` 失败；
    /// 单条 fill 写入失败会累计 `sink_drop_count()`。
    bool flush();

    // ── Statistics / explicit outputs ────────────────────────────────
    /// @brief 已接收的 fill 数量。
    int trade_count() const { return trade_count_; }

    /// @brief sink 写入失败累计次数。
    uint64_t sink_drop_count() const noexcept { return drop_count_; }

    /// @brief 将 daily breakdown 写到指定 JSON 路径。
    ///
    /// 无 daily 数据时不创建文件并返回 true；文件打开失败返回 false。
    /// `cfg_start_date`/`cfg_end_date` 为回测 config 窗口，透传进顶层字段。
    bool write_daily_breakdown(const std::string& out_path,
                               uint32_t cfg_start_date,
                               uint32_t cfg_end_date) const;

    /// @brief 日终账户快照（equity/cash/frozen，人类单位 double）。
    struct AccountEod {
        double equity = 0.0;
        double cash = 0.0;
        double frozen = 0.0;
    };

    /// @brief 注入账户探针；replay 线程在交易日切换与 flush 时采样，写入
    ///        daily breakdown 的日终字段。不注入则不产出 eod_* 字段。
    void set_account_probe(std::function<AccountEod()> probe) {
        account_probe_ = std::move(probe);
    }

    // ── Report events ────────────────────────────────────────────────
    /// @brief 记录成交报告事件。
    ///
    /// `fill` 是报告 sink 使用的标准行；`raw` 用于写 event stream 和补齐交易日。
    /// daily breakdown 的 trading date 取值顺序为 `fill.trading_day`、`raw.trade_date`、
    /// 最近一次 tick 的 trading date。
    void on_fill(const xt_report::XtFillRow& fill, const XtTradeRecord& raw);

    /// @brief 记录订单状态更新到事件流。
    void on_order_update(const XtOrderExecUpdate& u);

    /// @brief 记录订单提交事件。
    ///
    /// `ts` 为 packed timestamp；`price_fp` 和 `qty_fp` 为 fixed-point 值。
    void on_order_submit(int64_t ts,
                         uint64_t id,
                         const char* code,
                         bool is_buy,
                         int64_t price_fp,
                         int64_t qty_fp,
                         std::string_view user_tag,
                         bool is_close,
                         bool is_today);

    /// @brief 记录风控拒单事件（daily breakdown 聚合 + 限量 error log）。
    ///
    /// `ts` 为 packed timestamp；`price_fp` 和 `qty_fp` 为 fixed-point 值。
    void on_risk_reject(int64_t ts,
                        const char* code,
                        bool is_buy,
                        int64_t price_fp,
                        int64_t qty_fp,
                        const std::string& reason);

    // ── IRejectSink ──────────────────────────────────────────────────
    /// @brief margin/cash reject 入口；使用最近 tick 的 packed timestamp 记录报告事件。
    void on_margin_reject(const xt_core::XtOrder& req,
                          const XtRiskResult& decision,
                          int64_t now_ms,
                          std::string_view strategy_id = std::string_view{}) override;

    /// @brief 通用 reject 入口；使用最近 tick 的 packed timestamp 记录报告事件。
    void on_reject(const xt_core::XtOrder& req,
                   const XtRiskResult& decision,
                   std::string_view strategy_id = std::string_view{}) override;

private:
    // ── 依赖与输出 ──────────────────────────────────────────────────────
    xt_report::IReportSink* sink_ = nullptr;              ///< non-owning；由 init 注入
    std::unique_ptr<XtEventStreamSink> event_writer_;

    // ── 缓冲数据 ──────────────────────────────────────────────────────────
    std::vector<xt_report::XtFillRow> fills_;             ///< on_fill 时收集，flush 直接写入 sink

    // ── 运行时统计状态 ──────────────────────────────────────────────────
    std::unordered_map<std::string, int> risk_rejects_;
    int trade_count_ = 0;

    // ── Daily breakdown ──────────────────────────────────────────────
    XtBacktestDailyBreakdown daily_breakdown_;
    /// 当前交易日；由 on_tick_pre 更新，submit/reject 事件缺少交易日时使用。
    uint32_t current_trading_date_ = 0;
    /// 账户探针；日切/flush 时采样日终 equity/cash/frozen（可空 = 不采样）。
    std::function<AccountEod()> account_probe_;

    /// 最近一次 on_tick_pre 收到的 packed ts；供无独立时间戳的事件使用。
    int64_t last_tick_ts_ = 0;

    /// sink->on_* 返回 false 的累计次数；replay 线程单写。
    uint64_t drop_count_ = 0;
};
