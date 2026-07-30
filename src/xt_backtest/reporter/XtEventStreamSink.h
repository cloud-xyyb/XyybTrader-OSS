#pragma once
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>

#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "XtEventStreamSinkCfg.h"

struct XtRawTick;

/// @brief 回测事件流导出器。
///
/// 本类写出 tick、订单、成交和系统状态事件，并从订单生命周期派生 latency 事件。
/// Full 模式写入 `events_full.csv`；Off 模式下所有事件入口直接返回。
/// 所有时间参数使用 packed YYYYMMDD*1e9 + HHMMSSmmm。
class XtEventStreamSink {
public:
    using Config = XtEventStreamSinkCfg;

    XtEventStreamSink() = delete;

    explicit XtEventStreamSink(const Config& cfg, std::string base_dir = "",
                               std::string report_dir = "reports");

    bool enabled() const { return trace_mode_ != Config::TraceMode::Off; }

    void on_tick(int64_t ts, const XtRawTick& t);

    void on_order_update(int64_t ts, const XtOrderExecUpdate& u);

    void on_trade(int64_t ts, const XtTradeRecord& tr);

    void on_order_submit(int64_t ts,
                         uint64_t order_id,
                         const char* code,
                         bool is_buy,
                         int64_t price_fp,
                         int64_t qty_fp,
                         std::string_view user_tag,
                         bool is_close,
                         bool is_today);

    void on_sys_stats(int64_t ts, size_t live_orders, int drops = 0, int reconnects = 0);

    void flush();

private:

    struct LatencyInfo {
        std::string code;
        uint64_t symbol_id = 0;
        bool is_buy = true;
        int64_t signal_ts = 0;
        int64_t order_ts = 0;
        int64_t ack_ts = 0;
        int64_t fill_ts = 0;
        int64_t match_ts = 0;
        int64_t model_visible_ts = 0;
        XtOrderStatus last_status = XtOrderStatus::Submitted;
        bool terminal_seen = false;
        int64_t terminal_ts = 0;
    };

    /** @brief 单行事件（event row）。 */
    struct RingEventRow {
        uint64_t seq = 0;
        int64_t ts = 0;
        std::string event_type;
        std::string payload;
    };

    void set_config(const Config& cfg);

    void open_files();

    void append_ring_event(int64_t ts, const char* event_type, const char* payload, int payload_len);

    void write_full_event(const RingEventRow& row);

    void compute_micro_metrics(const XtRawTick& t, double& spread, double& depth, double& ofi);

    size_t sweep_terminal_latency_();

    void write_latency_if_ready(uint64_t order_id, const LatencyInfo& info, int64_t ts);

    Config::TraceMode trace_mode_ = Config::TraceMode::Off;
    bool export_tick_ = false;
    std::string report_dir_ = "reports";
    std::string base_dir_;
    size_t full_max_rows_ = 2000000;
    uint64_t full_max_bytes_ = 512ULL * 1024ULL * 1024ULL;
    size_t max_latency_map_entries_ = 4096;
    uint64_t ring_event_seq_ = 0;
    size_t full_rows_written_ = 0;
    uint64_t full_bytes_written_ = 0;
    bool full_row_cap_logged_      = false;
    bool full_bytes_cap_logged_    = false;
    bool latency_map_full_logged_  = false;
    size_t latency_terminal_no_trade_ = 0;
    size_t sys_stats_sample_n_ticks_ = 100;
    size_t sys_stats_call_seq_ = 0;

    std::ofstream full_events_;

    RingEventRow scratch_row_;

    std::unordered_map<uint64_t, LatencyInfo> latency_map_;

    char fmt_buf_[1024];
};
