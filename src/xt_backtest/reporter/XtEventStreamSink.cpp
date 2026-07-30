#include "XtEventStreamSink.h"

#include "XtLogger.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_config/yaml/ConfigYamlUtil.h"
#include "xt_util/path/PathUtil.h"
#include "xt_report/util/XtCsvReportUtil.h"
#include "xt_util/time/XtTime.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <utility>

#include <yaml-cpp/yaml.h>

using xt_report::csv_escape;

namespace {

void ensure_dir(const std::string& dir) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
}

std::string to_lower_ascii(std::string s) {
    for (char& c : s)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

template<typename T>
T yaml_get(const YAML::Node& n, T default_val) noexcept {
    try { return n.as<T>(); }
    catch (...) { return default_val; }
}

}

XtEventStreamSink::Config parse_event_stream_sink_config(const YAML::Node& node) {
    using Config  = XtEventStreamSink::Config;
    using TraceMode = Config::TraceMode;

    Config cfg;

    if (node["trace_mode"]) {
        const std::string mode = to_lower_ascii(node["trace_mode"].as<std::string>());
        if      (mode == "off")  cfg.trace_mode = TraceMode::Off;
        else if (mode == "full") cfg.trace_mode = TraceMode::Full;

    }

    cfg.export_tick = XtConfigYamlUtil::parse_bool_node(node["export_tick"], false);

    auto load_size = [&](const char* key, size_t min_val, size_t& field) {
        if (node[key]) field = std::max(min_val, yaml_get<size_t>(node[key], field));
    };
    auto load_u64 = [&](const char* key, uint64_t& field) {
        if (node[key]) field = yaml_get<uint64_t>(node[key], field);
    };

    load_size("full_max_rows",             1,  cfg.full_max_rows);
    load_u64 ("full_max_bytes",                cfg.full_max_bytes);
    load_size("sys_stats_sample_n_ticks",  1,  cfg.sys_stats_sample_n_ticks);
    load_size("max_latency_map_entries",  64,  cfg.max_latency_map_entries);

    return cfg;
}

const char* XtEventStreamSinkCfg::mode_name(TraceMode mode) {
    switch (mode) {
    case TraceMode::Off:  return "off";
    case TraceMode::Full: return "full";
    default:              return "unknown";
    }
}

void XtEventStreamSinkCfg::validate(std::vector<std::string>& errors) const {
    if (full_max_rows == 0)
        errors.push_back("backtest.full_max_rows 必须 > 0");
    if (full_max_bytes == 0)
        errors.push_back("backtest.full_max_bytes 必须 > 0");
    if (sys_stats_sample_n_ticks == 0)
        errors.push_back("backtest.sys_stats_sample_n_ticks 必须 > 0");
}

XtEventStreamSink::XtEventStreamSink(const Config& cfg, std::string base_dir,
                                     std::string report_dir)
    : report_dir_(std::move(report_dir)), base_dir_(std::move(base_dir)) {
    set_config(cfg);
}

void XtEventStreamSink::set_config(const Config& cfg) {
    if (full_events_.is_open()) full_events_.close();

    trace_mode_ = cfg.trace_mode;
    export_tick_ = cfg.export_tick;
    full_max_rows_ = cfg.full_max_rows;
    full_max_bytes_ = cfg.full_max_bytes;
    max_latency_map_entries_ = cfg.max_latency_map_entries;
    sys_stats_sample_n_ticks_ = std::max<size_t>(1, cfg.sys_stats_sample_n_ticks);
    sys_stats_call_seq_ = 0;

    ring_event_seq_ = 0;
    full_rows_written_ = 0;
    full_bytes_written_ = 0;
    full_row_cap_logged_     = false;
    full_bytes_cap_logged_   = false;
    latency_map_full_logged_ = false;

    latency_map_.clear();
    latency_map_.reserve(cfg.max_latency_map_entries);

    if (!enabled()) return;

    if (!base_dir_.empty()) {
        report_dir_ = PathUtil::resolve_relative(base_dir_, report_dir_);
    }
    ensure_dir(report_dir_);

    if (trace_mode_ == Config::TraceMode::Full) {
        open_files();
    }
}

void XtEventStreamSink::open_files() {
    full_events_.open(report_dir_ + "/events_full.csv", std::ios::out | std::ios::trunc);
    if (!full_events_.is_open()) {
        XtLogger::error_m("REPORT", "open full event file failed dir={}", report_dir_);
        return;
    }

    const std::string header = "ts,seq,event_type,payload\n";
    full_events_ << header;
    full_bytes_written_ += static_cast<uint64_t>(header.size());
}

void XtEventStreamSink::append_ring_event(int64_t ts, const char* event_type,
                                           const char* payload, int payload_len) {
    if (!enabled()) return;

    RingEventRow& row = scratch_row_;
    row.seq = ++ring_event_seq_;
    row.ts = ts;
    row.event_type = event_type ? event_type : "";

    size_t n = payload_len > 0 ? static_cast<size_t>(payload_len) : 0;
    if (n >= sizeof(fmt_buf_)) n = sizeof(fmt_buf_) - 1;
    row.payload.assign(payload, n);

    write_full_event(row);
}

void XtEventStreamSink::write_full_event(const RingEventRow& row) {
    if (!full_events_.is_open()) return;

    if (full_rows_written_ >= full_max_rows_) {
        if (!full_row_cap_logged_) {
            XtLogger::warn_m("REPORT", "events_full.csv capped at {} rows", full_max_rows_);
            full_row_cap_logged_ = true;
        }
        return;
    }

    int n = std::snprintf(fmt_buf_, sizeof(fmt_buf_), "%lld,%llu,%s,",
                          static_cast<long long>(row.ts),
                          static_cast<unsigned long long>(row.seq),
                          row.event_type.c_str());
    if (n <= 0 || static_cast<size_t>(n) >= sizeof(fmt_buf_)) return;

    const std::string escaped = csv_escape(row.payload);
    const uint64_t line_size = static_cast<uint64_t>(n) + escaped.size() + 1;

    if (full_bytes_written_ + line_size > full_max_bytes_) {
        if (!full_bytes_cap_logged_) {
            XtLogger::warn_m("REPORT", "events_full.csv capped at {} bytes", full_max_bytes_);
            full_bytes_cap_logged_ = true;
        }
        return;
    }

    full_events_.write(fmt_buf_, n);
    full_events_ << escaped << "\n";
    full_bytes_written_ += line_size;
    ++full_rows_written_;
}

void XtEventStreamSink::compute_micro_metrics(const XtRawTick& t, double& spread, double& depth, double& ofi) {
    const double bid1 = t.bid_prices[0];
    const double ask1 = t.ask_prices[0];
    spread = (bid1 > 0.0 && ask1 > 0.0) ? (ask1 - bid1) : 0.0;

    depth = 0.0;
    for (int i = 0; i < XtRawTick::DEPTH; ++i) {
        depth += t.bid_qty[i] + t.ask_qty[i];
    }

    const double denom = t.bid_qty[0] + t.ask_qty[0];
    ofi = denom > 0.0 ? (t.bid_qty[0] - t.ask_qty[0]) / denom : 0.0;
}

void XtEventStreamSink::on_tick(int64_t ts, const XtRawTick& t) {
    if (!enabled()) return;
    if (!export_tick_) return;

    double spread = 0.0, depth = 0.0, ofi = 0.0;
    compute_micro_metrics(t, spread, depth, ofi);

    int n = std::snprintf(fmt_buf_, sizeof(fmt_buf_),
                          "code=%s,price=%.6f,action_date=%d,action_time=%d"
                          ",spread=%.6f,depth=%.2f,ofi=%.6f",
                          t.code, t.price, t.action_date, t.action_time,
                          spread, depth, ofi);
    append_ring_event(ts, "tick_l2", fmt_buf_, n);
}

void XtEventStreamSink::on_order_submit(int64_t ts,
                                        uint64_t order_id,
                                        const char* code,
                                        bool is_buy,
                                        int64_t price_fp,
                                        int64_t qty_fp,
                                        std::string_view user_tag,
                                        bool is_close,
                                        bool is_today) {
    if (!enabled()) return;
    const double price = order_exec_from_fp(price_fp);
    const double qty = order_exec_from_fp(qty_fp);

    if (latency_map_.size() >= max_latency_map_entries_) {
        sweep_terminal_latency_();
    }
    if (latency_map_.size() >= max_latency_map_entries_) {
        if (!latency_map_full_logged_) {
            XtLogger::warn_m("REPORT", "latency_map full ({} entries), skipping order {}",
                             max_latency_map_entries_, order_id);
            latency_map_full_logged_ = true;
        }
    } else {
        LatencyInfo info;
        info.code = code ? code : "";
        info.is_buy = is_buy;
        info.signal_ts = ts;
        info.order_ts = ts;
        info.last_status = XtOrderStatus::Submitted;
        latency_map_.emplace(order_id, std::move(info));
    }

    int n = std::snprintf(fmt_buf_, sizeof(fmt_buf_),
                          "order_id=%llu,code=%s,side=%s,status=Submitted"
                          ",price=%.6f,qty=%.6f,is_close=%d,is_today=%d,user_tag=%.*s",
                          static_cast<unsigned long long>(order_id),
                          code ? code : "",
                          is_buy ? "BUY" : "SELL",
                          price, qty,
                          is_close ? 1 : 0,
                          is_today ? 1 : 0,
                          static_cast<int>(user_tag.size()), user_tag.data());
    append_ring_event(ts, "order_submit", fmt_buf_, n);
}

void XtEventStreamSink::on_order_update(int64_t ts, const XtOrderExecUpdate& u) {
    if (!enabled()) return;

    auto it = latency_map_.find(u.id);
    const char* code = (it != latency_map_.end()) ? it->second.code.c_str() : "";

    int n = std::snprintf(fmt_buf_, sizeof(fmt_buf_),
                          "order_id=%llu,code=%s,side=%s,status=%s"
                          ",price=%.6f,qty=%.6f,filled=%.6f,remaining=%.6f,last_fill=%.6f",
                          static_cast<unsigned long long>(u.id),
                          code,
                          u.is_buy ? "BUY" : "SELL",
                          order_status_to_str(u.status),
                          u.price(), u.qty(), u.filled(), u.remaining(), u.last_fill());
    append_ring_event(ts, "order_update", fmt_buf_, n);

    if (it == latency_map_.end()) {
        if (latency_map_.size() < max_latency_map_entries_) {

            LatencyInfo info;
            info.symbol_id = u.symbol_id;
            info.is_buy = u.is_buy;
            info.last_status = u.status;
            auto [ins, _] = latency_map_.emplace(u.id, std::move(info));
            it = ins;
        } else {
            return;
        }
    }

    if (u.status == XtOrderStatus::Accepted && it->second.ack_ts == 0)
        it->second.ack_ts = ts;
    if ((u.status == XtOrderStatus::PartiallyFilled || u.status == XtOrderStatus::Filled) &&
        it->second.fill_ts == 0)
        it->second.fill_ts = ts;
    it->second.last_status = u.status;

    if (u.status == XtOrderStatus::Filled ||
        u.status == XtOrderStatus::Canceled ||
        u.status == XtOrderStatus::Rejected) {

        it->second.terminal_seen = true;
        it->second.terminal_ts   = ts;

        const bool trade_expected =
            (u.status == XtOrderStatus::Filled) || (it->second.fill_ts != 0);
        const bool legs_already_stamped = it->second.match_ts != 0;
        if (!trade_expected || legs_already_stamped) {
            write_latency_if_ready(u.id, it->second, ts);
            latency_map_.erase(it);
        }
    }
}

size_t XtEventStreamSink::sweep_terminal_latency_() {

    size_t swept = 0;
    for (auto it = latency_map_.begin(); it != latency_map_.end();) {
        if (it->second.terminal_seen) {
            write_latency_if_ready(it->first, it->second, it->second.terminal_ts);
            it = latency_map_.erase(it);
            ++swept;
        } else {
            ++it;
        }
    }
    latency_terminal_no_trade_ += swept;
    return swept;
}

void XtEventStreamSink::on_trade(int64_t ts, const XtTradeRecord& tr) {
    if (!enabled()) return;

    int64_t visible_packed = 0;
    if (tr.sim_visible_at_ns >= 0) {
        uint32_t vis_date = 0, vis_time_ms = 0;
        xt_time::from_ns(tr.sim_visible_at_ns, vis_date, vis_time_ms);
        visible_packed = xt_time::packed_ts(vis_date, vis_time_ms);
    }

    if (auto it = latency_map_.find(tr.order_id); it != latency_map_.end()) {
        if (it->second.match_ts == 0)
            it->second.match_ts = xt_time::packed_ts(tr.trade_date, tr.trade_time);

        if (it->second.model_visible_ts == 0 && visible_packed != 0)
            it->second.model_visible_ts = visible_packed;

        if (it->second.terminal_seen) {
            write_latency_if_ready(tr.order_id, it->second, it->second.terminal_ts);
            latency_map_.erase(it);
        }
    }

    const double qty_d = order_exec_from_fp(tr.qty_fp);
    const double slip_cost = (tr.exec_price - tr.price) * (tr.is_buy ? 1.0 : -1.0) * qty_d;
    int n = std::snprintf(fmt_buf_, sizeof(fmt_buf_),
                          "order_id=%llu,code=%s,side=%s,qty=%.6f"
                          ",price=%.6f,exec_price=%.6f,fee=%.6f,slippage=%.6f,slippage_cost=%.6f"
                          ",visible_at=%lld",
                          static_cast<unsigned long long>(tr.order_id),
                          tr.code.c_str(),
                          tr.is_buy ? "BUY" : "SELL",
                          qty_d, tr.price, tr.exec_price,
                          tr.fee, tr.slippage, slip_cost,
                          static_cast<long long>(visible_packed));
    append_ring_event(ts, "trade", fmt_buf_, n);
}

void XtEventStreamSink::on_sys_stats(int64_t ts, size_t live_orders, int drops, int reconnects) {
    if (!enabled()) return;

    ++sys_stats_call_seq_;
    if (sys_stats_sample_n_ticks_ > 1 &&
        (sys_stats_call_seq_ % sys_stats_sample_n_ticks_) != 0) {
        return;
    }

    int n = std::snprintf(fmt_buf_, sizeof(fmt_buf_),
                          "live_orders=%zu,drops=%d,reconnects=%d",
                          live_orders, drops, reconnects);
    append_ring_event(ts, "sys_stats", fmt_buf_, n);
}

void XtEventStreamSink::write_latency_if_ready(uint64_t order_id, const LatencyInfo& info, int64_t ts) {

    const char* code_str = info.code.c_str();
    char sym_buf[32] = {};
    if (info.code.empty() && info.symbol_id != 0) {
        std::snprintf(sym_buf, sizeof(sym_buf), "sym:%llu",
                      static_cast<unsigned long long>(info.symbol_id));
        code_str = sym_buf;
    }

    int n = std::snprintf(fmt_buf_, sizeof(fmt_buf_),
                          "order_id=%llu,code=%s,side=%s"
                          ",signal_ts=%lld,order_ts=%lld,ack_ts=%lld,fill_ts=%lld,last_status=%s"
                          ",match_ts=%lld,visible_at=%lld",
                          static_cast<unsigned long long>(order_id),
                          code_str,
                          info.is_buy ? "BUY" : "SELL",
                          static_cast<long long>(info.signal_ts),
                          static_cast<long long>(info.order_ts),
                          static_cast<long long>(info.ack_ts),
                          static_cast<long long>(info.fill_ts),
                          order_status_to_str(info.last_status),
                          static_cast<long long>(info.match_ts),
                          static_cast<long long>(info.model_visible_ts));
    append_ring_event(ts, "latency", fmt_buf_, n);
}

void XtEventStreamSink::flush() {
    if (!enabled()) return;

    const size_t swept = sweep_terminal_latency_();
    if (swept > 0) {
        XtLogger::info_m("REPORT",
                         "latency sweep at flush: {} order(s) had a terminal report "
                         "but no trade", swept);
    }
    if (full_events_.is_open()) full_events_.flush();
}
