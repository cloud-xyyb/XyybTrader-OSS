#include "xt_backtest/data/event_stream/ReplayEventBuilder.h"

#include "xt_backtest/data/source/XtMarketDataSource.h"

#include "XtLogger.h"
#include "OrderFlowCsvUtil.h"
#include "xt_util/path/PathUtil.h"
#include "TickCsvUtil.h"

#include <algorithm>
#include <fstream>

namespace {

int replay_event_priority(XtReplayEventType type) {
    switch (type) {
    case XtReplayEventType::Tick: return 0;
    case XtReplayEventType::OrderDetail: return 1;
    case XtReplayEventType::Transaction: return 1;
    case XtReplayEventType::OrderQueue: return 1;
    }
    return 2;
}

bool is_bin_file(const std::string& path) {
    return PathUtil::lower_ext(path) == "bin";
}

template <typename RecordT, typename ParseFn, typename FillFn, typename KeyFn>
bool append_orderflow_files(const std::vector<std::string>& files,
                            const char* tag,
                            XtOrderFlowCsvKind kind,
                            XtReplayEventType type,
                            ParseFn&& parse_csv,
                            FillFn&& fill_event,
                            KeyFn&& key_of,
                            std::vector<XtReplayEvent>& out,
                            uint64_t& seq) {
    bool ok = true;
    for (const auto& path : files) {
        if (path.empty()) continue;
        if (!PathUtil::file_exists(path)) {
            XtLogger::warn_m("MD", "{} file missing {}", tag, path);
            ok = false;
            continue;
        }

        if (is_bin_file(path)) {
            std::ifstream in(path, std::ios::binary);
            if (!in.is_open()) {
                XtLogger::warn_m("MD", "failed to open {} bin {}", tag, path);
                ok = false;
                continue;
            }
            RecordT rec;
            while (in.read(reinterpret_cast<char*>(&rec), sizeof(RecordT))) {
                XtReplayEvent ev;
                ev.type = type;
                fill_event(ev, rec);
                ev.seq = seq++;
                ev.key = key_of(rec);
                out.push_back(ev);
            }
            continue;
        }

        std::ifstream in(path);
        if (!in.is_open()) {
            XtLogger::warn_m("MD", "failed to open {} csv {}", tag, path);
            ok = false;
            continue;
        }
        std::string line;
        XtOrderFlowCsvSpec spec;
        while (std::getline(in, line)) {
            if (line.empty() || line[0] == '#') continue;
            if (!spec.inited) {
                if (!build_orderflow_spec_from_header(line, kind, spec)) {
                    XtLogger::warn_m("MD", "{} csv missing required header {}", tag, path);
                    ok = false;
                    break;
                }
                continue;
            }
            RecordT rec;
            uint64_t key = 0;
            if (!parse_csv(line, spec, rec, key)) continue;
            XtReplayEvent ev;
            ev.type = type;
            fill_event(ev, rec);
            ev.seq = seq++;
            ev.key = key;
            out.push_back(ev);
        }
    }
    return ok;
}

void append_tick_events(const std::vector<XtRawTick>& ticks,
                        std::vector<XtReplayEvent>& out,
                        uint64_t& seq) {
    out.reserve(out.size() + ticks.size());
    for (const auto& t : ticks) {
        XtReplayEvent ev;
        ev.type = XtReplayEventType::Tick;
        ev.tick = t;
        ev.seq = seq++;
        ev.key = make_tick_sort_key(t.trading_date, t.action_time);
        out.push_back(ev);
    }
}

bool append_order_queue_events(const std::vector<std::string>& files,
                               std::vector<XtReplayEvent>& out,
                               uint64_t& seq) {
    return append_orderflow_files<XtOrderQueue>(
        files,
        "order_queue",
        XtOrderFlowCsvKind::OrderQueue,
        XtReplayEventType::OrderQueue,
        parse_order_queue_csv_line,
        [](XtReplayEvent& ev, const XtOrderQueue& rec) { ev.order_queue = rec; },
        [](const XtOrderQueue& rec) {
            return make_tick_sort_key(rec.trading_date, rec.action_time);
        },
        out,
        seq);
}

bool append_order_detail_events(const std::vector<std::string>& files,
                                std::vector<XtReplayEvent>& out,
                                uint64_t& seq) {
    return append_orderflow_files<XtOrderDetail>(
        files,
        "order_detail",
        XtOrderFlowCsvKind::OrderDetail,
        XtReplayEventType::OrderDetail,
        parse_order_detail_csv_line,
        [](XtReplayEvent& ev, const XtOrderDetail& rec) { ev.order_detail = rec; },
        [](const XtOrderDetail& rec) {
            return make_tick_sort_key(rec.trading_date, rec.action_time);
        },
        out,
        seq);
}

bool append_transaction_events(const std::vector<std::string>& files,
                               std::vector<XtReplayEvent>& out,
                               uint64_t& seq) {
    return append_orderflow_files<XtTransaction>(
        files,
        "transaction",
        XtOrderFlowCsvKind::Transaction,
        XtReplayEventType::Transaction,
        parse_transaction_csv_line,
        [](XtReplayEvent& ev, const XtTransaction& rec) { ev.transaction = rec; },
        [](const XtTransaction& rec) {
            return make_tick_sort_key(rec.trading_date, rec.action_time);
        },
        out,
        seq);
}

}

bool build_replay_event_stream(const std::vector<XtRawTick>& ticks,
                               const std::vector<std::string>& order_queue_files,
                               const std::vector<std::string>& order_detail_files,
                               const std::vector<std::string>& transaction_files,
                               std::vector<XtReplayEvent>& out) {
    XtLogger::warn_m("MD", "[event_stream] 此路径尚未经过真实 Level-2 数据验证，"
                           "请确认数据格式正确后再用于生产回测。");
    out.clear();

    uint64_t seq = 0;
    append_tick_events(ticks, out, seq);
    const bool order_detail_ok = append_order_detail_events(order_detail_files, out, seq);
    const bool transaction_ok  = append_transaction_events(transaction_files, out, seq);
    const bool order_queue_ok  = append_order_queue_events(order_queue_files, out, seq);
    const bool orderflow_files_ok = order_detail_ok && transaction_ok && order_queue_ok;

    std::stable_sort(out.begin(), out.end(), [](const XtReplayEvent& a, const XtReplayEvent& b) {
        if (a.key != b.key) return a.key < b.key;
        int pa = replay_event_priority(a.type);
        int pb = replay_event_priority(b.type);
        if (pa != pb) return pa < pb;
        return a.seq < b.seq;
    });

    size_t tick_n = 0;
    size_t oq_n = 0;
    size_t od_n = 0;
    size_t tr_n = 0;
    for (const auto& ev : out) {
        switch (ev.type) {
        case XtReplayEventType::Tick: ++tick_n; break;
        case XtReplayEventType::OrderQueue: ++oq_n; break;
        case XtReplayEventType::OrderDetail: ++od_n; break;
        case XtReplayEventType::Transaction: ++tr_n; break;
        }
    }
    if (oq_n + od_n + tr_n == 0) {
        XtLogger::error_m("MD", "event_stream requires at least one readable orderflow event");
        out.clear();
        return false;
    }
    if (!orderflow_files_ok) {
        XtLogger::error_m("MD", "event_stream orderflow file load failed");
        out.clear();
        return false;
    }
    XtLogger::info_m("MD", "event stream ready events={} tick={} ordque={} orddtl={} trans={}",
                     out.size(), tick_n, oq_n, od_n, tr_n);
    return !out.empty();
}
