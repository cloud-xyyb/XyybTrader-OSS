#include "xt_backtest/data/reader/XtTickCollector.h"

#include "xt_backtest/data/reader/XtCsvTickReader.h"
#include "TickCsvUtil.h"

#include <algorithm>
#include <cstdio>

bool collect_ticks_from_paths(const std::vector<std::string>& paths,
                              std::vector<XtTickWithKey>& out,
                              const std::unordered_set<std::string>& symbol_filter,
                              double qty_base,
                              XtTickVolumeMode vol_mode,
                              const std::function<bool(const XtRawTick&)>& keep,
                              CollectStats* out_stats) {
    out.clear();

    std::fprintf(stdout, "[tick-data] collect start: %zu files\n", paths.size());
    std::fflush(stdout);

    XtCsvTickReader::Options opts;
    opts.paths         = paths;
    opts.symbol_filter = symbol_filter;
    opts.qty_base      = qty_base;
    opts.volume_mode   = vol_mode;

    XtCsvTickReader reader(std::move(opts));
    if (!reader.init()) return false;

    XtRawTick t;
    size_t n_read = 0;
    int64_t window_drops = 0;
    while (reader.next_tick(t)) {
        if (++n_read % 1000000 == 0) {
            std::fprintf(stdout, "[tick-data] merge progress: %zu ticks read\n", n_read);
            std::fflush(stdout);
        }

        if (keep && !keep(t)) { ++window_drops; continue; }
        XtTickWithKey tw;
        tw.tick = t;

        tw.key = make_tick_sort_key(t.trading_date, t.action_time);
        out.push_back(tw);
    }

    if (out_stats) {
        const auto& rs = reader.read_stats();
        out_stats->lines_read = rs.lines_read;
        out_stats->symbol     = rs.symbol;
        out_stats->bad_row    = rs.bad_row;
        out_stats->window     = window_drops;
    }

    if (out.empty()) return false;

    std::fprintf(stdout, "[tick-data] merge sort: %zu ticks\n", out.size());
    std::fflush(stdout);

    std::stable_sort(out.begin(), out.end(), [](const XtTickWithKey& a, const XtTickWithKey& b) {
        return a.key < b.key;
    });
    return true;
}
