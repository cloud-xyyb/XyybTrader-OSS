#include "xt_backtest/data/reader/XtTickCollector.h"

#include "xt_backtest/data/reader/XtCsvTickReader.h"
#include "TickCsvUtil.h"

#include <algorithm>

bool collect_ticks_from_paths(const std::vector<std::string>& paths,
                              std::vector<XtTickWithKey>& out,
                              const std::unordered_set<std::string>& symbol_filter,
                              double qty_base,
                              XtTickVolumeMode vol_mode) {
    out.clear();

    XtCsvTickReader::Options opts;
    opts.paths         = paths;
    opts.symbol_filter = symbol_filter;
    opts.qty_base      = qty_base;
    opts.volume_mode   = vol_mode;

    XtCsvTickReader reader(std::move(opts));
    if (!reader.init()) return false;

    XtRawTick t;
    while (reader.next_tick(t)) {
        XtTickWithKey tw;
        tw.tick = t;

        tw.key = make_tick_sort_key(t.trading_date, t.action_time);
        out.push_back(tw);
    }

    if (out.empty()) return false;

    std::stable_sort(out.begin(), out.end(), [](const XtTickWithKey& a, const XtTickWithKey& b) {
        return a.key < b.key;
    });
    return true;
}
