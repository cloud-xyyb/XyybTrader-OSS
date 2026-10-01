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
    // stdout 心跳与 XT_PROGRESS 同流进 run.log（viewer live tail 可见；stderr 会被
    // run_engine.sh 引到 time.log），独立于 log_cfg；大数据集合并可持续数分钟，
    // 心跳让上层能区分「在读」与「卡死」。管道下全缓冲需逐行 fflush。
    std::fprintf(stdout, "[tick-data] collect start: %zu files\n", paths.size());
    std::fflush(stdout);

    // 复用 reader 的格式识别和 volume 解释规则，避免 memory 路径与 stream 路径分叉。
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
        // 准入谓词在排序前逐 tick 过滤：窗外 tick 不进 out，收敛峰值驻留；
        // 逐 tick 独立判定不影响留存 tick 的相对顺序，故与排序后过滤逐字节等价。
        if (keep && !keep(t)) { ++window_drops; continue; }
        XtTickWithKey tw;
        tw.tick = t;
        // 与流式 heap 归并路径使用同一 CTP exchange-TD sort key。
        tw.key = make_tick_sort_key(t.trading_date, t.action_time);
        out.push_back(tw);
    }

    // reader 层丢弃分类（symbol/bad_row）+ keep 谓词窗口丢弃，供 consumed manifest。
    if (out_stats) {
        const auto& rs = reader.read_stats();
        out_stats->lines_read = rs.lines_read;
        out_stats->symbol     = rs.symbol;
        out_stats->bad_row    = rs.bad_row;
        out_stats->window     = window_drops;
    }

    if (out.empty()) return false;

    // 千万级 tick 的 stable_sort 同属长静默段（夹在最后一条 merge progress 与写盘之间），
    // 落一条阶段标记，让上层能把「还在排序」与「卡死」区分开。
    std::fprintf(stdout, "[tick-data] merge sort: %zu ticks\n", out.size());
    std::fflush(stdout);

    // reader 的 heap 归并假设单文件内部有序；memory 路径额外 stable_sort，
    // 让离线 CSV 局部乱序也不会改变最终回放顺序。key 相等时保持读取顺序。
    std::stable_sort(out.begin(), out.end(), [](const XtTickWithKey& a, const XtTickWithKey& b) {
        return a.key < b.key;
    });
    return true;
}
