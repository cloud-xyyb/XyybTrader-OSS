#include "xt_backtest/data/legacy/XtBarModeDriver.h"
#include "xt_backtest/data/legacy/XtBarSimTickGen.h"
#include "XtLogger.h"
#include "xt_core/market/XtTickBinCodec.h"
#include "xt_core/market/XtTickConvert.h"
#include "xt_util/time/XtTimeParse.h"
#include "xt_util/path/PathUtil.h"
#include "TickFileUtil.h"
#include "xt_backtest/data/source/XtMarketDataSourceConfig.h"

#include <algorithm>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// 配置注入与 bar 输入源解析
// ---------------------------------------------------------------------------

void XtBarModeDriver::apply_config(const XtMarketDataSourceConfig& d) {
    read_cfg.spread      = d.simtick_spread;
    read_cfg.qty_base    = d.qty_base;
    read_cfg.range_start = d.start_date;
    read_cfg.range_end   = d.end_date;

    bar.interval_sec = d.bar_interval_sec;
    bar.code         = d.bar_code;
    bar.is_csv       = false;
    bar.path.clear();
    bar.paths.clear();

    // simtick.from_bar 与 save 共用一个配置开关：启用生成时同步允许落盘缓存。
    simtick.from_bar = d.simtick_enable;
    simtick.save     = d.simtick_enable;
    simtick.dir      = d.simtick_dir;
    simtick.format   = d.simtick_format;
    simtick.is_bin   = false;
    simtick.path.clear();
    simtick.paths.clear();

    hot.use_hot       = d.use_hot;
    hot.path          = d.hot_path;
    hot.months.clear();
    hot.bin_exists.clear();
    hot.range_enabled = false;
    hot.bin_path.clear();
    hot.bin_available = false;
}

bool XtBarModeDriver::prepare(const XtMarketDataSourceConfig& d) {
    // bar 数据源参数。
    bar.root   = d.bar_root;
    bar.symbol = d.bar_symbol;
    bar.suffix = d.bar_suffix;
    if (!d.has_bar_code && !bar.symbol.empty())
        bar.code = bar.symbol;

    // 主力合约滚动路径（hot）。
    if (hot.use_hot && !hot.path.empty()) {
        hot.months        = XtTimeParse::months_between(read_cfg.range_start, read_cfg.range_end);
        hot.range_enabled = !hot.months.empty();

        // 月份级 simtick bin 缓存必须全量命中；部分命中时退回 hot CSV 生成路径。
        if (simtick.from_bar && simtick.format == "bin") {
            XtLogger::info_m("MD", "HOT simtick bin cache check: start={} end={} months={}",
                read_cfg.range_start, read_cfg.range_end, hot.months.size());
            if (!hot.range_enabled) {
                XtLogger::warn_m("MD", "HOT simtick bin cache: SKIP (date range empty)");
            } else {
                simtick.paths.clear();
                hot.bin_exists.clear();
                for (const auto& m : hot.months) {
                    const std::string bin = build_simtick_path_for_month(m);
                    simtick.paths.push_back(bin);
                    hot.bin_exists.push_back(PathUtil::file_exists(bin));
                }
                const size_t total = hot.months.size();
                const size_t hit   = static_cast<size_t>(std::count(
                    hot.bin_exists.begin(), hot.bin_exists.end(), true));
                if (hit == total) {
                    simtick.is_bin = true;
                    XtLogger::info_m("MD", "HOT simtick bin cache: HIT count={}", total);
                    return true;
                }
                // 部分命中：is_bin 保持 false，本轮退回 hot CSV。
                XtLogger::info_m("MD", "HOT simtick bin cache: HIT={} MISS={}", hit, total - hit);
            }
        }

        bar.path   = hot.path;
        bar.is_csv = true;
        bar.paths  = { bar.path };
        return true;
    }

    if (!d.bar_path.empty()) {
        bar.path   = d.bar_path;
        bar.is_csv = true;
        bar.paths  = { d.bar_path };
        return true;
    }

    // 从 symbol 推导月份路径；validate 已约束 start_date/end_date 非零且有序。
    if (!bar.symbol.empty() && build_bar_paths_from_symbol())
        return true;

    XtLogger::error_m("MD",
        "bar mode has no resolvable input "
        "(need bar_path/hot_path or bar.root+bar.symbol with valid start_date/end_date)");
    return false;
}

// ---------------------------------------------------------------------------
// 路径推导
// ---------------------------------------------------------------------------

bool XtBarModeDriver::build_bar_paths_from_symbol() {
    bar.paths.clear();
    if (!(read_cfg.range_start && read_cfg.range_end)) {
        return false;
    }
    // 日期范围按月份展开；路径约定为 root/symbol/symbol.YYMM.suffix。
    int sy = static_cast<int>(read_cfg.range_start / 10000);
    int sm = static_cast<int>((read_cfg.range_start / 100) % 100);
    int ey = static_cast<int>(read_cfg.range_end / 10000);
    int em = static_cast<int>((read_cfg.range_end / 100) % 100);
    int cur = sy * 12 + sm;
    int end = ey * 12 + em;
    for (int v = cur; v <= end; ++v) {
        int yy = v / 12;
        int mm = v % 12;
        if (mm == 0) { mm = 12; yy -= 1; }
        std::string yymm = XtTimeParse::format_yymm(yy % 100, mm);
        std::string p = bar.root;
        if (!p.empty() && p.back() != '/' && p.back() != '\\') p += "/";
        p += bar.symbol + "/" + bar.symbol + "." + yymm + bar.suffix;
        bar.paths.push_back(p);
    }
    if (bar.paths.empty()) return false;
    build_simtick_paths_from_bar_paths();
    bar.path   = bar.paths.front();
    bar.is_csv = true;
    return true;
}

void XtBarModeDriver::build_simtick_paths_from_bar_paths() {
    if (!(simtick.from_bar && simtick.format == "bin")) return;
    simtick.paths.clear();
    for (const auto& p : bar.paths) {
        std::string yymm = extract_yymm_from_bar_path(p);
        simtick.paths.push_back(yymm.size() == 4 ? build_simtick_path_for_month(yymm) : std::string());
    }
    XtLogger::info_m("MD", "INIT simtick cache enabled (per-month)");
}

std::string XtBarModeDriver::build_simtick_path() const {
    std::string base = !bar.symbol.empty() ? bar.symbol : bar.code;
    if (base.empty()) base = "simtick";
    std::string suffix = PathUtil::strip_csv_ext(bar.suffix);
    std::string ext = (simtick.format == "bin") ? ".bin" : ".csv";
    std::string outp = simtick.dir;
    if (!outp.empty() && outp.back() != '/' && outp.back() != '\\') outp += "/";
    outp += base + suffix + "_sim" + ext;
    return outp;
}

std::string XtBarModeDriver::build_simtick_path_for_month(const std::string& yymm) const {
    std::string base = !bar.symbol.empty() ? bar.symbol : bar.code;
    if (base.empty()) base = "simtick";
    std::string suffix = PathUtil::strip_csv_ext(bar.suffix);
    std::string ext = (simtick.format == "bin") ? ".bin" : ".csv";
    std::string outp = simtick.dir;
    if (!outp.empty() && outp.back() != '/' && outp.back() != '\\') outp += "/";
    outp += base + "." + yymm + suffix + "_sim" + ext;
    return outp;
}

// ---------------------------------------------------------------------------
// 重置
// ---------------------------------------------------------------------------

void XtBarModeDriver::reset() {
    bar.in.reset();
    bar.ticks.clear();
    bar.tick_idx             = 0;
    bar.path_idx             = 0;
    bar.stream_is_bin        = false;
    bar.stream_bin_fmt       = XtTickBinFormat::DualV2;
    bar.stream_bin_fmt_inited = false;
    bar.pending_line.clear();
    bar.total_volume         = 0.0;
    simtick.cur_bin_yymm.clear();
    simtick.csv_out.reset();
    simtick.bin_out.reset();
    simtick.bin_out_fmt      = XtTickBinFormat::DualV2;
    simtick.bin_out_fmt_inited = false;
    simtick.bin_out_path.clear();
    hot.month_idx            = 0;
    hot.month_filter.clear();
}

// ---------------------------------------------------------------------------
// 流式 bar 文件管理
// ---------------------------------------------------------------------------

bool XtBarModeDriver::open_next_bar_file() {
    // hot + simtick 按月份遍历；bin 缓存命中时跳过 CSV，未命中时读 hot CSV。
    if (hot.use_hot && !simtick.paths.empty()) {
        while (hot.month_idx < simtick.paths.size()) {
            const std::string& m    = hot.months[hot.month_idx];
            const std::string& binp = simtick.paths[hot.month_idx];
            bool has_bin = (hot.month_idx < hot.bin_exists.size()) ? hot.bin_exists[hot.month_idx] : false;
            hot.month_idx++;
            if (has_bin) {
                bar.in = std::make_unique<std::ifstream>(binp, std::ios::binary);
                if (bar.in->is_open()) {
                    bar.stream_is_bin        = true;
                    bar.stream_bin_fmt       = XtTickBinFormat::DualV2;
                    bar.stream_bin_fmt_inited = false;
                    hot.month_filter.clear();
                    bar.pending_line.clear();
                    XtLogger::info_m("MD", "INIT open simtick bin {}", binp);
                    return true;
                }
            }
            bar.in = std::make_unique<std::ifstream>(bar.path);
            if (bar.in->is_open()) {
                bar.stream_is_bin        = false;
                bar.stream_bin_fmt       = XtTickBinFormat::DualV2;
                bar.stream_bin_fmt_inited = false;
                hot.month_filter = m;
                XtLogger::info_m("MD", "INIT open bar file {} (month={})", bar.path, m);
                return true;
            }
        }
        return false;
    }
    // 非 hot 模式逐文件遍历 bar.paths；每个 bar 文件优先尝试对应 simtick bin 缓存。
    while (bar.path_idx < bar.paths.size()) {
        const size_t idx = bar.path_idx;
        const std::string& p = bar.paths[bar.path_idx++];
        if (p.empty()) continue;
        std::string binp;
        if (simtick.from_bar && simtick.format == "bin" && !hot.use_hot) {
            if (idx < simtick.paths.size()) binp = simtick.paths[idx];
        }
        if (!binp.empty() && PathUtil::file_exists(binp)) {
            bar.in = std::make_unique<std::ifstream>(binp, std::ios::binary);
            if (bar.in->is_open()) {
                bar.stream_is_bin        = true;
                bar.stream_bin_fmt       = XtTickBinFormat::DualV2;
                bar.stream_bin_fmt_inited = false;
                bar.ticks.clear();
                bar.tick_idx = 0;
                XtLogger::info_m("MD", "INIT open simtick bin {}", binp);
                return true;
            }
        }
        bar.in = std::make_unique<std::ifstream>(p);
        if (bar.in->is_open()) {
            bar.stream_is_bin        = false;
            bar.stream_bin_fmt       = XtTickBinFormat::DualV2;
            bar.stream_bin_fmt_inited = false;
            XtLogger::info_m("MD", "INIT open bar file {}", p);
            return true;
        }
        XtLogger::warn_m("MD", "INIT failed to open {}", p);
    }
    // paths 为空但存在单文件 bar.path 时保留兼容读取路径。
    if (bar.paths.empty() && !bar.path.empty()) {
        if (simtick.from_bar && simtick.format == "bin" && hot.use_hot && hot.bin_available) {
            bar.in = std::make_unique<std::ifstream>(hot.bin_path, std::ios::binary);
            if (bar.in->is_open()) {
                bar.stream_is_bin        = true;
                bar.stream_bin_fmt       = XtTickBinFormat::DualV2;
                bar.stream_bin_fmt_inited = false;
                XtLogger::info_m("MD", "INIT open simtick bin {}", hot.bin_path);
                return true;
            }
        }
        bar.in = std::make_unique<std::ifstream>(bar.path);
        if (bar.in->is_open()) {
            bar.stream_is_bin        = false;
            bar.stream_bin_fmt       = XtTickBinFormat::DualV2;
            bar.stream_bin_fmt_inited = false;
            XtLogger::info_m("MD", "INIT open bar file {}", bar.path);
            return true;
        }
        XtLogger::error_m("MD", "INIT failed to open {}", bar.path);
    }
    return false;
}

// ---------------------------------------------------------------------------
// 流式 tick 读取
// ---------------------------------------------------------------------------

bool XtBarModeDriver::next_tick(XtRawTick& out) {
    if (!bar.in) {
        if (!open_next_bar_file()) return false;
    }
    // bin stream 每个文件独立探测 format，避免跨文件沿用旧 header 状态。
    if (bar.stream_is_bin) {
        if (!bar.stream_bin_fmt_inited) {
            if (!xt_tick_bin::detect_stream_format(*bar.in, bar.stream_bin_fmt)) {
                bar.in.reset();
                return false;
            }
            bar.stream_bin_fmt_inited = true;
        }
        while (!xt_tick_bin::read_next_tick(*bar.in, bar.stream_bin_fmt, out, nullptr)) {
            bar.in.reset();
            if (!open_next_bar_file()) return false;
            if (!bar.stream_is_bin) break;
            if (!xt_tick_bin::detect_stream_format(*bar.in, bar.stream_bin_fmt)) {
                bar.in.reset();
                return false;
            }
            bar.stream_bin_fmt_inited = true;
        }
        if (bar.stream_is_bin) return true;
    }
    // 已生成的 simtick 缓冲未消费完时先返回缓冲内容。
    if (bar.tick_idx < bar.ticks.size()) {
        out = bar.ticks[bar.tick_idx++];
        return true;
    }

    // CSV bar 逐行读取并生成 simtick。
    std::string line;
    while (true) {
        if (!bar.pending_line.empty()) {
            line = bar.pending_line;
            bar.pending_line.clear();
        } else if (!std::getline(*bar.in, line)) {
            bar.in.reset();
            if (!open_next_bar_file()) return false;
            continue;
        }
        if (line.empty() || line[0] == '#') continue;
        if (line.rfind("datetime", 0) == 0) continue;
        if (line.rfind("<Date>", 0) == 0) continue;

        ParsedBarRow row;
        if (!parse_bar_row_line(line, row)) continue;
        if (read_cfg.range_start && row.trading_date < read_cfg.range_start) continue;
        if (read_cfg.range_end   && row.trading_date > read_cfg.range_end)   continue;
        // hot 月份过滤：越过当前月份时保留 pending_line，再切换到下个月份文件。
        if (hot.use_hot && !hot.month_filter.empty()) {
            std::string cur_m = XtTimeParse::yymm_from_date(row.trading_date);
            if (!cur_m.empty() && cur_m < hot.month_filter) continue;
            if (!cur_m.empty() && cur_m > hot.month_filter) {
                bar.pending_line = line;
                bar.in.reset();
                if (!open_next_bar_file()) return false;
                continue;
            }
        }

        generate_ticks_from_bar_row(
            row, bar.code, bar.interval_sec,
            read_cfg.spread, read_cfg.qty_base,
            bar.total_volume, bar.ticks);
        bar.tick_idx = 0;

        // simtick CSV 落盘路径使用单个追加文件。
        if (simtick.save && simtick.format == "csv") {
            if (!simtick.csv_out) {
                std::string outp = simtick.dir;
                if (!outp.empty() && outp.back() != '/' && outp.back() != '\\') outp += "/";
                outp += "simtick.csv";
                PathUtil::ensure_dir(outp);
                simtick.csv_out = std::make_unique<std::ofstream>(outp, std::ios::app);
                if (simtick.csv_out->is_open()) {
                    std::ifstream chk(outp);
                    bool empty = !chk.good() || chk.peek() == std::ifstream::traits_type::eof();
                    if (empty) (*simtick.csv_out) << "date,time,code,price,bid1,ask1,bidqty1,askqty1\n";
                }
            }
            if (simtick.csv_out && simtick.csv_out->is_open())
                write_ticks_csv(*simtick.csv_out, bar.ticks);
        }
        // simtick bin 落盘按 hot month 或 bar 文件月份拆分。
        if (simtick.save && simtick.format == "bin") {
            auto open_bin_out = [&](const std::string& outp) {
                if (outp.empty()) return;
                if (simtick.bin_out && simtick.bin_out_path == outp && simtick.bin_out->is_open()) return;
                PathUtil::ensure_dir(outp);
                simtick.bin_out.reset(new std::ofstream(outp, std::ios::binary | std::ios::app));
                simtick.bin_out_fmt_inited = false;
                simtick.bin_out_path = outp;
                if (simtick.bin_out && simtick.bin_out->is_open()) {
                    simtick.bin_out_fmt = XtTickBinFormat::DualV2;
                    if (xt_tick_bin::prepare_append_stream(outp, *simtick.bin_out, simtick.bin_out_fmt)) {
                        simtick.bin_out_fmt_inited = true;
                    } else {
                        simtick.bin_out.reset();
                        simtick.bin_out_path.clear();
                    }
                }
            };
            do {
                std::string yymm = hot.use_hot ? XtTimeParse::yymm_from_date(row.trading_date) : "";
                if (hot.use_hot) {
                    if (!hot.range_enabled) break;
                    if (!hot.months.empty() && !XtTimeParse::month_in_list(hot.months, yymm)) break;
                    if (!yymm.empty() && yymm != simtick.cur_bin_yymm) {
                        simtick.cur_bin_yymm = yymm;
                        open_bin_out(build_simtick_path_for_month(yymm));
                    }
                }
                if (!hot.use_hot && bar.path_idx > 0 && bar.path_idx <= bar.paths.size()) {
                    const std::string& bp = bar.paths[bar.path_idx - 1];
                    yymm = extract_yymm_from_bar_path(bp);
                    if (!yymm.empty() && yymm != simtick.cur_bin_yymm) {
                        simtick.cur_bin_yymm = yymm;
                        open_bin_out(build_simtick_path_for_month(yymm));
                    }
                }
                if (!simtick.bin_out) {
                    std::string outp;
                    if (!hot.use_hot && bar.path_idx > 0 && bar.path_idx <= bar.paths.size()) {
                        const std::string& bp = bar.paths[bar.path_idx - 1];
                        const std::string yymm2 = extract_yymm_from_bar_path(bp);
                        if (yymm2.size() == 4) outp = build_simtick_path_for_month(yymm2);
                    }
                    if (outp.empty()) outp = build_simtick_path();
                    open_bin_out(outp);
                }
                if (simtick.bin_out && simtick.bin_out->is_open() && simtick.bin_out_fmt_inited)
                    write_ticks_bin(*simtick.bin_out, simtick.bin_out_fmt, bar.ticks);
            } while (false);
        }

        out = bar.ticks[bar.tick_idx++];
        return true;
    }
}

// ---------------------------------------------------------------------------
// 内存加载
// ---------------------------------------------------------------------------

bool XtBarModeDriver::load_to_memory(std::vector<XtRawTick>& out) {
    // simtick.is_bin 表示 prepare 阶段确认缓存可直接读取，memory 路径跳过 bar 解析。
    if (simtick.is_bin) {
        if (!simtick.paths.empty()) {
            for (const auto& sp : simtick.paths) { if (!sp.empty()) load_simtick_bin(sp, out); }
        } else if (!simtick.path.empty()) {
            load_simtick_bin(simtick.path, out);
        }
        return !out.empty();
    }
    // 多 bar 文件逐个尝试 simtick bin 缓存；未命中时退回 bar CSV 生成。
    if (bar.is_csv && !bar.paths.empty()) {
        size_t total = 0;
        for (size_t i = 0; i < bar.paths.size(); ++i) {
            const auto& p = bar.paths[i];
            if (p.empty()) continue;
            if (simtick.from_bar && simtick.format == "bin") {
                std::string bin;
                if (hot.use_hot && hot.bin_available) {
                    bin = hot.bin_path;
                } else if (i < simtick.paths.size()) {
                    bin = simtick.paths[i];
                }
                if (!bin.empty() && PathUtil::file_exists(bin)) {
                    if (load_simtick_bin(bin, out)) { total = out.size(); continue; }
                }
            }
            if (load_bar_file(p, out)) total = out.size();
        }
        if (total == 0) { XtLogger::error_m("MD", "no bars loaded"); return false; }
        XtLogger::info_m("MD", "total simticks {}", total);
        return true;
    }
    // 单文件 bar_path 兼容路径。
    if (bar.is_csv && !bar.path.empty()) {
        return load_bar_file(bar.path, out);
    }
    return false;
}

bool XtBarModeDriver::load_simtick_bin(const std::string& path, std::vector<XtRawTick>& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) { XtLogger::error_m("MD", "failed to open {}", path); return false; }
    size_t loaded = 0;
    XtRawTick t;
    XtTickBinFormat fmt = XtTickBinFormat::DualV2;
    if (!xt_tick_bin::detect_stream_format(in, fmt)) return false;
    while (xt_tick_bin::read_next_tick(in, fmt, t, nullptr)) { out.push_back(t); ++loaded; }
    XtLogger::info_m("MD", "loaded {} simticks from {}", loaded, path);
    return loaded > 0;
}

bool XtBarModeDriver::load_bar_file(const std::string& path, std::vector<XtRawTick>& out) {
    std::ifstream in(path);
    if (!in.is_open()) { XtLogger::error_m("MD", "failed to open {}", path); return false; }

    bar.ticks.clear();
    bar.tick_idx     = 0;
    bar.total_volume = 0.0;

    std::unique_ptr<std::ofstream> csv_out;
    if (simtick.save && simtick.format == "csv") {
        std::string outp = simtick.dir;
        if (!outp.empty() && outp.back() != '/' && outp.back() != '\\') outp += "/";
        outp += "simtick.csv";
        PathUtil::ensure_dir(outp);
        csv_out = std::make_unique<std::ofstream>(outp, std::ios::app);
        if (csv_out && csv_out->is_open()) {
            std::ifstream chk(outp);
            bool empty = !chk.good() || chk.peek() == std::ifstream::traits_type::eof();
            if (empty) (*csv_out) << "date,time,code,price,bid1,ask1,bidqty1,askqty1\n";
        }
    }
    std::unique_ptr<std::ofstream> bin_out;
    XtTickBinFormat bin_fmt = XtTickBinFormat::DualV2;
    if (simtick.save && simtick.format == "bin" && !hot.use_hot) {
        std::string outp;
        std::string yymm = extract_yymm_from_bar_path(path);
        if (yymm.size() == 4) outp = build_simtick_path_for_month(yymm);
        if (outp.empty()) outp = build_simtick_path();
        if (!outp.empty()) {
            PathUtil::ensure_dir(outp);
            bin_out = std::make_unique<std::ofstream>(outp, std::ios::binary | std::ios::app);
            if (bin_out && bin_out->is_open()) {
                if (!xt_tick_bin::prepare_append_stream(outp, *bin_out, bin_fmt)) {
                    XtLogger::warn_m("MD", "failed to init tick append stream {}, fallback disabled", outp);
                    bin_out.reset();
                }
            }
        }
    }

    std::string line;
    size_t loaded = 0;
    std::vector<XtRawTick> generated_ticks;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') continue;
        if (line.rfind("datetime", 0) == 0) continue;
        if (line.rfind("<Date>", 0) == 0) continue;

        ParsedBarRow row;
        if (!parse_bar_row_line(line, row)) continue;
        if (read_cfg.range_start && row.trading_date < read_cfg.range_start) continue;
        if (read_cfg.range_end   && row.trading_date > read_cfg.range_end)   continue;

        generate_ticks_from_bar_row(
            row, bar.code, bar.interval_sec,
            read_cfg.spread, read_cfg.qty_base,
            bar.total_volume, generated_ticks);

        if (csv_out && csv_out->is_open()) write_ticks_csv(*csv_out, generated_ticks);
        if (bin_out && bin_out->is_open() && !hot.use_hot) write_ticks_bin(*bin_out, bin_fmt, generated_ticks);

        for (const auto& t : generated_ticks) {
            out.push_back(t);
            // hot 模式下按月份切换 bin 输出文件。
            if (simtick.save && simtick.format == "bin" && hot.use_hot) {
                if (!hot.range_enabled) { ++loaded; continue; }
                std::string yymm = XtTimeParse::yymm_from_date(t.action_date);
                if (hot.months.empty() || XtTimeParse::month_in_list(hot.months, yymm)) {
                    if (!yymm.empty() && yymm != simtick.cur_bin_yymm) {
                        simtick.cur_bin_yymm = yymm;
                        std::string outp = build_simtick_path_for_month(yymm);
                        if (!outp.empty()) {
                            PathUtil::ensure_dir(outp);
                            bin_out.reset(new std::ofstream(outp, std::ios::binary | std::ios::app));
                            if (bin_out && bin_out->is_open()) {
                                if (!xt_tick_bin::prepare_append_stream(outp, *bin_out, bin_fmt)) {
                                    XtLogger::warn_m("MD", "failed to init hot tick append stream {}, skip", outp);
                                    bin_out.reset();
                                }
                            }
                        }
                    }
                    if (bin_out && bin_out->is_open()) {
                        const XtTick core = xt_tick_convert::to_core(t);
                        (void)xt_tick_bin::write_tick(*bin_out, bin_fmt, t, core);
                    }
                }
            }
            ++loaded;
        }
    }
    XtLogger::info_m("MD", "generated {} simticks from {}", loaded, path);
    return loaded > 0;
}
