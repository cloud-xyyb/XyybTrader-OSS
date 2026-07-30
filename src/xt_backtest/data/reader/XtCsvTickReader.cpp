#include "xt_backtest/data/reader/XtCsvTickReader.h"

#include "XtLogger.h"
#include "xt_util/string/XtStringUtil.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <utility>

XtCsvTickReader::XtCsvTickReader(Options opts)
    : opts_(std::move(opts)) {}

bool XtCsvTickReader::init() {
    reset();
    bool has_readable = false;
    for (const auto& path : opts_.paths) {
        std::ifstream in(path);
        if (in.is_open()) {
            has_readable = true;
            break;
        }
    }
    if (!has_readable) return false;
    src_days_.assign(opts_.paths.size(), {});
    init_done_ = true;
    multi_ = opts_.paths.size() > 1;
    return true;
}

void XtCsvTickReader::reset() {
    in_.reset();
    spec_ = XtTickCsvSpec();
    readers_.clear();
    while (!heap_.empty()) heap_.pop();
    seq_ = 0;
    last_single_key_ = 0;
    out_of_order_count_ = 0;
    read_stats_ = ReadStats{};
    src_days_.clear();
    init_done_ = false;
    io_error_ = false;
}

void XtCsvTickReader::note_source_day_(size_t path_idx, uint32_t trading_date) {
    if (path_idx >= src_days_.size()) return;
    auto& days = src_days_[path_idx];
    if (!days.empty() && days.back() == trading_date) return;
    const auto it = std::lower_bound(days.begin(), days.end(), trading_date);
    if (it != days.end() && *it == trading_date) return;
    days.insert(it, trading_date);
}

void XtCsvTickReader::note_key_regression_(const char* path_hint,
                                           uint64_t prev_key, uint64_t key) {
    ++out_of_order_count_;
    if (out_of_order_count_ == 1 || out_of_order_count_ % 10000 == 0) {
        XtLogger::warn_m("MD",
            "csv tick out-of-order: key {} < prev {} ({}, cum={})",
            key, prev_key, path_hint ? path_hint : "", out_of_order_count_);
    }
}

bool XtCsvTickReader::next_tick(XtRawTick& out) {

    if (!init_done_) {
        if (!init()) return false;
    }
    if (multi_) return next_multi(out);
    return next_single(out);
}

bool XtCsvTickReader::next_single(XtRawTick& out) {
    if (opts_.paths.empty()) return false;
    if (!in_) {
        in_ = std::make_unique<std::ifstream>(opts_.paths.front());
        if (!in_->is_open()) {
            XtLogger::error_m("MD", "next_single: failed to open {} (errno={})", opts_.paths.front(), strerror(errno));
            io_error_ = true;
            return false;
        }
    }
    uint64_t key = 0;
    if (!read_next_parsed_line(*in_, spec_, out, key)) return false;
    if (key < last_single_key_)
        note_key_regression_(opts_.paths.front().c_str(), last_single_key_, key);
    last_single_key_ = key;
    note_source_day_(0, out.trading_date);
    return true;
}

bool XtCsvTickReader::next_multi(XtRawTick& out) {
    if (!init_readers()) return false;
    if (heap_.empty()) return false;
    Node node = heap_.top();
    heap_.pop();
    if (node.reader_idx >= readers_.size()) return false;
    Reader& r = readers_[node.reader_idx];
    out = r.tick;

    note_source_day_(r.path_idx, out.trading_date);
    if (read_next_tick(r)) {
        Node next;
        next.reader_idx = node.reader_idx;
        next.key = r.key;
        next.seq = r.seq;
        heap_.push(next);
    }
    return true;
}

bool XtCsvTickReader::init_readers() {
    if (!readers_.empty() || opts_.paths.empty()) return !readers_.empty();

    for (size_t i = 0; i < opts_.paths.size(); ++i) {
        const std::string& path = opts_.paths[i];
        Reader r;
        r.path_idx = i;
        r.in = std::make_unique<std::ifstream>(path);
        if (!r.in->is_open()) {
            XtLogger::warn_m("MD", "init_readers: failed to open {} (errno={})", path, strerror(errno));
            io_error_ = true;
            continue;
        }
        if (read_next_tick(r)) {
            readers_.push_back(std::move(r));
            Node node;
            node.reader_idx = readers_.size() - 1;
            node.key = readers_.back().key;
            node.seq = readers_.back().seq;
            heap_.push(node);
        }
    }
    return !readers_.empty();
}

bool XtCsvTickReader::read_next_tick(Reader& reader) {
    if (!reader.in || !reader.in->is_open()) return false;
    uint64_t key = 0;
    if (read_next_parsed_line(*reader.in, reader.spec, reader.tick, key)) {

        if (reader.key != 0 && key < reader.key)
            note_key_regression_("multi", reader.key, key);
        reader.key = key;
        reader.seq = seq_++;
        return true;
    }
    return false;
}

bool XtCsvTickReader::read_next_parsed_line(std::ifstream& in,
                                          XtTickCsvSpec& spec,
                                          XtRawTick& out,
                                          uint64_t& key) {
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        if (line[0] == '#') continue;

        if (!spec.inited) {
            XtTickCsvSpec hdr;
            if (build_tick_spec_from_header(line, hdr)) {
                spec = hdr;
                spec.inited = true;
                continue;
            }

            {
                size_t ncols = 1;
                for (char c : line) { if (c == ',') ++ncols; }
                if (ncols >= 30) {
                    spec.idx_instrument     = 0;
                    spec.idx_trading_day    = 1;
                    spec.idx_update_time_ms = 2;

                    spec.idx_update_time    = 2;
                    spec.idx_turnover       = 3;
                    spec.idx_volume         = 4;
                    spec.idx_last_price     = 5;
                    spec.idx_open_interest  = 6;
                    spec.idx_upper_limit    = 7;
                    spec.idx_lower_limit    = 8;
                    spec.idx_epoch_us       = 9;
                    for (int i = 0; i < 5; ++i) {
                        spec.idx_bid_price_n[i] = 10 + i;
                        spec.idx_ask_price_n[i] = 15 + i;
                        spec.idx_bid_qty_n[i]   = 20 + i;
                        spec.idx_ask_qty_n[i]   = 25 + i;
                    }
                    spec.idx_bid_price1 = spec.idx_bid_price_n[0];
                    spec.idx_ask_price1 = spec.idx_ask_price_n[0];
                    spec.idx_bid_qty1   = spec.idx_bid_qty_n[0];
                    spec.idx_ask_qty1   = spec.idx_ask_qty_n[0];
                    spec.detected_depth = 5;
                    spec.simple = false;
                } else {
                    spec.simple = true;
                }
            }
            spec.inited = true;
        }

        ++read_stats_.lines_read;
        TickParseDrop reason = TickParseDrop::None;
        if (parse_tick_csv_line(line, spec, out, key, opts_.symbol_filter,
                                opts_.qty_base, opts_.volume_mode, &reason)) {
            return true;
        }
        if (reason == TickParseDrop::SymbolMiss) ++read_stats_.symbol;
        else                                     ++read_stats_.bad_row;
    }
    return false;
}
