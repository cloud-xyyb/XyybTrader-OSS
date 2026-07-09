#include "xt_backtest/data/reader/XtEventParquetReader.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <optional>
#include <tuple>
#include <utility>

#include <arrow/api.h>

#include "XtArrowReadUtil.h"

using namespace xt_arrow;

namespace {

std::string normalize_exchange(const std::string& exchange) {
    if (exchange == "binance_futures" || exchange == "binance-um") return "BINANCE";
    return exchange;
}

uint32_t yyyymmdd_from_ms(int64_t ts_ms) {
    const std::time_t sec = static_cast<std::time_t>(ts_ms / 1000);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &sec);
#else
    gmtime_r(&sec, &tm);
#endif
    return static_cast<uint32_t>((tm.tm_year + 1900) * 10000 + (tm.tm_mon + 1) * 100 + tm.tm_mday);
}

uint32_t hms_ms_from_ms(int64_t ts_ms) {
    int64_t ms_in_day = ts_ms % 86'400'000;
    if (ms_in_day < 0) ms_in_day += 86'400'000;
    const int64_t hour = ms_in_day / 3'600'000;
    ms_in_day -= hour * 3'600'000;
    const int64_t minute = ms_in_day / 60'000;
    ms_in_day -= minute * 60'000;
    const int64_t second = ms_in_day / 1000;
    const int64_t ms = ms_in_day - second * 1000;
    return static_cast<uint32_t>(((hour * 100 + minute) * 100 + second) * 1000 + ms);
}

bool is_nan(double v) {
    return v != v;
}

double zero_if_nan(double v) {
    return is_nan(v) ? 0.0 : v;
}

std::optional<int64_t> id_or_null(int64_t v) {
    return v < 0 ? std::optional<int64_t>{} : std::optional<int64_t>{v};
}

bool symbol_allowed(const std::unordered_set<std::string>& symbols, const std::string& symbol) {
    return symbols.empty() || symbols.count(symbol) > 0;
}

std::vector<std::string> candidate_names(const std::string& kind) {
    if (kind == "trades") return {"trades.parquet", "trades_events.parquet"};
    return {"book.parquet", "book_events.parquet"};
}

std::string date_dash(uint32_t yyyymmdd) {
    const int y = static_cast<int>(yyyymmdd / 10000);
    const int m = static_cast<int>((yyyymmdd / 100) % 100);
    const int d = static_cast<int>(yyyymmdd % 100);
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
    return buf;
}

uint32_t next_day(uint32_t yyyymmdd) {
    std::tm tm{};
    tm.tm_year = static_cast<int>(yyyymmdd / 10000) - 1900;
    tm.tm_mon = static_cast<int>((yyyymmdd / 100) % 100) - 1;
    tm.tm_mday = static_cast<int>(yyyymmdd % 100) + 1;
    tm.tm_isdst = -1;
    std::mktime(&tm);
    return static_cast<uint32_t>((tm.tm_year + 1900) * 10000 + (tm.tm_mon + 1) * 100 + tm.tm_mday);
}

void add_existing_pair(std::vector<std::tuple<std::string, std::string, std::string, std::string>>& out,
                       const std::filesystem::path& dir,
                       const std::string& exchange,
                       const std::string& symbol) {
    std::string trades;
    std::string book;
    for (const auto& name : candidate_names("trades")) {
        const auto p = dir / name;
        if (std::filesystem::exists(p)) {
            trades = p.string();
            break;
        }
    }
    for (const auto& name : candidate_names("book")) {
        const auto p = dir / name;
        if (std::filesystem::exists(p)) {
            book = p.string();
            break;
        }
    }
    if (!trades.empty() && !book.empty()) {
        out.emplace_back(exchange, symbol, trades, book);
    }
}

}

XtEventParquetReader::XtEventParquetReader(Options opts) : opts_(std::move(opts)) {}

bool XtEventParquetReader::init() {
    if (opts_.events_root.empty() || opts_.start_trading_day == 0 || opts_.end_trading_day == 0) {
        io_error_ = true;
        return false;
    }
    if (!std::filesystem::exists(opts_.events_root)) {
        io_error_ = true;
        return false;
    }
    symbol_filter_ = std::unordered_set<std::string>(opts_.symbols.begin(), opts_.symbols.end());
    initialized_ = load_streams();
    io_error_ = !initialized_;
    return initialized_;
}

void XtEventParquetReader::reset() {
    heap_ = {};
    seq_ = 0;
    for (auto& s : streams_) {
        s.trade_pos = 0;
        s.book_pos = 0;
        s.cum_volume = 0.0;
        s.replayer.reset();
    }
    for (std::size_t i = 0; i < streams_.size(); ++i) {
        push_stream_head(i);
    }
    io_error_ = false;
}

bool XtEventParquetReader::next_tick(XtRawTick& out) {
    if (!initialized_ || heap_.empty()) return false;

    const HeapNode node = heap_.top();
    heap_.pop();
    Stream& stream = streams_[node.stream_idx];
    if (stream.trade_pos >= stream.trades.size()) return false;

    const TradeRow& trade = stream.trades[stream.trade_pos++];
    try {

        while (stream.book_pos < stream.book_rows &&
               int64_at(stream.book_ts, static_cast<int64_t>(stream.book_pos), 0) <= trade.exch_ts_ms) {
            const int64_t pos = static_cast<int64_t>(stream.book_pos++);
            const std::string row_symbol = string_at(stream.book_symbol, pos);
            if (!stream.symbol.empty() && !row_symbol.empty() && row_symbol != stream.symbol) continue;
            if (!symbol_allowed(symbol_filter_, row_symbol.empty() ? stream.symbol : row_symbol)) continue;
            stream.replayer.process_event(
                string_at(stream.book_side, pos),
                double_at(stream.book_price, pos),
                double_at(stream.book_qty, pos),
                string_at(stream.book_event_type, pos),
                id_or_null(int64_at(stream.book_first, pos)),
                id_or_null(int64_at(stream.book_final, pos)),
                id_or_null(int64_at(stream.book_prev, pos)));
        }
    } catch (const std::exception&) {
        io_error_ = true;
        return false;
    }

    out = XtRawTick{};
    const std::string exchange_id = normalize_exchange(trade.exchange.empty() ? stream.exchange : trade.exchange);
    const std::string symbol = trade.symbol.empty() ? stream.symbol : trade.symbol;
    std::strncpy(out.exchg, exchange_id.c_str(), sizeof(out.exchg) - 1);
    std::strncpy(out.code, symbol.c_str(), sizeof(out.code) - 1);

    out.trading_date = stream.trading_day;
    out.action_date = stream.trading_day != 0 ? stream.trading_day : yyyymmdd_from_ms(trade.exch_ts_ms);
    out.action_time = hms_ms_from_ms(trade.exch_ts_ms);
    out.price = trade.price;

    if (opts_.volume_mode == XtTickVolumeMode::Incremental) {
        out.volume = trade.quantity;
    } else {

        const bool has_prev_volume = stream.cum_volume > 0.0;
        stream.cum_volume += trade.quantity;
        out.total_volume = stream.cum_volume;
        out.volume = has_prev_volume ? trade.quantity : 0.0;
    }

    const auto l5 = stream.replayer.l5_snapshot();
    for (int i = 0; i < 5; ++i) {

        out.bid_prices[i] = zero_if_nan(l5.bid_prices[i]);
        out.bid_qty[i] = zero_if_nan(l5.bid_qtys[i]);
        out.ask_prices[i] = zero_if_nan(l5.ask_prices[i]);
        out.ask_qty[i] = zero_if_nan(l5.ask_qtys[i]);
    }

    push_stream_head(node.stream_idx);
    return true;
}

bool XtEventParquetReader::load_streams() {
    streams_.clear();
    heap_ = {};
    const auto root = std::filesystem::path(opts_.events_root);

    for (uint32_t day = opts_.start_trading_day; day <= opts_.end_trading_day; day = next_day(day)) {
        std::vector<std::tuple<std::string, std::string, std::string, std::string>> pairs;
        const std::string day_compact = std::to_string(day);
        const auto root_trade_day = root / ("trades_events_" + day_compact + ".parquet");
        const auto root_book_day = root / ("book_events_" + day_compact + ".parquet");
        if (std::filesystem::exists(root_trade_day) && std::filesystem::exists(root_book_day)) {
            pairs.emplace_back("", opts_.symbols.size() == 1 ? opts_.symbols.front() : "",
                               root_trade_day.string(), root_book_day.string());
        }
        const std::vector<std::filesystem::path> day_dirs = {
            root / day_compact,
            root / date_dash(day),
        };

        for (const auto& day_dir : day_dirs) {
            if (!std::filesystem::exists(day_dir)) continue;
            add_existing_pair(pairs, day_dir, "", opts_.symbols.size() == 1 ? opts_.symbols.front() : "");

            for (const auto& exch_entry : std::filesystem::directory_iterator(day_dir)) {
                if (!exch_entry.is_directory()) continue;
                const std::string exchange = exch_entry.path().filename().string();
                for (const auto& sym_entry : std::filesystem::directory_iterator(exch_entry.path())) {
                    if (!sym_entry.is_directory()) continue;
                    const std::string symbol = sym_entry.path().filename().string();
                    if (!symbol_allowed(symbol_filter_, symbol)) continue;
                    add_existing_pair(pairs, sym_entry.path(), exchange, symbol);
                }
            }
        }

        for (const auto& item : pairs) {
            if (!load_one_stream(std::get<0>(item), std::get<1>(item), day,
                                 std::get<2>(item), std::get<3>(item))) {
                return false;
            }
        }
    }

    for (std::size_t i = 0; i < streams_.size(); ++i) {
        push_stream_head(i);
    }
    return !streams_.empty();
}

bool XtEventParquetReader::load_one_stream(const std::string& exchange,
                                           const std::string& symbol,
                                           uint32_t trading_day,
                                           const std::string& trades_path,
                                           const std::string& book_path) {
    Stream stream;
    stream.exchange = exchange;
    stream.symbol = symbol;
    stream.trading_day = trading_day;
    if (!read_parquet_table(trades_path, stream.trades_table) ||
        !read_parquet_table(book_path, stream.book_table)) {
        return false;
    }

    const auto trade_exchange = col_array(stream.trades_table, "exchange");
    const auto trade_symbol = col_array(stream.trades_table, "symbol");
    const auto trade_ts = col_array(stream.trades_table, "exch_ts_ms");
    const auto trade_price = col_array(stream.trades_table, "price");
    const auto trade_qty = col_array(stream.trades_table, "quantity");

    for (int64_t i = 0; i < stream.trades_table->num_rows(); ++i) {
        TradeRow row;
        row.exchange = string_at(trade_exchange, i);
        row.symbol = string_at(trade_symbol, i);
        if (!symbol.empty() && !row.symbol.empty() && row.symbol != symbol) continue;
        if (!symbol_allowed(symbol_filter_, row.symbol.empty() ? symbol : row.symbol)) continue;
        row.exch_ts_ms = int64_at(trade_ts, i, 0);
        row.price = double_at(trade_price, i);
        row.quantity = double_at(trade_qty, i);
        stream.trades.push_back(std::move(row));
    }

    stream.book_ts = col_array(stream.book_table, "exch_ts_ms");
    stream.book_event_type = col_array(stream.book_table, "event_type");
    stream.book_first = col_array(stream.book_table, "first_update_id");
    stream.book_final = col_array(stream.book_table, "final_update_id");
    stream.book_prev = col_array(stream.book_table, "prev_final_update_id");
    stream.book_side = col_array(stream.book_table, "side");
    stream.book_price = col_array(stream.book_table, "price");
    stream.book_qty = col_array(stream.book_table, "quantity");
    stream.book_symbol = col_array(stream.book_table, "symbol");
    stream.book_rows = static_cast<std::size_t>(std::max<int64_t>(0, stream.book_table->num_rows()));

    std::stable_sort(stream.trades.begin(), stream.trades.end(), [](const TradeRow& a, const TradeRow& b) {
        return a.exch_ts_ms < b.exch_ts_ms;
    });

    if (stream.exchange.empty()) {
        for (const auto& t : stream.trades) {
            if (!t.exchange.empty()) {
                stream.exchange = t.exchange;
                break;
            }
        }
    }
    if (stream.symbol.empty()) {
        for (const auto& t : stream.trades) {
            if (!t.symbol.empty()) {
                stream.symbol = t.symbol;
                break;
            }
        }
    }

    if (stream.trades.empty()) return true;
    streams_.push_back(std::move(stream));
    return true;
}

bool XtEventParquetReader::push_stream_head(std::size_t stream_idx) {
    if (stream_idx >= streams_.size()) return false;
    const Stream& stream = streams_[stream_idx];
    if (stream.trade_pos >= stream.trades.size()) return false;
    heap_.push({stream.trades[stream.trade_pos].exch_ts_ms, stream_idx, seq_++});
    return true;
}
