#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "XtLogger.h"
#include "TickCsvUtil.h"
#include "xt_backtest/data/reader/XtCsvTickReader.h"
#include "xt_backtest/data/source/detail/XtTickAdmission.h"
#include "xt_core/market/XtTick.h"

#include <exception>

XtMarketDataSource::~XtMarketDataSource() = default;

bool XtMarketDataSource::load(const XtMarketDataSourceConfig& cfg) {
    apply_common_config(cfg);
    if (mode_ == XtReplayMode::EventStream && cfg.mode != "tick") {
        XtLogger::error_m("MD", "event_stream requires datasource.mode=tick");
        return false;
    }
    const bool prepared = (mode_ == XtReplayMode::Tick || mode_ == XtReplayMode::EventStream)
        ? prepare_tick_mode(cfg)
        : bar_driver_.prepare(cfg);
    if (!prepared) return false;

    reset();
    if (mode_ == XtReplayMode::EventStream && events_.empty()) return false;
    return true;
}

void XtMarketDataSource::reset() {

    if (at_start_) return;
    reset_runtime_state();

    // 置位必须等物化真的完成：四条物化分支任一中途抛出（GB 级 bin 的 bad_alloc、
    // 解析异常）时闸门保持清位，下一次 reset() 仍会完整重建，不把半装载态锁成「已就绪」。
    struct ArmOnNormalExit {
        bool& flag;
        int entry_uncaught = std::uncaught_exceptions();
        ~ArmOnNormalExit() { if (std::uncaught_exceptions() == entry_uncaught) flag = true; }
    } arm{at_start_};

    if (mode_ == XtReplayMode::EventStream) {

        const bool saved_stream = use_stream_;
        if (tick_.source == TickSource::BinStream) use_stream_ = false;
        load_memory_source();
        use_stream_ = saved_stream;
        if (!build_event_stream_cache())
            XtLogger::error_m("MD", "event_stream cache is empty, load will fail");
        return;
    }

    if (tick_.source == TickSource::BinStream && !use_stream_) {
        if (load_tick_bin_into_memory()) return;

        XtLogger::error_m("MD", "failed to load tick bin {}",
                          tick_.bin_paths.empty() ? std::string{} : tick_.bin_paths.front());
        tick_.source = TickSource::None;
    }
    if (tick_.source == TickSource::ShardStream && !use_stream_) {
        if (load_tick_shards_into_memory()) return;

        XtLogger::error_m("MD", "failed to load tick shards ({} selected)",
                          tick_.shard_paths.size());
        tick_.source = TickSource::None;
    }
    if (use_stream_) { init_stream_reader(); return; }
    load_memory_source();
}

bool XtMarketDataSource::next_tick(XtRawTick& out) {
    if (cached_next_event_) {

        if (cached_next_event_->type != XtReplayEventType::Tick) return false;
        out = cached_next_event_->tick;
        cached_next_event_.reset();
        return true;
    }

    if (!use_stream_ || mode_ == XtReplayMode::EventStream) {
        if (mem_.cursor >= mem_.ticks.size()) return false;
        out = mem_.ticks[mem_.cursor++];
        return true;
    }
    if (tick_.reader) {

        if (tick_.reader_is_bar_simtick) {
            if (tick_.reader->next_tick(out)) return true;
            latch_reader_io_error_();
            return false;
        }

        if (skip_last_tick_per_day_) return next_tick_day_buffered_(out);

        while (tick_.reader->next_tick(out)) {
            ++stream_records_read_;
            if (admit_stream_tick_(out)) return true;
        }
        latch_reader_io_error_();
        finalize_stream_load_stats_();
        return false;
    }
    if (bar_driver_.stream_active())
        return bar_driver_.next_tick(out);
    return false;
}

bool XtMarketDataSource::next_tick_day_buffered_(XtRawTick& out) {

    while (true) {
        if (day_buf_.cursor < day_buf_.ready.size()) {
            out = day_buf_.ready[day_buf_.cursor++];
            return true;
        }
        if (day_buf_.exhausted && !day_buf_.has_pending) return false;

        day_buf_.ready.clear();
        day_buf_.cursor = 0;
        uint32_t day = 0;
        if (day_buf_.has_pending) {
            day = xt_bt_admission::grouping_day(day_buf_.pending);
            day_buf_.ready.push_back(day_buf_.pending);
            day_buf_.has_pending = false;
        }
        XtRawTick t;
        while (true) {
            if (!tick_.reader || !tick_.reader->next_tick(t)) {
                latch_reader_io_error_();
                finalize_stream_load_stats_();
                day_buf_.exhausted = true;
                break;
            }
            ++stream_records_read_;
            if (!admit_stream_tick_(t)) continue;
            const uint32_t d = xt_bt_admission::grouping_day(t);
            if (day == 0) day = d;
            if (d != day) {
                day_buf_.pending = t;
                day_buf_.has_pending = true;
                break;
            }
            day_buf_.ready.push_back(t);
        }

        const bool chunk_had_ticks = !day_buf_.ready.empty();
        const bool dataset_bigger_than_one =
            day_buf_.seen_any || day_buf_.has_pending || day_buf_.ready.size() > 1;
        if (day_buf_.ready.size() > 1) {
            const size_t before = day_buf_.ready.size();
            xt_bt_admission::drop_last_tick_per_symbol_day(day_buf_.ready);
            drop_stats_.skip_last += static_cast<int64_t>(before - day_buf_.ready.size());
        } else if (day_buf_.ready.size() == 1 && dataset_bigger_than_one) {
            day_buf_.ready.clear();
            ++drop_stats_.skip_last;
        }
        if (chunk_had_ticks) day_buf_.seen_any = true;
    }
}

void XtMarketDataSource::latch_reader_io_error_() {
    if (tick_.reader && tick_.reader->has_io_error()) stream_io_error_ = true;
}

void XtMarketDataSource::finalize_stream_load_stats_() {
    if (stream_stats_finalized_) return;
    stream_stats_finalized_ = true;
    switch (tick_.source) {
        case TickSource::Csv: {

            if (auto* csv = dynamic_cast<XtCsvTickReader*>(tick_.reader.get())) {
                const auto& rs = csv->read_stats();
                drop_stats_.rows_loaded = rs.lines_read;
                drop_stats_.symbol  += rs.symbol;
                drop_stats_.bad_row += rs.bad_row;
                drop_stats_.loaded_trackable = true;
            } else {
                drop_stats_.rows_loaded = stream_records_read_;
                drop_stats_.loaded_trackable = false;
            }
            break;
        }
        case TickSource::BinStream:
        case TickSource::ShardStream:

            drop_stats_.rows_loaded = stream_records_read_;
            drop_stats_.loaded_trackable = true;
            break;
        default:

            drop_stats_.rows_loaded = -1;
            drop_stats_.loaded_trackable = false;
            break;
    }
}

bool XtMarketDataSource::admit_stream_tick_(XtRawTick& t) {
    using xt_bt_admission::TickAdmit;

    if (!xt_bt_admission::in_day_range(t, range_.start, range_.end)) {
        ++drop_stats_.window;
        return false;
    }
    switch (xt_bt_admission::classify_session(t, session_)) {
        case TickAdmit::Drop:      ++drop_stats_.session; return false;
        case TickAdmit::Preopen:   t.kind = static_cast<uint32_t>(TickKind::Preopen); return true;
        case TickAdmit::Normal:    t.kind = static_cast<uint32_t>(TickKind::Normal);  return true;
        case TickAdmit::NoSession: return true;
    }
    return true;
}

bool XtMarketDataSource::next_event(XtReplayEvent& out) {

    if (cached_next_event_) {

        out = *cached_next_event_;
        cached_next_event_.reset();
        at_start_ = false;
        return true;
    }
    if (!read_next_event_uncached_(out)) return false;
    at_start_ = false;
    return true;
}

bool XtMarketDataSource::peek_next_key(uint64_t& out_key) {
    if (!cached_next_event_) {

        XtReplayEvent evt;
        if (!read_next_event_uncached_(evt)) return false;
        cached_next_event_ = evt;
    }
    out_key = cached_next_event_->key;
    return true;
}

bool XtMarketDataSource::peek_next_event_header(XtReplayEventHeader& out) {
    uint64_t key = 0;
    if (!peek_next_key(key)) return false;
    const XtReplayEvent& ev = *cached_next_event_;
    out.key = key;
    switch (ev.type) {
        case XtReplayEventType::Tick:
            out.action_date  = ev.tick.action_date;
            out.action_time  = ev.tick.action_time;
            out.trading_date = ev.tick.trading_date;
            break;
        case XtReplayEventType::OrderQueue:
            out.action_date  = ev.order_queue.action_date;
            out.action_time  = ev.order_queue.action_time;
            out.trading_date = ev.order_queue.trading_date;
            break;
        case XtReplayEventType::OrderDetail:
            out.action_date  = ev.order_detail.action_date;
            out.action_time  = ev.order_detail.action_time;
            out.trading_date = ev.order_detail.trading_date;
            break;
        case XtReplayEventType::Transaction:
            out.action_date  = ev.transaction.action_date;
            out.action_time  = ev.transaction.action_time;
            out.trading_date = ev.transaction.trading_date;
            break;
    }
    return true;
}

void XtMarketDataSource::reset_runtime_state() {

    mem_.cursor   = 0;
    tick_.reader.reset();
    tick_.reader_is_bar_simtick = false;
    day_buf_ = {};
    stream_io_error_ = false;
    drop_stats_ = DropStats{};
    stream_records_read_ = 0;
    stream_stats_finalized_ = false;
    events_.clear();
    event_cursor_ = 0;
    cached_next_event_.reset();
    bar_driver_.reset();
}

bool XtMarketDataSource::read_next_event_uncached_(XtReplayEvent& out) {

    if (mode_ != XtReplayMode::EventStream) {

        out.seq  = 0;
        out.type = XtReplayEventType::Tick;
        if (!next_tick(out.tick)) return false;

        const uint32_t td = out.tick.trading_date != 0 ? out.tick.trading_date
                                                       : out.tick.action_date;
        out.key = make_tick_sort_key(td, out.tick.action_time);
        return true;
    }

    if (event_cursor_ >= events_.size()) return false;
    out = events_[event_cursor_++];
    return true;
}
