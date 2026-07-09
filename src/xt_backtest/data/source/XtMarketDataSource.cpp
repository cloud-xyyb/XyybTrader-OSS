#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "XtLogger.h"
#include "TickCsvUtil.h"
#include "xt_backtest/data/source/detail/XtTickAdmission.h"
#include "xt_core/market/XtTick.h"

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
    reset_runtime_state();

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

        if (tick_.reader_is_bar_simtick) return tick_.reader->next_tick(out);

        if (skip_last_tick_per_day_) return next_tick_day_buffered_(out);

        while (tick_.reader->next_tick(out)) {
            if (admit_stream_tick_(out)) return true;
        }
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
                day_buf_.exhausted = true;
                break;
            }
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
            xt_bt_admission::drop_last_tick_per_symbol_day(day_buf_.ready);
        } else if (day_buf_.ready.size() == 1 && dataset_bigger_than_one) {
            day_buf_.ready.clear();
        }
        if (chunk_had_ticks) day_buf_.seen_any = true;
    }
}

bool XtMarketDataSource::admit_stream_tick_(XtRawTick& t) const {
    using xt_bt_admission::TickAdmit;
    if (!xt_bt_admission::in_day_range(t, range_.start, range_.end)) return false;
    switch (xt_bt_admission::classify_session(t, session_)) {
        case TickAdmit::Drop:      return false;
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
        return true;
    }
    return read_next_event_uncached_(out);
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

void XtMarketDataSource::reset_runtime_state() {

    mem_.cursor   = 0;
    tick_.reader.reset();
    tick_.reader_is_bar_simtick = false;
    day_buf_ = {};
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
