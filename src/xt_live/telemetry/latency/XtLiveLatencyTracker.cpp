#include "XtLiveLatencyTracker.h"

#include <array>

#include "xt_util/time/XtSteadyClock.h"
#include "XtMetricRegistry.h"
#include <prometheus/histogram.h>
#include <algorithm>

namespace {

template <typename T>
void erase_if_match(std::unordered_map<std::string, uint64_t>& index,
                    const std::string& trace_id,
                    T order_id) {
    if (trace_id.empty()) return;
    auto it = index.find(trace_id);
    if (it == index.end()) return;
    if (static_cast<T>(it->second) == order_id) {
        index.erase(it);
    }
}

template <typename RollingHistogram>
bool rolling_add_elapsed(RollingHistogram& hist,
                         uint64_t end_us,
                         uint64_t begin_us,
                         uint64_t sample_ts_us,
                         uint64_t slot_dur_us) {
    if (begin_us == 0 || end_us == 0 || end_us <= begin_us) return false;
    xt_util::rolling_add(hist, end_us - begin_us, sample_ts_us, slot_dur_us);
    return true;
}

template <typename StatsState>
void rotate_latency_windows(StatsState& stats,
                            uint64_t now_us,
                            uint64_t slot_dur_us) {
    xt_util::rolling_rotate_if_needed(stats.submit_wire, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.strategy_processing, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.ack_path, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.submit_to_ack, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.submit_to_ack_paired, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.submit_to_trade_ack, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.uds_first_savings, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.submit_to_fill, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.ack_to_fill, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.event_to_ack, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.event_to_fill, now_us, slot_dur_us);
    xt_util::rolling_rotate_if_needed(stats.cb_to_strategy, now_us, slot_dur_us);
}

}

XtLiveLatencyTracker::XtLiveLatencyTracker(const Config& cfg)
    : enabled_(cfg.enabled),
      stale_ttl_us_(cfg.stale_ttl_us > 0 ? cfg.stale_ttl_us : 60000000ULL) {

    const uint64_t win_s = cfg.rolling_window_s;
    if (win_s > 0) {
        const uint64_t win_us = win_s * 1'000'000ULL;
        slot_dur_us_ = win_us / kRollingSlots;
        if (slot_dur_us_ == 0) slot_dur_us_ = 1;
    } else {
        slot_dur_us_ = 0;
    }
}

void XtLiveLatencyTracker::set_enabled(bool enabled) {
    enabled_.store(enabled, std::memory_order_release);
}

bool XtLiveLatencyTracker::enabled() const {
    return enabled_.load(std::memory_order_acquire);
}

void XtLiveLatencyTracker::set_stale_ttl_us(uint64_t ttl_us) {

    stale_ttl_us_.store(ttl_us > 0 ? ttl_us : 60000000ULL, std::memory_order_release);
}

uint64_t XtLiveLatencyTracker::stale_ttl_us() const {
    return stale_ttl_us_.load(std::memory_order_acquire);
}

void XtLiveLatencyTracker::set_metric_registry(ct::metrics::XtMetricRegistry* reg,
                                                std::string_view venue) noexcept {
    std::lock_guard<std::mutex> guard(mu_);
    metric_registry_ = reg;
    metric_venue_.assign(venue.data(), venue.size());
    hist_ack_cache_.clear();
    hist_fill_cache_.clear();
    hist_paired_cache_.clear();
    hist_strategy_cache_.clear();
    hist_local_submit_cache_.clear();
}

void XtLiveLatencyTracker::set_meta_resolver(MetaResolver resolver) {
    std::lock_guard<std::mutex> guard(mu_);
    meta_resolver_ = std::move(resolver);
}

bool XtLiveLatencyTracker::ensure_emit_meta_unlocked(uint64_t order_id, OrderTrack& st) {
    if (!st.strategy.empty() && !st.code.empty()) return true;

    if (meta_resolver_) {
        std::string strategy, code;
        if (meta_resolver_(order_id, strategy, code)) {
            if (st.strategy.empty() && !strategy.empty()) st.strategy = std::move(strategy);
            if (st.code.empty() && !code.empty()) st.code = std::move(code);
        }
    }
    if (!st.strategy.empty() && !st.code.empty()) return true;

    if (!st.meta_miss_counted && metric_registry_ != nullptr) {
        st.meta_miss_counted = true;
        metric_registry_->inc_latency_meta_miss_total(metric_venue_);
    }
    return false;
}

uint64_t XtLiveLatencyTracker::now_us() {
    return XtSteadyClock::steady_now_us();
}

bool XtLiveLatencyTracker::is_final_order_status(XtOrderStatus status) {
    return order_status_is_final(status);
}

bool XtLiveLatencyTracker::parse_had_fill(const XtOrderExecUpdate& upd) {
    if (upd.last_fill_fp > 0) return true;
    if (upd.filled_fp > 0) return true;
    if (upd.status == XtOrderStatus::PartiallyFilled || upd.status == XtOrderStatus::Filled) return true;
    return false;
}

XtLiveLatencyTracker::StageStats XtLiveLatencyTracker::to_stage_stats(const RollingHistogram& ring) {
    const Histogram hist = xt_util::rolling_merge(ring);
    StageStats out;
    out.samples = hist.samples;
    out.avg_us = hist.samples > 0
                     ? static_cast<double>(hist.sum_us) / static_cast<double>(hist.samples)
                     : 0.0;
    const std::array<xt_util::QRatio, 5> queries = {{
        {50, 100}, {90, 100}, {95, 100}, {99, 100}, {999, 1000}
    }};
    std::array<uint64_t, 5> percs{};
    xt_util::hist_percentile_multi(hist, queries, percs);

    out.p50_us  = percs[0];
    out.p90_us  = percs[1];
    out.p95_us  = percs[2];
    out.p99_us  = percs[3];
    out.p999_us = percs[4];
    out.max_us = hist.max_us;
    return out;
}

void XtLiveLatencyTracker::cleanup_stale_unlocked(uint64_t now) {
    if (orders_.empty()) return;
    const uint64_t ttl = stale_ttl_us_.load(std::memory_order_relaxed);
    if (ttl == 0) return;
    for (auto it = orders_.begin(); it != orders_.end();) {
        const OrderTrack& st = it->second;
        const uint64_t last_touch = st.last_touch_us > 0 ? st.last_touch_us : st.submit_end_us;
        if (last_touch > 0 && now > last_touch && (now - last_touch) > ttl) {
            erase_if_match(trace_to_order_, st.trace_id, it->first);
            it = orders_.erase(it);
            ++stats_.stale_evicted;
        } else {
            ++it;
        }
    }
}

uint64_t XtLiveLatencyTracker::resolve_order_id_unlocked(uint64_t order_id, const std::string& trace_id) const {
    if (order_id > 0) return order_id;
    if (trace_id.empty()) return 0;
    auto it = trace_to_order_.find(trace_id);
    if (it == trace_to_order_.end()) return 0;
    return it->second;
}

void XtLiveLatencyTracker::maybe_finish_order_unlocked(uint64_t order_id, const OrderTrack& st) {
    if (!st.final_order_seen) return;
    if (!st.submit_recorded) return;
    if (raw_latency_correlation_pending(st)) return;
    erase_if_match(trace_to_order_, st.trace_id, order_id);
    remember_completed_order_unlocked(order_id);
    orders_.erase(order_id);
}

bool XtLiveLatencyTracker::raw_latency_correlation_pending(const OrderTrack& st) {
    if (!st.final_order_seen) return false;
    if (st.raw_uds_us == 0) return false;
    if (st.raw_trade_ack_us > 0 || st.raw_trade_ack_unavailable) return false;
    return true;
}

bool XtLiveLatencyTracker::completed_order_seen_unlocked(uint64_t order_id) const {
    return completed_orders_.find(order_id) != completed_orders_.end();
}

void XtLiveLatencyTracker::remember_completed_order_unlocked(uint64_t order_id) {
    if (completed_orders_.insert(order_id).second) {
        completed_order_fifo_.push_back(order_id);
    }
    while (completed_orders_.size() > kCompletedOrderCacheCap && !completed_order_fifo_.empty()) {
        const uint64_t old = completed_order_fifo_.front();
        completed_order_fifo_.pop_front();
        completed_orders_.erase(old);
    }
}

void XtLiveLatencyTracker::push_strategy_processing_if_ready_unlocked(OrderTrack& st,
                                                                      uint64_t sample_ts_us) {
    if (st.strategy_processing_pushed) return;
    if (st.trigger_tick_us == 0 || st.submit_begin_us == 0) return;
    if (rolling_add_elapsed(stats_.strategy_processing,
                            st.submit_begin_us,
                            st.trigger_tick_us,
                            sample_ts_us,
                            slot_dur_us_)) {
        st.strategy_processing_pushed = true;

        if (metric_registry_ != nullptr && !st.strategy.empty() && !st.code.empty()
            && st.submit_begin_us > st.trigger_tick_us) {
            LabelKey key{st.strategy, st.strategy  , metric_venue_, st.code};
            auto it = hist_strategy_cache_.find(key);
            if (it == hist_strategy_cache_.end()) {
                auto* h = metric_registry_->get_strategy_us_histogram(
                    st.strategy, st.strategy, metric_venue_, st.code);
                it = hist_strategy_cache_.emplace(std::move(key), h).first;
            }
            if (it->second) {
                it->second->Observe(
                    static_cast<double>(st.submit_begin_us - st.trigger_tick_us));
            }
        }
    }
}

void XtLiveLatencyTracker::push_ack_if_ready_unlocked(OrderTrack& st,
                                                      uint64_t sample_ts_us) {
    if (st.first_ack_us == 0 || !st.submit_recorded) return;

    const bool has_uds = st.raw_uds_us > 0;
    const bool has_ack = st.raw_trade_ack_us > 0;
    const bool ack_closed = has_ack || st.raw_trade_ack_unavailable;
    const bool raw_used = has_uds || has_ack || st.raw_trade_ack_unavailable;
    uint64_t effective_end_us = st.first_ack_us;
    if (raw_used) {
        if (has_uds && ack_closed) {

            effective_end_us = has_ack ? std::min(st.raw_uds_us, st.raw_trade_ack_us)
                                       : st.raw_uds_us;
        } else if (st.final_order_seen) {

            if (has_ack && has_uds) {
                effective_end_us = std::min(st.raw_uds_us, st.raw_trade_ack_us);
            } else if (has_ack) {
                effective_end_us = st.raw_trade_ack_us;
            } else if (has_uds) {
                effective_end_us = st.raw_uds_us;
            } else {

                return;
            }
        } else {

            return;
        }
    }
    if (!st.ack_latency_pushed) {
        if (rolling_add_elapsed(stats_.submit_to_ack,
                                effective_end_us,
                                st.submit_begin_us,
                                sample_ts_us,
                                slot_dur_us_)) {
            st.ack_latency_pushed = true;

            if (metric_registry_ && !st.strategy.empty() && !st.code.empty()
                && effective_end_us > st.submit_begin_us) {
                LabelKey key{st.strategy, st.strategy  , metric_venue_, st.code};
                auto it = hist_ack_cache_.find(key);
                if (it == hist_ack_cache_.end()) {
                    auto* h = metric_registry_->get_submit_to_ack_us_histogram(
                        st.strategy, st.strategy, metric_venue_, st.code);
                    it = hist_ack_cache_.emplace(std::move(key), h).first;
                }
                if (it->second) {
                    it->second->Observe(
                        static_cast<double>(effective_end_us - st.submit_begin_us));
                }
            }
        }
    }
    if (!st.ack_path_pushed && st.submit_end_us > 0) {
        if (rolling_add_elapsed(stats_.ack_path,
                                st.first_ack_us,
                                st.submit_end_us,
                                sample_ts_us,
                                slot_dur_us_)) {
            st.ack_path_pushed = true;
        }
    }
    if (!st.event_ack_pushed && st.trigger_tick_us > 0) {
        if (rolling_add_elapsed(stats_.event_to_ack,
                                st.first_ack_us,
                                st.trigger_tick_us,
                                sample_ts_us,
                                slot_dur_us_)) {
            st.event_ack_pushed = true;
        }
    }
}

void XtLiveLatencyTracker::push_paired_ack_and_savings_if_ready_unlocked(OrderTrack& st,
                                                                         uint64_t sample_ts_us) {
    if (!st.submit_recorded) return;
    if (st.raw_uds_us == 0 || st.raw_trade_ack_us == 0) return;
    const uint64_t effective_end_us = std::min(st.raw_uds_us, st.raw_trade_ack_us);
    if (!st.ack_paired_latency_pushed) {
        if (rolling_add_elapsed(stats_.submit_to_ack_paired,
                                effective_end_us,
                                st.submit_begin_us,
                                sample_ts_us,
                                slot_dur_us_)) {
            st.ack_paired_latency_pushed = true;

            if (metric_registry_ && !st.strategy.empty() && !st.code.empty()
                && effective_end_us > st.submit_begin_us) {
                LabelKey key{st.strategy, st.strategy  , metric_venue_, st.code};
                auto it = hist_paired_cache_.find(key);
                if (it == hist_paired_cache_.end()) {
                    auto* h = metric_registry_->get_submit_to_ack_paired_us_histogram(
                        st.strategy, st.strategy, metric_venue_, st.code);
                    it = hist_paired_cache_.emplace(std::move(key), h).first;
                }
                if (it->second) {
                    it->second->Observe(
                        static_cast<double>(effective_end_us - st.submit_begin_us));
                }
            }
        }
    }

    if (!st.uds_savings_pushed) {
        const uint64_t savings_us = st.raw_trade_ack_us > effective_end_us
                                        ? st.raw_trade_ack_us - effective_end_us
                                        : 0;
        xt_util::rolling_add(stats_.uds_first_savings,
                             savings_us,
                             sample_ts_us,
                             slot_dur_us_);
        st.uds_savings_pushed = true;
    }
}

void XtLiveLatencyTracker::push_trade_ack_if_ready_unlocked(OrderTrack& st,
                                                            uint64_t sample_ts_us) {

    if (st.raw_trade_ack_us == 0 || !st.submit_recorded) return;
    if (st.raw_trade_ack_pushed) return;
    if (rolling_add_elapsed(stats_.submit_to_trade_ack,
                            st.raw_trade_ack_us,
                            st.submit_begin_us,
                            sample_ts_us,
                            slot_dur_us_)) {
        st.raw_trade_ack_pushed = true;
    }
}

void XtLiveLatencyTracker::push_ack_to_fill_if_ready_unlocked(OrderTrack& st,
                                                             uint64_t sample_ts_us) {
    if (st.ack_to_fill_pushed) return;
    if (st.first_ack_us == 0 || st.first_fill_us == 0) return;
    if (rolling_add_elapsed(stats_.ack_to_fill,
                            st.first_fill_us,
                            st.first_ack_us,
                            sample_ts_us,
                            slot_dur_us_)) {
        st.ack_to_fill_pushed = true;
    }
}

void XtLiveLatencyTracker::push_fill_if_ready_unlocked(OrderTrack& st,
                                                       uint64_t sample_ts_us) {
    if (st.first_fill_us == 0) return;
    if (!st.fill_latency_pushed && st.submit_recorded) {
        if (rolling_add_elapsed(stats_.submit_to_fill,
                                st.first_fill_us,
                                st.submit_begin_us,
                                sample_ts_us,
                                slot_dur_us_)) {
            st.fill_latency_pushed = true;

            if (metric_registry_ && !st.strategy.empty() && !st.code.empty()
                && st.first_fill_us > st.submit_begin_us) {
                LabelKey key{st.strategy, st.strategy  , metric_venue_, st.code};
                auto it = hist_fill_cache_.find(key);
                if (it == hist_fill_cache_.end()) {
                    auto* h = metric_registry_->get_submit_to_fill_us_histogram(
                        st.strategy, st.strategy, metric_venue_, st.code);
                    it = hist_fill_cache_.emplace(std::move(key), h).first;
                }
                if (it->second) {
                    it->second->Observe(
                        static_cast<double>(st.first_fill_us - st.submit_begin_us));
                }
            }
        }
    }
    if (!st.event_fill_pushed && st.trigger_tick_us > 0) {
        if (rolling_add_elapsed(stats_.event_to_fill,
                                st.first_fill_us,
                                st.trigger_tick_us,
                                sample_ts_us,
                                slot_dur_us_)) {
            st.event_fill_pushed = true;
        }
    }
    push_ack_to_fill_if_ready_unlocked(st, sample_ts_us);
}

void XtLiveLatencyTracker::on_submit_result(uint64_t order_id,
                                          const std::string& trace_id,
                                          uint64_t submit_begin_us,
                                          uint64_t submit_end_us,
                                          uint64_t trigger_tick_us,
                                          std::string_view strategy_id,
                                          std::string_view code) {
    if (!enabled()) return;

    const uint64_t begin = submit_begin_us > 0 ? submit_begin_us : submit_end_us;
    const uint64_t end = submit_end_us > 0 ? submit_end_us : now_us();
    std::lock_guard<std::mutex> guard(mu_);
    ++stats_.submit_attempts;
    rolling_add_elapsed(stats_.submit_wire, end, begin, end, slot_dur_us_);

    if (order_id == 0) {
        ++stats_.submit_failed;
        return;
    }
    ++stats_.submit_success;
    OrderTrack& st = orders_[order_id];
    st.submit_begin_us = begin;
    st.submit_end_us = end;
    if (trigger_tick_us > 0) {
        st.trigger_tick_us = trigger_tick_us;
    }
    st.submit_recorded = true;

    if (!strategy_id.empty()) {
        st.strategy.assign(strategy_id.data(), strategy_id.size());
    }
    if (!code.empty()) {
        st.code.assign(code.data(), code.size());
    }
    st.last_touch_us = end;
    if (!trace_id.empty()) {
        st.trace_id = trace_id;
    }
    if (!trace_id.empty()) {
        trace_to_order_[trace_id] = order_id;
    }
    ensure_emit_meta_unlocked(order_id, st);

    push_strategy_processing_if_ready_unlocked(st, end);
    push_ack_if_ready_unlocked(st, end);
    push_trade_ack_if_ready_unlocked(st, end);
    push_paired_ack_and_savings_if_ready_unlocked(st, end);
    push_fill_if_ready_unlocked(st, end);

    if (metric_registry_ != nullptr && !st.strategy.empty() && !st.code.empty()
        && end > begin) {
        LabelKey key{st.strategy, st.strategy  , metric_venue_, st.code};
        auto it = hist_local_submit_cache_.find(key);
        if (it == hist_local_submit_cache_.end()) {
            auto* h = metric_registry_->get_local_submit_us_histogram(
                st.strategy, st.strategy, metric_venue_, st.code);
            it = hist_local_submit_cache_.emplace(std::move(key), h).first;
        }
        if (it->second) {
            it->second->Observe(static_cast<double>(end - begin));
        }
    }
    maybe_finish_order_unlocked(order_id, st);
}

void XtLiveLatencyTracker::record_raw_trade_ack(uint64_t order_id, uint64_t ack_us) {
    if (!enabled() || order_id == 0 || ack_us == 0) return;
    const uint64_t now = now_us();
    std::lock_guard<std::mutex> guard(mu_);
    cleanup_stale_unlocked(now);
    if (completed_order_seen_unlocked(order_id)) return;
    OrderTrack& st = orders_[order_id];
    if (st.raw_trade_ack_us == 0 || ack_us < st.raw_trade_ack_us) {
        st.raw_trade_ack_us = ack_us;
    }

    if (st.first_ack_us == 0 || ack_us < st.first_ack_us) {
        st.first_ack_us = ack_us;
    }
    st.last_touch_us = ack_us;
    ensure_emit_meta_unlocked(order_id, st);
    push_trade_ack_if_ready_unlocked(st, now);

    push_ack_if_ready_unlocked(st, now);
    push_paired_ack_and_savings_if_ready_unlocked(st, now);
    maybe_finish_order_unlocked(order_id, st);
}

void XtLiveLatencyTracker::record_raw_uds_arrival(uint64_t order_id, uint64_t uds_ts_us) {
    if (!enabled() || order_id == 0 || uds_ts_us == 0) return;
    const uint64_t now = now_us();
    std::lock_guard<std::mutex> guard(mu_);
    cleanup_stale_unlocked(now);
    if (completed_order_seen_unlocked(order_id)) return;
    OrderTrack& st = orders_[order_id];
    if (st.raw_uds_us == 0 || uds_ts_us < st.raw_uds_us) {
        st.raw_uds_us = uds_ts_us;
    }

    if (st.first_ack_us == 0 || uds_ts_us < st.first_ack_us) {
        st.first_ack_us = uds_ts_us;
    }
    st.last_touch_us = uds_ts_us;
    ensure_emit_meta_unlocked(order_id, st);
    push_ack_if_ready_unlocked(st, now);
    push_paired_ack_and_savings_if_ready_unlocked(st, now);
    maybe_finish_order_unlocked(order_id, st);
}

void XtLiveLatencyTracker::record_raw_trade_ack_unavailable(uint64_t order_id, uint64_t mark_us) {
    if (!enabled() || order_id == 0) return;
    const uint64_t now = now_us();
    std::lock_guard<std::mutex> guard(mu_);
    cleanup_stale_unlocked(now);
    if (completed_order_seen_unlocked(order_id)) return;
    OrderTrack& st = orders_[order_id];
    st.raw_trade_ack_unavailable = true;
    st.last_touch_us = mark_us > 0 ? mark_us : now;
    ensure_emit_meta_unlocked(order_id, st);
    push_ack_if_ready_unlocked(st, now);
    maybe_finish_order_unlocked(order_id, st);
}

void XtLiveLatencyTracker::on_order_update(const XtOrderExecUpdate& upd) {
    if (!enabled()) return;
    const uint64_t now = now_us();
    const uint64_t event_ts = upd.local_cb_us > 0 ? upd.local_cb_us : now;
    std::lock_guard<std::mutex> guard(mu_);
    cleanup_stale_unlocked(now);
    const uint64_t order_id = upd.id;
    if (order_id == 0) return;
    if (completed_order_seen_unlocked(order_id)) return;
    OrderTrack& st = orders_[order_id];

    if (st.submit_begin_us == 0) st.submit_begin_us = st.submit_end_us;
    if (st.submit_end_us == 0) st.submit_end_us = event_ts;
    st.last_touch_us = event_ts;
    ensure_emit_meta_unlocked(order_id, st);

    const bool is_terminal_now = is_final_order_status(upd.status);
    if (is_terminal_now) {
        st.final_order_seen = true;
    }

    if (upd.status != XtOrderStatus::Submitted &&
        (st.first_ack_us == 0 ||
         (!st.ack_latency_pushed &&
          ((st.submit_begin_us > 0 && st.first_ack_us <= st.submit_begin_us) ||
           event_ts < st.first_ack_us)))) {
        st.first_ack_us = event_ts;
        if (st.submit_recorded) {
            push_ack_if_ready_unlocked(st, now);
        }
        push_ack_to_fill_if_ready_unlocked(st, now);
    }

    if (parse_had_fill(upd) &&
        (st.first_fill_us == 0 ||
         (!st.fill_latency_pushed &&
          ((st.submit_begin_us > 0 && st.first_fill_us <= st.submit_begin_us) ||
           event_ts < st.first_fill_us)))) {
        st.first_fill_us = event_ts;
        if (st.submit_recorded) {
            push_fill_if_ready_unlocked(st, now);
        }
        push_ack_to_fill_if_ready_unlocked(st, now);
    }

    if (st.first_strategy_us == 0 && upd.local_strategy_us > 0) {
        st.first_strategy_us = upd.local_strategy_us;
        rolling_add_elapsed(stats_.cb_to_strategy,
                            st.first_strategy_us,
                            upd.local_cb_us,
                            now,
                            slot_dur_us_);
    }

    if (is_terminal_now) {

        if (st.submit_recorded) {
            push_ack_if_ready_unlocked(st, now);
        }
        maybe_finish_order_unlocked(order_id, st);
    }
}

void XtLiveLatencyTracker::on_trade(const XtTradeRecord& tr) {
    if (!enabled()) return;
    const uint64_t now = now_us();
    const uint64_t event_ts = tr.local_cb_us > 0 ? tr.local_cb_us : now;
    std::lock_guard<std::mutex> guard(mu_);
    cleanup_stale_unlocked(now);
    const uint64_t order_id = resolve_order_id_unlocked(tr.order_id, tr.trace_id);
    if (order_id == 0) return;
    if (completed_order_seen_unlocked(order_id)) return;
    OrderTrack& st = orders_[order_id];

    if (st.submit_begin_us == 0 && tr.local_submit_begin_us > 0) {
        st.submit_begin_us = tr.local_submit_begin_us;
        st.submit_recorded = true;
    }
    if (st.submit_end_us == 0 && tr.local_submit_end_us > 0) {
        st.submit_end_us = tr.local_submit_end_us;
    }
    st.last_touch_us = event_ts;
    if (!tr.trace_id.empty() && st.trace_id.empty()) {
        st.trace_id = tr.trace_id;
        trace_to_order_[tr.trace_id] = order_id;
    }

    if (st.strategy.empty() && !tr.strategy_id.empty()) st.strategy = tr.strategy_id;
    if (st.code.empty() && !tr.code.empty()) st.code = tr.code;
    ensure_emit_meta_unlocked(order_id, st);

    if (st.first_fill_us == 0 ||
        (!st.fill_latency_pushed &&
         ((st.submit_begin_us > 0 && st.first_fill_us <= st.submit_begin_us) ||
          event_ts < st.first_fill_us))) {
        st.first_fill_us = event_ts;
        if (st.submit_recorded) {
            push_fill_if_ready_unlocked(st, now);
        }
        push_ack_to_fill_if_ready_unlocked(st, now);
    }
    if (st.first_strategy_us == 0 && tr.local_strategy_us > 0) {
        st.first_strategy_us = tr.local_strategy_us;
        rolling_add_elapsed(stats_.cb_to_strategy,
                            st.first_strategy_us,
                            tr.local_cb_us,
                            now,
                            slot_dur_us_);
    }
    maybe_finish_order_unlocked(order_id, st);
}

XtLiveLatencyTracker::Snapshot XtLiveLatencyTracker::snapshot() {
    Snapshot out;
    out.enabled = enabled();
    const uint64_t now = now_us();
    std::lock_guard<std::mutex> guard(mu_);
    cleanup_stale_unlocked(now);
    rotate_latency_windows(stats_, now, slot_dur_us_);
    out.submit_attempts = stats_.submit_attempts;
    out.submit_success = stats_.submit_success;
    out.submit_failed = stats_.submit_failed;
    out.stale_evicted = stats_.stale_evicted;
    out.inflight_orders = orders_.size();
    out.submit_wire = to_stage_stats(stats_.submit_wire);
    out.strategy_processing = to_stage_stats(stats_.strategy_processing);
    out.ack_path = to_stage_stats(stats_.ack_path);
    out.submit_to_ack = to_stage_stats(stats_.submit_to_ack);
    out.submit_to_ack_paired = to_stage_stats(stats_.submit_to_ack_paired);
    out.submit_to_trade_ack = to_stage_stats(stats_.submit_to_trade_ack);
    out.uds_first_savings = to_stage_stats(stats_.uds_first_savings);
    out.submit_to_fill = to_stage_stats(stats_.submit_to_fill);
    out.ack_to_fill = to_stage_stats(stats_.ack_to_fill);
    out.event_to_ack = to_stage_stats(stats_.event_to_ack);
    out.event_to_fill = to_stage_stats(stats_.event_to_fill);
    out.cb_to_strategy = to_stage_stats(stats_.cb_to_strategy);
    return out;
}
