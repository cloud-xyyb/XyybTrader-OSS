#include "XtOrderManager.h"
#include <algorithm>
#include <climits>
#include <yaml-cpp/yaml.h>
#include "xt_util/string/XtCStringUtil.h"
#include "xt_util/time/XtTime.h"
#include "xt_util/time/XtTimeParse.h"

namespace {

bool is_aggressive(bool is_buy,
                   int64_t price_fp,
                   int64_t best_bid_fp,
                   int64_t best_ask_fp) {
    if (is_buy)  return best_ask_fp > 0 && price_fp >= best_ask_fp;
    return              best_bid_fp > 0 && price_fp <= best_bid_fp;
}

double level_qty_at_price(int64_t price_fp,
                          const int64_t* px_arr_fp,
                          const double* qty_arr,
                          int depth) {
    for (int i = 0; i < depth; ++i) {
        if (px_arr_fp[i] == price_fp) {
            return qty_arr[i];
        }
    }
    return 0.0;
}

double stable_level_drain(double prev_qty,
                          double last_qty,
                          int64_t prev_px_fp,
                          int64_t last_px_fp) noexcept {
    if (prev_qty <= 0.0 || last_px_fp <= 0 || prev_px_fp != last_px_fp) {
        return 0.0;
    }
    return std::max(0.0, prev_qty - last_qty);
}

void remove_from_pending(std::vector<uint64_t>& vec, uint64_t id) {
    auto it = std::find(vec.begin(), vec.end(), id);
    if (it != vec.end()) vec.erase(it);
}

}

void XtOrderManager::configure_runtime(const RuntimeConfig& cfg) {
    queue_cancel_ratio_          = cfg.queue_cancel_ratio;
    ack_delay_ticks_             = cfg.ack_delay_ticks;
    ttl_ticks_                   = cfg.ttl_ticks;
    ack_delay_us_                = cfg.ack_delay_us;
    ttl_us_                      = cfg.ttl_us;
    ofi_queue_scale_             = cfg.ofi_queue_scale;
    market_impact_enabled_       = cfg.market_impact_enabled;
    market_impact_decay_         = cfg.market_impact_decay;

    orders_.reserve(kMaxConcurrentOrders);
    clock_scratch_.reserve(kMaxConcurrentOrders);
    id_to_slot_.reserve(kMaxConcurrentOrders);
}

void XtOrderManager::advance_clock(uint64_t tick, uint32_t date, uint32_t time_ms) {
    cur_tick_ = tick;
    cur_date_ = date;
    cur_time_ = XtTimeParse::action_time_to_ms(time_ms);

    cur_clock_ns_ = xt_time::to_ns(date, time_ms);

    if (!pending_acks_.empty()) {
        clock_scratch_.clear();
        for (uint64_t pid : pending_acks_) {
            XtSimOrder* ord = find_live(pid);
            if (!ord || ord->status != XtOrderStatus::Submitted) continue;

            const bool due = ord->ack_due_time_ms != 0
                             ? cur_time_ >= ord->ack_due_time_ms
                             : cur_tick_ >= ord->ack_due_tick;
            if (!due) {
                clock_scratch_.push_back(pid);
                continue;
            }

            ord->status = XtOrderStatus::Accepted;
            push_update(*ord, 0);
            if (ord->fill_eligible_time_ms != 0 && cur_time_ < ord->fill_eligible_time_ms) {
                pending_fills_.push_back(pid);
            } else {
                order_book_.add_order(*ord);
            }
        }
        pending_acks_.swap(clock_scratch_);
    }

    if (!pending_fills_.empty()) {
        clock_scratch_.clear();
        for (uint64_t pid : pending_fills_) {
            XtSimOrder* ord = find_live(pid);
            if (!ord) continue;
            if (cur_time_ >= ord->fill_eligible_time_ms) {
                order_book_.add_order(*ord);
            } else {
                clock_scratch_.push_back(pid);
            }
        }
        pending_fills_.swap(clock_scratch_);
    }

    if (!pending_cancels_.empty()) {
        clock_scratch_.clear();
        for (uint64_t pid : pending_cancels_) {
            XtSimOrder* ord = find_live(pid);
            if (!ord || order_status_is_final(ord->status)) continue;
            if (cur_time_ >= ord->cancel_due_time_ms) {
                cancel_now_(pid);
            } else {
                clock_scratch_.push_back(pid);
            }
        }
        pending_cancels_.swap(clock_scratch_);
    }

    const bool use_time_ttl = (ttl_us_ > 0);
    if (!use_time_ttl && ttl_ticks_ == 0) {
        recompute_earliest_due_();
        return;
    }

    const uint32_t ttl_ms = use_time_ttl
                            ? static_cast<uint32_t>((ttl_us_ + 999) / 1000)
                            : 0;

    clock_scratch_.clear();
    for (const auto& ord : orders_) {
        const bool expired = use_time_ttl
            ? (cur_time_ >= ord.create_time_ms
               && (cur_time_ - ord.create_time_ms) >= ttl_ms)
            : (cur_tick_ >= ord.create_tick
               && (cur_tick_ - ord.create_tick) >= ttl_ticks_);
        if (expired) clock_scratch_.push_back(ord.id);
    }
    for (uint64_t id : clock_scratch_) {
        XtSimOrder* ord = find_live(id);
        if (!ord) continue;
        ord->status = XtOrderStatus::Canceled;
        archive_order(id,  true,  true, 0);
    }

    recompute_earliest_due_();
}

void XtOrderManager::advance_clock_to(int64_t now_ns) noexcept {

    if (now_ns < earliest_due_ns_) {
        cur_clock_ns_ = now_ns;
        return;
    }
    cur_clock_ns_ = now_ns;
    uint32_t d = 0, t = 0;
    xt_time::from_ns(now_ns, d, t);
    advance_clock( cur_tick_, d, t);
}

void XtOrderManager::recompute_earliest_due_() noexcept {

    const bool has_pending =
        !pending_acks_.empty() || !pending_fills_.empty() || !pending_cancels_.empty()
        || ((ttl_us_ > 0 || ttl_ticks_ > 0) && !orders_.empty());
    if (has_pending) {
        earliest_due_ns_ = (cur_clock_ns_ != INT64_MIN) ? cur_clock_ns_ : 0;
    } else {
        earliest_due_ns_ = INT64_MAX;
    }
}

uint64_t XtOrderManager::submit(uint64_t order_id,
                                const char* code,
                                bool is_buy,
                                int64_t price_fp,
                                int64_t qty_fp,
                                XtTimeInForce tif,
                                uint32_t order_date,
                                uint32_t order_time_ms,
                                std::string_view user_tag,
                                XtOffset close_offset,
                                bool is_resubmit,
                                uint32_t ack_delay_ms_sampled,
                                uint32_t fill_latency_ms_sampled,
                                int64_t response_latency_ns_sampled) {
    if (XtCStringUtil::is_empty(code)) {
        ++engine_drops_.blank_code;
        return 0;
    }
    if (qty_fp <= 0) {
        ++engine_drops_.zero_qty;
        return 0;
    }

    if (price_fp <= 0) {
        ++engine_drops_.nonpos_price;
        return 0;
    }

    if (orders_.size() >= static_cast<size_t>(kMaxConcurrentOrders)) {
        ++engine_drops_.capacity_max_concurrent;
        return 0;
    }

    XtSimOrder ord;
    ord.id                    = order_id;
    ord.code                  = code;
    ord.is_buy                 = is_buy;
    ord.price_fp              = price_fp;
    ord.qty_fp                = qty_fp;
    ord.filled_fp             = 0;
    ord.status                = XtOrderStatus::Submitted;
    ord.tif                   = tif;
    ord.create_tick           = cur_tick_;
    ord.create_time_ms        = cur_time_;
    ord.queue_ahead           = 0.0;
    ord.order_date            = order_date;
    ord.order_time            = order_time_ms;
    ord.user_tag              = user_tag;
    ord.close_offset          = close_offset;
    ord.is_resubmit           = is_resubmit;
    ord.fill_eligible_time_ms = fill_latency_ms_sampled > 0
                                    ? cur_time_ + fill_latency_ms_sampled
                                    : 0;
    ord.response_latency_ns   = response_latency_ns_sampled;

    if (last_depth_ > 0) {
        const bool aggressive = is_aggressive(
            is_buy, price_fp, last_bid_px_fp_[0], last_ask_px_fp_[0]);

        ord.is_passive = !aggressive;

        if (!aggressive) {
            if (queue_model_) {
                const int64_t* px_arr_fp = is_buy ? last_bid_px_fp_ : last_ask_px_fp_;
                const double*  qty_arr   = is_buy ? last_bid_qty_   : last_ask_qty_;
                const double level_qty = level_qty_at_price(
                    price_fp, px_arr_fp, qty_arr, last_depth_);
                ord.queue_ahead = queue_model_->estimate_queue_ahead(level_qty, is_resubmit);
            }
        }
    }

    orders_.push_back(ord);
    id_to_slot_[ord.id] = orders_.size() - 1;
    push_update(orders_.back(), 0);

    if (ttl_us_ > 0 || ttl_ticks_ > 0) {
        if (cur_clock_ns_ != INT64_MIN && cur_clock_ns_ < earliest_due_ns_) {
            earliest_due_ns_ = cur_clock_ns_;
        }
    }

    XtSimOrder* ref = find_live(ord.id);

    if (ack_delay_ms_sampled > 0) {
        ref->ack_due_time_ms = cur_time_ + ack_delay_ms_sampled;
        pending_acks_.push_back(ord.id);

    } else if (ack_delay_us_ > 0) {
        const uint32_t delay_ms = static_cast<uint32_t>((ack_delay_us_ + 999) / 1000);
        ref->ack_due_time_ms = cur_time_ + delay_ms;
        pending_acks_.push_back(ord.id);

    } else if (ack_delay_ticks_ > 0) {
        ref->ack_due_tick = cur_tick_ + ack_delay_ticks_;
        pending_acks_.push_back(ord.id);

    } else {

        ref->status = XtOrderStatus::Accepted;
        push_update(*ref, 0);
        if (ref->fill_eligible_time_ms != 0 && cur_time_ < ref->fill_eligible_time_ms) {
            pending_fills_.push_back(ref->id);
        } else {
            order_book_.add_order(*ref);
        }
    }

    if ((!pending_acks_.empty() || !pending_fills_.empty() || !pending_cancels_.empty())
        && cur_clock_ns_ != INT64_MIN && cur_clock_ns_ < earliest_due_ns_) {
        earliest_due_ns_ = cur_clock_ns_;
    }

    return ord.id;
}

bool XtOrderManager::cancel(uint64_t id, uint32_t cancel_delay_ms_sampled) {
    if (cancel_delay_ms_sampled == 0) return cancel_now_(id);

    XtSimOrder* ord = find_live(id);
    if (!ord || order_status_is_final(ord->status)) return false;

    if (ord->cancel_due_time_ms != 0) return true;

    ord->cancel_due_time_ms = cur_time_ + cancel_delay_ms_sampled;
    pending_cancels_.push_back(id);

    if (cur_clock_ns_ != INT64_MIN && cur_clock_ns_ < earliest_due_ns_) {
        earliest_due_ns_ = cur_clock_ns_;
    }
    return true;
}

bool XtOrderManager::cancel_now_(uint64_t id) {
    XtSimOrder* ord = find_live(id);
    if (!ord || order_status_is_final(ord->status)) return false;

    const bool was_submitted = (ord->status == XtOrderStatus::Submitted);
    ord->status = XtOrderStatus::Canceled;

    if (was_submitted) {

        remove_from_pending(pending_acks_, id);
        archive_order(id,  false,  true, 0);
    } else {

        auto fit = std::find(pending_fills_.begin(), pending_fills_.end(), id);
        if (fit != pending_fills_.end()) {
            pending_fills_.erase(fit);
            archive_order(id,  false,  true, 0);
        } else {
            archive_order(id,  true,   true, 0);
        }
    }
    return true;
}

bool XtOrderManager::reject(uint64_t id, const char* reason_code) {
    XtSimOrder* ord = find_live(id);
    if (!ord || order_status_is_final(ord->status)) return false;

    const bool was_submitted = (ord->status == XtOrderStatus::Submitted);
    ord->status = XtOrderStatus::Rejected;

    if (was_submitted) {
        remove_from_pending(pending_acks_, id);
        archive_order(id,  false,  true, 0);
    } else {
        auto fit = std::find(pending_fills_.begin(), pending_fills_.end(), id);
        if (fit != pending_fills_.end()) {
            pending_fills_.erase(fit);
            archive_order(id,  false,  true, 0);
        } else {
            archive_order(id,  true,   true, 0);
        }
    }

    (void)reason_code;
    return true;
}

void XtOrderManager::set_depth_snapshot_fp(const int64_t* bid_px_fp,
                                           const int64_t* ask_px_fp,
                                           const int64_t* bid_qty_fp,
                                           const int64_t* ask_qty_fp,
                                           int depth) {

    for (int i = 0; i < XtRawTick::DEPTH; ++i) {
        prev_bid_px_fp_[i] = last_bid_px_fp_[i];
        prev_ask_px_fp_[i] = last_ask_px_fp_[i];
        prev_bid_qty_[i]   = last_bid_qty_[i];
        prev_ask_qty_[i]   = last_ask_qty_[i];
    }

    last_depth_ = std::clamp(depth, 0, XtRawTick::DEPTH);

    for (int i = 0; i < XtRawTick::DEPTH; ++i) {
        if (i >= last_depth_) {
            last_bid_px_fp_[i] = 0;
            last_ask_px_fp_[i] = 0;
            last_bid_qty_[i]   = 0.0;
            last_ask_qty_[i]   = 0.0;
            if (market_impact_enabled_) {
                consumed_bid_carry_[i] = 0.0;
                consumed_ask_carry_[i] = 0.0;
            }
            continue;
        }

        last_bid_px_fp_[i] = bid_px_fp ? bid_px_fp[i] : 0;
        last_ask_px_fp_[i] = ask_px_fp ? ask_px_fp[i] : 0;

        double adj_bid = bid_qty_fp ? order_exec_from_fp(bid_qty_fp[i]) : 0.0;
        double adj_ask = ask_qty_fp ? order_exec_from_fp(ask_qty_fp[i]) : 0.0;

        if (market_impact_enabled_) {

            const bool bid_px_unchanged = bid_px_fp
                && prev_bid_px_fp_[i] > 0
                && last_bid_px_fp_[i] == prev_bid_px_fp_[i];
            const bool ask_px_unchanged = ask_px_fp
                && prev_ask_px_fp_[i] > 0
                && last_ask_px_fp_[i] == prev_ask_px_fp_[i];

            if (consumed_bid_carry_[i] > 0.0 && bid_px_unchanged)
                adj_bid = std::max(0.0, adj_bid - consumed_bid_carry_[i]);

            if (consumed_ask_carry_[i] > 0.0 && ask_px_unchanged)
                adj_ask = std::max(0.0, adj_ask - consumed_ask_carry_[i]);

            consumed_bid_carry_[i] *= market_impact_decay_;
            consumed_ask_carry_[i] *= market_impact_decay_;
        }

        last_bid_qty_[i] = adj_bid;
        last_ask_qty_[i] = adj_ask;
    }
}

void XtOrderManager::match_tick(const char* code,
                                const int64_t* bid_px_fp,
                                const int64_t* ask_px_fp,
                                const int64_t* bid_qty_fp,
                                const int64_t* ask_qty_fp,
                                int depth,
                                double max_fill_qty,
                                std::vector<XtTradeRecord>& trades) {
    if (XtCStringUtil::is_empty(code)) return;

    if (orders_.empty()) return;

    refresh_queue_ahead(depth);
    apply_queue_cancel_erosion(depth);

    const auto& res = order_book_.match_tick(
        code, bid_px_fp, ask_px_fp, bid_qty_fp, ask_qty_fp, depth,
        max_fill_qty, 0.0,
        [](void* ctx, uint64_t id) -> XtSimOrder& {
            return *static_cast<XtOrderManager*>(ctx)->find_live(id);
        },
        this, upper_limit_fp_, lower_limit_fp_);

    FillRecord filled_buf[kMaxFillsPerTick];
    int        filled_count = 0;

    apply_fill_updates(res.fills, trades, filled_buf, filled_count);

    accumulate_market_impact(res.fills);

    apply_canceled_ids(res.canceled);
    apply_done_ids(res.done);

    cancel_ioc_remainders(filled_buf, filled_count);

    const bool buy_frozen = upper_limit_fp_ > 0 && last_bid_px_fp_[0] >= upper_limit_fp_;
    const bool sell_frozen = lower_limit_fp_ > 0 && last_ask_px_fp_[0] > 0
                             && last_ask_px_fp_[0] <= lower_limit_fp_;
    if (buy_frozen || sell_frozen) {
        clock_scratch_.clear();
        for (const auto& ord : orders_) {
            if (ord.tif != XtTimeInForce::IOC) continue;
            if (order_status_is_final(ord.status)) continue;

            if (ord.status == XtOrderStatus::Submitted) continue;
            if (ord.code != code) continue;
            if ((ord.is_buy && buy_frozen) || (!ord.is_buy && sell_frozen)) {
                clock_scratch_.push_back(ord.id);
            }
        }
        for (uint64_t id : clock_scratch_) {
            XtSimOrder* o = find_live(id);
            if (!o) continue;
            o->status = XtOrderStatus::Canceled;
            archive_order(id,  true,  true, 0);
        }
    }
}

void XtOrderManager::refresh_queue_ahead(int depth) {
    if (last_depth_ <= 0) return;
    const int d = std::min(depth, last_depth_);

    for (auto& ord : orders_) {
        if (ord.status != XtOrderStatus::Accepted &&
            ord.status != XtOrderStatus::PartiallyFilled)
            continue;
        if (ord.queue_ahead <= 0.0) continue;

        if (is_board_price_(ord.price_fp)) continue;

        const double*  cur_qty    = ord.is_buy ? last_bid_qty_    : last_ask_qty_;
        const int64_t* cur_px_fp  = ord.is_buy ? last_bid_px_fp_  : last_ask_px_fp_;
        const int64_t* prev_px_fp = ord.is_buy ? prev_bid_px_fp_  : prev_ask_px_fp_;

        for (int i = 0; i < d; ++i) {
            if (cur_px_fp[i] <= 0) break;
            if (cur_px_fp[i] != ord.price_fp) continue;

            if (prev_px_fp[i] != cur_px_fp[i]) {

                if (queue_model_) {
                    ord.queue_ahead = queue_model_->estimate_queue_ahead(cur_qty[i], false);
                }
            }
            break;
        }
    }

    if (ofi_queue_scale_ <= 0.0) return;

    const double ask0_drain = stable_level_drain(
        prev_ask_qty_[0], last_ask_qty_[0], prev_ask_px_fp_[0], last_ask_px_fp_[0]);
    const double bid0_drain = stable_level_drain(
        prev_bid_qty_[0], last_bid_qty_[0], prev_bid_px_fp_[0], last_bid_px_fp_[0]);

    for (auto& ord : orders_) {
        if (ord.status != XtOrderStatus::Accepted &&
            ord.status != XtOrderStatus::PartiallyFilled)
            continue;
        if (ord.queue_ahead <= 0.0) continue;
        if (is_board_price_(ord.price_fp)) continue;

        if (!ord.is_buy && ask0_drain > 0.0
            && last_ask_px_fp_[0] == ord.price_fp) {
            ord.queue_ahead = std::max(0.0, ord.queue_ahead - ask0_drain * ofi_queue_scale_);
        }
        if (ord.is_buy && bid0_drain > 0.0
            && last_bid_px_fp_[0] == ord.price_fp) {
            ord.queue_ahead = std::max(0.0, ord.queue_ahead - bid0_drain * ofi_queue_scale_);
        }
    }
}

void XtOrderManager::apply_queue_cancel_erosion(int depth) {
    if (queue_cancel_ratio_ <= 0.0 || last_depth_ <= 0) return;
    const int d = std::min(depth, last_depth_);

    for (auto& ord : orders_) {
        if (ord.status != XtOrderStatus::Accepted &&
            ord.status != XtOrderStatus::PartiallyFilled)
            continue;
        if (ord.queue_ahead <= 0.0) continue;
        if (is_board_price_(ord.price_fp)) continue;

        const double*  cur_qty    = ord.is_buy ? last_bid_qty_   : last_ask_qty_;
        const double*  prev_qty   = ord.is_buy ? prev_bid_qty_   : prev_ask_qty_;
        const int64_t* cur_px_fp  = ord.is_buy ? last_bid_px_fp_ : last_ask_px_fp_;
        const int64_t* prev_px_fp = ord.is_buy ? prev_bid_px_fp_ : prev_ask_px_fp_;

        for (int i = 0; i < d; ++i) {
            if (cur_px_fp[i] <= 0) break;
            if (cur_px_fp[i] != ord.price_fp) continue;

            if (prev_px_fp[i] != cur_px_fp[i]) break;

            const double delta = prev_qty[i] - cur_qty[i];
            if (delta > 0.0) {
                ord.queue_ahead -= std::min(delta * queue_cancel_ratio_, ord.queue_ahead);
            }
            break;
        }
    }
}

void XtOrderManager::apply_fill_updates(const std::vector<XtOrderBook::Fill>& fills,
                                        std::vector<XtTradeRecord>& trades,
                                        FillRecord* filled_buf,
                                        int& filled_count) {
    for (const auto& fill : fills) {
        XtSimOrder* ord = find_live(fill.id);
        if (!ord) continue;

        ord->filled_fp += fill.qty_fp;

        bool found = false;
        for (int i = 0; i < filled_count; ++i) {
            if (filled_buf[i].id == fill.id) {
                filled_buf[i].qty_fp += fill.qty_fp;
                found = true;
                break;
            }
        }
        if (!found && filled_count < kMaxFillsPerTick) {
            filled_buf[filled_count++] = {fill.id, fill.qty_fp};
        }

        ord->status = (ord->remaining_fp() <= 0)
                      ? XtOrderStatus::Filled
                      : XtOrderStatus::PartiallyFilled;
        push_update(*ord, fill.qty_fp);

        XtTradeRecord trd;
        trd.order_id     = ord->id;
        trd.code         = ord->code;
        trd.is_buy        = ord->is_buy;
        trd.qty_fp       = fill.qty_fp;
        trd.price        = order_exec_from_fp(fill.price_fp);
        trd.order_date   = ord->order_date;
        trd.order_time   = ord->order_time;
        trd.user_tag     = ord->user_tag;
        trd.is_close     = (ord->close_offset != XtOffset::Open);
        trd.is_today     = (ord->close_offset == XtOffset::CloseToday);
        trd.close_offset = ord->close_offset;
        trd.is_passive   = ord->is_passive;
        trd.sim_response_latency_ns = ord->response_latency_ns;
        trades.push_back(trd);
    }
}

void XtOrderManager::apply_canceled_ids(const std::vector<uint64_t>& ids) {
    for (uint64_t id : ids) {
        XtSimOrder* ord = find_live(id);
        if (!ord) continue;
        ord->status = XtOrderStatus::Canceled;
        archive_order(id,  true,  true, 0);
    }
}

void XtOrderManager::apply_done_ids(const std::vector<uint64_t>& ids) {
    for (uint64_t id : ids) {
        XtSimOrder* ord = find_live(id);
        if (!ord || ord->remaining_fp() > 0) continue;
        ord->status = XtOrderStatus::Filled;
        archive_order(id,  true,  false, 0);
    }
}

void XtOrderManager::cancel_ioc_remainders(const FillRecord* filled_buf, int filled_count) {

    for (int i = 0; i < filled_count; ++i) {
        XtSimOrder* ord = find_live(filled_buf[i].id);
        if (!ord) continue;
        if (ord->tif == XtTimeInForce::IOC && ord->remaining_fp() > 0) {
            ord->status = XtOrderStatus::Canceled;
            archive_order(filled_buf[i].id,  true,
                           true, 0);
        }
    }
}

void XtOrderManager::accumulate_market_impact(const std::vector<XtOrderBook::Fill>& fills) {

    if (!market_impact_enabled_ || fills.empty()) return;

    for (const auto& fill : fills) {

        bool is_buy = false;
        if (const XtSimOrder* p = find_live(fill.id)) {
            is_buy = p->is_buy;
        } else {
            for (const auto& d : done_) {
                if (d.id == fill.id) { is_buy = d.is_buy; break; }
            }
        }

        const int64_t fill_price_fp = fill.price_fp;
        if (is_buy) {
            for (int i = 0; i < last_depth_; ++i) {
                if (last_ask_px_fp_[i] <= 0) break;
                if (last_ask_px_fp_[i] == fill_price_fp) {
                    consumed_ask_carry_[i] += order_exec_from_fp(fill.qty_fp);
                    break;
                }
            }
        } else {
            for (int i = 0; i < last_depth_; ++i) {
                if (last_bid_px_fp_[i] <= 0) break;
                if (last_bid_px_fp_[i] == fill_price_fp) {
                    consumed_bid_carry_[i] += order_exec_from_fp(fill.qty_fp);
                    break;
                }
            }
        }
    }
}

void XtOrderManager::archive_order(uint64_t id,
                                   bool remove_from_book,
                                   bool emit_update,
                                   int64_t last_fill_fp) {
    auto it = id_to_slot_.find(id);
    if (it == id_to_slot_.end()) return;
    const size_t idx = it->second;

    XtSimOrder& ord = orders_[idx];
    if (emit_update)      push_update(ord, last_fill_fp);
    if (remove_from_book) order_book_.remove_order(ord);

    done_.push_back(ord);

    if (done_.size() > kDoneOrderCacheCap) done_.pop_front();

    id_to_slot_.erase(it);

    const size_t last = orders_.size() - 1;
    if (idx != last) {
        orders_[idx] = orders_.back();
        id_to_slot_[orders_[idx].id] = idx;
    }
    orders_.pop_back();
}

const XtSimOrder* XtOrderManager::find(uint64_t id) const {
    if (const XtSimOrder* p = find_live(id)) return p;

    for (const auto& ord : done_) {
        if (ord.id == id) return &ord;
    }
    return nullptr;
}

const std::vector<XtSimOrder>& XtOrderManager::live_orders() const {
    return orders_;
}

void XtOrderManager::drain_updates(std::vector<XtOrderExecUpdate>& out) {
    out.insert(out.end(), updates_.begin(), updates_.end());
    updates_.clear();
}

void XtOrderManager::push_update(const XtSimOrder& ord, int64_t last_fill_fp) {
    XtOrderExecUpdate u;
    u.id           = ord.id;
    u.is_buy        = ord.is_buy;
    u.close_offset = ord.close_offset;
    u.status       = ord.status;
    u.price_fp     = ord.price_fp;
    u.qty_fp       = ord.qty_fp;
    u.filled_fp    = ord.filled_fp;
    u.leaves_fp    = ord.remaining_fp();
    u.last_fill_fp = last_fill_fp;

    u.local_cb_ns  = xt_yyyymmdd_to_day_start_ns(cur_date_)
                   + xt_ms_to_ns(static_cast<uint64_t>(cur_time_));
    updates_.push_back(u);
}

namespace engine_config {

template <typename T>
static inline void try_read(const YAML::Node& node, const char* key, T& out) {
    if (node && node[key]) out = node[key].as<T>();
}

XtOrderManager::RuntimeConfig parse_order_manager_runtime_config(const YAML::Node& node) {
    XtOrderManager::RuntimeConfig cfg;
    try_read(node, "queue_cancel_ratio",          cfg.queue_cancel_ratio);
    try_read(node, "ack_delay_ticks",             cfg.ack_delay_ticks);

    try_read(node, "order_timeout_ticks",         cfg.ttl_ticks);
    try_read(node, "ack_delay_us",                cfg.ack_delay_us);
    try_read(node, "ttl_us",                      cfg.ttl_us);
    try_read(node, "ofi_queue_scale",             cfg.ofi_queue_scale);
    try_read(node, "market_impact_enabled",       cfg.market_impact_enabled);
    try_read(node, "market_impact_decay",         cfg.market_impact_decay);
    return cfg;
}

}
