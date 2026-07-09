#include "HFTMatchEngine.h"

#include "ConstantLatencyModel.h"
#include "RatioQueueModel.h"

#include <algorithm>
#include <cstring>
#include <random>
#include <stdexcept>
#include <yaml-cpp/yaml.h>

#include "xt_core/market/XtTickConvert.h"
#include "xt_util/time/XtTime.h"

namespace {

void split_qty(double qty,
               double min_qty,
               double max_qty,
               double qty_tick,
               std::mt19937& rng,
               std::vector<double>& out) {
    out.clear();
    if (qty <= 0.0) return;
    if (qty_tick <= 0.0) qty_tick = 1.0;
    if (min_qty <= 0.0) min_qty = qty_tick;
    if (max_qty < min_qty) max_qty = min_qty;
    int min_steps = static_cast<int>(min_qty / qty_tick);
    int max_steps = static_cast<int>(max_qty / qty_tick);
    if (min_steps <= 0) min_steps = 1;
    if (max_steps < min_steps) max_steps = min_steps;
    if (max_steps <= 0) {
        out.push_back(qty);
        return;
    }
    std::uniform_int_distribution<int> dist(min_steps, max_steps);
    double left = qty;
    int max_iter = 1000;
    while (left > 1e-9 && --max_iter > 0) {
        double cur = dist(rng) * qty_tick;
        if (cur > left) cur = left;
        if (cur <= 0.0) continue;
        out.push_back(cur);
        left -= cur;
    }
}

void reject_retired_split_fill_keys(const YAML::Node& node) {
    if (!node) return;
    if (node["split_fills"]) throw std::runtime_error("split_fills 已退役，请改用 fill.split");
    if (node["split_min_qty"]) throw std::runtime_error("split_min_qty 已退役，请改用 fill.min_qty");
    if (node["split_max_qty"]) throw std::runtime_error("split_max_qty 已退役，请改用 fill.max_qty");
    if (node["split_qty_tick"]) throw std::runtime_error("split_qty_tick 已退役，请改用 fill.qty_tick");
}

}

void XtHftMatchEngine::set_config(const Config& cfg) {
    XtOrderManager::RuntimeConfig om_cfg;
    om_cfg.queue_cancel_ratio         = cfg.queue_cancel_ratio;
    om_cfg.ack_delay_ticks            = cfg.ack_delay_ticks;
    om_cfg.ttl_ticks                  = cfg.ttl_ticks;
    om_cfg.ack_delay_us               = 0;
    om_cfg.ttl_us                     = cfg.ttl_us;
    om_cfg.ofi_queue_scale            = cfg.ofi_queue_scale;
    om_cfg.market_impact_enabled      = cfg.market_impact_enabled;
    om_cfg.market_impact_decay        = cfg.market_impact_decay;
    order_manager_.configure_runtime(om_cfg);

    error_rate_    = cfg.error_rate;
    reject_rate_   = cfg.reject_rate;
    split_fills_   = cfg.split_fills;
    split_min_qty_ = cfg.split_min_qty;
    split_max_qty_ = cfg.split_max_qty;
    split_qty_tick_ = cfg.split_qty_tick;
    volume_limit_  = cfg.volume_limit;
    cancel_latency_enabled_ = cfg.cancel_latency_enabled;
    price_limit_enabled_ = cfg.price_limit_enabled;
    ack_delay_us_          = cfg.ack_delay_us;
    ack_delay_stddev_us_   = cfg.ack_delay_stddev_us;

    if (cfg.random_seed > 0) {
        rng_.seed(static_cast<uint32_t>(cfg.random_seed));
    } else {
        rng_.seed(std::random_device{}());
    }
}

void XtHftMatchEngine::reset_volume_cache() {
    for (auto& e : volume_table_) e = VolumeEntry{};
}

void XtHftMatchEngine::advance_clock(uint64_t tick_idx, uint32_t date, uint32_t time_ms) {
    prev_date_ = current_date_;
    prev_time_ms_ = current_time_ms_;
    current_date_ = date;
    current_time_ms_ = time_ms;
    order_manager_.advance_clock(tick_idx, date, time_ms);
}

void XtHftMatchEngine::advance_clock(uint32_t date, uint32_t time_ms) {
    prev_date_ = current_date_;
    prev_time_ms_ = current_time_ms_;
    current_date_    = date;
    current_time_ms_ = time_ms;
    const int64_t now_ns = xt_time::to_ns(date, time_ms);
    order_manager_.advance_clock_to(now_ns);
}

uint64_t XtHftMatchEngine::submit(uint64_t order_id,
                                   const char* code,
                                   bool is_buy,
                                   int64_t price_fp,
                                   int64_t qty_fp,
                                   XtTimeInForce tif,
                                   uint32_t order_date,
                                   uint32_t order_time_ms,
                                   std::string_view user_tag,
                                   XtOffset close_offset,
                                   bool is_resubmit) {
    if (order_delay_ticks_ > 0) {
        DeferredOrder d{};
        d.order_id = order_id;
        std::strncpy(d.code, code ? code : "", sizeof(d.code) - 1);
        d.is_buy = is_buy;
        d.price_fp = price_fp;
        d.qty_fp = qty_fp;
        d.tif = tif;
        d.order_date = order_date;
        d.order_time_ms = order_time_ms;
        if (!user_tag.empty()) {
            std::strncpy(d.user_tag,
                         user_tag.data(),
                         std::min(user_tag.size(), sizeof(d.user_tag) - 1));
        }
        d.close_offset = close_offset;
        d.is_resubmit = is_resubmit;
        const uint32_t target = (wheel_cursor_ + order_delay_ticks_) % delay_wheel_.size();
        delay_wheel_[target].push_back(d);
        return order_id;
    }
    const uint32_t ack_delay_ms  = sample_ack_delay_ms();
    const uint32_t fill_latency_ms = sample_fill_latency_ms();

    const int64_t response_ns = latency_model_ ? sample_response_latency_ns() : -1;

    const uint64_t id = order_manager_.submit(order_id,
                                              code,
                                              is_buy,
                                              price_fp,
                                              qty_fp,
                                              tif,
                                              order_date,
                                              order_time_ms,
                                              user_tag,
                                              close_offset,
                                              is_resubmit,
                                              ack_delay_ms,
                                              fill_latency_ms,
                                              response_ns);
    if (id == 0) return 0;
    if (reject_rate_ > 0.0 && next_rand01() < reject_rate_) {
        (void)order_manager_.reject(id, "sim_reject_rate");
    }
    return id;
}

bool XtHftMatchEngine::cancel(uint64_t id) {
    return order_manager_.cancel(id, sample_cancel_delay_ms());
}

void XtHftMatchEngine::on_tick(const XtTick& tick, std::vector<XtTradeRecord>& trades) {
    advance_wheel_and_flush_();
    inject_cancel_errors_();

    const size_t first_new_trade = trades.size();
    match_orders_on_tick_depth_(tick, trades);
    stamp_new_trade_times_(first_new_trade, trades);
    split_new_fills_(first_new_trade, trades);
}

void XtHftMatchEngine::drain_updates(std::vector<XtOrderExecUpdate>& updates) {
    order_manager_.drain_updates(updates);
}

size_t XtHftMatchEngine::live_orders() const {
    return order_manager_.live_orders().size();
}

const std::vector<XtSimOrder>& XtHftMatchEngine::live_orders_view() const {
    return order_manager_.live_orders();
}

const XtSimOrder* XtHftMatchEngine::find(uint64_t id) const {
    return order_manager_.find(id);
}

void XtHftMatchEngine::set_order_delay_ticks(uint32_t n) {
    order_delay_ticks_ = n;
    if (n > 0) {
        delay_wheel_.resize(n + 1);
        for (auto& slot : delay_wheel_) slot.clear();
        wheel_cursor_ = 0;
    } else {
        delay_wheel_.clear();
    }
}

std::size_t XtHftMatchEngine::delay_wheel_pending_count() const noexcept {
    if (order_delay_ticks_ == 0) return 0;
    std::size_t total = 0;
    for (const auto& slot : delay_wheel_) total += slot.size();
    return total;
}

std::size_t XtHftMatchEngine::discard_in_transit_orders() noexcept {

    if (order_delay_ticks_ == 0) return 0;
    const std::size_t before = delay_wheel_pending_count();
    if (before == 0) return 0;
    for (auto& slot : delay_wheel_) slot.clear();
    lost_in_transit_count_ += static_cast<std::uint64_t>(before);
    return before;
}

double XtHftMatchEngine::next_rand01() {
    return uniform_dist_(rng_);
}

double XtHftMatchEngine::calc_max_fill_qty(const XtTick& tick) {

    const double total_volume = xt_tick_convert::qty_from_fp(tick.total_volume_fp);
    const double volume = xt_tick_convert::qty_from_fp(tick.volume_fp);
    if (total_volume > 0.0) {
        double& last = find_or_insert_volume(tick.code);
        double diff = (last > 0.0) ? (total_volume - last) : total_volume;
        if (diff < 0.0) diff = total_volume;
        last = total_volume;
        if (volume > 0.0) {
            return (diff > 0.0) ? std::min(diff, volume) : volume;
        }
        return diff;
    }
    if (volume > 0.0) {
        return volume;
    }
    return 0.0;
}

uint32_t XtHftMatchEngine::sample_ack_delay_ms() {
    if (ack_delay_us_ == 0) return 0;

    if (ack_delay_stddev_us_ == 0) {
        return static_cast<uint32_t>((ack_delay_us_ + 999ULL) / 1000ULL);
    }

    const double mean_us  = static_cast<double>(ack_delay_us_);
    const double stddev_us = static_cast<double>(ack_delay_stddev_us_);
    const double sample_us = mean_us + stddev_us * latency_dist_(rng_);
    const double clamped_us = std::max(1.0, std::min(sample_us, mean_us * 5.0));
    return static_cast<uint32_t>((clamped_us + 999.0) / 1000.0);
}

uint32_t XtHftMatchEngine::sample_fill_latency_ms() {

    if (!latency_model_) return 0;
    const int64_t ns = latency_model_->entry_latency_ns(0);
    if (ns <= 0) return 0;
    return static_cast<uint32_t>((ns + 999'999LL) / 1'000'000LL);
}

uint32_t XtHftMatchEngine::sample_cancel_delay_ms() {

    if (!cancel_latency_enabled_ || !latency_model_) return 0;
    const int64_t ns = latency_model_->entry_latency_ns(0);

    (void)latency_model_->response_latency_ns(0);
    if (ns <= 0) return 0;
    return static_cast<uint32_t>((ns + 999'999LL) / 1'000'000LL);
}

double& XtHftMatchEngine::find_or_insert_volume(const char* code) noexcept {

    for (auto& e : volume_table_) {
        if (e.valid && std::strncmp(e.code, code, sizeof(e.code)) == 0)
            return e.volume;
    }
    for (auto& e : volume_table_) {
        if (!e.valid) {
            std::strncpy(e.code, code, sizeof(e.code) - 1);
            e.code[sizeof(e.code) - 1] = '\0';
            e.valid  = true;
            e.volume = 0.0;
            return e.volume;
        }
    }

    volume_table_[0].volume = 0.0;
    return volume_table_[0].volume;
}

void XtHftMatchEngine::advance_wheel_and_flush_() {
    if (order_delay_ticks_ == 0) return;
    auto& slot = delay_wheel_[wheel_cursor_];
    for (auto& d : slot) {
        submit_immediate_(d);
    }
    slot.clear();
    wheel_cursor_ = (wheel_cursor_ + 1) % delay_wheel_.size();
}

void XtHftMatchEngine::inject_cancel_errors_() {
    if (!(error_rate_ > 0.0)) return;

    uint64_t to_cancel_buf[64];
    int cancel_n = 0;
    for (const auto& ord : order_manager_.live_orders()) {
        if (next_rand01() < error_rate_ && cancel_n < 64)
            to_cancel_buf[cancel_n++] = ord.id;
    }
    for (int i = 0; i < cancel_n; ++i) order_manager_.cancel(to_cancel_buf[i]);
}

void XtHftMatchEngine::match_orders_on_tick_depth_(const XtTick& tick,
                                                   std::vector<XtTradeRecord>& trades) {
    order_manager_.set_depth_snapshot_fp(tick.bid_price_fp,
                                         tick.ask_price_fp,
                                         tick.bid_qty_fp,
                                         tick.ask_qty_fp,
                                         XtTick::DEPTH);

    order_manager_.set_price_limits_fp(
        price_limit_enabled_ ? tick.upper_limit_fp : 0,
        price_limit_enabled_ ? tick.lower_limit_fp : 0);

    double max_fill_qty = volume_limit_ ? calc_max_fill_qty(tick) : 0.0;

    constexpr int64_t kDepthQtyInflation = 1'000'000;
    int64_t inflated_bid_qty[XtTick::DEPTH];
    int64_t inflated_ask_qty[XtTick::DEPTH];
    const int64_t* bid_qty_ptr = tick.bid_qty_fp;
    const int64_t* ask_qty_ptr = tick.ask_qty_fp;
    if (!volume_limit_) {
        for (int i = 0; i < XtTick::DEPTH; ++i) {
            inflated_bid_qty[i] = tick.bid_qty_fp[i] > 0
                                      ? tick.bid_qty_fp[i] * kDepthQtyInflation
                                      : 0;
            inflated_ask_qty[i] = tick.ask_qty_fp[i] > 0
                                      ? tick.ask_qty_fp[i] * kDepthQtyInflation
                                      : 0;
        }
        bid_qty_ptr = inflated_bid_qty;
        ask_qty_ptr = inflated_ask_qty;
    }

    order_manager_.match_tick(tick.code,
                              tick.bid_price_fp,
                              tick.ask_price_fp,
                              bid_qty_ptr,
                              ask_qty_ptr,
                              XtTick::DEPTH,
                              max_fill_qty,
                              trades);
}

void XtHftMatchEngine::stamp_new_trade_times_(size_t first_new_trade,
                                              std::vector<XtTradeRecord>& trades) {
    const bool has_dispatch = (dispatch_date_ != 0);
    const uint32_t trade_date = has_dispatch ? dispatch_date_ : current_date_;
    const uint32_t trade_time_ms = has_dispatch ? dispatch_time_ms_ : current_time_ms_;
    dispatch_date_ = 0;
    dispatch_time_ms_ = 0;
    for (size_t i = first_new_trade; i < trades.size(); ++i) {
        trades[i].trade_date = trade_date;
        trades[i].trade_time = trade_time_ms;
        trades[i].fill_ts_us = static_cast<uint64_t>(
            xt_time::to_ns(trade_date, trade_time_ms) / 1000LL);
    }
}

void XtHftMatchEngine::split_new_fills_(size_t first_new_trade,
                                        std::vector<XtTradeRecord>& trades) {
    if (!split_fills_ || trades.size() <= first_new_trade) return;

    expanded_buf_.clear();
    expanded_buf_.reserve((trades.size() - first_new_trade) * 2);
    for (size_t i = first_new_trade; i < trades.size(); ++i) {
        const uint64_t order_seed = static_cast<uint64_t>(trades[i].order_id) * 2654435761ULL
                                  + static_cast<uint64_t>(current_date_) * 65537ULL
                                  + static_cast<uint64_t>(current_time_ms_);
        std::mt19937 local_rng(static_cast<uint32_t>(order_seed));
        split_qty(order_exec_from_fp(trades[i].qty_fp),
                  split_min_qty_,
                  split_max_qty_,
                  split_qty_tick_,
                  local_rng,
                  parts_buf_);
        for (double part_qty : parts_buf_) {
            XtTradeRecord part = trades[i];
            part.qty_fp = order_exec_to_fp(part_qty);
            expanded_buf_.push_back(part);
        }
    }

    trades.resize(first_new_trade);
    for (auto& t : expanded_buf_) trades.push_back(std::move(t));
}

uint64_t XtHftMatchEngine::submit_immediate_(const DeferredOrder& d) {
    const uint32_t ack_delay_ms = sample_ack_delay_ms();
    const uint32_t fill_latency_ms = sample_fill_latency_ms();

    const int64_t response_ns = latency_model_ ? sample_response_latency_ns() : -1;
    const uint64_t id = order_manager_.submit(d.order_id, d.code, d.is_buy,
                                              d.price_fp, d.qty_fp, d.tif,
                                              d.order_date, d.order_time_ms,
                                              std::string_view(d.user_tag),
                                              d.close_offset, d.is_resubmit,
                                              ack_delay_ms, fill_latency_ms,
                                              response_ns);
    if (id == 0) return 0;
    if (reject_rate_ > 0.0 && next_rand01() < reject_rate_) {
        (void)order_manager_.reject(id, "sim_reject_rate");
    }
    return id;
}

namespace engine_config {

XtHftMatchEngine::Config parse_match_engine_config(const YAML::Node& node) {
    XtHftMatchEngine::Config cfg;
    if (node && node["queue_cancel_ratio"]) cfg.queue_cancel_ratio = node["queue_cancel_ratio"].as<double>();
    if (node && node["ack_delay_ticks"])    cfg.ack_delay_ticks    = node["ack_delay_ticks"].as<uint64_t>();

    if (node && node["order_timeout_ticks"]) cfg.ttl_ticks         = node["order_timeout_ticks"].as<uint64_t>();
    if (node && node["ack_delay_us"])       cfg.ack_delay_us       = node["ack_delay_us"].as<uint64_t>();
    if (node && node["ttl_us"])             cfg.ttl_us             = node["ttl_us"].as<uint64_t>();
    if (node && node["ack_delay_stddev_us"]) cfg.ack_delay_stddev_us = node["ack_delay_stddev_us"].as<uint64_t>();
    cfg.error_rate  = node["error_rate"]  ? node["error_rate"].as<double>()  : 0.0;
    cfg.reject_rate = node["reject_rate"] ? node["reject_rate"].as<double>() : 0.0;
    if (node && node["seed"]) cfg.random_seed = node["seed"].as<int64_t>();
    if (node && node["volume_limit"]) cfg.volume_limit = node["volume_limit"].as<bool>();
    if (node && node["ofi_queue_scale"])
        cfg.ofi_queue_scale = node["ofi_queue_scale"].as<double>();
    if (node && node["market_impact_enabled"])
        cfg.market_impact_enabled = node["market_impact_enabled"].as<bool>();
    if (node && node["market_impact_decay"])
        cfg.market_impact_decay = node["market_impact_decay"].as<double>();
    if (node && node["cancel_latency_enabled"])
        cfg.cancel_latency_enabled = node["cancel_latency_enabled"].as<bool>();
    if (node && node["price_limit_enabled"])
        cfg.price_limit_enabled = node["price_limit_enabled"].as<bool>();
    reject_retired_split_fill_keys(node);
    const YAML::Node fill_node = node ? node["fill"] : YAML::Node{};
    if (fill_node && fill_node["split"]) cfg.split_fills = fill_node["split"].as<bool>();
    if (fill_node && fill_node["min_qty"]) cfg.split_min_qty = fill_node["min_qty"].as<double>();
    if (fill_node && fill_node["max_qty"]) cfg.split_max_qty = fill_node["max_qty"].as<double>();
    if (fill_node && fill_node["qty_tick"]) cfg.split_qty_tick = fill_node["qty_tick"].as<double>();
    return cfg;
}

}
