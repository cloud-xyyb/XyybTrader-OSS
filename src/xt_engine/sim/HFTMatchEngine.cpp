#include "HFTMatchEngine.h"

#include "ConstantLatencyModel.h"
#include "RatioQueueModel.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <random>
#include <stdexcept>
#include <yaml-cpp/yaml.h>

#include "xt_core/market/XtTickConvert.h"
#include "xt_util/time/XtTime.h"

namespace {

// 将单笔成交按 fixed-point 整数步长拆成多笔较小成交；输出缓冲由调用方复用。
// 数量守恒不变式：Σout == qty_fp 逐整数相等。采样预算耗尽后残量按最大对齐块
// 确定性切完；末片可小于 min / 不对齐 tick，其余每片 tick 对齐且在 tick<=max 时
// <= max（退化配置 tick>max 下每片 = tick）。病态大单超片数上限时整笔单片不拆。
// 前置：min_fp/max_fp/tick_fp 已由 set_config 归一化为正值且 min<=max。
void split_qty_fp(int64_t qty_fp,
                  int64_t min_fp,
                  int64_t max_fp,
                  int64_t tick_fp,
                  std::mt19937& rng,
                  std::vector<int64_t>& out) {
    out.clear();
    if (qty_fp <= 0) return;
    int64_t min_steps = min_fp / tick_fp;
    int64_t max_steps = max_fp / tick_fp;
    if (min_steps <= 0) min_steps = 1;
    if (max_steps < min_steps) max_steps = min_steps;
    // 病态大单防御：片数上限（shipped 配置远不可达）；超限整笔单片，守恒优先于拆分粒度。
    constexpr int64_t kMaxSplitParts = 100'000;
    const int64_t chunk = max_steps * tick_fp;
    if (qty_fp / chunk >= kMaxSplitParts) {
        out.push_back(qty_fp);
        return;
    }
    std::uniform_int_distribution<int64_t> dist(min_steps, max_steps);
    int64_t left = qty_fp;
    int sample_budget = 1000;
    while (left > 0 && --sample_budget > 0) {
        int64_t cur = dist(rng) * tick_fp;
        if (cur > left) cur = left;  // 末片残量：可小于 min / 不对齐 tick。
        out.push_back(cur);
        left -= cur;
    }
    // 采样预算耗尽（qty 远大于 max×预算）：残量按最大对齐块确定性切完，保守恒。
    while (left > 0) {
        const int64_t cur = std::min(left, chunk);
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

} // namespace

void XtHftMatchEngine::set_config(const Config& cfg) {
    XtOrderManager::RuntimeConfig om_cfg;
    om_cfg.queue_cancel_ratio         = cfg.queue_cancel_ratio;
    om_cfg.ack_delay_ticks            = cfg.ack_delay_ticks;
    om_cfg.ttl_ticks                  = cfg.ttl_ticks;
    om_cfg.ack_delay_us               = 0;          // 由 submit 层采样覆盖
    om_cfg.ttl_us                     = cfg.ttl_us;
    om_cfg.ofi_queue_scale            = cfg.ofi_queue_scale;
    om_cfg.market_impact_enabled      = cfg.market_impact_enabled;
    om_cfg.market_impact_decay        = cfg.market_impact_decay;
    order_manager_.configure_runtime(om_cfg);

    error_rate_    = cfg.error_rate;
    reject_rate_   = cfg.reject_rate;
    split_fills_   = cfg.split_fills;
    // split 参数 NaN/Inf fail-fast（脏值进整数换算未定义）；<=0/倒挂沿用宽松归一化。
    if (!std::isfinite(cfg.split_min_qty) || !std::isfinite(cfg.split_max_qty) ||
        !std::isfinite(cfg.split_qty_tick)) {
        throw std::runtime_error("fill.min_qty/max_qty/qty_tick 必须是有限数值");
    }
    constexpr double kMaxSplitQty = 9.0e12;  // ×1e6 后仍在 int64 表示范围内。
    const double tick_q = std::min(cfg.split_qty_tick > 0.0 ? cfg.split_qty_tick : 1.0, kMaxSplitQty);
    const double min_q  = std::min(cfg.split_min_qty > 0.0 ? cfg.split_min_qty : tick_q, kMaxSplitQty);
    const double max_q  = std::min(cfg.split_max_qty < min_q ? min_q : cfg.split_max_qty, kMaxSplitQty);
    split_tick_fp_ = std::max<int64_t>(1, order_exec_to_fp(tick_q));
    split_min_fp_  = std::max<int64_t>(1, order_exec_to_fp(min_q));
    split_max_fp_  = std::max(split_min_fp_, order_exec_to_fp(max_q));
    volume_limit_  = cfg.volume_limit;
    cancel_latency_enabled_ = cfg.cancel_latency_enabled;
    price_limit_enabled_ = cfg.price_limit_enabled;
    ack_delay_us_          = cfg.ack_delay_us;
    ack_delay_stddev_us_   = cfg.ack_delay_stddev_us;

    // model 注入独立于 Config；未注入时 latency 和 queue_ahead 都按 0 处理。

    if (cfg.random_seed > 0) {
        rng_.seed(static_cast<uint32_t>(cfg.random_seed));
    } else {
        rng_.seed(std::random_device{}());
    }
}

void XtHftMatchEngine::reset_volume_cache() {
    volume_table_.clear();  // 保留 capacity，重建时不重新分配。
}

void XtHftMatchEngine::advance_clock(uint64_t tick_idx, int64_t event_now_ns,
                                     uint32_t trading_date, uint32_t action_time_ms) {
    prev_date_ = current_date_;
    prev_time_ms_ = current_time_ms_;
    current_date_ = trading_date;
    current_time_ms_ = action_time_ms;
    if (event_now_ns > cur_event_ns_) cur_event_ns_ = event_now_ns;
    order_manager_.advance_clock(tick_idx, event_now_ns, trading_date, action_time_ms);
}

void XtHftMatchEngine::advance_clock(uint64_t tick_idx, uint32_t date, uint32_t time_ms) {
    advance_clock(tick_idx, xt_time::to_ns(date, time_ms), date, time_ms);
}

void XtHftMatchEngine::advance_clock_to(int64_t event_now_ns,
                                        uint32_t trading_date, uint32_t action_time_ms) {
    prev_date_ = current_date_;
    prev_time_ms_ = current_time_ms_;
    current_date_    = trading_date;
    current_time_ms_ = action_time_ms;
    if (event_now_ns > cur_event_ns_) cur_event_ns_ = event_now_ns;
    order_manager_.advance_clock_to(event_now_ns, trading_date, action_time_ms);
}

void XtHftMatchEngine::advance_clock(uint32_t date, uint32_t time_ms) {
    advance_clock_to(xt_time::to_ns(date, time_ms), date, time_ms);
}

int64_t XtHftMatchEngine::query_event_ts_() const {
    if (cur_event_ns_ == INT64_MIN) {
        if (latency_model_ && latency_model_->requires_event_timestamp()) {
            throw std::runtime_error(
                "latency model 需要 event timestamp，但首个 event 前发生 submit/cancel"
                "（interpolated 时变模型不允许静默取首样本；改用 type:constant/cdf 或"
                "确保首个行情事件先于首次下单）");
        }
        return 0;
    }
    return cur_event_ns_;
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
        d.lookup_event_ns = query_event_ts_();  // 原始 lookup 时刻；flush 时采样用它
        const uint32_t target = (wheel_cursor_ + order_delay_ticks_) % delay_wheel_.size();
        delay_wheel_[target].push_back(d);
        return order_id;
    }
    const int64_t sample_ts = query_event_ts_();
    const uint32_t ack_delay_ms  = sample_ack_delay_ms();
    const uint32_t fill_latency_ms = sample_fill_latency_ms(sample_ts);
    // response 与 entry 相邻配对采样存单（paired CDF 单槽在相邻调用间无交错窗口）；
    // fill 时刻再采样会在多在途单交错序下错配 percentile。-1 = 无模型。
    const int64_t response_ns = latency_model_ ? sample_response_latency_ns(sample_ts) : -1;

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
    // 先扫 delay wheel：上行在途（尚未到达 venue）的单可撤，命中即移除并
    // 合成 Canceled 终态（gateway 已冻结 margin/注册 slot，销账靠终态；
    // 旧行为 cancel 恕不到 wheel 单 → 策略永远撤不掉它）。简化：撤单请求
    // 视为在同一上行链路追上并拦截了 deferred submit。
    if (order_delay_ticks_ > 0) {
        for (auto& slot : delay_wheel_) {
            for (auto it = slot.begin(); it != slot.end(); ++it) {
                if (it->order_id == id) {
                    order_manager_.emit_in_transit_canceled(
                        it->order_id, it->code, it->is_buy, it->price_fp,
                        it->qty_fp, it->tif, it->user_tag, it->close_offset);
                    slot.erase(it);
                    return true;
                }
            }
        }
    }
    return order_manager_.cancel(id, sample_cancel_delay_ms(query_event_ts_()));
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

void XtHftMatchEngine::collect_in_transit_order_ids(std::vector<uint64_t>& out) const {
    if (order_delay_ticks_ == 0) return;
    for (const auto& slot : delay_wheel_) {
        for (const auto& d : slot) out.push_back(d.order_id);
    }
}

std::size_t XtHftMatchEngine::discard_in_transit_orders() noexcept {
    // EOF 后不会再推进 wheel，尚未提交到 OrderManager 的订单视为 in-transit
    // lost；逐单合成 Canceled 终态（销账链靠终态，旧行为零终态 → frozen 永泄）。
    if (order_delay_ticks_ == 0) return 0;
    const std::size_t before = delay_wheel_pending_count();
    if (before == 0) return 0;
    for (auto& slot : delay_wheel_) {
        for (const auto& d : slot) {
            order_manager_.emit_in_transit_canceled(
                d.order_id, d.code, d.is_buy, d.price_fp, d.qty_fp,
                d.tif, d.user_tag, d.close_offset);
        }
        slot.clear();
    }
    lost_in_transit_count_ += static_cast<std::uint64_t>(before);
    return before;
}

void XtHftMatchEngine::discard_eof_live_orders() {
    order_manager_.discard_eof_live_orders();
}

// ── Private Helpers ────────────────────────────────────────────────────

double XtHftMatchEngine::next_rand01() {
    return uniform_dist_(rng_);
}

double XtHftMatchEngine::calc_max_fill_qty(const XtTick& tick) {
    // total_volume 差分更稳定；同时存在 tick volume 时取两者较小值。
    // 返回语义与 XtOrderBook::match_tick 对齐：<0 不限制，0 = 本 tick 零成交量。
    const double total_volume = xt_tick_convert::qty_from_fp(tick.total_volume_fp);
    const double volume = xt_tick_convert::qty_from_fp(tick.volume_fp);
    if (total_volume > 0.0) {
        double& last = find_or_insert_volume(tick.code);
        // 首观测只建 baseline：此时 total 是开盘以来全量累计而非本 tick 增量，
        // 当 cap 用会一次性放行全天成交量（系统性乐观）；带 per-tick volume
        // 的数据源不受影响（下方 volume>0 分支接管）。
        const bool first_obs = (last <= 0.0);
        double diff = first_obs ? 0.0 : (total_volume - last);
        if (diff < 0.0) diff = total_volume;  // 累计量归零后的保守 fallback。
        last = total_volume;
        if (volume > 0.0) {
            return (diff > 0.0) ? std::min(diff, volume) : volume;
        }
        return diff;  // diff==0 → 零成交量 tick，禁止 fill。
    }
    if (volume > 0.0) {
        return volume;
    }
    return -1.0;  // 数据源不带成交量信息 → 维持 fail-open 不限制。
}

uint32_t XtHftMatchEngine::sample_ack_delay_ms() {
    if (ack_delay_us_ == 0) return 0;

    if (ack_delay_stddev_us_ == 0) {
        return static_cast<uint32_t>((ack_delay_us_ + 999ULL) / 1000ULL);
    }

    // ACK 抖动按 N(mean, stddev^2) 采样；5x mean 是尾部保护上限。
    const double mean_us  = static_cast<double>(ack_delay_us_);
    const double stddev_us = static_cast<double>(ack_delay_stddev_us_);
    const double sample_us = mean_us + stddev_us * latency_dist_(rng_);
    const double clamped_us = std::max(1.0, std::min(sample_us, mean_us * 5.0));
    return static_cast<uint32_t>((clamped_us + 999.0) / 1000.0);
}

uint32_t XtHftMatchEngine::sample_fill_latency_ms(int64_t event_ts_ns) {
    // fill 资格延迟（submit→可撮合 fill-eligibility）使用 entry latency，对齐
    // ILatencyModel 契约（entry = submit→match 排队）；fill 可见性（成交→策略可见）
    // 由 gateway 用随单存的 response stamp 设。paired CDF 下 entry 在此抽的 percentile
    // 由紧随的相邻 response 采样复用，保持上行/回程延迟相关。ACK 延迟由 sample_ack_delay_ms 处理。
    if (!latency_model_) return 0;
    const int64_t ns = xt_latency::validate_latency_ns(
        latency_model_->entry_latency_ns(event_ts_ns), "entry");
    if (ns <= 0) return 0;
    return static_cast<uint32_t>((ns + 999'999LL) / 1'000'000LL);
}

uint32_t XtHftMatchEngine::sample_cancel_delay_ms(int64_t event_ts_ns) {
    // cancel 请求与 submit 走同一条上行链路（hftbacktest 同口径），用 entry latency 采样。
    if (!cancel_latency_enabled_ || !latency_model_) return 0;
    const int64_t ns = xt_latency::validate_latency_ns(
        latency_model_->entry_latency_ns(event_ts_ns), "cancel_entry");
    // paired CDF 的 entry 采样必须由相邻 response 采样消费掉 percentile 槽；
    // Canceled 回报沿用 order-update 本 tick 即可见语义（与 Accepted 对称），弃值。
    (void)latency_model_->response_latency_ns(event_ts_ns);
    if (ns <= 0) return 0;
    return static_cast<uint32_t>((ns + 999'999LL) / 1'000'000LL);
}

double& XtHftMatchEngine::find_or_insert_volume(const char* code) {
    // warm path 线性扫描，无 hash/rehash；新 symbol 仅首见时 append（冷路径分配）。
    // 槽一经建立身份不变：任何 symbol 的累计量状态不被其它 symbol 覆写。
    for (auto& e : volume_table_) {
        if (std::strncmp(e.code, code, sizeof(e.code)) == 0) return e.volume;
    }
    VolumeEntry entry{};
    std::strncpy(entry.code, code, sizeof(entry.code) - 1);
    volume_table_.push_back(entry);
    return volume_table_.back().volume;
}

// ── Order Delay Wheel ────────────────────────────────────────────────────

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
    order_manager_.set_depth_snapshot_fp(tick.code,
                                         tick.bid_price_fp,
                                         tick.ask_price_fp,
                                         tick.bid_qty_fp,
                                         tick.ask_qty_fp,
                                         XtTick::DEPTH);

    // limit-lock：off 恒传 0 → 下游（冻结/erosion 免疫）全 bypass，结构性等价。
    // tick 与 depth snapshot 同源同刻，无 skew。
    order_manager_.set_price_limits_fp(
        price_limit_enabled_ ? tick.upper_limit_fp : 0,
        price_limit_enabled_ ? tick.lower_limit_fp : 0);

    // volume_limit 关闭走 <0 不限制 sentinel（0 = 零可成交量，见 calc_max_fill_qty）。
    double max_fill_qty = volume_limit_ ? calc_max_fill_qty(tick) : -1.0;

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
        split_qty_fp(trades[i].qty_fp,
                     split_min_fp_,
                     split_max_fp_,
                     split_tick_fp_,
                     local_rng,
                     parts_buf_);
        int64_t sum_fp = 0;
        for (int64_t part_fp : parts_buf_) sum_fp += part_fp;
        if (sum_fp != trades[i].qty_fp) {
            // 守恒兜底：按构造不可达；触发即放弃拆分保留原始成交，绝不丢量。
            std::fprintf(stderr,
                         "[XtHftMatchEngine] split conservation mismatch "
                         "qty_fp=%lld sum_fp=%lld parts=%zu, fill kept unsplit\n",
                         static_cast<long long>(trades[i].qty_fp),
                         static_cast<long long>(sum_fp), parts_buf_.size());
            expanded_buf_.push_back(trades[i]);
            continue;
        }
        for (int64_t part_fp : parts_buf_) {
            XtTradeRecord part = trades[i];
            part.qty_fp = part_fp;
            expanded_buf_.push_back(part);
        }
    }

    trades.resize(first_new_trade);
    for (auto& t : expanded_buf_) trades.push_back(std::move(t));
}

uint64_t XtHftMatchEngine::submit_immediate_(const DeferredOrder& d) {
    // 采样用入队时的原始 lookup 时刻（时变模型按 submit 时刻查样本）；
    // 而 deadline base 在 OrderManager 内取 flush 时 event——两段严格串联。
    const uint32_t ack_delay_ms = sample_ack_delay_ms();
    const uint32_t fill_latency_ms = sample_fill_latency_ms(d.lookup_event_ns);
    // 同 submit 主路径：response 与 entry 相邻配对采样存单。
    const int64_t response_ns = latency_model_ ? sample_response_latency_ns(d.lookup_event_ns) : -1;
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
    // yaml 兼容字段仍叫 order_timeout_ticks，DTO/manager 内部统一为 ttl_ticks。
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

} // namespace engine_config
