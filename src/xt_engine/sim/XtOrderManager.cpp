#include "XtOrderManager.h"
#include <algorithm>
#include <cstring>
#include <climits>
#include <cstdio>
#include <yaml-cpp/yaml.h>
#include "xt_util/string/XtCStringUtil.h"
#include "xt_util/time/XtTime.h"
#include "XtLatencyArith.h"
#include "xt_util/time/XtTimeParse.h"

namespace {

// 限价单入队前的 aggressor 判定；best bid/ask 缺失时按 passive 处理，
// 让订单进入本地簿等待后续 tick。
bool is_aggressive(bool is_buy,
                   int64_t price_fp,
                   int64_t best_bid_fp,
                   int64_t best_ask_fp) {
    if (is_buy)  return best_ask_fp > 0 && price_fp >= best_ask_fp;
    return              best_bid_fp > 0 && price_fp <= best_bid_fp;
}

// queue model 只需要当前 depth 内同价位可见量；价位不在快照内时按 0 排队。
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

// 只把同一最优价位上的可见量缩减计为 drain；换价表示队列重建，不跨价位扣减。
double stable_level_drain(double prev_qty,
                          double last_qty,
                          int64_t prev_px_fp,
                          int64_t last_px_fp) noexcept {
    if (prev_qty <= 0.0 || last_px_fp <= 0 || prev_px_fp != last_px_fp) {
        return 0.0;
    }
    return std::max(0.0, prev_qty - last_qty);
}

// 从 pending 队列中移除：Submitted 状态的订单尚未入订单簿，无需触碰 order_book_。
// pending 向量规模受 kMaxConcurrentOrders 限制，撤单/reject 路径可接受线性 erase。
void remove_from_pending(std::vector<uint64_t>& vec, uint64_t id) {
    auto it = std::find(vec.begin(), vec.end(), id);
    if (it != vec.end()) vec.erase(it);
}

// ceil(ttl_us / 1000) ms → ns（submit/expire/recompute 三处 TTL deadline 单源）。
// __int128 中间量防两处 int64/uint64 溢出：uint64 `+999` 回绕（UINT64_MAX 级脏
// ttl_us 会得 ttl_ns==0 → 巨大 TTL 立即到期）与后续有符号 `* 1'000'000LL` 溢出 UB。
// 越 int64 上界饱和到 INT64_MAX（config 校验已 fail-fast 拦下越界 ttl_us，此处兜底
// 不产生 UB）。调用方对 ttl_us==0（TTL 关闭）短路，不入本函数。
inline int64_t ttl_us_to_event_ns(uint64_t ttl_us) noexcept {
    const __int128 ms = (static_cast<__int128>(ttl_us) + 999) / 1000;
    const __int128 ns = ms * 1'000'000;
    return ns > static_cast<__int128>(INT64_MAX) ? INT64_MAX : static_cast<int64_t>(ns);
}

} // namespace

// ── Configuration ───────────────────────────────────────────────────────

void XtOrderManager::configure_runtime(const RuntimeConfig& cfg) {
    queue_cancel_ratio_          = cfg.queue_cancel_ratio;
    ack_delay_ticks_             = cfg.ack_delay_ticks;
    ttl_ticks_                   = cfg.ttl_ticks;
    ack_delay_us_                = cfg.ack_delay_us;
    ttl_us_                      = cfg.ttl_us;
    ofi_queue_scale_             = cfg.ofi_queue_scale;
    market_impact_enabled_       = cfg.market_impact_enabled;
    market_impact_decay_         = cfg.market_impact_decay;
    // 预分配热路径容器，避免盘中扩容；configure_runtime 不清状态，
    // 因此只适合在启动/bootstrap 阶段调用。
    orders_.reserve(kMaxConcurrentOrders);
    clock_scratch_.reserve(kMaxConcurrentOrders);
    id_to_slot_.reserve(kMaxConcurrentOrders);
    depth_table_.reserve(8);  // typical HFT BT symbol 数；超出仅冷路径 realloc
}

// ── Clock ─────────────────────────────────────────────────────

int64_t XtOrderManager::clamp_event_now_(int64_t event_now_ns) noexcept {
    if (cur_event_ns_ != INT64_MIN && event_now_ns < cur_event_ns_) {
        // 时间在撮合域内永不倒流；脏数据源（如 legacy simtick 夜盘
        // action_date=trading_date 回跳）降级为冻结推进，不 kill run。
        if (event_clock_clamp_count_++ == 0) {
            std::fprintf(stderr,
                         "[XtOrderManager] WARN event clock regression clamped "
                         "(now=%lld < cur=%lld); 后续同类回退只计数不重复告警\n",
                         static_cast<long long>(event_now_ns),
                         static_cast<long long>(cur_event_ns_));
        }
        return cur_event_ns_;
    }
    return event_now_ns;
}

void XtOrderManager::advance_clock(uint64_t tick, int64_t event_now_ns,
                                   uint32_t trading_date, uint32_t action_time_ms) {
    cur_tick_ = tick;
    cur_date_ = trading_date;
    cur_time_ = XtTimeParse::action_time_to_ms(action_time_ms);
    cur_event_ns_ = clamp_event_now_(event_now_ns);

    // Submitted -> Accepted；clock_scratch_ 复用为“未到期 id”缓冲。
    if (!pending_acks_.empty()) {
        clock_scratch_.clear();
        for (uint64_t pid : pending_acks_) {
            XtSimOrder* ord = find_live(pid);
            if (!ord || ord->status != XtOrderStatus::Submitted) continue;

            const bool due = ord->ack_due_event_ns != 0
                             ? cur_event_ns_ >= ord->ack_due_event_ns
                             : cur_tick_ >= ord->ack_due_tick;
            if (!due) {
                clock_scratch_.push_back(pid);
                continue;
            }

            ord->status = XtOrderStatus::Accepted;
            push_update(*ord, 0);
            if (ord->fill_eligible_event_ns != 0 && cur_event_ns_ < ord->fill_eligible_event_ns) {
                pending_fills_.push_back(pid);
            } else if (!order_book_.add_order(*ord)) {
                // book 容量拒收：合成 Canceled 终态回报（frozen 释放链靠终态），
                // 不留永不参与撮合的幽灵 Accepted。不走 reject()/cancel_now_()：
                // 它们会扫 pending 列表，在本循环内 erase 会炸迭代器。
                ord->status = XtOrderStatus::Canceled;
                archive_order(pid, /*remove_from_book=*/false, /*emit_update=*/true, 0);
            }
        }
        pending_acks_.swap(clock_scratch_);
    }

    // Accepted -> order book；未到撮合资格的 id 留在 pending_fills_。
    if (!pending_fills_.empty()) {
        clock_scratch_.clear();
        for (uint64_t pid : pending_fills_) {
            XtSimOrder* ord = find_live(pid);
            if (!ord) continue;
            if (cur_event_ns_ >= ord->fill_eligible_event_ns) {
                if (!order_book_.add_order(*ord)) {
                    // 同上：容量拒收合成终态；inline 处理避免碰 pending_fills_。
                    ord->status = XtOrderStatus::Canceled;
                    archive_order(pid, /*remove_from_book=*/false, /*emit_update=*/true, 0);
                }
            } else {
                clock_scratch_.push_back(pid);
            }
        }
        pending_fills_.swap(clock_scratch_);
    }

    // 在途撤单请求到期；到期前订单继续参与撮合（cancel-fill race）。
    // 已终态（成交/IOC/TTL）的 id 直接丢弃 = 撤单落空，不补发回报。
    if (!pending_cancels_.empty()) {
        clock_scratch_.clear();
        for (uint64_t pid : pending_cancels_) {
            XtSimOrder* ord = find_live(pid);
            if (!ord || order_status_is_final(ord->status)) continue;
            if (cur_event_ns_ >= ord->cancel_due_event_ns) {
                cancel_now_(pid);  // archive 会 swap-with-last，后续只用 pid 不用 ord
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

    // ms 量化保持旧口径（ceil us→ms），只换比较轴：event ns 单调，午夜不回绕。
    const int64_t ttl_ns = use_time_ttl ? ttl_us_to_event_ns(ttl_us_) : 0;

    // archive_order 会 swap-with-last，先收集 id 再批量归档。
    clock_scratch_.clear();
    for (const auto& ord : orders_) {
        const bool expired = use_time_ttl
            ? (cur_event_ns_ - ord.create_event_ns) >= ttl_ns
            : (cur_tick_ >= ord.create_tick
               && (cur_tick_ - ord.create_tick) >= ttl_ticks_);
        if (expired) clock_scratch_.push_back(ord.id);
    }
    for (uint64_t id : clock_scratch_) {
        XtSimOrder* ord = find_live(id);
        if (!ord) continue;
        ord->status = XtOrderStatus::Canceled;
        archive_order(id, /*remove_from_book=*/true, /*emit_update=*/true, 0);
    }

    recompute_earliest_due_();
}

void XtOrderManager::advance_clock(uint64_t tick, uint32_t date, uint32_t time_ms) {
    advance_clock(tick, xt_time::to_ns(date, time_ms), date, time_ms);
}

void XtOrderManager::advance_clock_to(int64_t event_now_ns,
                                      uint32_t trading_date, uint32_t action_time_ms) {
    const int64_t now = clamp_event_now_(event_now_ns);
    if (now < earliest_due_event_ns_) {
        cur_event_ns_ = now;
        return;
    }
    advance_clock(cur_tick_, now, trading_date, action_time_ms);
}

void XtOrderManager::advance_clock_to(int64_t event_now_ns) {
    const int64_t now = clamp_event_now_(event_now_ns);
    if (now < earliest_due_event_ns_) {
        cur_event_ns_ = now;
        return;
    }
    // 便捷入口（测试/单轴场景）：回报戳由 from_ns 派生（action==trading 语义）。
    uint32_t d = 0, t = 0;
    xt_time::from_ns(now, d, t);
    advance_clock(cur_tick_, now, d, t);
}

void XtOrderManager::recompute_earliest_due_() noexcept {
    int64_t min_ns = INT64_MAX;
    for (uint64_t pid : pending_acks_) {
        const XtSimOrder* o = find_live(pid);
        if (o && o->status == XtOrderStatus::Submitted && o->ack_due_event_ns != 0
            && o->ack_due_event_ns < min_ns) {
            min_ns = o->ack_due_event_ns;
        }
    }
    for (uint64_t pid : pending_fills_) {
        const XtSimOrder* o = find_live(pid);
        if (o && o->fill_eligible_event_ns != 0 && o->fill_eligible_event_ns < min_ns) {
            min_ns = o->fill_eligible_event_ns;
        }
    }
    for (uint64_t pid : pending_cancels_) {
        const XtSimOrder* o = find_live(pid);
        if (o && !order_status_is_final(o->status) && o->cancel_due_event_ns != 0
            && o->cancel_due_event_ns < min_ns) {
            min_ns = o->cancel_due_event_ns;
        }
    }
    if (ttl_us_ > 0 && !orders_.empty()) {
        const int64_t ttl_ns = ttl_us_to_event_ns(ttl_us_);
        for (const auto& o : orders_) {
            const int64_t due = o.create_event_ns + ttl_ns;
            if (due < min_ns) min_ns = due;
        }
    }
    earliest_due_event_ns_ = min_ns;
}

// ── Order Entry ─────────────────────────────────────────────────────────

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
    // price_fp <= 0 在撮合层非法：市价单已在 XtStrategyContext 解析为激进限价 IOC 后才下达
    // （marketable-limit-IOC 设计），撮合层只接收 price>0 的限价单。
    if (price_fp <= 0) {
        ++engine_drops_.nonpos_price;
        return 0;
    }
    // reserve 只是减少 reallocation，不是容量契约；submit 入口执行真正的在途订单上限，
    // 避免盘中越过预留容量扩容，并把超限拒单显式计入 drop counter。
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
    ord.create_event_ns       = event_base_();
    ord.queue_ahead           = 0.0;
    ord.order_date            = order_date;
    ord.order_time            = order_time_ms;
    ord.user_tag              = user_tag;
    ord.close_offset          = close_offset;
    ord.is_resubmit           = is_resubmit;
    ord.fill_eligible_event_ns = fill_latency_ms_sampled > 0
                                    ? xt_latency::checked_add_event_ns(
                                          event_base_(),
                                          static_cast<int64_t>(fill_latency_ms_sampled) * 1'000'000LL)
                                    : 0;
    ord.response_latency_ns   = response_latency_ns_sampled;

    // 有盘口快照时判定 maker/taker；只有 passive limit order 需要初始化 queue_ahead。
    // 快照按 code 隔离：读本单 symbol 的 slot；miss = 该 symbol 尚无盘口
    //（维持旧 last_depth==0 语义：is_passive 保持默认，queue_ahead=0）。
    const DepthSnapshot* snap = find_depth_(code);
    if (snap && snap->last_depth > 0) {
        const bool aggressive = is_aggressive(
            is_buy, price_fp, snap->last_bid_px_fp[0], snap->last_ask_px_fp[0]);

        ord.is_passive = !aggressive;

        if (!aggressive) {
            if (queue_model_) {
                const int64_t* px_arr_fp = is_buy ? snap->last_bid_px_fp : snap->last_ask_px_fp;
                const double*  qty_arr   = is_buy ? snap->last_bid_qty   : snap->last_ask_qty;
                const double level_qty = level_qty_at_price(
                    price_fp, px_arr_fp, qty_arr, snap->last_depth);
                ord.queue_ahead = queue_model_->estimate_queue_ahead(level_qty, is_resubmit);
            }
        }
    }

    orders_.push_back(ord);
    id_to_slot_[ord.id] = orders_.size() - 1;
    push_update(orders_.back(), 0);

    // TTL 是全局运行参数，不进入 pending 队列；新增 live order 后把其 TTL
    // deadline 纳入精确下界（tick 维度 TTL 属 tick 域，不影响时间 fast path）。
    // deadline = create_event_ns + ttl_ns 走 checked 加法：create_event_ns 若被脏
    // 未来日期 tick 毒化成近 int64 max，裸加溢出成负 deadline（TTL 恒到期/永不到期）；
    // checked 在此 order 创建点即 fail-fast 穿透（clean data 逐位等价）。此加法先于任何
    // recompute_earliest_due_ 的同式计算，故 recompute 侧无需重复 checked。
    if (ttl_us_ > 0) {
        const int64_t ttl_ns = ttl_us_to_event_ns(ttl_us_);
        note_event_deadline_(
            xt_latency::checked_add_event_ns(orders_.back().create_event_ns, ttl_ns));
    }

    XtSimOrder* ref = find_live(ord.id);

    // ACK 到期来源按顺序选择：逐单 ms -> RuntimeConfig::ack_delay_us -> tick 数；
    // 前两者写 ack_due_event_ns（绝对 event ns），tick 分支写 ack_due_tick，
    // 三者都为 0 时立即 Accepted。
    if (ack_delay_ms_sampled > 0) {
        ref->ack_due_event_ns = xt_latency::checked_add_event_ns(
            event_base_(), static_cast<int64_t>(ack_delay_ms_sampled) * 1'000'000LL);
        pending_acks_.push_back(ord.id);
        note_event_deadline_(ref->ack_due_event_ns);

    } else if (ack_delay_us_ > 0) {
        const int64_t delay_ms = static_cast<int64_t>((ack_delay_us_ + 999) / 1000);
        ref->ack_due_event_ns = xt_latency::checked_add_event_ns(
            event_base_(), delay_ms * 1'000'000LL);
        pending_acks_.push_back(ord.id);
        note_event_deadline_(ref->ack_due_event_ns);

    } else if (ack_delay_ticks_ > 0) {
        ref->ack_due_tick = cur_tick_ + ack_delay_ticks_;
        pending_acks_.push_back(ord.id);

    } else {
        // 无 ACK 延迟时立即确认；fill eligibility 仍可延后进入订单簿。
        ref->status = XtOrderStatus::Accepted;
        push_update(*ref, 0);
        if (ref->fill_eligible_event_ns != 0 && event_base_() < ref->fill_eligible_event_ns) {
            pending_fills_.push_back(ref->id);
            note_event_deadline_(ref->fill_eligible_event_ns);
        } else if (!order_book_.add_order(*ref)) {
            // book 容量拒收：合成 Canceled 终态（同 advance_clock 入簿点口径）。
            ref->status = XtOrderStatus::Canceled;
            archive_order(ref->id, /*remove_from_book=*/false, /*emit_update=*/true, 0);
        }
    }

    // ACK 时间 arm 与 fill arm 已在各自分支 note_event_deadline_；tick 域 arm
    //（ack_due_tick）不进时间下界——它只能由 market tick 推进。

    return ord.id;
}

bool XtOrderManager::cancel(uint64_t id, uint32_t cancel_delay_ms_sampled) {
    if (cancel_delay_ms_sampled == 0) return cancel_now_(id);

    XtSimOrder* ord = find_live(id);
    if (!ord || order_status_is_final(ord->status)) return false;
    // 已有在途撤单请求：幂等，保留更早的到期时刻。
    if (ord->cancel_due_event_ns != 0) return true;
    ord->cancel_due_event_ns = xt_latency::checked_add_event_ns(
        event_base_(), static_cast<int64_t>(cancel_delay_ms_sampled) * 1'000'000LL);
    pending_cancels_.push_back(id);
    note_event_deadline_(ord->cancel_due_event_ns);
    return true;
}

bool XtOrderManager::cancel_now_(uint64_t id) {
    XtSimOrder* ord = find_live(id);
    if (!ord || order_status_is_final(ord->status)) return false;

    const bool was_submitted = (ord->status == XtOrderStatus::Submitted);
    ord->status = XtOrderStatus::Canceled;

    if (was_submitted) {
        // 尚未 ACK 的订单从未进入订单簿。
        remove_from_pending(pending_acks_, id);
        archive_order(id, /*remove_from_book=*/false, /*emit_update=*/true, 0);
    } else {
        // Accepted 后可能等待 fill eligibility，也可能已经进入订单簿。
        auto fit = std::find(pending_fills_.begin(), pending_fills_.end(), id);
        if (fit != pending_fills_.end()) {
            pending_fills_.erase(fit);
            archive_order(id, /*remove_from_book=*/false, /*emit_update=*/true, 0);
        } else {
            archive_order(id, /*remove_from_book=*/true,  /*emit_update=*/true, 0);
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
        archive_order(id, /*remove_from_book=*/false, /*emit_update=*/true, 0);
    } else {
        auto fit = std::find(pending_fills_.begin(), pending_fills_.end(), id);
        if (fit != pending_fills_.end()) {
            pending_fills_.erase(fit);
            archive_order(id, /*remove_from_book=*/false, /*emit_update=*/true, 0);
        } else {
            archive_order(id, /*remove_from_book=*/true,  /*emit_update=*/true, 0);
        }
    }
    // 当前回测层不消费 reason_code；签名保留用于与上层 reject 路径一致。
    (void)reason_code;
    return true;
}

// ── Market Snapshot ─────────────────────────────────────────

const XtOrderManager::DepthSnapshot* XtOrderManager::find_depth_(
        const char* code) const noexcept {
    // warm path 线性扫描（同 volume_table_ 惯例）；典型 symbol 数 1-8。
    for (const auto& s : depth_table_) {
        if (std::strncmp(s.code, code, sizeof(s.code)) == 0) return &s.snap;
    }
    return nullptr;
}

XtOrderManager::DepthSnapshot& XtOrderManager::find_or_insert_depth_(const char* code) {
    for (auto& s : depth_table_) {
        if (std::strncmp(s.code, code, sizeof(s.code)) == 0) return s.snap;
    }
    // 新 symbol 首见 append（冷路径分配）；槽一经建立身份不变。
    depth_table_.emplace_back();
    std::strncpy(depth_table_.back().code, code, sizeof(depth_table_.back().code) - 1);
    return depth_table_.back().snap;
}

void XtOrderManager::set_depth_snapshot_fp(const char* code,
                                           const int64_t* bid_px_fp,
                                           const int64_t* ask_px_fp,
                                           const int64_t* bid_qty_fp,
                                           const int64_t* ask_qty_fp,
                                           int depth) {
    if (XtCStringUtil::is_empty(code)) return;
    DepthSnapshot& s = find_or_insert_depth_(code);

    // 先把该 symbol 上一轮 last 快照滚到 prev，供 queue refresh、cancel erosion
    // 和 OFI 计算同 symbol 的 delta；跨 symbol tick 不互相滚动。
    for (int i = 0; i < XtRawTick::DEPTH; ++i) {
        s.prev_bid_px_fp[i] = s.last_bid_px_fp[i];
        s.prev_ask_px_fp[i] = s.last_ask_px_fp[i];
        s.prev_bid_qty[i]   = s.last_bid_qty[i];
        s.prev_ask_qty[i]   = s.last_ask_qty[i];
    }

    s.last_depth = std::clamp(depth, 0, XtRawTick::DEPTH);

    // 写入新 last 快照；depth 外档位必须清零，避免上一 tick 的深档残留进入 queue model。
    for (int i = 0; i < XtRawTick::DEPTH; ++i) {
        if (i >= s.last_depth) {
            s.last_bid_px_fp[i] = 0;
            s.last_ask_px_fp[i] = 0;
            s.last_bid_qty[i]   = 0.0;
            s.last_ask_qty[i]   = 0.0;
            if (market_impact_enabled_) {
                s.consumed_bid_carry[i] = 0.0;
                s.consumed_ask_carry[i] = 0.0;
            }
            continue;
        }

        s.last_bid_px_fp[i] = bid_px_fp ? bid_px_fp[i] : 0;
        s.last_ask_px_fp[i] = ask_px_fp ? ask_px_fp[i] : 0;

        // queue/erosion 模型在 double 数量域运算；价格保持 fixed-point。
        double adj_bid = bid_qty_fp ? order_exec_from_fp(bid_qty_fp[i]) : 0.0;
        double adj_ask = ask_qty_fp ? order_exec_from_fp(ask_qty_fp[i]) : 0.0;

        if (market_impact_enabled_) {
            // carry 只扣减价格未变的档位；换价表示队列重建，旧 carry 不跨价位迁移。
            // decay 按该 symbol 自身 tick 节奏推进（carry 描述本合约盘口的残留冲击）。
            const bool bid_px_unchanged = bid_px_fp
                && s.prev_bid_px_fp[i] > 0
                && s.last_bid_px_fp[i] == s.prev_bid_px_fp[i];
            const bool ask_px_unchanged = ask_px_fp
                && s.prev_ask_px_fp[i] > 0
                && s.last_ask_px_fp[i] == s.prev_ask_px_fp[i];

            if (s.consumed_bid_carry[i] > 0.0 && bid_px_unchanged)
                adj_bid = std::max(0.0, adj_bid - s.consumed_bid_carry[i]);

            if (s.consumed_ask_carry[i] > 0.0 && ask_px_unchanged)
                adj_ask = std::max(0.0, adj_ask - s.consumed_ask_carry[i]);

            s.consumed_bid_carry[i] *= market_impact_decay_;
            s.consumed_ask_carry[i] *= market_impact_decay_;
        }

        s.last_bid_qty[i] = adj_bid;
        s.last_ask_qty[i] = adj_ask;
    }
}

// ── Matching ────────────────────────────────────────────────────────────

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

    // 撮合前先用本 tick 盘口刷新排队状态。XtOrderBook 只负责价格/数量撮合，
    // queue_ahead 和 OFI erosion 由 manager 在订单状态层维护。
    // 快照按 code 隔离；prod 调用序（引擎每 tick 先 set_depth_snapshot_fp(code)）
    // 保证此处命中，miss（裸调 match_tick 的测试路径）按 last_depth==0 短路。
    DepthSnapshot& snap = find_or_insert_depth_(code);
    refresh_queue_ahead(code, snap, depth);
    apply_queue_cancel_erosion(code, snap, depth);

    const auto& res = order_book_.match_tick(
        code, bid_px_fp, ask_px_fp, bid_qty_fp, ask_qty_fp, depth,
        max_fill_qty, 0.0,
        [](void* ctx, uint64_t id) -> XtSimOrder& {
            return *static_cast<XtOrderManager*>(ctx)->find_live(id);
        },
        this, upper_limit_fp_, lower_limit_fp_);

    // filled_buf 只记录本 tick 成交量，供 IOC 剩余撤单使用；成交记录本身直接
    // append 到 trades，不依赖该 scratch。
    FillRecord filled_buf[kMaxFillsPerTick];
    int        filled_count = 0;

    apply_fill_updates(res.fills, trades, filled_buf, filled_count);

    // 盘口冲击归档需在 archive_order 前执行，此时 orders_ 中订单信息仍有效。
    accumulate_market_impact(res.fills, snap);

    apply_canceled_ids(res.canceled);
    apply_done_ids(res.done);

    cancel_ioc_remainders(filled_buf, filled_count);

    // 冻结侧零成交 IOC 无法经 book canceled 集合撤（match_one_side 被整侧跳过），
    // 也不在 cancel_ioc_remainders 的有成交清单里 —— tick 末显式撤，保证 IOC
    // 在 TTL=0 profile 下仍有终态（limit-lock 语义，判据与 XtOrderBook 一致）。
    const bool buy_frozen = upper_limit_fp_ > 0 && snap.last_bid_px_fp[0] >= upper_limit_fp_;
    const bool sell_frozen = lower_limit_fp_ > 0 && snap.last_ask_px_fp[0] > 0
                             && snap.last_ask_px_fp[0] <= lower_limit_fp_;
    if (buy_frozen || sell_frozen) {
        clock_scratch_.clear();
        for (const auto& ord : orders_) {
            if (ord.tif != XtTimeInForce::IOC) continue;
            if (order_status_is_final(ord.status)) continue;
            // 冻结旗标属于当前 tick 的 symbol；orders_ 跨 code 扁平，不过滤会误撤
            // 其它 symbol 等待自己 tick 的 IOC。ack 在途（Submitted）未达 venue，
            // 不参与冻结清扫（默认 ack_delay=0 时提交即 Accepted，不受影响）。
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
            archive_order(id, /*remove_from_book=*/true, /*emit_update=*/true, 0);
        }
    }
}

void XtOrderManager::refresh_queue_ahead(const char* code,
                                         const DepthSnapshot& snap, int depth) {
    if (snap.last_depth <= 0) return;
    const int d = std::min(depth, snap.last_depth);

    // 同价位所在档位发生重建时，重新估算 queue_ahead。
    for (auto& ord : orders_) {
        if (ord.status != XtOrderStatus::Accepted &&
            ord.status != XtOrderStatus::PartiallyFilled)
            continue;
        if (ord.queue_ahead <= 0.0) continue;
        // 快照/限价都属于当前 tick 的 symbol；orders_ 跨 code 扁平，必须过滤。
        if (ord.code != code) continue;
        // 板价单不参与重估与 OFI erosion：板上排队几乎不前进（limit-lock）。
        if (is_board_price_(ord.price_fp)) continue;

        const double*  cur_qty    = ord.is_buy ? snap.last_bid_qty    : snap.last_ask_qty;
        const int64_t* cur_px_fp  = ord.is_buy ? snap.last_bid_px_fp  : snap.last_ask_px_fp;
        const int64_t* prev_px_fp = ord.is_buy ? snap.prev_bid_px_fp  : snap.prev_ask_px_fp;

        for (int i = 0; i < d; ++i) {
            if (cur_px_fp[i] <= 0) break;
            if (cur_px_fp[i] != ord.price_fp) continue;

            if (prev_px_fp[i] != cur_px_fp[i]) {
                // refresh 不是撤单重入队，按普通入队参数估算。
                if (queue_model_) {
                    ord.queue_ahead = queue_model_->estimate_queue_ahead(cur_qty[i], false);
                }
            }
            break;
        }
    }

    // OFI 只作用于最优档：最优档缩量代表净消耗流，加速该档被动单 queue_ahead 消耗。
    if (ofi_queue_scale_ <= 0.0) return;

    const double ask0_drain = stable_level_drain(
        snap.prev_ask_qty[0], snap.last_ask_qty[0],
        snap.prev_ask_px_fp[0], snap.last_ask_px_fp[0]);
    const double bid0_drain = stable_level_drain(
        snap.prev_bid_qty[0], snap.last_bid_qty[0],
        snap.prev_bid_px_fp[0], snap.last_bid_px_fp[0]);

    for (auto& ord : orders_) {
        if (ord.status != XtOrderStatus::Accepted &&
            ord.status != XtOrderStatus::PartiallyFilled)
            continue;
        if (ord.queue_ahead <= 0.0) continue;
        if (ord.code != code) continue;               // OFI drain 只属本 symbol
        if (is_board_price_(ord.price_fp)) continue;  // 板价单 OFI 免疫（limit-lock）

        if (!ord.is_buy && ask0_drain > 0.0
            && snap.last_ask_px_fp[0] == ord.price_fp) {
            ord.queue_ahead = std::max(0.0, ord.queue_ahead - ask0_drain * ofi_queue_scale_);
        }
        if (ord.is_buy && bid0_drain > 0.0
            && snap.last_bid_px_fp[0] == ord.price_fp) {
            ord.queue_ahead = std::max(0.0, ord.queue_ahead - bid0_drain * ofi_queue_scale_);
        }
    }
}

void XtOrderManager::apply_queue_cancel_erosion(const char* code,
                                                const DepthSnapshot& snap, int depth) {
    if (queue_cancel_ratio_ <= 0.0 || snap.last_depth <= 0) return;
    const int d = std::min(depth, snap.last_depth);

    // 价格不变但数量减少时，按比例削减我方 queue_ahead；只作用于本 symbol 挂单。
    for (auto& ord : orders_) {
        if (ord.status != XtOrderStatus::Accepted &&
            ord.status != XtOrderStatus::PartiallyFilled)
            continue;
        if (ord.queue_ahead <= 0.0) continue;
        if (ord.code != code) continue;
        if (is_board_price_(ord.price_fp)) continue;  // 板价单 cancel erosion 免疫（limit-lock）

        const double*  cur_qty    = ord.is_buy ? snap.last_bid_qty   : snap.last_ask_qty;
        const double*  prev_qty   = ord.is_buy ? snap.prev_bid_qty   : snap.prev_ask_qty;
        const int64_t* cur_px_fp  = ord.is_buy ? snap.last_bid_px_fp : snap.last_ask_px_fp;
        const int64_t* prev_px_fp = ord.is_buy ? snap.prev_bid_px_fp : snap.prev_ask_px_fp;

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

        // 按订单累计本 tick 成交量，供 IOC 剩余撤单判断；多笔同单成交合并到一槽。
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
        archive_order(id, /*remove_from_book=*/true, /*emit_update=*/true, 0);
    }
}

// 全量成交的最后一笔状态已由 apply_fill_updates 发布，归档时不重复 emit。
void XtOrderManager::apply_done_ids(const std::vector<uint64_t>& ids) {
    for (uint64_t id : ids) {
        XtSimOrder* ord = find_live(id);
        if (!ord || ord->remaining_fp() > 0) continue;
        ord->status = XtOrderStatus::Filled;
        archive_order(id, /*remove_from_book=*/true, /*emit_update=*/false, 0);
    }
}

void XtOrderManager::cancel_ioc_remainders(const FillRecord* filled_buf, int filled_count) {
    // 本 tick 已有成交的 IOC 单在 tick 末撤销剩余量；完全未成交的 IOC 由
    // XtOrderBook canceled 集合处理，避免每 tick 全量遍历 orders_。
    for (int i = 0; i < filled_count; ++i) {
        XtSimOrder* ord = find_live(filled_buf[i].id);
        if (!ord) continue;
        if (ord->tif == XtTimeInForce::IOC && ord->remaining_fp() > 0) {
            ord->status = XtOrderStatus::Canceled;
            archive_order(filled_buf[i].id, /*remove_from_book=*/true,
                          /*emit_update=*/true, 0);
        }
    }
}

void XtOrderManager::accumulate_market_impact(const std::vector<XtOrderBook::Fill>& fills,
                                              DepthSnapshot& snap) {
    // carry 在该 symbol 下一 tick 的同价位深度上扣减，用于模拟我方成交对可见深度的
    // 残留影响；fills 全部来自当前 code 的 book（books_ per-symbol），写入同一 slot。
    if (!market_impact_enabled_ || fills.empty()) return;

    for (const auto& fill : fills) {
        // find_live miss 时不能把方向默认成 sell；done_ 兜底保证 carry 方向可恢复。
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
            for (int i = 0; i < snap.last_depth; ++i) {
                if (snap.last_ask_px_fp[i] <= 0) break;
                if (snap.last_ask_px_fp[i] == fill_price_fp) {
                    snap.consumed_ask_carry[i] += order_exec_from_fp(fill.qty_fp);
                    break;
                }
            }
        } else {
            for (int i = 0; i < snap.last_depth; ++i) {
                if (snap.last_bid_px_fp[i] <= 0) break;
                if (snap.last_bid_px_fp[i] == fill_price_fp) {
                    snap.consumed_bid_carry[i] += order_exec_from_fp(fill.qty_fp);
                    break;
                }
            }
        }
    }
}

// ── Archive, Queries And Updates ────────────────────────────────────────

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
    // FIFO 淘汰头部：market impact 回扫只触尾部近期成交，头部淘汰不影响。
    if (done_.size() > kDoneOrderCacheCap) done_.pop_front();

    // 先释放当前 iterator，避免后续 slot 映射更新触发 rehash 后再使用失效 iterator。
    id_to_slot_.erase(it);

    // swap-with-last 不保留 live order 顺序；调用方不能跨归档持有 orders_ 引用。
    const size_t last = orders_.size() - 1;
    if (idx != last) {
        orders_[idx] = orders_.back();
        id_to_slot_[orders_[idx].id] = idx;
    }
    orders_.pop_back();
}

const XtSimOrder* XtOrderManager::find(uint64_t id) const {
    if (const XtSimOrder* p = find_live(id)) return p;
    // done_ 查询只用于诊断/测试和归档订单查找，不在撮合热路径；
    // 超过 kDoneOrderCacheCap 被 FIFO 淘汰的订单返回 nullptr。
    for (const auto& ord : done_) {
        if (ord.id == id) return &ord;
    }
    return nullptr;
}

const std::vector<XtSimOrder>& XtOrderManager::live_orders() const {
    return orders_;
}

void XtOrderManager::emit_in_transit_canceled(uint64_t order_id, const char* code,
                                              bool is_buy, int64_t price_fp,
                                              int64_t qty_fp, XtTimeInForce tif,
                                              std::string_view user_tag,
                                              XtOffset close_offset) {
    // 该单从未进入 orders_，只需合成一条终态回报（filled=0, leaves=qty）。
    XtSimOrder ord;
    ord.id           = order_id;
    ord.code         = code ? code : "";
    ord.is_buy       = is_buy;
    ord.price_fp     = price_fp;
    ord.qty_fp       = qty_fp;
    ord.filled_fp    = 0;
    ord.status       = XtOrderStatus::Canceled;
    ord.tif          = tif;
    ord.user_tag     = user_tag;
    ord.close_offset = close_offset;
    push_update(ord, 0);
}

void XtOrderManager::discard_eof_live_orders() {
    // EOF 收尾纯清理：撮合永不再推进，把仍 alive 的在途单从 orders_/book 归档，
    // 不 emit 终态回报（delivered 终态由 gateway 按 delivered 口径另行合成派发，
    // 此处只收 sim 撮合簿，使 EOF 后 live_orders()==0）。
    if (orders_.empty()) return;
    // archive_order swap-with-last，先收集 id 再批量归档（同 TTL 到期循环模式）。
    std::vector<uint64_t> ids;
    ids.reserve(orders_.size());
    for (const auto& ord : orders_) ids.push_back(ord.id);
    for (uint64_t id : ids) {
        if (!find_live(id)) continue;
        archive_order(id, /*remove_from_book=*/true, /*emit_update=*/false, 0);
    }
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
    // 回测合成 callback epoch ns：当前交易日 day-start + ms-since-midnight；
    // 调用方按契约先 advance_clock() 时，cur_time_ 已是线性毫秒。
    u.local_cb_ns  = xt_yyyymmdd_to_day_start_ns(cur_date_)
                   + xt_ms_to_ns(static_cast<uint64_t>(cur_time_));
    updates_.push_back(u);
}

namespace engine_config {

// YAML 可选字段读取：字段缺失时保留 RuntimeConfig 默认值，存在时覆盖。
template <typename T>
static inline void try_read(const YAML::Node& node, const char* key, T& out) {
    if (node && node[key]) out = node[key].as<T>();
}

XtOrderManager::RuntimeConfig parse_order_manager_runtime_config(const YAML::Node& node) {
    XtOrderManager::RuntimeConfig cfg;
    try_read(node, "queue_cancel_ratio",          cfg.queue_cancel_ratio);
    try_read(node, "ack_delay_ticks",             cfg.ack_delay_ticks);
    // `order_timeout_ticks` 是现有 yaml 字段名，内部映射为 TTL tick。
    try_read(node, "order_timeout_ticks",         cfg.ttl_ticks);
    try_read(node, "ack_delay_us",                cfg.ack_delay_us);
    try_read(node, "ttl_us",                      cfg.ttl_us);
    try_read(node, "ofi_queue_scale",             cfg.ofi_queue_scale);
    try_read(node, "market_impact_enabled",       cfg.market_impact_enabled);
    try_read(node, "market_impact_decay",         cfg.market_impact_decay);
    return cfg;
}

} // namespace engine_config
