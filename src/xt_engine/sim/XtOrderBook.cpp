#include "XtOrderBook.h"
#include "xt_core/market/XtRawTick.h"

#include <atomic>

namespace {
std::atomic<uint64_t> g_orders_per_level_drops_{0};
std::atomic<uint64_t> g_price_levels_full_drops_{0};
}

namespace xt_engine_sim_book_drops {
uint64_t orders_per_level_drops() noexcept {
    return g_orders_per_level_drops_.load(std::memory_order_relaxed);
}

uint64_t price_levels_full_drops() noexcept {
    return g_price_levels_full_drops_.load(std::memory_order_relaxed);
}
}

void XtOrderBook::add_order(const XtSimOrder& ord) {
    if (ord.code.empty() || ord.price_fp <= 0 || ord.qty_fp <= 0) return;

    Book& book = books_[ord.code];
    const int64_t fp = ord.price_fp;

    if (ord.is_buy) {
        PriceLevel* lvl = book.bids.get_or_insert(fp);
        if (!lvl) {
            g_price_levels_full_drops_.fetch_add(1, std::memory_order_relaxed);
        } else if (!lvl->push(ord.id)) {
            g_orders_per_level_drops_.fetch_add(1, std::memory_order_relaxed);
        }
    } else {
        PriceLevel* lvl = book.asks.get_or_insert(fp);
        if (!lvl) {
            g_price_levels_full_drops_.fetch_add(1, std::memory_order_relaxed);
        } else if (!lvl->push(ord.id)) {
            g_orders_per_level_drops_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void XtOrderBook::remove_order(const XtSimOrder& ord) {
    if (ord.code.empty()) return;
    auto it = books_.find(ord.code);
    if (it == books_.end()) return;

    Book& book = it->second;
    const int64_t fp = ord.price_fp;

    if (ord.is_buy) {
        PriceLevel* lvl = book.bids.find(fp);
        if (lvl) {
            lvl->remove(ord.id);

            if (lvl->empty()) {
                int idx = static_cast<int>(lvl - book.bids.levels.data());
                book.bids.erase_at(idx);
            }
        }
    } else {
        PriceLevel* lvl = book.asks.find(fp);
        if (lvl) {
            lvl->remove(ord.id);
            if (lvl->empty()) {
                int idx = static_cast<int>(lvl - book.asks.levels.data());
                book.asks.erase_at(idx);
            }
        }
    }

    if (book.bids.empty() && book.asks.empty()) books_.erase(it);
}

namespace {

template <bool IsBuy>
inline bool price_fits(int64_t opp_px_fp, int64_t limit_fp) noexcept {
    if constexpr (IsBuy) {
        return opp_px_fp <= limit_fp;
    } else {
        return opp_px_fp >= limit_fp;
    }
}

template <bool IsBuy>
bool fok_can_fully_fill(const XtSimOrder& ord, const int64_t* opp_px, const double* opp_left,
                        int depth, double liq_factor, int64_t limit_fp, double remaining_capacity) {
    int64_t total_fp = 0;
    for (int i = 0; i < depth; ++i) {
        const int64_t px_fp = opp_px ? opp_px[i] : 0;
        if (px_fp <= 0) break;
        if (!price_fits<IsBuy>(px_fp, limit_fp)) break;
        total_fp += order_exec_to_fp(opp_left[i] * liq_factor);
    }

    if (ord.queue_ahead > 0.0) {
        const int64_t qa_fp = order_exec_to_fp(ord.queue_ahead);
        total_fp = (qa_fp < total_fp) ? (total_fp - qa_fp) : 0;
    }

    if (remaining_capacity > 0.0) {
        const int64_t cap_fp = order_exec_to_fp(remaining_capacity);
        if (cap_fp < total_fp) total_fp = cap_fp;
    }
    return total_fp >= ord.remaining_fp();
}

template <bool IsBuy>
int64_t fill_order_against_depth(XtSimOrder& ord, uint64_t oid, const int64_t* opp_px,
                                 double* opp_left, int depth, double liq_factor, int64_t limit_fp,
                                 double& remaining_capacity, XtOrderBook::MatchResult& result) {
    int64_t filled_fp = 0;
    for (int i = 0; i < depth; ++i) {
        const int64_t px_fp = opp_px ? opp_px[i] : 0;
        if (px_fp <= 0) break;
        if (!price_fits<IsBuy>(px_fp, limit_fp)) break;
        double avail = opp_left[i] * liq_factor;
        if (avail <= 0.0) continue;

        if (ord.queue_ahead > 0.0) {
            const double consume = (ord.queue_ahead > avail) ? avail : ord.queue_ahead;
            ord.queue_ahead -= consume;
            avail -= consume;
            if (avail <= 0.0) continue;
        }

        const double need_d = order_exec_from_fp(ord.remaining_fp() - filled_fp);
        if (need_d <= 0.0) break;
        double fill_qty_d = (need_d > avail) ? avail : need_d;
        if (remaining_capacity > 0.0 && fill_qty_d > remaining_capacity)
            fill_qty_d = remaining_capacity;
        const int64_t fill_fp = order_exec_to_fp(fill_qty_d);
        if (fill_fp <= 0) break;

        opp_left[i] -= fill_qty_d / (liq_factor > 0.0 ? liq_factor : 1.0);
        filled_fp += fill_fp;
        result.fills.push_back({oid, fill_fp, px_fp});
        if (remaining_capacity > 0.0) remaining_capacity -= fill_qty_d;
        if (filled_fp >= ord.remaining_fp()) break;
        if (remaining_capacity == 0.0) break;
    }
    return filled_fp;
}

}

template <bool IsBuy>
void XtOrderBook::match_one_side(
    typename std::conditional<IsBuy, BidLevels, AskLevels>::type& side_levels,
    const int64_t* bid_px_fp,
    const int64_t* ask_px_fp,
    double* bid_left,
    double* ask_left,
    int depth,
    double liq_factor,
    double& remaining_capacity,
    MatchResult& result,
    XtSimOrder& (*lookup_fn)(void*, uint64_t),
    void* lookup_ctx)
{

    const int64_t* opp_px = IsBuy ? ask_px_fp : bid_px_fp;
    double* opp_left = IsBuy ? ask_left : bid_left;

    for (int li = 0; li < side_levels.size; ) {
        PriceLevel& lvl = side_levels.levels[li];
        const int64_t limit_fp = lvl.price_fp;
        bool level_blocked = false;

        while (!lvl.empty()) {
            const uint64_t oid = lvl.front();
            XtSimOrder& ord = lookup_fn(lookup_ctx, oid);

            if (ord.status != XtOrderStatus::Accepted &&
                ord.status != XtOrderStatus::PartiallyFilled) {
                lvl.pop_front();
                continue;
            }
            if (ord.remaining_fp() <= 0) {
                result.done.push_back(oid);
                lvl.pop_front();
                continue;
            }

            if (opp_px && opp_px[0] > 0 &&
                !price_fits<IsBuy>(opp_px[0], limit_fp)) {
                level_blocked = true;
                break;
            }

            if (remaining_capacity == 0.0) {
                if (ord.tif == XtTimeInForce::IOC || ord.tif == XtTimeInForce::FOK) {
                    result.canceled.push_back(oid);
                    lvl.pop_front();
                    continue;
                }
                level_blocked = true;
                break;
            }

            if (ord.tif == XtTimeInForce::FOK &&
                !fok_can_fully_fill<IsBuy>(ord, opp_px, opp_left, depth, liq_factor, limit_fp,
                                           remaining_capacity)) {
                result.canceled.push_back(oid);
                lvl.pop_front();
                continue;
            }

            const int64_t filled_fp =
                fill_order_against_depth<IsBuy>(ord, oid, opp_px, opp_left, depth,
                                                liq_factor, limit_fp, remaining_capacity, result);

            if (filled_fp <= 0) {
                if (ord.tif == XtTimeInForce::IOC || ord.tif == XtTimeInForce::FOK) {
                    result.canceled.push_back(oid);
                    lvl.pop_front();
                    continue;
                }
                level_blocked = true;
                break;
            }

            if (filled_fp >= ord.remaining_fp()) {
                result.done.push_back(oid);
                lvl.pop_front();
            } else {
                if (ord.tif == XtTimeInForce::IOC) {
                    result.canceled.push_back(oid);
                    lvl.pop_front();
                } else {
                    break;
                }
            }
        }

        if (lvl.empty()) {
            side_levels.erase_at(li);
        } else if (level_blocked) {
            break;
        } else {
            ++li;
        }
    }
}

const XtOrderBook::MatchResult& XtOrderBook::match_tick(
    const char* code,
    const int64_t* bid_px_fp,
    const int64_t* ask_px_fp,
    const int64_t* bid_qty_fp,
    const int64_t* ask_qty_fp,
    int depth,
    double max_fill_qty,
    double liquidity_ratio,
    XtSimOrder& (*lookup_fn)(void*, uint64_t),
    void* lookup_ctx,
    int64_t upper_limit_fp,
    int64_t lower_limit_fp)
{

    match_result_.fills.clear();
    match_result_.done.clear();
    match_result_.canceled.clear();

    if (!code || code[0] == '\0' || depth <= 0) return match_result_;

    auto it = books_.find(code);
    if (it == books_.end()) return match_result_;

    depth = depth > XtRawTick::DEPTH ? XtRawTick::DEPTH : depth;

    double bid_left[XtRawTick::DEPTH] = {};
    double ask_left[XtRawTick::DEPTH] = {};
    for (int i = 0; i < depth; ++i) {
        bid_left[i] = bid_qty_fp ? order_exec_from_fp(bid_qty_fp[i]) : 0.0;
        ask_left[i] = ask_qty_fp ? order_exec_from_fp(ask_qty_fp[i]) : 0.0;
    }

    Book& book = it->second;
    if (liquidity_ratio < 0.0) liquidity_ratio = 0.0;
    if (liquidity_ratio > 1.0) liquidity_ratio = 1.0;

    const double liq_factor = 1.0 - liquidity_ratio;

    double remaining_capacity = max_fill_qty > 0.0 ? max_fill_qty : -1.0;

    const bool buy_frozen = upper_limit_fp > 0 && bid_px_fp
                            && bid_px_fp[0] >= upper_limit_fp;
    const bool sell_frozen = lower_limit_fp > 0 && ask_px_fp
                             && ask_px_fp[0] > 0 && ask_px_fp[0] <= lower_limit_fp;

    if (!buy_frozen) {
        match_one_side<true>(book.bids,
                             bid_px_fp, ask_px_fp,
                             bid_left, ask_left,
                             depth, liq_factor,
                             remaining_capacity,
                             match_result_,
                             lookup_fn, lookup_ctx);
    }

    if (!sell_frozen) {
        match_one_side<false>(book.asks,
                              bid_px_fp, ask_px_fp,
                              bid_left, ask_left,
                              depth, liq_factor,
                              remaining_capacity,
                              match_result_,
                              lookup_fn, lookup_ctx);
    }

    return match_result_;
}
