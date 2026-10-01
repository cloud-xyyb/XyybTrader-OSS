#include "XtOrderBook.h"
#include "xt_core/market/XtRawTick.h"

#include <atomic>

// 进程级 drop counter：统计 PriceLevel slot 满和 price level 满两类容量拒单。
// 仅用于回测健康指标，不建立跨线程同步关系，因此使用 relaxed memory order。
namespace {
std::atomic<uint64_t> g_orders_per_level_drops_{0};
std::atomic<uint64_t> g_price_levels_full_drops_{0};
}  // namespace

namespace xt_engine_sim_book_drops {
uint64_t orders_per_level_drops() noexcept {
    return g_orders_per_level_drops_.load(std::memory_order_relaxed);
}

uint64_t price_levels_full_drops() noexcept {
    return g_price_levels_full_drops_.load(std::memory_order_relaxed);
}
}  // namespace xt_engine_sim_book_drops

// ── Book Mutation ───────────────────────────────────────────────────────

bool XtOrderBook::add_order(const XtSimOrder& ord) {
    if (ord.code.empty() || ord.price_fp <= 0 || ord.qty_fp <= 0) return false;

    Book& book = books_[ord.code];
    const int64_t fp = ord.price_fp;

    if (ord.is_buy) {
        PriceLevel* lvl = book.bids.get_or_insert(fp);
        if (!lvl) {
            g_price_levels_full_drops_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        if (!lvl->push(ord.id)) {
            g_orders_per_level_drops_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
    } else {
        PriceLevel* lvl = book.asks.get_or_insert(fp);
        if (!lvl) {
            g_price_levels_full_drops_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        if (!lvl->push(ord.id)) {
            g_orders_per_level_drops_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
    }
    return true;
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
            // 空层级立即删除，保持数组紧凑和排序不变量。
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

// ── Matching ────────────────────────────────────────────────────────────

namespace {

// 单侧撮合 helper 不访问 XtOrderBook 成员；模板参数只承担买/卖方向分派，
// 保持热路径没有运行期 side enum 分支。

/// @brief 限价单价格是否触及对手档：Buy 要求对手 ask ≤ limit，Sell 要求 bid ≥ limit。
template <bool IsBuy>
inline bool price_fits(int64_t opp_px_fp, int64_t limit_fp) noexcept {
    if constexpr (IsBuy) {
        return opp_px_fp <= limit_fp;
    } else {
        return opp_px_fp >= limit_fp;
    }
}

/// @brief FOK 预检：沿深度累加可成交量，判断能否全量成交（不改盘口 / 不产生 fill）。
/// @param remaining_capacity 本 tick 剩余成交容量（volume_limit）；>0 为上限，<0 表示不限。
///        0 不会到达本函数（match_one_side 的零容量分支在 FOK 预检前已撤单/阻塞）。
/// @note 可成交量须扣本单前方排队量 ord.queue_ahead（fill 路径先消耗 queue_ahead 再成交）
///       并受 remaining_capacity 上限约束；两维都要与 fill 路径同口径，否则预检通过但实撮被
///       queue_ahead / cap 截断会留下部分成交滞留簿中，破坏 FOK all-or-nothing 语义。
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
    // 本单前方排队量先被消耗（与 fill 路径一致），可成交量相应减少。
    if (ord.queue_ahead > 0.0) {
        const int64_t qa_fp = order_exec_to_fp(ord.queue_ahead);
        total_fp = (qa_fp < total_fp) ? (total_fp - qa_fp) : 0;
    }
    // 成交容量受限时（volume_limit），可成交量同样受 remaining_capacity 约束。
    if (remaining_capacity > 0.0) {
        const int64_t cap_fp = order_exec_to_fp(remaining_capacity);
        if (cap_fp < total_fp) total_fp = cap_fp;
    }
    return total_fp >= ord.remaining_fp();
}

/// @brief 逐档消耗对手盘深度，产生 fill 并更新 opp_left / remaining_capacity / queue_ahead。
/// @return 本次累计成交量 fixed-point。
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

}  // namespace

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
    // 编译期选择对手盘价格 / 量数组：buy 消耗 asks，sell 消耗 bids。
    const int64_t* opp_px = IsBuy ? ask_px_fp : bid_px_fp;
    double* opp_left = IsBuy ? ask_left : bid_left;

    for (int li = 0; li < side_levels.size; ) {
        PriceLevel& lvl = side_levels.levels[li];
        const int64_t limit_fp = lvl.price_fp;
        bool level_blocked = false;

        while (!lvl.empty()) {
            const uint64_t oid = lvl.front();
            XtSimOrder& ord = lookup_fn(lookup_ctx, oid);

            // 订单状态由 XtOrderManager 持有；异常状态从本地队列剔除。
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

            // 价格未触及对手盘最优档时，同价位后续订单也被阻塞。
            // 未穿越的 IOC/FOK 由 match_tick 尾部 sweep_ioc_fok_ 统一撤单。
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

            // FOK 必须先确认全量可成交，否则整单撤销。
            if (ord.tif == XtTimeInForce::FOK &&
                !fok_can_fully_fill<IsBuy>(ord, opp_px, opp_left, depth, liq_factor, limit_fp,
                                           remaining_capacity)) {
                result.canceled.push_back(oid);
                lvl.pop_front();
                continue;
            }

            // 逐档消耗对手盘深度；fill 内核保持单一线性循环，避免隐藏 queue_ahead、
            // depth 剩余量和全局成交容量三者的同步更新关系。
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
                    break;  // 剩余量留存，等待下一 tick。
                }
            }
        }

        // 空层级从数组移除；li 不前进以继续检查移入当前位置的层级。
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
    // 清空但保留 capacity，减少回测热路径分配。
    match_result_.fills.clear();
    match_result_.done.clear();
    match_result_.canceled.clear();

    if (!code || code[0] == '\0' || depth <= 0) return match_result_;

    auto it = books_.find(code);
    if (it == books_.end()) return match_result_;

    depth = depth > XtRawTick::DEPTH ? XtRawTick::DEPTH : depth;

    // 数量转 double 后参与 liquidity_ratio 运算；价格仍保持 fixed-point。
    double bid_left[XtRawTick::DEPTH] = {};
    double ask_left[XtRawTick::DEPTH] = {};
    for (int i = 0; i < depth; ++i) {
        bid_left[i] = bid_qty_fp ? order_exec_from_fp(bid_qty_fp[i]) : 0.0;
        ask_left[i] = ask_qty_fp ? order_exec_from_fp(ask_qty_fp[i]) : 0.0;
    }

    Book& book = it->second;
    if (liquidity_ratio < 0.0) liquidity_ratio = 0.0;
    if (liquidity_ratio > 1.0) liquidity_ratio = 1.0;
    // liquidity_ratio 不跨 tick 累积，使用 double 不影响状态确定性。
    const double liq_factor = 1.0 - liquidity_ratio;

    // 容量三态：<0 不限制；0 = 本 tick 零可成交量（走下方零容量分支：IOC/FOK 撤、
    // GTC 留簿）；>0 = 双侧共享 cap。0 不得映射为不限制，否则累计量零增量 tick 会凭空成交。
    double remaining_capacity = max_fill_qty >= 0.0 ? max_fill_qty : -1.0;

    // 触板整侧冻结（limit-lock）：判据用 limit 价比较而非盘口空档——CSV L1 fallback
    // 会在锁板 tick 伪造板价假深度。涨停锁板 = 买一顶板 → buy taker 无真实对手
    // 流动性；跌停对称。冻结侧订单留簿（与 level_blocked 语义一致，IOC 由上层 TTL
    // 收割）。0 价 ask 不判跌停（0 <= lower 恒真，需 >0 防误判）；0 = 无限制 fail-open。
    const bool buy_frozen = upper_limit_fp > 0 && bid_px_fp
                            && bid_px_fp[0] >= upper_limit_fp;
    const bool sell_frozen = lower_limit_fp > 0 && ask_px_fp
                             && ask_px_fp[0] > 0 && ask_px_fp[0] <= lower_limit_fp;

    // 买单消耗卖盘深度；bid level 按价格从高到低遍历。
    if (!buy_frozen) {
        match_one_side<true>(book.bids,
                             bid_px_fp, ask_px_fp,
                             bid_left, ask_left,
                             depth, liq_factor,
                             remaining_capacity,
                             match_result_,
                             lookup_fn, lookup_ctx);
    }

    // 卖单消耗买盘深度；ask level 按价格从低到高遍历。
    if (!sell_frozen) {
        match_one_side<false>(book.asks,
                              bid_px_fp, ask_px_fp,
                              bid_left, ask_left,
                              depth, liq_factor,
                              remaining_capacity,
                              match_result_,
                              lookup_fn, lookup_ctx);
    }

    // IOC/FOK 永不留簿：撮合两侧跑完后扫簿，仍在簿的 IOC/FOK 一律撤
    // （未穿价 head、GTC 身后、深档、冻结侧——frozen 侧 FOK 此前连 manager
    // 冻结清扫都不覆盖）。旧行为退化 GTC 留簿等 TTL，shipped 主流 TTL=0
    // → IOC 常态幽灵成交。
    sweep_ioc_fok_(book.bids, match_result_, lookup_fn, lookup_ctx);
    sweep_ioc_fok_(book.asks, match_result_, lookup_fn, lookup_ctx);

    // 保留空 book 条目，避免同一 symbol 反复插入/删除。

    return match_result_;
}

template <bool Descending>
void XtOrderBook::sweep_ioc_fok_(FlatLevels<Descending>& side_levels,
                                 MatchResult& result,
                                 XtSimOrder& (*lookup_fn)(void*, uint64_t),
                                 void* lookup_ctx) {
    for (int li = 0; li < side_levels.size; ) {
        PriceLevel& lvl = side_levels.levels[li];
        for (int k = 0; k < lvl.count; ) {
            const uint64_t oid = lvl.ids[k];
            const XtSimOrder& ord = lookup_fn(lookup_ctx, oid);
            if (ord.tif == XtTimeInForce::IOC || ord.tif == XtTimeInForce::FOK) {
                // 已终态的残留 slot 不重发 cancel（与 match 循环异常状态剔除同语义）。
                if (ord.status == XtOrderStatus::Accepted ||
                    ord.status == XtOrderStatus::PartiallyFilled) {
                    result.canceled.push_back(oid);
                }
                for (int j = k; j < lvl.count - 1; ++j) lvl.ids[j] = lvl.ids[j + 1];
                --lvl.count;
            } else {
                ++k;
            }
        }
        if (lvl.empty()) side_levels.erase_at(li); else ++li;
    }
}
