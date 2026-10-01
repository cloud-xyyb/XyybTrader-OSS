#include "state/AccountState.h"

#include "XtFixedPoint.h"

#include <algorithm>
#include <stdexcept>

namespace {

// 冻结释放只信任本地 ledger 中的剩余额。重复终态、乱序回报或坏 replay
// 最多释放到 slot.frozen；frozen_cash 的非负 clamp 是审计兜底，不是正常分支。
// frozen_by_orders 影子和随 slot 侧同值同点递减（min 后的 release ≤ slot.frozen ≤ Σslots，
// 不另 clamp：若失衡应由 audit 暴露而非被掩盖）。
inline void release_order_frozen(int64_t& frozen_cash,
                                 int64_t& frozen_by_orders,
                                 AccountState::FrozenOrderSlot& slot,
                                 int64_t release) {
    if (release <= 0 || slot.frozen <= 0) return;
    release = std::min(release, slot.frozen);
    frozen_cash -= release;
    if (frozen_cash < 0) frozen_cash = 0;
    frozen_by_orders -= release;
    slot.frozen -= release;
    if (slot.frozen < 0) slot.frozen = 0;
}

} // namespace

AccountState::AccountState(const Config& cfg)
    : cash_(xt_fp::to_fp(cfg.initial_cash)),
      t1_enabled_(cfg.t1_enabled),
      futures_mode_(cfg.futures_mode),
      catalog_(cfg.catalog) {
    // 期货 realized/unrealized 经 catalog vol_scale 归一化；缺 catalog 静默退 1×。
    // 构造期拒绝该非法配置（fail-fast 取代运行期静默错值）；空 catalog（非 null）
    // = 显式 1× opt-out，合法。
    if (cfg.futures_mode && cfg.catalog == nullptr)
        throw std::invalid_argument(
            "AccountState: futures_mode requires a non-null commodity catalog (vol_scale source)");
}

// reducer 边界正常会丢弃空 code；这里保留静态 sink 只防御坏 replay 或测试绕过
// reducer 直接调用写入口，避免向 pos_ 写入空 key。读路径仍把空 code 当作缺失。
AccountState::Position& AccountState::get_pos(const char* code) {
    if (__builtin_expect(!code || code[0] == '\0', 0)) {
        static Position empty{};
        return empty;
    }
    return pos_[code];
}

const AccountState::Position* AccountState::find_pos(const char* code) const {
    if (!code || code[0] == '\0') return nullptr;
    auto it = pos_.find(code);
    if (it == pos_.end()) return nullptr;
    return &it->second;
}

void AccountState::on_session_begin() {
    // 只有具备今昨仓语义的模式才滚动 today 计数；spot 非 T+1 路径保留
    // recovery 快照携带的 today 字段，避免无拆分 venue 被 session_begin 清空。
    if (!t1_enabled_ && !futures_mode_) return;
    for (auto& kv : pos_) {
        kv.second.long_today  = 0;
        kv.second.short_today = 0;
    }
}

void AccountState::reset_positions() {
    // 只擦仓位域。cash/realized/fees 是 fill-stream 账务域：recovery 快照
    // （如 binance positionRisk）不携带资金真值，周期 reconcile_resync 下
    // 清零会让 margin precheck 的 available_cash 永久归零（无后续 SetCash 再锚定）。
    frozen_cash_ = 0;
    frozen_by_orders_ = 0;
    pos_.clear();
    order_slots_.clear();
}

void AccountState::set_cash(int64_t cash) {
    cash_ = cash;
}

void AccountState::set_position(const char* code,
                           int64_t long_qty,
                           int64_t long_avg_price,
                           int64_t short_qty,
                           int64_t short_avg_price,
                           int64_t long_today,
                           int64_t short_today,
                           int64_t broker_long_frozen_fp,
                           int64_t broker_short_frozen_fp,
                           int64_t broker_frozen_last_update_ms) {
    Position& p = get_pos(code);
    // broker/recovery 持仓快照是绝对覆盖；today 字段来自外部快照时仍要
    // clamp 到总持仓范围内，防止坏快照污染可平仓计算。
    p.long_qty        = long_qty;
    p.long_avg_price  = long_avg_price;
    p.short_qty       = short_qty;
    p.short_avg_price = short_avg_price;
    p.long_today  = (long_today  > 0) ? std::min(long_today,  long_qty)  : 0;
    p.short_today = (short_today > 0) ? std::min(short_today, short_qty) : 0;

    // broker frozen 是 venue 回报的总冻结量（不分今/昨）；负值按缺失处理。
    p.broker_long_frozen_fp  = broker_long_frozen_fp  > 0 ? broker_long_frozen_fp  : 0;
    p.broker_short_frozen_fp = broker_short_frozen_fp > 0 ? broker_short_frozen_fp : 0;
    // staleness 由 risk path 判断；这里保留 producer 的 steady ms 原值。
    // 非 CTP / 无 broker frozen 的路径传 0，读侧自动降级为 local outstanding。
    p.broker_frozen_last_update_ms = broker_frozen_last_update_ms;
}

void AccountState::on_order_accepted(uint64_t order_id, int64_t required_margin, int64_t per_unit_margin) {
    if (order_id == 0 || required_margin <= 0) return;
    // 上游不保证 OrderAccepted 去重；这里不吞重复 accepted，
    // 否则会掩盖事件源重复冻结的问题，后续 frozen audit 应能暴露异常。
    frozen_cash_ += required_margin;
    // 影子和 overwrite-aware：同 id 重复 accepted 覆盖旧 slot 时先退旧残余冻结，
    // 保持 ≡ Σslots——frozen_cash_ 的重复加由此在 audit 中成为可见分歧而非被镜像。
    auto it = order_slots_.find(order_id);
    if (it != order_slots_.end()) frozen_by_orders_ -= it->second.frozen;
    frozen_by_orders_ += required_margin;
    order_slots_[order_id] = {required_margin, 0, per_unit_margin};
}

void AccountState::on_order_update(uint64_t order_id, int64_t filled, bool is_final) {
    if (order_id == 0) return;
    auto it = order_slots_.find(order_id);
    // 无本地冻结槽就没有可释放金额；reconcile/cold-start 可能带来未知终态回报。
    if (it == order_slots_.end()) return;

    FrozenOrderSlot& slot = it->second;
    // filled 是累计成交量，只按增量释放，重复 non-final update 因此保持幂等。
    const int64_t filled_delta = filled - slot.filled;
    if (filled_delta > 0) {
        const int64_t release = xt_fp::mul(slot.per_unit, filled_delta);
        release_order_frozen(frozen_cash_, frozen_by_orders_, slot, release);
        slot.filled = filled;
    }
    if (!is_final) return;

    // 终态释放以本地 ledger 剩余冻结为准，不看 venue 回报的 remaining：
    // 空 remaining 或陈旧 remaining 会留下 frozen 残值。
    release_order_frozen(frozen_cash_, frozen_by_orders_, slot, slot.frozen);
    order_slots_.erase(it);
}

void AccountState::on_daily_settlement(int64_t realized_delta, int64_t settlement_fee) {
    // 结算确认的 PnL 既进入累计 realized，也落到 cash；结算手续费单独累计并扣现金。
    realized_pnl_ += realized_delta;
    cash_ += realized_delta;
    cash_ -= settlement_fee;
    fees_ += settlement_fee;
}

void AccountState::on_cash_transfer(int64_t amount) {
    cash_ += amount;
}

// 单边平仓 helper：调用方负责选择多头/空头槽，并在返回 close_qty 后处理剩余开仓。
// PnL 方向由 is_long_side 决定；期货模式传入合约乘数 vol_scale_fp，spot / catalog
// 缺失时传 1.0 fp。is_today 只约束已有 today 计数；today 缺失时 reducer 保持
// replay 宽容，不在账本层做 reject；平今合法性由账本外侧校验。
// 非平今 close 优先消耗昨仓，超出昨仓的部分才扣 today。
[[gnu::always_inline]] static int64_t apply_close(int64_t qty, int64_t price,
                            bool is_close, bool is_today,
                            int64_t& side_qty, int64_t& side_today, int64_t& side_avg,
                            bool is_long_side, int64_t& realized_pnl,
                            int64_t vol_scale_fp) {
    if (side_qty <= 0) return 0;

    const int64_t yesterday = std::max<int64_t>(0, side_qty - side_today);
    const int64_t can_close = (is_close && is_today && side_today > 0) ? side_today : side_qty;
    const int64_t close_qty = std::min(qty, can_close);

    const int64_t price_diff_pnl = is_long_side
        ? xt_fp::mul(price - side_avg, close_qty)
        : xt_fp::mul(side_avg - price, close_qty);
    realized_pnl += xt_fp::mul(price_diff_pnl, vol_scale_fp);

    side_qty -= close_qty;
    if (side_qty <= 0) {
        side_qty   = 0;
        side_avg   = 0;
        side_today = 0;
    } else if (side_today > 0) {
        const int64_t close_today = is_today
            ? close_qty
            : std::max<int64_t>(0, close_qty - yesterday);
        side_today -= std::min(close_today, side_today);
    }
    return close_qty;
}

// 单边开仓 helper：调用方只在剩余开仓量为正时进入。均价按 fixed-point 加权平均
// 更新；只有账户维护今昨仓拆分时才把本次开仓累加到 today。
[[gnu::always_inline]] static void apply_open(int64_t open_qty, int64_t price, bool track_today,
                       int64_t& side_qty, int64_t& side_today, int64_t& side_avg) {
    const int64_t new_qty = side_qty + open_qty;
    side_avg = (new_qty > 0)
        ? xt_fp::div(xt_fp::mul(side_avg, side_qty) + xt_fp::mul(price, open_qty), new_qty)
        : 0;
    side_qty = new_qty;
    if (track_today) side_today += open_qty;
}

void AccountState::on_trade_ex(const char* code,
                                bool is_buy, int64_t qty, int64_t price, int64_t fee,
                                bool is_close, bool is_today) {
    Position&     pos         = get_pos(code);
    const int64_t pnl_before  = realized_pnl_;
    const bool    track_today = t1_enabled_ || futures_mode_;

    // 期货账户 realized PnL 需要乘合约乘数；catalog 缺失时按 1.0 fp fallback，
    // 保持 replay 可执行，但不会得到合约乘数修正。
    int64_t vol_scale_fp = xt_fp::kScale;  // 1.0 fp
    if (futures_mode_ && catalog_) {
        const XtCommodityInfo* info = catalog_->find(code);
        if (info && info->vol_scale > 0.0)
            vol_scale_fp = xt_fp::to_fp(info->vol_scale);
    }

    if (is_buy) {
        // ── 买入：先平空，再开多 ──────────────────────────────────────────
        const int64_t close_qty = apply_close(qty, price, is_close, is_today,
                                               pos.short_qty, pos.short_today, pos.short_avg_price,
                                               /*is_long_side=*/false, realized_pnl_,
                                               vol_scale_fp);
        if (!is_close && close_qty < qty)
            apply_open(qty - close_qty, price, track_today,
                       pos.long_qty, pos.long_today, pos.long_avg_price);
        if (!futures_mode_) cash_ -= xt_fp::mul(price, qty);
    } else {
        // ── 卖出：先平多，再开空 ──────────────────────────────────────────
        const int64_t close_qty = apply_close(qty, price, is_close, is_today,
                                               pos.long_qty, pos.long_today, pos.long_avg_price,
                                               /*is_long_side=*/true, realized_pnl_,
                                               vol_scale_fp);
        if (!is_close && close_qty < qty)
            apply_open(qty - close_qty, price, track_today,
                       pos.short_qty, pos.short_today, pos.short_avg_price);
        if (!futures_mode_) cash_ += xt_fp::mul(price, qty);
    }

    // Spot 每笔成交移动名义本金；futures 只把本次已实现 PnL delta 进 cash，
    // 保证金占用由订单冻结 ledger 维护。
    if (futures_mode_) cash_ += realized_pnl_ - pnl_before;
    cash_ -= fee;
    fees_ += fee;
}

// ── 查询方法 ──────────────────────────────────

int64_t AccountState::cash() const { return cash_; }
int64_t AccountState::frozen_cash() const { return frozen_cash_; }
int64_t AccountState::frozen_by_orders() const { return frozen_by_orders_; }
int64_t AccountState::available_cash() const { return cash_ - frozen_cash_; }

int64_t AccountState::realized_pnl() const { return realized_pnl_; }
int64_t AccountState::fees() const { return fees_; }
bool AccountState::futures_mode() const { return futures_mode_; }
const XtCommodityCatalog* AccountState::commodity_catalog() const { return catalog_; }

const std::unordered_map<uint64_t, AccountState::FrozenOrderSlot>& AccountState::order_slots() const {
    return order_slots_;
}

int64_t AccountState::avg_price_of_(const Position& p) {
    if (p.long_qty > 0 && p.short_qty == 0) return p.long_avg_price;
    if (p.short_qty > 0 && p.long_qty == 0) return p.short_avg_price;
    if (p.long_qty > 0 && p.short_qty > 0) {
        const int64_t net = p.long_qty - p.short_qty;
        if (net > 0) return p.long_avg_price;
        if (net < 0) return p.short_avg_price;
    }
    return 0;
}

int64_t AccountState::available_sell_of_(const Position& p, bool is_today, bool t1_enabled) {
    int64_t available = 0;
    if (is_today) {
        available = p.long_today;
    } else {
        available = p.long_qty;
        if (t1_enabled) available -= p.long_today;
    }
    return available < 0 ? 0 : available;
}

int64_t AccountState::available_cover_of_(const Position& p, bool is_today, bool t1_enabled) {
    int64_t available = 0;
    if (is_today) {
        available = p.short_today;
    } else {
        available = p.short_qty;
        if (t1_enabled) available -= p.short_today;
    }
    return available < 0 ? 0 : available;
}

AccountState::PositionReadout AccountState::position_readout(const char* code) const {
    PositionReadout out;
    const Position* p = find_pos(code);
    if (!p) return out;
    out.qty = p->long_qty - p->short_qty;
    out.avg_price = avg_price_of_(*p);
    out.long_qty = p->long_qty;
    out.long_avg_price = p->long_avg_price;
    out.short_qty = p->short_qty;
    out.short_avg_price = p->short_avg_price;
    out.long_today_qty = p->long_today;
    out.short_today_qty = p->short_today;
    out.broker_long_frozen_fp = p->broker_long_frozen_fp;
    out.broker_short_frozen_fp = p->broker_short_frozen_fp;
    out.broker_frozen_last_update_ms = p->broker_frozen_last_update_ms;
    out.available_sell_today_qty = available_sell_of_(*p, /*is_today=*/true, t1_enabled_);
    out.available_sell_total_qty = available_sell_of_(*p, /*is_today=*/false, t1_enabled_);
    out.available_cover_today_qty = available_cover_of_(*p, /*is_today=*/true, t1_enabled_);
    out.available_cover_total_qty = available_cover_of_(*p, /*is_today=*/false, t1_enabled_);
    return out;
}

bool AccountState::has_today_split() const {
    return t1_enabled_ || futures_mode_;
}
