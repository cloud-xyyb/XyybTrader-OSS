#include "xt_risk/engine/XtRiskEngine.h"

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <shared_mutex>

#include "XtFixedPoint.h"
#include "XtLogger.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_cost/XtFillCostModel.h"
#include "api/IFeeRuleProvider.h"

namespace {

XtRiskResult make_reject(XtRiskReasonCode code, XtRiskStage stage, const std::string& text = std::string()) {
    XtRiskResult out;
    out.passed = false;
    out.reason_code = code;
    out.stage = stage;
    out.reason_text = text.empty() ? risk_reason_text(code) : text;
    return out;
}

XtRiskResult make_pass() {
    XtRiskResult out;
    out.passed = true;
    out.reason_code = XtRiskReasonCode::None;
    out.stage = XtRiskStage::CoreRule;
    out.reason_text = risk_reason_text(XtRiskReasonCode::None);
    return out;
}

/// @brief 订单全量成交后的净仓位（带符号，fixed-point）。
int64_t projected_position_fp(const xt_core::XtOrder& order, const XtRiskSnapshot& snapshot) {
    return order.is_buy ? (snapshot.position_qty_fp + order.qty_fp)
                        : (snapshot.position_qty_fp - order.qty_fp);
}

/// @brief 订单是否朝 0 收缩净仓位（reduce）。
/// 不能只看 close_offset：Binance/crypto 的 reduce-only 单可能仍使用 Open offset，
/// 因此按 |new_pos| < |cur_pos| 的仓位变化方向判定。
bool reduces_position(const xt_core::XtOrder& order, const XtRiskSnapshot& snapshot) {
    return std::llabs(projected_position_fp(order, snapshot))
         < std::llabs(snapshot.position_qty_fp);
}

}  // namespace

const char* risk_reason_text(XtRiskReasonCode code) {
    switch (code) {
    case XtRiskReasonCode::None: return "ok";
    case XtRiskReasonCode::EmptyCode: return "empty code";
    case XtRiskReasonCode::InvalidPriceOrQty: return "invalid price or qty";
    case XtRiskReasonCode::UnknownStrategySid: return "unknown strategy sid";
    case XtRiskReasonCode::Cooldown: return "gateway in cooldown";
    case XtRiskReasonCode::RateLimited: return "gateway rate limited";
    case XtRiskReasonCode::PriceTickMismatch: return "price tick mismatch";
    case XtRiskReasonCode::TradingNotReady: return "trading not ready";
    case XtRiskReasonCode::CloseoutNoOpen: return "closeout no-open window";
    case XtRiskReasonCode::TdRecovering: return "trade gateway recovering";
    case XtRiskReasonCode::SessionClosed: return "session closed (outside active sections)";
    case XtRiskReasonCode::KillSwitchActive: return "kill switch active";
    case XtRiskReasonCode::SelfTradeDetected: return "self-trade detected (STP)";
    case XtRiskReasonCode::VelocityLimitExceeded: return "position velocity limit exceeded";
    case XtRiskReasonCode::CancelRateLimited: return "cancel rate limited";
    case XtRiskReasonCode::HardCircuitBreakerTripped: return "hard circuit breaker tripped (restart required)";
    case XtRiskReasonCode::TdNotInitialized: return "trade gateway not initialized";
    case XtRiskReasonCode::TdCapCloseTodayUnsupported: return "close today unsupported by gateway";
    case XtRiskReasonCode::TdCapL2OrderFlowUnsupported: return "L2 orderflow unsupported";
    case XtRiskReasonCode::TdCapIocFokUnsupported: return "IOC/FOK unsupported by gateway";
    case XtRiskReasonCode::MaxOrderQty: return "order qty exceeds max_order_qty";
    case XtRiskReasonCode::MaxOrderNotional: return "order notional exceeds max_order_notional";
    case XtRiskReasonCode::MinOrderNotional: return "order notional below min_order_notional (venue MIN_NOTIONAL; opens only, closes exempt)";
    case XtRiskReasonCode::MaxPosQty: return "position exceeds max_pos_qty";
    case XtRiskReasonCode::ShortNotAllowed: return "short not allowed";
    case XtRiskReasonCode::InsufficientAvailablePosition: return "insufficient available position (T+1)";
    case XtRiskReasonCode::InsufficientMargin: return "insufficient margin";
    case XtRiskReasonCode::DailyLossExceeded: return "daily loss limit exceeded";
    case XtRiskReasonCode::AnnualLossExceeded: return "annual loss limit exceeded";
    case XtRiskReasonCode::StrategyExposureExceeded: return "per-strategy exposure limit exceeded";
    case XtRiskReasonCode::StrategyMaxPosQtyExceeded: return "per-strategy max position qty exceeded";
    // 保留 reason 文本供 journal/log replay；运行期不再产生该 code。
    case XtRiskReasonCode::StrategyMarginPctExceeded: return "per-strategy margin pct exceeded";
    case XtRiskReasonCode::StrategyDailyLossExceeded: return "per-strategy daily loss pct exceeded";
    case XtRiskReasonCode::StrategyKillSwitchActive: return "per-strategy kill switch active";
    case XtRiskReasonCode::StrategyBlockedByWatchdog: return "per-strategy watchdog blocked";
    case XtRiskReasonCode::SiblingForceFlattenInProgress: return "sibling sid force-flatten in progress (cross-strategy)";
    case XtRiskReasonCode::PositionSyncStale: return "position snapshot stale";
    case XtRiskReasonCode::CloseSyncPending: return "close sync pending";
    case XtRiskReasonCode::InsufficientCloseToday: return "insufficient close today";
    case XtRiskReasonCode::InsufficientCloseYesterday: return "insufficient close yesterday";
    case XtRiskReasonCode::PositionStateUnknown: return "position state unknown (closeable unconfirmed; reject-on-unknown policy)";
    case XtRiskReasonCode::InCallAuctionLimitNotAllowed: return "in call auction: ioc/fok not allowed";
    case XtRiskReasonCode::PriceOutOfDailyLimit: return "price out of daily limit (limit up/down)";
    case XtRiskReasonCode::SymbolBlacklisted: return "symbol blacklisted";
    case XtRiskReasonCode::OrderCountExceeded: return "per-symbol order count exceeded";
    case XtRiskReasonCode::OrderRateExceeded: return "per-symbol order rate exceeded";
    case XtRiskReasonCode::SelfMatchPostTrade: return "post-trade self-match detected";
    default: return "unknown";
    }
}

const char* risk_stage_text(XtRiskStage stage) {
    switch (stage) {
    case XtRiskStage::InputGuard: return "input_guard";
    case XtRiskStage::CoreRule: return "core_rule";
    case XtRiskStage::PositionPrecheck: return "position_precheck";
    case XtRiskStage::MarginPrecheck: return "margin_precheck";
    case XtRiskStage::CapabilityGuard: return "capability_guard";
    default: return "unknown";
    }
}

// cfg_mu_ 串行化配置快照、fixed-point cache、annual latch、blacklist 和
// per-symbol throttle 状态。evaluate() 会推进计数/rate window 并可能 latch
// cutoff，因此与 set_config/restore/session reset 共享同一个写锁域。
XtRiskEngine::XtRiskEngine(const XtRiskCfg& cfg, InitDeps deps)
    : cfg_(cfg),
      fee_provider_(deps.fee_provider),
      commodity_catalog_(deps.commodity_catalog) {
    // 构造期尚未发布给其他线程，直接初始化 fp cache。
    refresh_fp_cache();
}

void XtRiskEngine::set_config(const XtRiskCfg& cfg) {
    std::unique_lock lock(cfg_mu_);
    cfg_ = cfg;
    refresh_fp_cache();
}

XtRiskCfg XtRiskEngine::config() const {
    std::shared_lock lock(cfg_mu_);
    return cfg_;
}

void XtRiskEngine::refresh_fp_cache() {
    // 调用方持 cfg_mu_ unique_lock；构造期例外，因为对象尚未发布。
    max_order_qty_fp_         = cfg_.max_order_qty > 0.0      ? xt_fp::to_fp(cfg_.max_order_qty)      : 0;
    max_order_notional_fp_    = cfg_.max_order_notional > 0.0  ? xt_fp::to_fp(cfg_.max_order_notional) : 0;
    min_order_notional_fp_    = cfg_.min_order_notional > 0.0  ? xt_fp::to_fp(cfg_.min_order_notional) : 0;
    max_daily_loss_fp_        = cfg_.max_daily_loss > 0.0      ? xt_fp::to_fp(cfg_.max_daily_loss)     : 0;
    max_pos_qty_fp_           = cfg_.max_pos_qty > 0.0         ? xt_fp::to_fp(cfg_.max_pos_qty)        : 0;
    // per-strategy 配额预转为 fixed-point，避免 evaluate() 热路径做 double 转换。
    per_strategy_max_pos_qty_fp_.clear();
    for (const auto& kv : cfg_.per_strategy_max_pos_qty) {
        if (kv.second > 0.0) {
            per_strategy_max_pos_qty_fp_[kv.first] = xt_fp::to_fp(kv.second);
        }
    }
    per_strategy_daily_loss_pct_fp_.clear();
    for (const auto& kv : cfg_.per_strategy_daily_loss_pct) {
        if (kv.second > 0.0) {
            per_strategy_daily_loss_pct_fp_[kv.first] = xt_fp::to_fp(kv.second);
        }
    }
}

void XtRiskEngine::on_year_change(int year, int64_t equity_fp, int64_t process_net_pnl_fp) {
    std::unique_lock lock(cfg_mu_);
    if (year == current_year_) return;
    current_year_         = year;
    year_start_equity_fp_ = equity_fp;
    // 年度止损基准使用纯交易净 PnL；出入金不改变亏损比例。
    year_start_net_pnl_fp_ = nmf_offset_fp_ + process_net_pnl_fp;
    annual_cutoff_        = false;
}

// annual latch 单字段 accessor 走 shared_lock，与写路径同锁域；单字段读自洽，
// 但跨多次加锁的组合读不原子 —— 组合消费方用 annual_latch_snapshot()。
int XtRiskEngine::current_year() const {
    std::shared_lock lock(cfg_mu_);
    return current_year_;
}

int64_t XtRiskEngine::year_start_equity_fp() const {
    std::shared_lock lock(cfg_mu_);
    return year_start_equity_fp_;
}

bool XtRiskEngine::annual_cutoff_active() const {
    std::shared_lock lock(cfg_mu_);
    return annual_cutoff_;
}

int64_t XtRiskEngine::year_start_net_pnl_fp() const {
    std::shared_lock lock(cfg_mu_);
    return year_start_net_pnl_fp_;
}

int64_t XtRiskEngine::nmf_offset_fp() const {
    std::shared_lock lock(cfg_mu_);
    return nmf_offset_fp_;
}

XtRiskEngine::AnnualLatchSnapshot XtRiskEngine::annual_latch_snapshot() const {
    std::shared_lock lock(cfg_mu_);
    AnnualLatchSnapshot s;
    s.current_year          = current_year_;
    s.year_start_equity_fp  = year_start_equity_fp_;
    s.annual_cutoff_active  = annual_cutoff_;
    s.year_start_net_pnl_fp = year_start_net_pnl_fp_;
    s.nmf_offset_fp         = nmf_offset_fp_;
    return s;
}

uint64_t XtRiskEngine::per_strategy_daily_loss_baseline_missing_count() const noexcept {
    return per_strategy_dll_baseline_missing_.load(std::memory_order_relaxed);
}

void XtRiskEngine::restore_annual_state(int year, int64_t year_start_equity_fp, bool cutoff_active,
                                        int64_t year_start_net_pnl_fp, int64_t nmf_offset_fp) {
    // 无有效年份或年初权益基准时视为 cold-start，不 arm annual latch。
    if (year <= 0 || year_start_equity_fp <= 0) return;
    std::unique_lock lock(cfg_mu_);
    current_year_         = year;
    year_start_equity_fp_ = year_start_equity_fp;
    annual_cutoff_        = cutoff_active;
    year_start_net_pnl_fp_ = year_start_net_pnl_fp;
    nmf_offset_fp_         = nmf_offset_fp;
}

bool XtRiskEngine::is_annual_loss_exceeded(int64_t process_net_pnl_fp) {
    // 查询可能把 annual_cutoff_ 从 false 推进到 true，必须持写锁。
    std::unique_lock lock(cfg_mu_);
    return is_annual_loss_exceeded_unlocked_(process_net_pnl_fp);
}

bool XtRiskEngine::is_annual_loss_exceeded_unlocked_(int64_t process_net_pnl_fp) {
    // 年度止损使用纯交易 PnL，外部出入金不参与亏损判定。
    //   net_pnl      = nmf_offset (跨重启 cum) + process_net_pnl (realized−fees+unrealized)
    //   trading_loss = year_start_net_pnl − net_pnl
    //   reject ⇔ trading_loss / year_start_equity ≥ pct
    if (cfg_.max_annual_loss_pct <= 0.0) return false;
    if (year_start_equity_fp_ <= 0) return false;
    if (annual_cutoff_) return true;
    const int64_t net_pnl = nmf_offset_fp_ + process_net_pnl_fp;
    const double trading_loss = static_cast<double>(year_start_net_pnl_fp_ - net_pnl);
    if (trading_loss > 0.0 &&
        trading_loss / static_cast<double>(year_start_equity_fp_) >= cfg_.max_annual_loss_pct) {
        annual_cutoff_ = true;
        return true;
    }
    return false;
}

void XtRiskEngine::insert_blacklist(const char* code, const char* /*reason_tag*/) {
    if (!code || code[0] == '\0') return;
    std::unique_lock lock(cfg_mu_);
    blacklisted_codes_.emplace(code);
}

bool XtRiskEngine::is_blacklisted(const char* code) const {
    if (!code || code[0] == '\0') return false;
    std::shared_lock lock(cfg_mu_);
    return is_blacklisted_unlocked_(code);
}

bool XtRiskEngine::is_blacklisted_unlocked_(const char* code) const {
    // 内部 unlocked 版；调用方已持 cfg_mu_，避免 public API 重入 shared_mutex。
    if (!code || code[0] == '\0') return false;
    return blacklisted_codes_.find(code) != blacklisted_codes_.end();
}

void XtRiskEngine::on_session_begin(uint32_t /*trading_date*/) {
    std::unique_lock lock(cfg_mu_);
    symbol_order_count_.clear();
    symbol_recent_orders_.clear();
    if (!cfg_.blacklist_persist_across_session) {
        blacklisted_codes_.clear();
    }
}

XtRiskResult XtRiskEngine::evaluate(const xt_core::XtOrder& order,
                                  const XtRiskSnapshot& snapshot,
                                  const XtRiskPolicy& policy) {
    // 无策略上下文时使用 sid=0 语义，per-strategy 规则显式 bypass。
    return evaluate(order, snapshot, policy, XtRiskEvalContext{});
}

// ── evaluate() compose-method helpers ─────────────────────────────────────

std::optional<XtRiskResult> XtRiskEngine::eval_input_guard_(const xt_core::XtOrder& order) {
    // 基础输入错误不依赖配置，锁外拒绝可避免无效请求碰 cfg_mu_。
    if (order.code[0] == '\0') {
        return make_reject(XtRiskReasonCode::EmptyCode, XtRiskStage::InputGuard);
    }
    if (order.price_fp <= 0 || order.qty_fp <= 0) {
        return make_reject(XtRiskReasonCode::InvalidPriceOrQty, XtRiskStage::InputGuard);
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtRiskEngine::eval_symbol_throttle_(const xt_core::XtOrder& order,
                                                               const XtRiskSnapshot& snapshot,
                                                               const std::string& code_key,
                                                               const XtRiskEvalContext& eval_ctx) {
    // 减仓单豁免 per-symbol blacklist/count/rate，避免限频状态阻断降仓路径。
    // 豁免不对称是有意的：减仓单未超限时仍计入 count/rate（占配额），只在
    // 超限后豁免拒单——保守方向，防止用减仓口径绕开计数。
    const bool is_reduce = reduces_position(order, snapshot);

    // per-symbol blacklist pre-check + 累计计数 + 频率窗口。
    // 黑名单命中直接拒单；触发新 violation 时先加入黑名单再拒绝本单。
    if (!is_reduce && is_blacklisted_unlocked_(code_key.c_str())) {
        return make_reject(XtRiskReasonCode::SymbolBlacklisted, XtRiskStage::CoreRule);
    }
    if (cfg_.max_order_count_per_symbol > 0) {
        // code_key 已在锁外构造，count/rate 两个 map 共用同一份 key。
        auto& count = symbol_order_count_[code_key];
        if (count + 1 > cfg_.max_order_count_per_symbol) {
            if (!is_reduce) {
                // code_key 与 count/rate map 同源（strnlen 兜底缺 NUL），避免 blacklist
                // 与计数 map 在病态输入下 key 不一致。
                blacklisted_codes_.emplace(code_key);
                return make_reject(XtRiskReasonCode::OrderCountExceeded, XtRiskStage::CoreRule);
            }
        } else {
            ++count;
        }
    }
    if (cfg_.max_order_rate_per_symbol > 0) {
        auto& window = symbol_recent_orders_[code_key];
        const int64_t now_ms  = eval_ctx.now_ms;
        const int64_t cutoff  = now_ms - static_cast<int64_t>(cfg_.order_rate_window_ms);
        while (!window.empty() && window.front() < cutoff) {
            window.pop_front();
        }
        if (window.size() + 1 > cfg_.max_order_rate_per_symbol) {
            if (!is_reduce) {
                blacklisted_codes_.emplace(code_key);
                return make_reject(XtRiskReasonCode::OrderRateExceeded, XtRiskStage::CoreRule);
            }
        } else {
            window.push_back(now_ms);
        }
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtRiskEngine::eval_order_limits_(const xt_core::XtOrder& order,
                                                            const XtRiskSnapshot& snapshot,
                                                            int64_t notional_fp) const {
    if (max_order_qty_fp_ > 0 && order.qty_fp > max_order_qty_fp_) {
        return make_reject(XtRiskReasonCode::MaxOrderQty, XtRiskStage::CoreRule);
    }
    if (max_order_notional_fp_ > 0 && notional_fp > max_order_notional_fp_) {
        return make_reject(XtRiskReasonCode::MaxOrderNotional, XtRiskStage::CoreRule);
    }
    // limit_up/down_fp 为 caller-builds 字段；0 表示该 venue 未启用涨跌停检查。
    // 该规则放在 open-only pivot 前，平仓单同样受交易所价格带约束。
    if (snapshot.limit_up_fp > 0 && order.price_fp > snapshot.limit_up_fp) {
        return make_reject(XtRiskReasonCode::PriceOutOfDailyLimit, XtRiskStage::CoreRule,
            "price " + std::to_string(order.price_fp) + " > limit_up "
            + std::to_string(snapshot.limit_up_fp));
    }
    // `price_fp > 0` 是防御性冗余：evaluate 的 eval_input_guard_ 已无条件拒
    // price_fp<=0，本 helper 无其它 caller，此处恒真。
    if (snapshot.limit_down_fp > 0 && order.price_fp > 0
        && order.price_fp < snapshot.limit_down_fp) {
        return make_reject(XtRiskReasonCode::PriceOutOfDailyLimit, XtRiskStage::CoreRule,
            "price " + std::to_string(order.price_fp) + " < limit_down "
            + std::to_string(snapshot.limit_down_fp));
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtRiskEngine::eval_open_loss_guards_(const xt_core::XtOrder& order,
                                                                const XtRiskSnapshot& snapshot,
                                                                const XtRiskPolicy& policy,
                                                                int64_t notional_fp) {
    // 下界名义金额校验 — 仅约束**增仓** open;**任何减仓单一律豁免**。
    // sub-min-notional 增仓会被交易所 MIN_NOTIONAL filter 拒，先在本层 fail-fast。
    if (min_order_notional_fp_ > 0 && !reduces_position(order, snapshot) &&
        notional_fp < min_order_notional_fp_) {
        return make_reject(XtRiskReasonCode::MinOrderNotional, XtRiskStage::CoreRule);
    }

    // 日内止损检查（open only）：today_pnl_fp 负值为亏损,超限则拒绝开仓。
    // policy.enforce_daily_loss=false 时跳过（强制平仓路径，确保平仓能放行）。
    if (policy.enforce_daily_loss &&
        cfg_.max_daily_loss > 0.0 &&
        snapshot.today_pnl_fp < -max_daily_loss_fp_) {
        return make_reject(XtRiskReasonCode::DailyLossExceeded, XtRiskStage::CoreRule);
    }

    // 年度止损检查（open only）。用 _unlocked 版,evaluate 已持 unique_lock。
    if (policy.enforce_annual_loss &&
        snapshot.equity_fp > 0 &&
        is_annual_loss_exceeded_unlocked_(snapshot.process_net_pnl_fp)) {
        return make_reject(XtRiskReasonCode::AnnualLossExceeded, XtRiskStage::CoreRule);
    }
    return std::nullopt;
}

std::optional<XtRiskResult> XtRiskEngine::eval_per_strategy_(const xt_core::XtOrder& /*order*/,
                                                            const XtRiskSnapshot& snapshot,
                                                            const XtRiskEvalContext& eval_ctx) const {
    // per-strategy kill switch 是 snapshot 驱动的 hard stop，优先于资金类规则。
    // evaluate 的 open-only pivot 已过滤平仓请求，这里命中即可直接拒绝开仓。
    if (snapshot.strategy_killed) {
        return make_reject(XtRiskReasonCode::StrategyKillSwitchActive, XtRiskStage::CoreRule,
            "strategy " + std::to_string(eval_ctx.strategy_id) + " kill switch active");
    }

    // per-strategy daily_loss_pct check (open only, equity-based)。
    // 空 map 或 sid=0 = 规则未配置，静默 bypass（未启用 profile 不产生噪声）。
    if (eval_ctx.strategy_id > 0 && !per_strategy_daily_loss_pct_fp_.empty()) {
        auto pct_it = per_strategy_daily_loss_pct_fp_.find(eval_ctx.strategy_id);
        if (pct_it != per_strategy_daily_loss_pct_fp_.end() && pct_it->second > 0) {
            if (snapshot.starting_balance_fp <= 0) {
                // 规则对本 sid 已配置，但 session 起点权益尚未冻结（阈值无分母）。
                // 仍然放行：拒单会在 bootstrap / session_begin 之前的时序窗口误伤正常开仓。
                // 放行是 trading fail-open，必须可观测——否则规则静默失效无从发现。
                per_strategy_dll_baseline_missing_.fetch_add(1, std::memory_order_relaxed);
                if (!per_strategy_dll_baseline_warned_.exchange(true, std::memory_order_relaxed)) {
                    XtLogger::warn_m("RISK",
                        "per_strategy_daily_loss_pct configured for sid={} but starting_balance_fp "
                        "is not frozen (0): check bypassed (fail-open). Expected transiently before "
                        "session_begin; persistent growth of "
                        "per_strategy_daily_loss_baseline_missing_count means the rule is inert.",
                        eval_ctx.strategy_id);
                }
            } else {
                const int64_t loss_threshold_fp =
                    xt_fp::mul(pct_it->second, snapshot.starting_balance_fp);
                if (snapshot.per_strategy_today_pnl_fp < -loss_threshold_fp) {
                    return make_reject(XtRiskReasonCode::StrategyDailyLossExceeded, XtRiskStage::CoreRule,
                        "strategy " + std::to_string(eval_ctx.strategy_id) + " daily loss pct exceeded");
                }
            }
        }
    }

    return std::nullopt;
}

std::optional<XtRiskResult> XtRiskEngine::eval_position_rules_(const xt_core::XtOrder& order,
                                                             const XtRiskSnapshot& snapshot,
                                                             const XtRiskPolicy& policy,
                                                             const XtRiskEvalContext& eval_ctx,
                                                             const XtCommodityInfo* comm) const {
    if (!policy.enforce_position) return std::nullopt;

    // 卖空许可取严：全局 risk.allow_short 与品种级 commodities.<p>.allow_short 都放行才算允许
    // （A 股主板品种级 false、融券标的 true；venue 内部并非一刀切）。品种缺配 → 只看全局。
    const bool short_allowed = cfg_.allow_short && (!comm || comm->allow_short);

    const int64_t new_pos_fp = projected_position_fp(order, snapshot);
    // Reduce-only 豁免：如果当前仓位已经超过 quota，朝 0 收缩的订单必须放行，
    // 否则降仓/自恢复路径会被同一个 quota 卡住。判定带 quota 条件
    // （abs_cur > quota 才豁免），与 reduces_position() 的纯方向判定不同。
    const int64_t abs_cur_fp = std::llabs(snapshot.position_qty_fp);
    const int64_t abs_new_fp = std::llabs(new_pos_fp);
    // per-strategy quota 优先于全局 max_pos_qty (双层 check)。
    // 注：abs_new_fp 来自账户级 snapshot.position_qty_fp（account_store 单源，无 per-sid
    // 仓位源）。single-symbol-per-sid 下账户净仓 == 该 sid 净仓，正确；same-symbol 多策略
    // 会退化为账户级限额，该场景已由 config validator fail-closed（本路径不应 reach same-symbol）。
    const uint32_t sid_id = eval_ctx.strategy_id;
    if (sid_id != 0 && !per_strategy_max_pos_qty_fp_.empty()) {
        auto it = per_strategy_max_pos_qty_fp_.find(sid_id);
        if (it != per_strategy_max_pos_qty_fp_.end() && it->second > 0 &&
            abs_new_fp > it->second) {
            const bool sid_reduce_only = (abs_cur_fp > it->second) && (abs_new_fp < abs_cur_fp);
            if (!sid_reduce_only) {
                return make_reject(XtRiskReasonCode::StrategyMaxPosQtyExceeded, XtRiskStage::CoreRule,
                    "per-strategy quota exceeded for sid=" + std::to_string(sid_id));
            }
        }
    }
    // 全局 max_pos_qty 顶层 cap (per-strategy 启用时仍生效)。
    if (max_pos_qty_fp_ > 0 &&
        abs_new_fp > max_pos_qty_fp_) {
        const bool global_reduce_only = (abs_cur_fp > max_pos_qty_fp_) && (abs_new_fp < abs_cur_fp);
        if (!global_reduce_only) {
            return make_reject(XtRiskReasonCode::MaxPosQty, XtRiskStage::CoreRule);
        }
    }
    if (!short_allowed && new_pos_fp < 0) {
        return make_reject(XtRiskReasonCode::ShortNotAllowed, XtRiskStage::CoreRule);
    }
    // A 股 T+1 长仓约束（sell qty ≤ yesterday_long_qty_fp）。enforce=false 时 bypass。
    if (cfg_.enforce_long_t_plus_1 && !order.is_buy &&
        order.qty_fp > snapshot.yesterday_long_qty_fp) {
        return make_reject(XtRiskReasonCode::InsufficientCloseYesterday, XtRiskStage::CoreRule);
    }
    // A 股集合竞价时段 IOC/FOK 一律 reject (限价 LIMIT 仍允许)。
    if (eval_ctx.is_in_call_auction && order.tif != XtTimeInForce::GTC) {
        return make_reject(XtRiskReasonCode::InCallAuctionLimitNotAllowed, XtRiskStage::CoreRule);
    }
    if (policy.enforce_short_t1 && !short_allowed && !order.is_buy &&
        order.qty_fp > snapshot.available_close_qty_fp) {
        return make_reject(XtRiskReasonCode::InsufficientAvailablePosition, XtRiskStage::CoreRule);
    }
    return std::nullopt;
}

XtRiskResult XtRiskEngine::eval_margin_and_pass_(const xt_core::XtOrder& order,
                                                const XtRiskSnapshot& snapshot,
                                                const XtRiskPolicy& policy,
                                                int64_t notional_fp,
                                                const XtCommodityInfo* comm) const {
    // 资金门禁分层：本层只保留 A 股 stock-mode capital reservation（notional + 预估费用，
    // BUY-only、无卖空；pass 携资金占用供 accept 侧冻结现金）。期货/加密的资金约束归
    // IRiskGuardContext（live/BT guard 的 per-unit margin 检查 + accept 冻结）——
    // notional 口径对杠杆品种是错误约束（要求现金覆盖全额名义值会假拒 >1x 杠杆开仓）。
    if (order.is_buy && commodity_catalog_) {
        // comm 由 evaluate 一次查表下传（== commodity_catalog_->find(order.code)）。
        if (comm && comm->is_t_plus_1) {
            // fee rule 缺失（fees 节未配 / unknown code）时 fee 记 0，现金检查仍按
            // notional 兜底——股票 1x 口径下 notional 是正确约束，不随费率配置缺失失效。
            const XtFeeRule* rule = (fee_provider_ && !fee_provider_->empty_rules())
                                        ? fee_provider_->lookup_fee_rule(order.code)
                                        : nullptr;
            double fee_d = 0.0;
            if (rule) {
                const double price_d = xt_fp::from_fp(order.price_fp);
                const double qty_d   = xt_fp::from_fp(order.qty_fp);
                const bool is_open  = (order.close_offset == XtOffset::Open);
                const bool is_today = (order.close_offset == XtOffset::CloseToday);
                fee_d = xt_fill_cost::estimate_fee(
                    *rule, price_d, qty_d, is_open, is_today, order.is_buy, comm);
            }
            const int64_t required_capital_fp =
                notional_fp + (fee_d > 0.0 ? xt_fp::to_fp(fee_d) : 0);
            if (policy.enforce_margin &&
                snapshot.cash_available_fp < required_capital_fp) {
                return make_reject(XtRiskReasonCode::InsufficientMargin, XtRiskStage::CoreRule);
            }
            // stock-mode pass 携资金占用，供 accept-side accounting 冻结现金。
            if (order.qty_fp > 0) {
                XtRiskResult pass_with_margin;
                pass_with_margin.passed = true;
                pass_with_margin.reason_code = XtRiskReasonCode::None;
                pass_with_margin.stage = XtRiskStage::CoreRule;
                pass_with_margin.required_margin_fp = required_capital_fp;
                pass_with_margin.per_unit_margin_fp = xt_fp::div(required_capital_fp, order.qty_fp);
                return pass_with_margin;
            }
        }
    }
    return make_pass();
}

XtRiskResult XtRiskEngine::evaluate(const xt_core::XtOrder& order,
                                  const XtRiskSnapshot& snapshot,
                                  const XtRiskPolicy& policy,
                                  const XtRiskEvalContext& eval_ctx) {
    // evaluate 是 reject pipeline：按优先级逐段 early-return；全部放行落到终态 pass。
    if (auto r = eval_input_guard_(order)) return *r;

    // code_key 在锁外构造，避免持 cfg_mu_ 时发生 string 分配；strnlen 兜底缺失 NUL。
    const std::string code_key(order.code, ::strnlen(order.code, sizeof(order.code)));

    // 单写锁覆盖配置读取与状态推进；helper 内只调用 unlocked 版本，避免重入 shared_mutex。
    std::unique_lock lock(cfg_mu_);

    if (auto r = eval_symbol_throttle_(order, snapshot, code_key, eval_ctx)) return *r;

    // 名义金额含合约乘数：max/min_order_notional 的字面口径是「一张单的名义金额」，
    // 漏 vol_scale 会让期货上限实际放宽 vol_scale 倍（ag=15 时 max 形同虚设）。
    // catalog 缺失 / vol_scale 非有限或非正 → 退 1×（与 per_unit_margin_fp 同 fail-safe
    // 口径）；info 一次查表下传 eval_margin_and_pass_ 复用，避免同一 evaluate 二次 find。
    const XtCommodityInfo* comm =
        commodity_catalog_ ? commodity_catalog_->find(order.code) : nullptr;
    const double vol_scale = (comm && std::isfinite(comm->vol_scale) && comm->vol_scale > 0.0)
                                 ? comm->vol_scale : 1.0;
    const int64_t notional_fp =
        xt_fp::mul(xt_fp::mul(order.price_fp, order.qty_fp), xt_fp::to_fp(vol_scale));
    if (auto r = eval_order_limits_(order, snapshot, notional_fp)) return *r;

    // 平仓请求不做净仓位/现金/止损方向限制（由 closeable/margin 层处理）；
    // 此为 open-only pivot —— 以下所有规则只对开仓生效。
    if (order.close_offset != XtOffset::Open) {
        return make_pass();
    }

    if (auto r = eval_open_loss_guards_(order, snapshot, policy, notional_fp)) return *r;
    if (auto r = eval_per_strategy_(order, snapshot, eval_ctx)) return *r;

    if (auto r = eval_position_rules_(order, snapshot, policy, eval_ctx, comm)) return *r;
    return eval_margin_and_pass_(order, snapshot, policy, notional_fp, comm);
}
