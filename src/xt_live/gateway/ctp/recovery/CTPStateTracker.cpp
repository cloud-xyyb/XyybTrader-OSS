#include "ctp/recovery/CTPStateTracker.h"
#include "XtFixedPoint.h"
#include "ctp/common/CTPUtil.h"

#include "xt_identity/OrderAttributionStore.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_util/time/XtTime.h"
#include "xt_util/time/XtTimeParse.h"

#include <algorithm>

// CTP 回报归一化与状态去重；CTP 特有字段转换不泄漏到上层。

namespace {

using ctp_util::parse_local_id;

XtOrderStatus map_ctp_order_status(const CThostFtdcOrderField* pOrder) {
    if (!pOrder) return XtOrderStatus::Submitted;
    if (pOrder->OrderSubmitStatus == THOST_FTDC_OSS_InsertRejected) {
        return XtOrderStatus::Rejected;
    }
    return ctp_util::map_order_status(pOrder->OrderStatus);
}

} // namespace

// ── 本地状态注册与映射 ──────────────────────────────────────────

void CtpStateTracker::register_order(uint64_t local_id,
                                     const char* order_ref,
                                     const char* code,
                                     bool is_buy,
                                     double price,
                                     int64_t qty_fp,
                                     const char* user_tag,
                                     bool is_close,
                                     bool is_today,
                                     uint64_t submit_begin_us,
                                     uint64_t submit_end_us,
                                     int64_t absorbed_watermark_fp) {
    LocalOrderState state;
    state.ref = order_ref ? order_ref : "";
    state.code = code ? code : "";
    state.is_buy = is_buy;
    state.price = price;
    state.qty_fp = qty_fp;
    state.user_tag = user_tag ? user_tag : "";
    state.is_close = is_close;
    state.is_today = is_today;
    state.submit_begin_us = submit_begin_us;
    state.submit_end_us = submit_end_us;
    {
        std::lock_guard<std::mutex> guard(mu_);
        // 重用 local_id 时须清理旧的去重/终态记录，否则新订单回报会被误判为重复
        deduper_.forget(local_id);
        clear_terminal_local(local_id);
        local_[local_id] = state;
    }
    // 交付投影登记（submit 与 reverse-sync rebuild 共用本入口）。venue 键待
    // 回报回填（update_local_cancel_fields / backfill_cancel_routing）。
    if (local_id != 0) {
        xt_live::gateway::XtExecDeliveryLedger::OrderIdentity ident;
        ident.sid_id =
            xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
        ident.code = state.code;
        ident.qty_fp = qty_fp;
        ident.is_buy = is_buy;
        ident.close_offset =
            is_close ? (is_today ? XtOffset::CloseToday : XtOffset::CloseYesterday)
                     : XtOffset::Open;
        ident.absorbed_watermark_fp = absorbed_watermark_fp;
        exec_ledger_.register_order(local_id, std::move(ident));
    }
}

void CtpStateTracker::rollback_register(uint64_t local_id) {
    if (local_id == 0) return;
    {
        std::lock_guard<std::mutex> guard(mu_);
        local_.erase(local_id);
        // 预注册→发送失败之间无任何回调在途（请求未上 wire），forget 仅为对称清理。
        deduper_.forget(local_id);
    }
    exec_ledger_.rollback_register(local_id);
}

void CtpStateTracker::note_submit_end(uint64_t local_id, uint64_t submit_end_us) {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = local_.find(local_id);
    if (it == local_.end()) return;
    it->second.submit_end_us = submit_end_us;
}

bool CtpStateTracker::get_order_ref(uint64_t local_id, std::string& out_ref) const {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = local_.find(local_id);
    if (it == local_.end()) return false;
    out_ref = it->second.ref;
    return !out_ref.empty();
}

bool CtpStateTracker::has_local_order_ref(const char* order_ref) const {
    const uint64_t local_id = parse_local_id(order_ref);
    if (local_id == 0) return false;
    std::lock_guard<std::mutex> guard(mu_);
    return local_.find(local_id) != local_.end();
}

bool CtpStateTracker::get_cancel_target(uint64_t local_id, CancelTarget& out) const {
    std::lock_guard<std::mutex> guard(mu_);
    // 双重终态检查：local_id 级别 + exchange_id|order_sys_id 级别。
    // 后者覆盖同一交易所订单被不同 local_id 引用的场景。
    if (terminal_local_ids_.find(local_id) != terminal_local_ids_.end()) {
        return false;
    }
    auto it = local_.find(local_id);
    if (it == local_.end()) return false;
    const LocalOrderState& state = it->second;
    const std::string key = build_terminal_order_key(state.exchange_id, state.order_sys_id);
    if (!key.empty() && terminal_order_keys_.find(key) != terminal_order_keys_.end()) {
        return false;
    }
    out.ref = state.ref;
    out.code = state.code;
    out.exchange_id = state.exchange_id;
    out.order_sys_id = state.order_sys_id;
    out.front_id = state.front_id;
    out.session_id = state.session_id;
    return !out.ref.empty();
}

bool CtpStateTracker::is_local_terminal(uint64_t local_id) const {
    if (local_id == 0) return false;
    std::lock_guard<std::mutex> guard(mu_);
    return terminal_local_ids_.find(local_id) != terminal_local_ids_.end();
}

// ── 事件归一化 ──────────────────────────────────────────────

bool CtpStateTracker::on_order_insert_reject(const CThostFtdcInputOrderField* pInputOrder,
                                             const CThostFtdcRspInfoField* pRspInfo,
                                             uint64_t local_cb_us,
                                             XtOrderExecUpdate& out) {
    if (!pInputOrder) return false;
    const uint64_t id = parse_local_id(pInputOrder->OrderRef);
    LocalOrderState state;
    const bool has_state = try_get_local(id, state);
    XtOrderExecUpdate upd;
    upd.id = id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(id).raw();
    upd.is_buy = (pInputOrder->Direction == THOST_FTDC_D_Buy);
    upd.status = XtOrderStatus::Rejected;
    upd.price_fp = order_exec_to_fp(pInputOrder->LimitPrice);
    upd.qty_fp = order_exec_to_fp(pInputOrder->VolumeTotalOriginal);
    upd.filled_fp = 0;
    upd.leaves_fp = upd.qty_fp;
    upd.last_fill_fp = 0;
    const char offset = pInputOrder->CombOffsetFlag[0];
    upd.close_offset = (offset == THOST_FTDC_OF_Open) ? XtOffset::Open
                                                      : ctp_util::to_xt_close_offset(offset);
    // reject reason 走 TraderCTP 日志 + recorder note_order_reject 侧信道；DTO 不扩字段。
    (void)pRspInfo;
    upd.local_cb_us = local_cb_us;
    if (has_state) {
        apply_local_to_update(state, upd);
    }
    mark_terminal(id, has_state ? &state : nullptr);
    record_terminal_projection(id, upd, xt_live::gateway::TerminalProvenance::kVenuePush);
    if (!should_emit(id, upd)) return false;
    out = std::move(upd);
    return true;
}

bool CtpStateTracker::on_order(const CThostFtdcOrderField* pOrder,
                               uint64_t local_cb_us,
                               XtOrderExecUpdate& out,
                               bool* out_suppressed_terminal) {
    if (!pOrder) return false;
    const uint64_t id = parse_local_id(pOrder->OrderRef);
    if (id != 0) {
        update_local_cancel_fields(id, pOrder);
    }
    LocalOrderState state;
    const bool has_state = try_get_local(id, state);
    XtOrderExecUpdate upd;
    upd.id = id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(id).raw();
    upd.is_buy = (pOrder->Direction == THOST_FTDC_D_Buy);
    upd.status = map_ctp_order_status(pOrder);
    upd.price_fp = order_exec_to_fp(pOrder->LimitPrice);
    upd.qty_fp = order_exec_to_fp(pOrder->VolumeTotalOriginal);
    upd.filled_fp = order_exec_to_fp(pOrder->VolumeTraded);
    upd.leaves_fp = order_exec_to_fp(pOrder->VolumeTotal);
    upd.last_fill_fp = 0;
    const char offset = pOrder->CombOffsetFlag[0];
    upd.close_offset = (offset == THOST_FTDC_OF_Open) ? XtOffset::Open
                                                      : ctp_util::to_xt_close_offset(offset);
    upd.local_cb_us = local_cb_us;
    if (has_state) {
        apply_local_to_update(state, upd);
    }
    // CTP 偶发 effective_remaining<=0 但 OrderStatus 映射为非终态的更新。deduper 宽谓词
    // is_final_update（status_final || effective_remaining<=0）会把它当 final 记入
    // final_seen_，随后真终态 status 更新被当重复吞掉 → mark_terminal 已擦 cancel-target
    // 而 book 留活 slot（幽灵 + frozen 泄漏）。在 CTP 边界把这类更新的 status 规整为终态，
    // 使 mark_terminal/emit/book-erase/frozen 释放同步。
    if (id != 0 && !order_status_is_final(upd.status) &&
        order_lifecycle::is_final_update(upd)) {
        upd.status = (upd.filled_fp >= upd.qty_fp && upd.qty_fp > 0)
                         ? XtOrderStatus::Filled
                         : XtOrderStatus::Canceled;
    }
    bool newly_terminal = false;
    if (order_status_is_final(upd.status)) {
        newly_terminal = mark_terminal(id, has_state ? &state : nullptr);
        record_terminal_projection(id, upd, xt_live::gateway::TerminalProvenance::kVenuePush);
    }
    if (!should_emit(id, upd)) {
        // 终态被 deduper 吞（progress_regressed/rank 残余乱序）。mark_terminal
        // 已擦 cancel-target，但 book/账户/策略未收终态 → 幽灵 + frozen 泄漏。回传 out + 标记，
        // 让 caller 非-recovery 路径合成终态投递。仅首次终态（newly_terminal）触发，重复终态不重投。
        if (out_suppressed_terminal && newly_terminal && id != 0) {
            out = std::move(upd);
            *out_suppressed_terminal = true;
        }
        return false;
    }
    out = std::move(upd);
    return true;
}

bool CtpStateTracker::on_trade(const CThostFtdcTradeField* pTrade,
                               uint64_t local_cb_us,
                               XtTradeRecord& out,
                               xt_live::gateway::FillSource fill_source,
                               bool absorbed,
                               uint32_t query_gen) {
    if (!pTrade) return false;
    const uint64_t id = parse_local_id(pTrade->OrderRef);
    XtTradeRecord tr;
    tr.order_id = id;
    // sid_id 与 strategy_id 同源 stamp（同一 attribution lookup）；miss 时两键保持 0/空。
    tr.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(id).raw();
    if (const char* sid = xt_identity::StrategySidRegistry::resolve_sid_cstr_or_null(tr.sid_id)) {
        tr.strategy_id = sid;
    }
    tr.code = pTrade->InstrumentID;
    tr.exchange_id = pTrade->ExchangeID;
    tr.trade_id = pTrade->TradeID;
    tr.order_ref = pTrade->OrderRef;
    tr.order_sys_id = pTrade->OrderSysID;
    tr.is_buy = (pTrade->Direction == THOST_FTDC_D_Buy);
    tr.is_close = (pTrade->OffsetFlag != THOST_FTDC_OF_Open);
    tr.is_today = (pTrade->OffsetFlag == THOST_FTDC_OF_CloseToday);
    tr.close_offset = ctp_util::to_xt_close_offset(pTrade->OffsetFlag);
    tr.qty_fp = order_exec_to_fp(pTrade->Volume);  // CTP Volume 是整数手数
    tr.price = pTrade->Price;
    tr.exec_price = pTrade->Price;
    tr.trade_date = XtTimeParse::parse_fixed_date_yyyymmdd(pTrade->TradeDate);
    tr.trade_time = XtTimeParse::parse_fixed_time_hms_ms(pTrade->TradeTime, 0);
    // CTP broker TradingDay 权威 — 夜盘自动归次日, 节假日/临时调休跟交易所一致.
    // 本字段驱动下游 fills.csv 的 trading_day 列, exporter 按此聚合 "今日".
    tr.trading_day = XtTimeParse::parse_fixed_date_yyyymmdd(pTrade->TradingDay);
    tr.local_cb_us = local_cb_us;
    // fill_ts_us 必须是真实 fill 时刻 (UTC epoch µs),不是本地 steady 接收时间。
    // CTP 只给 TradeTime(秒级 CST 墙上)+TradeDate(自然日),无 ms/epoch,也无 binance
    // 那样的 clock-skew estimator。故按 trade_date(自然日,夜盘不翻)+trade_time 还原
    // 绝对时刻(秒级分辨率)。local_cb_us 仅留作 latency aux,不再当 wall-clock fill 时间。
    tr.fill_ts_us = xt_time::cst_wall_to_epoch_us(tr.trade_date, tr.trade_time);

    // 去重键包含 TradingDay 防止跨日 TradeID 碰撞导致成交漏报
    std::string tkey;
    tkey.reserve(80);
    tkey.append(pTrade->TradingDay);
    tkey.push_back('|');
    tkey.append(pTrade->ExchangeID);
    tkey.push_back('|');
    tkey.append(pTrade->TradeID);
    tkey.push_back('|');
    tkey.append(pTrade->OrderSysID);
    tkey.push_back('|');
    tkey.append(pTrade->OrderRef);
    // exactly-once 判据：交付投影查+记一体。记忆按进程生命周期保留（recovery
    // 不清），live push 与 query 重放任一先到恰好记账一次，后到被抑制。
    {
        xt_live::gateway::XtExecDeliveryLedger::FillArgs fill;
        fill.fill_key = std::move(tkey);
        fill.qty_fp = tr.qty_fp;
        fill.source = fill_source;
        fill.absorbed = absorbed;
        fill.query_gen = query_gen;
        if (!exec_ledger_.record_fill(id, std::move(fill))) return false;
    }
    // O7 备机制（仅观测）：交付 query 行时若该记录 absorbed 累计仍低于注册封印
    // 水位，说明 pre-restart 行可能越过 TradeTime 封印在被交付 → 计数不仲裁。
    if (!absorbed && fill_source == xt_live::gateway::FillSource::kQueryReplay &&
        id != 0) {
        xt_live::gateway::XtExecDeliveryLedger::ExecOrderRecord rec;
        if (exec_ledger_.get_record(id, rec) && rec.absorbed_watermark_fp > 0) {
            int64_t absorbed_sum = 0;
            for (const auto& f : rec.fills) {
                if (f.absorbed) absorbed_sum += f.qty_fp;
            }
            if (absorbed_sum < rec.absorbed_watermark_fp) {
                watermark_breach_total_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }
    out = std::move(tr);
    LocalOrderState state;
    if (try_get_local(id, state)) apply_local_to_trade(state, out);
    return true;
}

bool CtpStateTracker::on_cancel_action_error(const char* order_ref,
                                             int error_id,
                                             uint64_t local_cb_us,
                                             XtOrderExecUpdate& out) {
    // CTP error=26 表示报单已全成交或已撤销；利用此信号收敛本地在途状态。
    // 其他 error_id 不构造合成终态回报。
    if (error_id != 26) return false;
    const uint64_t id = parse_local_id(order_ref);
    if (id == 0) return false;

    LocalOrderState state;
    if (!try_get_local(id, state)) return false;

    XtOrderExecUpdate upd;
    upd.id = id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(id).raw();
    upd.is_buy = state.is_buy;
    upd.price_fp = order_exec_to_fp(state.price);
    upd.qty_fp = state.qty_fp;
    // 累计成交量取交付投影（fills 记账 + absorbed 水位口径）；重放行被投影
    // 抑制天然不重复累计，error(26) 推断不再受 recovery 重放翻倍影响。
    const int64_t filled_fp =
        std::min(std::max<int64_t>(0, ledger_effective_filled_fp_(id)), state.qty_fp);
    const int64_t leaves_fp = state.qty_fp - filled_fp;
    upd.filled_fp = filled_fp;
    upd.leaves_fp = leaves_fp;
    upd.last_fill_fp = 0;
    upd.close_offset = state.is_close ? (state.is_today ? XtOffset::CloseToday : XtOffset::CloseYesterday)
                                      : XtOffset::Open;
    upd.local_cb_us = local_cb_us;
    upd.status = (state.qty_fp > 0 && leaves_fp <= 0) ? XtOrderStatus::Filled
                                                      : XtOrderStatus::Canceled;
    apply_local_to_update(state, upd);

    mark_terminal(id, &state);
    record_terminal_projection(id, upd,
                               xt_live::gateway::TerminalProvenance::kSynthCancelNoTarget);
    if (!should_emit(id, upd)) return false;
    out = std::move(upd);
    return true;
}

bool CtpStateTracker::make_cum_filled_completion(uint64_t local_id,
                                                 uint64_t local_cb_us,
                                                 XtOrderExecUpdate& out) {
    if (local_id == 0) return false;
    {
        // 已有终态（真实回报或先前合成）→ 不派生；重复由 mark_terminal/去重器双重幂等。
        xt_live::gateway::XtExecDeliveryLedger::TerminalRecord existing;
        if (exec_ledger_.terminal_record(local_id, existing)) return false;
    }
    LocalOrderState state;
    if (!try_get_local(local_id, state)) return false;
    if (state.qty_fp <= 0) return false;
    // venue 口径累计（注册封印水位 + Σ 非 absorbed fill）；未达委托量不派生。
    if (ledger_effective_filled_fp_(local_id) < state.qty_fp) return false;

    XtOrderExecUpdate upd;
    upd.id = local_id;
    upd.sid_id = xt_identity::OrderAttributionStore::lookup_order_strategy_sid(local_id).raw();
    upd.is_buy = state.is_buy;
    upd.status = XtOrderStatus::Filled;
    upd.price_fp = order_exec_to_fp(state.price);
    upd.qty_fp = state.qty_fp;
    upd.filled_fp = state.qty_fp;
    upd.leaves_fp = 0;
    upd.last_fill_fp = 0;
    upd.local_cb_us = local_cb_us;
    apply_local_to_update(state, upd);

    mark_terminal(local_id, &state);
    record_terminal_projection(local_id, upd,
                               xt_live::gateway::TerminalProvenance::kSynthCumFilled);
    synth_cum_filled_total_.fetch_add(1, std::memory_order_relaxed);
    if (!should_emit(local_id, upd)) return false;
    out = std::move(upd);
    return true;
}

// ── 本地元数据回填 ──────────────────────────────────────────────

bool CtpStateTracker::try_get_local(uint64_t local_id, LocalOrderState& out) const {
    std::lock_guard<std::mutex> guard(mu_);
    auto it = local_.find(local_id);
    if (it == local_.end()) return false;
    out = it->second;
    return true;
}

void CtpStateTracker::apply_local_to_update(const LocalOrderState& state, XtOrderExecUpdate& upd) {
    if (state.is_close) {
        upd.close_offset = state.is_today ? XtOffset::CloseToday : XtOffset::CloseYesterday;
    } else {
        upd.close_offset = XtOffset::Open;
    }
}

void CtpStateTracker::apply_local_to_trade(const LocalOrderState& state, XtTradeRecord& tr) {
    tr.user_tag = state.user_tag;
    // trace_id 此处留空，不从 user_tag 复制；报表侧（XtLiveReportAdapter）空值时 fallback user_tag。
    tr.local_submit_begin_us = state.submit_begin_us;
    tr.local_submit_end_us = state.submit_end_us;
    if (tr.close_offset == XtOffset::Close && state.is_close) {
        tr.close_offset = state.is_today ? XtOffset::CloseToday : XtOffset::CloseYesterday;
    }
}

void CtpStateTracker::update_local_cancel_fields(uint64_t local_id, const CThostFtdcOrderField* pOrder) {
    if (local_id == 0 || !pOrder) return;
    std::string venue_key;
    {
        std::lock_guard<std::mutex> guard(mu_);
        auto it = local_.find(local_id);
        if (it == local_.end()) return;
        LocalOrderState& state = it->second;
        if (pOrder->OrderRef[0] != '\0') state.ref = pOrder->OrderRef;
        if (pOrder->InstrumentID[0] != '\0') state.code = pOrder->InstrumentID;
        if (pOrder->ExchangeID[0] != '\0') state.exchange_id = pOrder->ExchangeID;
        if (pOrder->OrderSysID[0] != '\0') state.order_sys_id = pOrder->OrderSysID;
        // CTP FrontID/SessionID 合法值可为负；0 是「未回填」哨兵。
        if (pOrder->FrontID != 0) state.front_id = pOrder->FrontID;
        if (pOrder->SessionID != 0) state.session_id = pOrder->SessionID;
        venue_key = build_terminal_order_key(state.exchange_id, state.order_sys_id);
    }
    // 交付投影侧回填 venue 键（exchange|sys_id 齐备才有键；空键不入索引）。
    if (!venue_key.empty()) exec_ledger_.set_venue_key(local_id, venue_key);
}

void CtpStateTracker::backfill_cancel_routing(uint64_t local_id,
                                              const std::string& exchange_id,
                                              const std::string& order_sys_id,
                                              int front_id,
                                              int session_id) {
    if (local_id == 0) return;
    std::string venue_key;
    {
        std::lock_guard<std::mutex> guard(mu_);
        auto it = local_.find(local_id);
        if (it == local_.end()) return;
        LocalOrderState& state = it->second;
        if (!exchange_id.empty()) state.exchange_id = exchange_id;
        if (!order_sys_id.empty()) state.order_sys_id = order_sys_id;
        if (front_id != 0) state.front_id = front_id;
        if (session_id != 0) state.session_id = session_id;
        venue_key = build_terminal_order_key(state.exchange_id, state.order_sys_id);
    }
    // 交付投影侧回填 venue 键（reverse-sync rebuild 路由与投影索引同步）。
    if (!venue_key.empty()) exec_ledger_.set_venue_key(local_id, venue_key);
}

int64_t CtpStateTracker::ledger_effective_filled_fp_(uint64_t local_id) const {
    xt_live::gateway::XtExecDeliveryLedger::ExecOrderRecord rec;
    if (!exec_ledger_.get_record(local_id, rec)) return 0;
    // venue 口径 = 注册封印水位（pre-restart 已成交；重放行多以 orderless 记账
    // 不附着本记录，由水位承接）+ Σ 非 absorbed fill（本进程交付的新成交）。
    // absorbed 附着行属水位口径，跳过防止双计。
    int64_t filled = rec.absorbed_watermark_fp;
    for (const auto& f : rec.fills) {
        if (!f.absorbed) filled += f.qty_fp;
    }
    return filled;
}

uint64_t CtpStateTracker::fill_epoch_us(const CThostFtdcTradeField* pTrade) {
    if (!pTrade) return 0;
    // CTP TradeDate 是自然日（夜盘不翻）、TradeTime 是秒级 CST 墙上时间；
    // 与 on_trade 填 fill_ts_us 同一还原口径。
    const uint32_t date = XtTimeParse::parse_fixed_date_yyyymmdd(pTrade->TradeDate);
    const uint32_t time_ms = XtTimeParse::parse_fixed_time_hms_ms(pTrade->TradeTime, 0);
    return xt_time::cst_wall_to_epoch_us(date, time_ms);
}

// ── 去重辅助 ──────────────────────────────────────────────

bool CtpStateTracker::should_emit(uint64_t local_id, const XtOrderExecUpdate& upd) {
    if (local_id == 0) return true;
    std::lock_guard<std::mutex> guard(mu_);
    // 去重 + 状态单调性保障：仅当状态有实质推进时才返回给调用方处理。
    // final update 不清元数据：终态后的迟到 fill 仍需归因（user_tag/sid/
    // close-offset），元数据随终态窗 FIFO 淘汰（见 mark_terminal cap 联动）。
    return deduper_.should_emit(local_id, upd, ctp_util::status_rank);
}

std::string CtpStateTracker::build_terminal_order_key(const std::string& exchange_id,
                                                      const std::string& order_sys_id) {
    if (exchange_id.empty() || order_sys_id.empty()) return "";
    std::string key;
    key.reserve(exchange_id.size() + order_sys_id.size() + 1);
    key.append(exchange_id);
    key.push_back('|');
    key.append(order_sys_id);
    return key;
}

bool CtpStateTracker::mark_terminal(uint64_t local_id, const LocalOrderState* state) {
    if (local_id == 0) return false;
    std::lock_guard<std::mutex> guard(mu_);
    const bool newly_terminal = terminal_local_ids_.insert(local_id).second;
    if (newly_terminal) {
        terminal_local_fifo_.push_back(local_id);
        constexpr size_t kTerminalLocalCap = 200000;
        while (terminal_local_fifo_.size() > kTerminalLocalCap) {
            const uint64_t old = terminal_local_fifo_.front();
            terminal_local_ids_.erase(old);
            // 元数据保留期与终态窗同界：淘汰出窗即失忆（与交付投影终态域
            // cap 同量级，之后的迟到 fill 走无记录规则）。
            local_.erase(old);
            terminal_local_fifo_.pop_front();
        }
    }

    if (state) {
        const std::string key = build_terminal_order_key(state->exchange_id, state->order_sys_id);
        remember_terminal_key_unlocked(key);
    }

    // 终态后元数据保留（迟到 fill 归因 / error-26 与 completion 推断），随终态窗
    // FIFO 淘汰或同 id 重注册清理；此处只回填交易所终态键。
    auto it = local_.find(local_id);
    if (it != local_.end()) {
        const std::string key = build_terminal_order_key(it->second.exchange_id, it->second.order_sys_id);
        remember_terminal_key_unlocked(key);
    }
    return newly_terminal;
}

void CtpStateTracker::clear_terminal_local(uint64_t local_id) {
    if (local_id == 0) return;
    if (terminal_local_ids_.erase(local_id) == 0) return;
    for (auto it = terminal_local_fifo_.begin(); it != terminal_local_fifo_.end(); ++it) {
        if (*it != local_id) continue;
        terminal_local_fifo_.erase(it);
        break;
    }
}

void CtpStateTracker::remember_terminal_key_unlocked(const std::string& key) {
    if (key.empty()) return;
    if (!terminal_order_keys_.insert(key).second) return;
    terminal_order_key_fifo_.push_back(key);
    constexpr size_t kTerminalOrderKeyCap = 200000;
    while (terminal_order_key_fifo_.size() > kTerminalOrderKeyCap) {
        const std::string& old = terminal_order_key_fifo_.front();
        terminal_order_keys_.erase(old);
        terminal_order_key_fifo_.pop_front();
    }
}

// ── 交付投影写点 ──────────────────────────────────────────────

void CtpStateTracker::record_terminal_projection(uint64_t local_id,
                                                 const XtOrderExecUpdate& upd,
                                                 xt_live::gateway::TerminalProvenance prov) {
    if (local_id == 0) return;
    xt_live::gateway::XtExecDeliveryLedger::TerminalRecord term;
    term.status = upd.status;
    term.filled_fp = upd.filled_fp;
    term.prov = prov;
    term.seen_us = upd.local_cb_us;
    // 首个终态生效；重复终态由投影幂等吸收（duplicate_terminal_ignored 计数）。
    if (exec_ledger_.record_terminal(local_id, term)) return;
    // keep-Filled 冲突观测：合成 Filled（cum>=qty 由记账 fills 背书）后迟到的
    // 非 Filled 部分成交终态不降级重放（去重器幂等吸收），仅侧道计数
    // （observe before arbitrate）。
    xt_live::gateway::XtExecDeliveryLedger::TerminalRecord existing;
    if (exec_ledger_.terminal_record(local_id, existing) &&
        existing.status == XtOrderStatus::Filled &&
        existing.prov == xt_live::gateway::TerminalProvenance::kSynthCumFilled &&
        upd.status != XtOrderStatus::Filled &&
        upd.filled_fp < upd.qty_fp) {
        cum_filled_conflict_total_.fetch_add(1, std::memory_order_relaxed);
    }
}
