#pragma once
#include <atomic>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "ctp/common/CTPUtil.h"
#include "xt_core/order/XtOrderLifecycle.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_live/gateway/common/XtExecDeliveryLedger.h"
#include "ThostFtdcTraderApi.h"

/// @brief CTP 回报的翻译与去重层：把 CTP 各种回报转成内部统一订单/成交事件。
///
/// CTP 的委托回报、成交回报、查询应答和撤单错误分散在多个 SPI 回调里，
/// 字段口径也不统一。CtpStateTracker 做三件事：
/// - 把这些回报翻译成内部 `XtOrderExecUpdate` / `XtTradeRecord`
/// - 记住每笔订单的 CTP 路由字段（OrderRef/OrderSysID/FrontID/SessionID），撤单时反查
/// - 对重复回报去重，防止同一事件被处理两次
///
/// 成交去重判据 = 交付投影（`XtExecDeliveryLedger`）的 fill 记账（查+记一体，
/// exactly-once：同 key 任一来源先到恰好记账一次；记忆按进程生命周期保留，
/// recovery 不清）。终态 payload 同步写入投影。fill 的 absorbed/delivered 语义
/// 由 caller 按来源规则决定（`TraderCTP::on_rtn_trade_`）。
///
/// 只做翻译和记录，不调 gateway callbacks、不记账、不做风控。
/// 内部由 mutex 保护，线程安全。
class CtpStateTracker {
public:
    /// @brief CTP 撤单请求所需的路由字段快照。
    ///
    /// 由 `get_cancel_target()` 返回给 `TraderCTP::cancel_order()`，用于填充
    /// `CThostFtdcInputOrderActionField`。`ref` 是最小必需字段；其余字段来自
    /// `OnRtnOrder` 回填，缺失时调用方可按当前 front/session 做 conservative fallback。
    struct CancelTarget {
        std::string ref;
        std::string code;
        std::string exchange_id;
        std::string order_sys_id;
        int front_id = 0;
        int session_id = 0;
    };

    void register_order(uint64_t local_id,
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
                        int64_t absorbed_watermark_fp = 0);

    void rollback_register(uint64_t local_id);

    void note_submit_end(uint64_t local_id, uint64_t submit_end_us);

    bool get_order_ref(uint64_t local_id, std::string& out_ref) const;

    bool has_local_order_ref(const char* order_ref) const;

    bool get_cancel_target(uint64_t local_id, CancelTarget& out) const;

    bool is_local_terminal(uint64_t local_id) const;

    void backfill_cancel_routing(uint64_t local_id,
                                 const std::string& exchange_id,
                                 const std::string& order_sys_id,
                                 int front_id,
                                 int session_id);

    bool on_order_insert_reject(const CThostFtdcInputOrderField* pInputOrder,
                                const CThostFtdcRspInfoField* pRspInfo,
                                uint64_t local_cb_us,
                                XtOrderExecUpdate& out);

    bool on_order(const CThostFtdcOrderField* pOrder,
                  uint64_t local_cb_us,
                  XtOrderExecUpdate& out,
                  bool* out_suppressed_terminal = nullptr);

    bool on_trade(const CThostFtdcTradeField* pTrade,
                  uint64_t local_cb_us,
                  XtTradeRecord& out,
                  xt_live::gateway::FillSource fill_source =
                      xt_live::gateway::FillSource::kLivePush,
                  bool absorbed = false,
                  uint32_t query_gen = 0);

    bool on_cancel_action_error(const char* order_ref,
                                int error_id,
                                uint64_t local_cb_us,
                                XtOrderExecUpdate& out);

    bool make_cum_filled_completion(uint64_t local_id,
                                    uint64_t local_cb_us,
                                    XtOrderExecUpdate& out);

    const xt_live::gateway::XtExecDeliveryLedger& exec_ledger() const { return exec_ledger_; }

    uint64_t watermark_breach_total() const {
        return watermark_breach_total_.load(std::memory_order_relaxed);
    }

    uint64_t synth_cum_filled_total() const {
        return synth_cum_filled_total_.load(std::memory_order_relaxed);
    }

    uint64_t cum_filled_conflict_total() const {
        return cum_filled_conflict_total_.load(std::memory_order_relaxed);
    }

    static uint64_t fill_epoch_us(const CThostFtdcTradeField* pTrade);

private:

    struct LocalOrderState {

        std::string ref;
        std::string code;
        std::string exchange_id;
        std::string order_sys_id;
        int front_id = 0;
        int session_id = 0;

        bool is_buy = true;
        double price = 0.0;
        int64_t qty_fp = 0;
        std::string user_tag;
        bool is_close = false;
        bool is_today = false;

        uint64_t submit_begin_us = 0;
        uint64_t submit_end_us = 0;
    };

    bool try_get_local(uint64_t local_id, LocalOrderState& out) const;

    static void apply_local_to_update(const LocalOrderState& state, XtOrderExecUpdate& upd);

    static void apply_local_to_trade(const LocalOrderState& state, XtTradeRecord& tr);

    void update_local_cancel_fields(uint64_t local_id, const CThostFtdcOrderField* pOrder);

    int64_t ledger_effective_filled_fp_(uint64_t local_id) const;

    bool should_emit(uint64_t local_id, const XtOrderExecUpdate& upd);

    static std::string build_terminal_order_key(const std::string& exchange_id,
                                                const std::string& order_sys_id);

    bool mark_terminal(uint64_t local_id, const LocalOrderState* state);

    void clear_terminal_local(uint64_t local_id);

    void remember_terminal_key_unlocked(const std::string& key);

    void record_terminal_projection(uint64_t local_id,
                                    const XtOrderExecUpdate& upd,
                                    xt_live::gateway::TerminalProvenance prov);

    mutable std::mutex mu_;

    std::unordered_map<uint64_t, LocalOrderState> local_;
    order_lifecycle::XtOrderUpdateDeduper deduper_;

    std::unordered_set<uint64_t> terminal_local_ids_;
    std::deque<uint64_t> terminal_local_fifo_;
    std::unordered_set<std::string> terminal_order_keys_;
    std::deque<std::string> terminal_order_key_fifo_;

    xt_live::gateway::XtExecDeliveryLedger exec_ledger_;
    std::atomic<uint64_t> watermark_breach_total_{0};
    std::atomic<uint64_t> synth_cum_filled_total_{0};
    std::atomic<uint64_t> cum_filled_conflict_total_{0};
};
