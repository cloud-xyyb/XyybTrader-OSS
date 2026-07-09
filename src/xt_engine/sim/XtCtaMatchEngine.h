#pragma once
#include <climits>
#include <cstdint>
#include <iterator>
#include <string>
#include <unordered_map>
#include <vector>

#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrderTypes.h"

/// @brief 提供 CTA 回测 pending 订单撮合与结果输出。
///
/// 调用方通过 `enqueue_pending()` / `cancel()` 写入订单请求，通过
/// `process_pending()`、`fill_immediate()` 和 `flush_session_end()` 推进成交或撤单。
/// 成交与订单状态更新通过 `drain_trades()` / `drain_updates()` 输出。
///
/// 由回测网关，`advance_clock(date, HHMMSSmmm)` 释放到期的
/// 延迟事件；fill/cancel 延迟为 0 时事件立即可见。
class XtCtaMatchEngine {
public:
    /// @brief 撮合结果可见性延迟；单位 ns，0 表示立即可见。
    struct Config {
        int64_t fill_latency_ns   = 0;
        int64_t cancel_latency_ns = 0;
    };

    /// @brief enqueue_pending() 接收的待撮合订单。
    struct PendingFill {
        char          code[32]       = {};
        bool          is_buy         = true;
        int64_t       qty_fp         = 0;
        XtOffset      offset         = XtOffset::Open;
        XtTimeInForce tif            = XtTimeInForce::IOC;
        int64_t       limit_price_fp = 0;
        char          tag[64]        = {};
        uint64_t      order_id       = 0;
    };

    /// @brief drain_updates() 输出的订单状态更新。
    struct PendingOrderUpdate {
        uint64_t      order_id    = 0;
        char          code[32]    = {};
        bool          is_buy      = true;
        XtOffset      offset      = XtOffset::Open;
        int64_t       price_fp    = 0;
        int64_t       qty_fp      = 0;
        XtOrderStatus status      = XtOrderStatus::Submitted;
        int64_t       filled_fp   = 0;
        int64_t       leaves_fp   = 0;
        int64_t       last_fill_fp = 0;
        uint32_t      date        = 0;
        uint32_t      time_ms     = 0;
    };

    void configure(const Config& cfg) { cfg_ = cfg; }

    uint64_t alloc_order_id() { return next_order_id_++; }

    void enqueue_pending(const PendingFill& pf) { pending_.push_back(pf); }

    bool cancel(uint64_t order_id);

    int process_pending(const char* code, int64_t price_fp,
                        uint32_t date, uint32_t time_ms);

    int process_pending_ignore_limit(const char* code, int64_t price_fp,
                                     uint32_t date, uint32_t time_ms);

    bool fill_immediate(const char* code, bool is_buy, int64_t qty_fp,
                        int64_t price_fp, const char* tag, XtOffset offset,
                        uint32_t date, uint32_t time_ms);

    int flush_session_end(uint32_t date, uint32_t time_ms);

    void update_price(const char* code, int64_t price_fp) {
        last_price_fp_[code] = price_fp;
    }

    int advance_clock(uint32_t date, uint32_t time_ms);

    int64_t peek_earliest_due_ns() const noexcept { return earliest_due_ns_; }

    void drain_filled_trades(std::vector<XtTradeRecord>& out) {
        if (visible_trades_.empty()) return;
        out.insert(out.end(),
                   std::make_move_iterator(visible_trades_.begin()),
                   std::make_move_iterator(visible_trades_.end()));
        visible_trades_.clear();
    }

    void drain_order_updates(std::vector<PendingOrderUpdate>& out) {
        if (visible_updates_.empty()) return;
        out.insert(out.end(),
                   std::make_move_iterator(visible_updates_.begin()),
                   std::make_move_iterator(visible_updates_.end()));
        visible_updates_.clear();
    }

    size_t pending_count() const { return pending_.size(); }

private:
    /// 延迟 trade 事件；visible_at_ns 到期后进入 visible_trades_。
    struct DelayedTrade {
        XtTradeRecord trade;
        int64_t       visible_at_ns;
    };

    /// 延迟 order update 事件；visible_at_ns 到期后进入 visible_updates_。
    struct DelayedUpdate {
        PendingOrderUpdate update;
        int64_t            visible_at_ns;
    };

    void publish_order_update_(uint64_t order_id,
                               const char* code,
                               bool is_buy,
                               XtOffset offset,
                               int64_t price_fp,
                               int64_t qty_fp,
                               XtOrderStatus status,
                               int64_t filled_fp,
                               int64_t leaves_fp,
                               int64_t last_fill_fp,
                               uint32_t date,
                               uint32_t time_ms);

    bool do_fill_(const char* code, bool is_buy, int64_t qty_fp,
                  int64_t price_fp, const char* tag, XtOffset offset,
                  uint32_t date, uint32_t time_ms,
                  uint64_t order_id = 0);

    void enqueue_delayed_trade_(const XtTradeRecord& tr, int64_t now_ns);
    void enqueue_delayed_update_(const PendingOrderUpdate& upd, int64_t now_ns);
    int  promote_due_(int64_t now_ns);
    void recompute_earliest_due_();

    std::vector<PendingFill>                 pending_;
    std::unordered_map<std::string, int64_t> last_price_fp_;
    uint64_t next_order_id_ = 1;

    Config  cfg_;
    int64_t cur_clock_ns_    = INT64_MIN;
    int64_t earliest_due_ns_ = INT64_MAX;

    std::vector<DelayedTrade>  delay_trades_;
    std::vector<DelayedUpdate> delay_updates_;

    std::vector<XtTradeRecord>      visible_trades_;
    std::vector<PendingOrderUpdate> visible_updates_;
};
