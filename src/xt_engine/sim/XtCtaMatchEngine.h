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
        int64_t fill_latency_ns   = 0;  ///< 成交回报（trade + 同源 Filled update）可 drain 的延迟
        int64_t cancel_latency_ns = 0;  ///< cancel 类 order update 可 drain 的延迟
    };

    /// @brief enqueue_pending() 接收的待撮合订单。
    struct PendingFill {
        char          code[32]       = {};  ///< nul-terminated symbol
        bool          is_buy         = true;
        int64_t       qty_fp         = 0;
        XtOffset      offset         = XtOffset::Open;
        XtTimeInForce tif            = XtTimeInForce::IOC;
        int64_t       limit_price_fp = 0;   ///< <= 0 表示市价语义
        char          tag[64]        = {};  ///< 透传到 XtTradeRecord::user_tag
        uint64_t      order_id       = 0;
    };

    /// @brief drain_updates() 输出的订单状态更新。
    struct PendingOrderUpdate {
        uint64_t      order_id    = 0;
        char          code[32]    = {};  ///< nul-terminated symbol
        bool          is_buy      = true;
        XtOffset      offset      = XtOffset::Open;
        int64_t       price_fp    = 0;
        int64_t       qty_fp      = 0;
        XtOrderStatus status      = XtOrderStatus::Submitted;
        int64_t       filled_fp   = 0;
        int64_t       leaves_fp   = 0;
        int64_t       last_fill_fp = 0;
        uint32_t      date        = 0;  ///< YYYYMMDD
        uint32_t      time_ms     = 0;  ///< HHMMSSmmm
    };

    // ── Configuration ──────────────────────────────────────────────

    /// @brief 更新后续事件使用的延迟参数；已入队事件的 visible_at 不重算。
    /// @note 预期 bootstrap 装配阶段调用；运行期改 latency 只影响其后入队的事件。
    void configure(const Config& cfg) { cfg_ = cfg; }

    // 订单 ID

    /// @brief 分配本地订单 ID；调用方写入 PendingFill::order_id。
    uint64_t alloc_order_id() { return next_order_id_++; }

    // 挂单管理

    /// @brief 将已构造的订单加入 pending 队列。
    void enqueue_pending(const PendingFill& pf) { pending_.push_back(pf); }

    /// @brief 撤销 pending 队列中的订单；成功时输出 Canceled update。
    bool cancel(uint64_t order_id);

    // 撮合驱动

    /// @brief 按 tick 价格撮合指定合约的 pending 订单。
    ///
    /// `code` 必须非空且与 `PendingFill::code` 使用同一 nul-terminated symbol 口径。
    /// @param time_ms HHMMSSmmm
    /// @return 成交笔数
    int process_pending(const char* code, int64_t price_fp,
                        uint32_t date, uint32_t time_ms);

    /// @brief next-bar-open 回测语义：按传入价格成交 pending 订单，跳过限价检查。
    ///
    /// `code` 必须非空且与 `PendingFill::code` 使用同一 nul-terminated symbol 口径。
    /// @param time_ms HHMMSSmmm
    /// @return 成交笔数
    int process_pending_ignore_limit(const char* code, int64_t price_fp,
                                     uint32_t date, uint32_t time_ms);

    /// @brief 直接产生成交，不进入 pending 队列。
    /// @param time_ms HHMMSSmmm
    bool fill_immediate(const char* code, bool is_buy, int64_t qty_fp,
                        int64_t price_fp, const char* tag, XtOffset offset,
                        uint32_t date, uint32_t time_ms);

    /// @brief 日结收尾：市价单按最新价成交，限价单输出 Canceled update。
    /// @param time_ms HHMMSSmmm
    /// @param allow_market_fill 传入时间戳是否为 in-session 合法时刻。false 时市价单
    ///        不成交、走 Canceled（与限价单及「无法定价」两分支同形）——本引擎不认识
    ///        交易时段，该判定由持有 session 配置的调用方下传。
    /// @return 日结成交笔数（仅计本次 flush 内新产成交，不含被结清的在途延迟事件）
    /// @note 返回后双延迟队列已结清：本 flush 产物与 session 内在途延迟事件全部
    ///       立即可 drain，不遗留到下一 session（调用方 flush 后的 drain 依赖此契约）。
    int flush_session_end(uint32_t date, uint32_t time_ms, bool allow_market_fill = true);

    /// @brief 更新合约最新价；flush_session_end 用于市价单成交价。
    void update_price(const char* code, int64_t price_fp) {
        last_price_fp_[code] = price_fp;
    }

    // 时钟与延迟事件

    /// @brief 推进撮合时钟并释放到期延迟事件。
    /// @param time_ms HHMMSSmmm
    /// @return 本次释放的 trade + order update 数量
    int advance_clock(uint32_t date, uint32_t time_ms);

    /// @brief 查询最早到期事件的 epoch ns。
    /// @return INT64_MAX 表示无 pending 延迟事件
    int64_t peek_earliest_due_ns() const noexcept { return earliest_due_ns_; }

    // 结果取出

    /// @brief 追加并清空已可见的 raw trades。
    void drain_filled_trades(std::vector<XtTradeRecord>& out) {
        if (visible_trades_.empty()) return;
        out.insert(out.end(),
                   std::make_move_iterator(visible_trades_.begin()),
                   std::make_move_iterator(visible_trades_.end()));
        visible_trades_.clear();
    }

    /// @brief 追加并清空已可见的订单状态更新。
    void drain_order_updates(std::vector<PendingOrderUpdate>& out) {
        if (visible_updates_.empty()) return;
        out.insert(out.end(),
                   std::make_move_iterator(visible_updates_.begin()),
                   std::make_move_iterator(visible_updates_.end()));
        visible_updates_.clear();
    }

    // 状态查询

    /// @brief 当前尚未撮合/撤销的 pending 订单数。
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

    /// @brief 输出订单状态更新；order_id == 0 时忽略。
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

    /// @brief 构造 raw XtTradeRecord 并写入成交可见性队列。
    bool do_fill_(const char* code, bool is_buy, int64_t qty_fp,
                  int64_t price_fp, const char* tag, XtOffset offset,
                  uint32_t date, uint32_t time_ms,
                  uint64_t order_id = 0);

    // 延迟队列
    void enqueue_delayed_trade_(const XtTradeRecord& tr, int64_t now_ns);
    /// @brief 按 latency_ns 挂入延迟队列；fill 驱动的 update 传 fill_latency_ns
    ///        （与同源 trade 同回程到达），cancel 类传 cancel_latency_ns。
    void enqueue_delayed_update_(const PendingOrderUpdate& upd, int64_t now_ns,
                                 int64_t latency_ns);
    int  promote_due_(int64_t now_ns);
    void recompute_earliest_due_();

    // 挂单与价格状态
    std::vector<PendingFill>                 pending_;
    std::unordered_map<std::string, int64_t> last_price_fp_;
    uint64_t next_order_id_ = 1;

    // 撮合时钟与延迟参数
    Config  cfg_;
    int64_t cur_clock_ns_    = INT64_MIN;  ///< advance_clock 前无有效时钟
    int64_t earliest_due_ns_ = INT64_MAX;  ///< 无延迟事件时为 INT64_MAX

    std::vector<DelayedTrade>  delay_trades_;
    std::vector<DelayedUpdate> delay_updates_;

    // 已释放可见缓冲；drain 后清空
    std::vector<XtTradeRecord>      visible_trades_;
    std::vector<PendingOrderUpdate> visible_updates_;
};
