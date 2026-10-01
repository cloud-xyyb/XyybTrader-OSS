#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>

#include "xt_core/container/XtLockFreeQueue.h"
#include "xt_util/string/XtCStringUtil.h"

namespace ct::data { class IEventStore; }   // fbs sink 前向声明

/// @brief 实盘事件录制器：producer 热路径入队，后台线程写 JSONL / fbs。
///
/// `emit_*` 只填充固定大小 Event 并 try-push 到 MPSC 队列；队列满或 recorder
/// 停止时丢弃并累加 `drop_count`。价格/金额字段统一使用 fixed-point ×1e6。
/// @warning start()/stop() 非线程安全，调用方必须串行化 lifecycle 操作。
class XtLiveRecorder {
public:
    struct Config {
        struct IntentTraceConfig {
            bool enabled = false;                 ///< 策略意图链路录制开关，默认关闭
            double sample_rate = 0.01;            ///< trace_id 维 deterministic sampling，范围 [0, 1]
            bool include_wire_debug = false;      ///< 预留 wire payload 诊断字段；默认不输出原始 payload
        };

        bool enabled = false;              ///< 总开关（master switch）
        std::string file_path;             ///< JSONL 输出文件路径（绝对/相对均可）
        size_t queue_capacity = 8192;      ///< MPSC 队列容量（向上取整至 2^n）
        // ── 运行时滚动（runtime rotation）防单文件无界增长 ──────────────
        // start() 只在进程启动时滚一次做 session 隔离；下面两项让 drain 线程在
        // 同一 session 内按大小/时间继续滚动，产出 live.jsonl.<epoch> 供 etl+prune。
        uint64_t rotate_bytes        = 1ull << 30;  ///< 当前文件 ≥ 此字节触发滚动；0=禁用按大小
        uint32_t rotate_interval_sec = 3600;        ///< 距上次滚动 ≥ 此秒触发滚动；0=禁用按时间
        IntentTraceConfig intent_trace;             ///< JSONL-only intent trace 子开关
    };

    enum EventType : uint8_t {
        kEvtMdTick            = 0,
        kEvtStrategyDecision  = 1,
        kEvtOrderSubmit       = 2,
        kEvtOrderRtn          = 3,
        kEvtTradeRtn          = 4,
        kEvtConnState         = 5,
        kEvtStrategyMetric    = 6,   ///< 策略指标连续流 (reuse report_metric API)
        // ── 多策略与控制面事件 ──────────────────────────────────────────────
        kEvtRiskReject        = 7,   ///< 风控拒单 (panel risk attribution)
        kEvtParamUpdate       = 8,   ///< 参数热更新 ack (control plane)
        kEvtKillSwitch        = 9,   ///< Kill switch 翻转 (incident timeline)
        kEvtPositionLifecycle = 10,  ///< Per-(sid, symbol) qty 0↔非零 transition
        kEvtIntentTrace       = 11,  ///< 策略意图保真度链路 tap (JSONL-only)
    };

    // JSONL 固定 20 槽（缺档补 0）；fbs event-store 按 venue 真实档数
    // md_depth_levels 写变长 depth（契约 v2，见 EventDepthContract.h）。
    static constexpr int kMdDepth = 20;

    /// @brief 录制事件（trivially copyable）— 所有 payload 字段平铺，每条事件仅填充一种。
    struct Event {
        uint8_t  type        = 0;
        uint64_t ts_local_ns = 0;
        uint64_t ts_exch_ns  = 0;

        // ── MD_TICK ─────────────────────────────────────────────────────────
        char     md_code[32]  = {};
        char     md_exchange_id[16] = {};  ///< CTP ExchangeID (SHFE/DCE/...)；binance "BINANCE"。fbs sink routing 用
        uint32_t md_trading_date = 0;      ///< trading_day (YYYYMMDD)，fbs sink 分片键
        uint32_t md_action_date = 0;       ///< action_date (YYYYMMDD)；binance 由 ts_exch_ms (UTC) 派生
        uint32_t md_action_time = 0;       ///< action_time (HHMMSSMMM)；binance 由 ts_exch_ms (UTC) 派生
        int64_t  md_bid_fp    = 0;         ///< best bid price × 1e6（= md_bid_prices_fp[0]）
        int64_t  md_ask_fp    = 0;         ///< best ask price × 1e6（= md_ask_prices_fp[0]）
        int64_t  md_last_fp   = 0;         ///< last price × 1e6
        int64_t  md_volume    = 0;
        int64_t  md_bid_prices_fp[kMdDepth] = {}; ///< kMdDepth 档买价 × 1e6（CTP 路径只填 [0..4]）
        int64_t  md_ask_prices_fp[kMdDepth] = {}; ///< kMdDepth 档卖价 × 1e6
        int64_t  md_bid_qty[kMdDepth]       = {}; ///< kMdDepth 档买量 × 1e6
        int64_t  md_ask_qty[kMdDepth]       = {}; ///< kMdDepth 档卖量 × 1e6
        int32_t  md_depth_levels = 0;             ///< venue 真实档数（CTP 5 / XTP 10 / binance 20）；fbs sink 按此写变长 depth

        // ── STRATEGY_DECISION ───────────────────────────────────────────────
        char     sd_strat_id[32]      = {};
        char     sd_code[32]          = {};
        char     sd_signal[8]         = {};   ///< "long" / "short" / "flat"
        int64_t  sd_target_pos        = 0;
        uint64_t sd_trigger_tick_ts   = 0;    ///< 触发此决策的 tick 的 ts_local_ns
        // ── book/account routing ───────────────────────────────────────────
        char     sd_book_id[32]       = {};
        char     sd_account_id[32]    = {};

        // ── ORDER_SUBMIT ────────────────────────────────────────────────────
        uint64_t os_logical_id        = 0;
        char     os_code[32]          = {};
        char     os_dir[8]            = {};   ///< "buy" / "sell"
        char     os_offset[8]         = {};   ///< "open" / "close" / "closetoday"
        int64_t  os_price_fp          = 0;    ///< × 1e6
        int64_t  os_qty               = 0;
        char     os_parent_strat[32]  = {};   ///< 兼容字段：填 user_tag，供既有 reader 读取。
        uint64_t os_decision_ts       = 0;    ///< 策略决定下单时刻 ts_local_ns (submit API entry)
        uint64_t os_trigger_tick_ts   = 0;    ///< 触发此单的 tick ts_local_ns (框架自动捕获, 0 = 非 on_tick 路径)
        char     os_reason[48]        = {};   ///< 策略决策原因 (例: "ob_imb 0.35>0.3")
        // ── multi-strategy metadata ───────────────────────────────────────
        char     os_strat_id[32]      = {};   ///< 全局唯一策略实例 ID (multi-strategy demux 主键)
        char     os_book_id[32]       = {};   ///< per-strategy sub-book id (一般 = strat_id)
        char     os_account_id[32]    = {};   ///< venue 内 account id
        char     os_user_tag[32]      = {};   ///< 策略自定义标签。
        uint64_t os_decision_mono_ns  = 0;    ///< CLOCK_MONOTONIC 提交时刻 (0 = 未知)
        bool     os_event_clock_valid = false; ///< event clock anchor 有效（BestEffort）
        int64_t  os_event_ts_local_ns = 0;     ///< event-axis 投影时刻（valid 才写 JSONL）

        // ── ORDER_RTN ───────────────────────────────────────────────────────
        uint64_t or_logical_id        = 0;
        uint64_t or_phys_id           = 0;
        char     or_status[16]        = {};   ///< "accepted"/"partial"/"filled"/"canceled"/"rejected"
        int64_t  or_filled_qty        = 0;
        char     or_err_msg[64]       = {};
        char     or_cancel_initiator[16] = {};  ///< status==canceled 时填: "user"/"ioc_timeout"/"auto"
        // ── multi-strategy demux 主键 ─────────────────────────────────────
        char     or_strat_id[32]      = {};
        char     or_book_id[32]       = {};
        char     or_account_id[32]    = {};

        // ── TRADE_RTN ───────────────────────────────────────────────────────
        uint64_t tr_trade_id          = 0;
        uint64_t tr_logical_id        = 0;
        uint32_t tr_trading_day       = 0;    ///< trading day from trade record; 0 = use current MD day
        uint32_t tr_trade_action_date = 0;    ///< trade action date (YYYYMMDD)；CTP 直读，binance 由 UTC transact time 派生
        uint32_t tr_trade_action_time = 0;    ///< trade action time (HHMMSSMMM)；CTP 秒精度，binance 毫秒精度
        char     tr_code[32]          = {};
        char     tr_exchange_id[16]   = {};
        char     tr_side[8]           = {};    ///< "buy" / "sell"
        int64_t  tr_price_fp          = 0;    ///< × 1e6
        int64_t  tr_qty               = 0;
        bool     tr_is_maker          = false; ///< maker(true)/taker(false), 来自 XtTradeRecord::is_passive
        // ── multi-strategy accounting metadata ────────────────────────────
        char     tr_strat_id[32]      = {};   ///< 拆账主键
        char     tr_book_id[32]       = {};
        char     tr_account_id[32]    = {};
        int64_t  tr_fee_fp            = 0;    ///< 成交手续费 ×1e6 (signed: 返佣 → 负值)
        int64_t  tr_realized_pnl_fp   = 0;    ///< 该笔实现 PnL ×1e6 (close 单才非零)
        bool     tr_has_realized      = false; ///< true=使用 supplied realized；false=ledger self-recompute
        bool     tr_is_close          = false;///< 是否平仓单 (派生自 OrderSubmit.offset)
        char     tr_route_status[20]  = {};   ///< routed / unmatched / operator / fallback_recovery / unknown

        // ── CONN_STATE ──────────────────────────────────────────────────────
        char     cs_gateway[8]        = {};   ///< "td" / "md"
        char     cs_state[16]         = {};
        char     cs_err_msg[64]       = {};
        // ── venue routing ─────────────────────────────────────────────────
        char     cs_venue_id[16]      = {};   ///< "binance_u" / "ctp" — 多 venue 部署区分

        // ── STRATEGY_METRIC ─────────────────────────────────────────────────
        char     sm_name[32]          = {};   ///< 指标名 (复用 IStrategyContext::report_metric)
        double   sm_value             = 0.0;  ///< 指标值 (瞬时)
        // ── book/account routing ───────────────────────────────────────────
        char     sm_strat_id[32]      = {};   ///< 此指标归属策略 (老路径 strat_id 不带，serialize 用 "")
        char     sm_book_id[32]       = {};
        char     sm_account_id[32]    = {};

        // ── RISK_REJECT ──────────────────────────────────────────────────────
        char     rj_strat_id[32]      = {};
        char     rj_book_id[32]       = {};
        char     rj_account_id[32]    = {};
        char     rj_code[32]          = {};
        char     rj_dir[8]            = {};
        int64_t  rj_price_fp          = 0;
        int64_t  rj_qty               = 0;
        char     rj_reason[48]        = {};   ///< "max_pos_qty" / "max_order_qty" / "kill_switch" / ...
        int64_t  rj_required_margin_fp = 0;
        int64_t  rj_available_margin_fp = 0;

        // ── PARAM_UPDATE ──────────────────────────────────────────────────────
        char     pu_actor[48]         = {};   ///< panel actor (IP / user) / "system"
        char     pu_strat_id[32]      = {};   ///< 空 / "_global" = 全局 risk 参数
        char     pu_key[64]           = {};   ///< e.g. "tick_window" / "risk.max_order_qty"
        char     pu_value_str[64]     = {};   ///< caller 序列化后的字符串
        uint64_t pu_apply_mono_ns     = 0;

        // ── KILL_SWITCH ──────────────────────────────────────────────────────
        char     ks_actor[48]         = {};   ///< panel actor / "auto:max_drawdown" / ...
        char     ks_state[16]         = {};   ///< "tripped" / "resumed"
        char     ks_reason[80]        = {};
        char     ks_venue_id[16]      = {};

        // ── POSITION_LIFECYCLE ───────────────────────────────────────────────
        // qty 0 → 非零 (open) / 非零 → 0 (close) / sign-flip 拆 close+open。
        // panel 读这里取真 open_time，替换 _hydrate_position_open_times heuristic。
        char     lc_strat_id[32]      = {};
        char     lc_book_id[32]       = {};
        char     lc_account_id[32]    = {};
        char     lc_code[32]          = {};
        char     lc_event_type[8]     = {};   ///< "open" / "close"
        int64_t  lc_qty_fp            = 0;    ///< signed; open=after qty, close=before qty (=0 之前的仓位)
        int64_t  lc_avg_price_fp      = 0;    ///< open=after avg, close=before avg
        int64_t  lc_realized_pnl_fp   = 0;    ///< close 时该笔实现 PnL ×1e6 (留 0,+ 填)
        uint64_t lc_hold_duration_ms  = 0;    ///< close 时 hold duration; open 时 0
        uint64_t lc_cause_trade_id    = 0;    ///< 触发 fill 的 local_cb_us (steady µs 唯一键, correlate 至 TRADE_RTN.trade_id)

        // ── INTENT_TRACE ────────────────────────────────────────────────────
        // JSONL-only，不写 EventExt/FBS。所有字符串固定长截断，避免 producer hot path 分配。
        uint64_t it_trace_id           = 0;
        char     it_stage[32]          = {};
        char     it_venue[16]          = {};
        char     it_strategy_id[32]    = {};
        char     it_book_id[32]        = {};
        char     it_account_id[32]     = {};
        char     it_code_raw[32]       = {};
        char     it_code_effective[32] = {};
        char     it_dir[8]             = {};
        int64_t  it_price_raw_fp       = 0;
        int64_t  it_price_effective_fp = 0;
        int64_t  it_price_wire_fp      = 0;
        int64_t  it_qty_raw_fp         = 0;
        int64_t  it_qty_effective_fp   = 0;
        int64_t  it_qty_wire_fp        = 0;
        char     it_tif_raw[8]         = {};
        char     it_tif_effective[8]   = {};
        char     it_tif_wire[8]        = {};
        char     it_offset_raw[16]     = {};
        char     it_offset_effective[16] = {};
        bool     it_reduce_only        = false;
        char     it_exec_flags[48]     = {};
        uint64_t it_local_id           = 0;
        uint64_t it_logical_id         = 0;
        uint64_t it_phys_id            = 0;
        uint32_t it_slice_index        = 0;
        uint32_t it_slice_count        = 0;
        bool     it_partial            = false;
        uint32_t it_failure_slice_index = 0;
        uint32_t it_accepted_slice_count = 0;
        uint32_t it_expected_slice_count = 0;
        uint64_t it_trigger_tick_ts    = 0;
        uint64_t it_decision_mono_ns   = 0;
        int64_t  it_best_bid_fp        = 0;
        int64_t  it_best_ask_fp        = 0;
        char     it_reject_scope[32]   = {};
        char     it_reject_code[32]    = {};
        char     it_reject_reason[96]  = {};
        uint64_t it_user_tag_hash      = 0;
        uint64_t it_wire_client_order_id_hash = 0;
    };
    static_assert(std::is_trivially_copyable<Event>::value,
                  "Event must be trivially copyable for lock-free queue");

    explicit XtLiveRecorder(const Config& cfg);
    ~XtLiveRecorder();

    XtLiveRecorder(const XtLiveRecorder&)            = delete;
    XtLiveRecorder& operator=(const XtLiveRecorder&) = delete;

    /// @brief 启动后台 drain 线程。若 `Config::enabled == false` 为 no-op。
    void start();

	    /// @brief 停止后台线程并 flush 剩余队列内容到文件。
	    void stop();

	    bool enabled() const noexcept { return cfg_.enabled; }
	    bool intent_trace_enabled() const noexcept { return cfg_.enabled && cfg_.intent_trace.enabled; }
	    bool should_sample_intent_trace(uint64_t trace_id) const noexcept {
	        return intent_trace_enabled() && sample_intent_trace(trace_id, cfg_.intent_trace.sample_rate);
	    }
	    uint64_t emit_count() const noexcept { return emit_count_.load(std::memory_order_relaxed); }
    uint64_t drop_count() const noexcept { return drop_count_.load(std::memory_order_relaxed); }
    uint64_t written_count() const noexcept { return written_count_.load(std::memory_order_relaxed); }
    uint64_t write_failed_count() const noexcept { return write_failed_count_.load(std::memory_order_relaxed); }
    uint64_t intent_trace_emit_count() const noexcept { return intent_trace_emit_count_.load(std::memory_order_relaxed); }
    uint64_t intent_trace_drop_count() const noexcept { return intent_trace_drop_count_.load(std::memory_order_relaxed); }
    uint64_t intent_trace_written_count() const noexcept { return intent_trace_written_count_.load(std::memory_order_relaxed); }
    uint64_t intent_trace_write_failed_count() const noexcept { return intent_trace_write_failed_count_.load(std::memory_order_relaxed); }
    /// @brief 运行时滚动累计次数（不含 start-time 滚动）。供监控/测试观测。
    uint64_t rotation_count() const noexcept { return rotation_count_.load(std::memory_order_relaxed); }

    /// @brief trace_id 维 deterministic sampling；同一 trace 的所有 stage 决策一致。
    static bool sample_intent_trace(uint64_t trace_id, double sample_rate) noexcept;

    // ── Hot-path emit API（try-push、drop-on-full）──────────────────────────

    /// @brief MD tick 录制。
    /// @note `ts_local_ns` 是本机 steady ns；交易日/action day/time 由 gateway 先归一化。
    ///       `exchange_id` 同时作为 fbs sink 分片键的一部分。
    /// @param depth_levels venue 真实盘口档数（1..kMdDepth）；fbs sink 按此写
    ///        变长 depth（JSONL 恒 20 槽不受影响），越界值由 sink 侧 clamp。
    void emit_md_tick(uint64_t ts_local_ns,
                      uint32_t trading_date,
                      uint32_t action_date,
                      uint32_t action_time,
                      const char* code,
                      const char* exchange_id,
                      int64_t last_fp,
                      int64_t volume,
                      const int64_t bid_prices_fp[kMdDepth],
                      const int64_t ask_prices_fp[kMdDepth],
                      const int64_t bid_qty[kMdDepth],
                      const int64_t ask_qty[kMdDepth],
                      int depth_levels);

    // fbs event store sink（并列 JSONL，drain 线程内写）──

    /// @brief 注入 fbs event store sink；下一条 drain 事件起生效。
    /// @note caller 保证 store 生命周期覆盖至 stop() 完成；写 release / 读 acquire。
    void set_event_store(ct::data::IEventStore* store) noexcept;

    /// @brief 设置 venue kind，影响 fbs sink 的 SourceKind 枚举值。
    /// @note MD_TICK 优先由 `md_exchange_id` 派生；其他 EventExt 使用此值兜底。
    ///       0=UNKNOWN，1=LIVE_CTP，4=LIVE_BINANCE，5=LIVE_XTP。
    void set_venue_kind(int8_t kind) noexcept;

    /// @brief 策略决策事件录制。
    /// @note 调用方需显式接入 producer；未接入时 recorder 不会自动生成 STRATEGY_DECISION。
    ///       timeline.py/etl 已按 STRATEGY_DECISION_SCHEMA 消费该事件。
    /// @param strat_id      全局唯一策略实例 ID（multi-strategy demux 主键）
    /// @param book_id       per-strategy sub-book id (default null = 走 strat_id 别名)
    /// @param account_id    venue 内 account id (default null = "")
    void emit_strategy_decision(uint64_t ts_local_ns,
                                const char* strat_id,
                                const char* code,
                                const char* signal,
                                int64_t target_pos,
                                uint64_t trigger_tick_ts,
                                const char* book_id    = nullptr,
                                const char* account_id = nullptr);

    /// @brief 订单提交录制参数；使用 aggregate 避免长参数列表传错位置。
    struct OrderSubmitParams {
        uint64_t    ts_local_ns      = 0;
        uint64_t    logical_id       = 0;
        const char* code             = nullptr;
        const char* dir              = nullptr;   ///< "buy" / "sell"
        const char* offset           = nullptr;   ///< "open" / "close" / "close_today" / "close_yesterday"
        int64_t     price_fp         = 0;
        int64_t     qty              = 0;
        const char* parent_strat_id  = nullptr;   ///< 兼容字段；callsite 应传 strat_id。
        uint64_t    decision_ts      = 0;         ///< 策略 submit API entry ns; 0 = unknown
        uint64_t    trigger_tick_ts  = 0;         ///< 触发该决策的 tick ts; 0 = 非 on_tick 路径
        const char* reason           = nullptr;   ///< 策略自报原因
        const char* strat_id         = nullptr;   ///< multi-strategy demux primary key
        const char* book_id          = nullptr;   ///< per-strategy sub-book
        const char* account_id       = nullptr;   ///< venue-internal account
        const char* user_tag         = nullptr;   ///< 策略自定义标签 (separate from strat_id)
        uint64_t    decision_mono_ns = 0;         ///< CLOCK_MONOTONIC 提交时刻
        bool        event_clock_valid = false;    ///< event clock anchor 有效（BestEffort observability）
        int64_t     event_ts_local_ns = 0;         ///< event-axis 投影时刻；invalid 时 0
    };

    /// @brief 订单提交事件录制。
    /// @note `parent_strat_id` 为兼容字段；主维度以 strat_id / book_id / account_id 为准。
    void emit_order_submit(const OrderSubmitParams& p);

    /// @brief 订单回报事件录制（multi-strategy 升级版本签名）。
    ///
    /// strat_id / book_id / account_id 为顶层字段，ORDER_RTN 不再需反查 OrderSubmit。
    void emit_order_rtn(uint64_t ts_local_ns,
                        uint64_t logical_id,
                        uint64_t phys_id,
                        const char* status,
                        int64_t filled_qty,
                        const char* err_msg,
                        const char* cancel_initiator,             ///< 仅 status==Canceled 时生效; null/"" = 未分类
                        const char* strat_id    = nullptr,
                        const char* book_id     = nullptr,
                        const char* account_id  = nullptr);

    /// @brief 成交回报录制参数；使用 aggregate 避免长参数列表。
    struct TradeRtnParams {
        uint64_t    ts_local_ns        = 0;
        uint64_t    ts_exch_ns         = 0;
        uint32_t    trading_day        = 0;       ///< YYYYMMDD; 0 = recorder current MD day fallback
        uint32_t    trade_action_date  = 0;       ///< YYYYMMDD; CTP 直读, binance 由 UTC 派生
        uint32_t    trade_action_time  = 0;       ///< HHMMSSmmm
        uint64_t    trade_id           = 0;       ///< local_cb_us (steady µs 唯一关联键); 非交易所 TradeID 字符串
        uint64_t    logical_id         = 0;
        const char* code               = nullptr;
        const char* exchange_id        = nullptr;
        const char* side               = nullptr; ///< "buy" / "sell"
        int64_t     price_fp           = 0;
        int64_t     qty                = 0;
        bool        is_maker           = false;
        const char* strat_id           = nullptr; ///< 拆账主键
        const char* book_id            = nullptr;
        const char* account_id         = nullptr;
        int64_t     fee_fp             = 0;       ///< signed (rebate negative)
        int64_t     realized_pnl_fp    = 0;       ///< 引擎 stamp 的权威 realized（close 单非零）
        bool        has_realized       = false;   ///< true=采用 supplied realized；false=未 stamp，realized 计 0
        bool        is_close           = false;   ///< 引擎 stamp（XtTradeRecord::is_close）
        const char* route_status       = nullptr; ///< sidecar triage status
    };

    /// @brief 成交回报事件录制；拆账字段直接写入事件，不依赖反查 OrderSubmit。
    void emit_trade_rtn(const TradeRtnParams& p);

    /// @brief 网关连接状态事件录制（multi-strategy 升级版本签名）。
    void emit_conn_state(uint64_t ts_local_ns,
                         const char* gateway,
                         const char* state,
                         const char* err_msg,
                         const char* venue_id = nullptr);  ///< multi-venue routing

    /// @brief 策略指标 JSONL 落盘，由 XtStrategyContext::report_metric 驱动。
    /// @note Prometheus 用于 1s 聚合告警，JSONL 保留逐次调用用于离线分析。
    void emit_strategy_metric(uint64_t ts_local_ns,
                              const char* name,
                              double value,
                              const char* strat_id    = nullptr,
                              const char* book_id     = nullptr,
                              const char* account_id  = nullptr);

    // ─── 控制与风控事件类型 ────────────────────────────────────────────────
    // @note 下列 emit_risk_reject / emit_param_update / emit_kill_switch 只在调用方显式接入
    //       producer 时落盘；etl/parquet schema 已接受 RISK_REJECT / PARAM_UPDATE /
    //       KILL_SWITCH。xt_live::control::detail::emit_kill_switch_transition
    //       (HftControlSafetyState.cpp) 是 prom gauge 路径，不调用本 recorder emit_kill_switch。

    /// @brief 风控拒单事件录制，供 panel / report 按 strategy 与 reason 拆账。
    void emit_risk_reject(uint64_t ts_local_ns,
                          const char* strat_id,
                          const char* book_id,
                          const char* account_id,
                          const char* code,
                          const char* dir,
                          int64_t price_fp,
                          int64_t qty,
                          const char* reason,
                          int64_t required_margin_fp = 0,
                          int64_t available_margin_fp = 0);

    /// @brief 参数热更新事件录制，追踪 actor/key/value/apply time。
    void emit_param_update(uint64_t ts_local_ns,
                           const char* actor,
                           const char* strat_id,    ///< 空 / "_global" = 全局 risk 参数
                           const char* key,
                           const char* value_str,   ///< caller 序列化后的字符串
                           uint64_t apply_mono_ns);

    /// @brief Kill switch 翻转事件录制，保留 actor/state/reason/venue。
    void emit_kill_switch(uint64_t ts_local_ns,
                          const char* actor,
                          const char* state,       ///< "tripped" / "resumed"
                          const char* reason,
                          const char* venue_id);

    /// @brief 持仓 lifecycle 录制参数；由 per-(strategy_id, code) zero-cross 触发。
    /// @note sign-flip 拆成 close+open；panel 读取 open_time / hold_duration / realized。
    struct PositionLifecycleParams {
        uint64_t    ts_local_ns      = 0;     ///< steady ns (frame ts; 与其它 event 一致)
        uint64_t    ts_exch_ns       = 0;     ///< wall ns (= cause trade fill_ts_us × 1000)
        const char* strat_id         = nullptr;
        const char* book_id          = nullptr;
        const char* account_id       = nullptr;
        const char* code             = nullptr;
        const char* event_type       = nullptr;  ///< "open" / "close"
        int64_t     qty_fp           = 0;        ///< signed; open=after, close=before (=0 之前的仓)
        int64_t     avg_price_fp     = 0;
        int64_t     realized_pnl_fp  = 0;        ///< close only (留 0)
        uint64_t    hold_duration_ms = 0;        ///< close only
        uint64_t    cause_trade_id   = 0;        ///< correlate 至 TRADE_RTN
    };

    /// @brief Per-strategy 持仓 lifecycle 事件录制。
    void emit_position_lifecycle(const PositionLifecycleParams& p);

    /// @brief 策略意图链路 trace 参数；字段名与 tests/fixtures/intent_trace/schema.json 对齐。
    struct IntentTraceParams {
        uint64_t    trace_id           = 0;
        const char* stage              = nullptr;
        const char* venue              = nullptr;
        const char* strategy_id        = nullptr;
        const char* book_id            = nullptr;
        const char* account_id         = nullptr;
        const char* code_raw           = nullptr;
        const char* code_effective     = nullptr;
        const char* dir                = nullptr;
        int64_t     price_raw_fp       = 0;
        int64_t     price_effective_fp = 0;
        int64_t     price_wire_fp      = 0;
        int64_t     qty_raw_fp         = 0;
        int64_t     qty_effective_fp   = 0;
        int64_t     qty_wire_fp        = 0;
        const char* tif_raw            = nullptr;
        const char* tif_effective      = nullptr;
        const char* tif_wire           = nullptr;
        const char* offset_raw         = nullptr;
        const char* offset_effective   = nullptr;
        bool        reduce_only        = false;
        const char* exec_flags         = nullptr;
        uint64_t    local_id           = 0;
        uint64_t    logical_id         = 0;
        uint64_t    phys_id            = 0;
        uint32_t    slice_index        = 0;
        uint32_t    slice_count        = 0;
        bool        partial            = false;
        uint32_t    failure_slice_index = 0;
        uint32_t    accepted_slice_count = 0;
        uint32_t    expected_slice_count = 0;
        uint64_t    trigger_tick_ts    = 0;
        uint64_t    decision_mono_ns   = 0;
        uint64_t    event_mono_ns      = 0;
        int64_t     best_bid_fp        = 0;
        int64_t     best_ask_fp        = 0;
        const char* reject_scope       = nullptr;
        const char* reject_code        = nullptr;
        const char* reject_reason      = nullptr;
        uint64_t    user_tag_hash      = 0;
        uint64_t    wire_client_order_id_hash = 0;
    };

    /// @brief JSONL-only 策略意图链路 trace；默认关闭，且按 trace_id 采样。
    void emit_intent_trace(const IntentTraceParams& p);

    // ── Cancel initiator registry ──────────────────────────────────────────────
    // 发起 cancel 时 mark，ORDER_RTN status==canceled 时 consume；未命中统一为 "auto"。
    static void note_cancel_initiated(uint64_t order_id, const char* initiator) noexcept;
    /// @brief 消费并清除 order_id 对应的 initiator; 未命中写入 "auto".
    static void consume_cancel_initiator(uint64_t order_id, char* out, size_t cap) noexcept;

    // ── Venue reject err registry ──────────────────────────────────────────────
    // gateway 在产生终态 Rejected 回报处 mark（err_code/err_msg 不进
    // XtOrderExecUpdate plugin ABI），ORDER_RTN status==rejected 时 consume 填
    // err_msg；local 风控拒单不走此路（record_reject 直填 reason）。
    /// @brief 登记 venue 拒单原因；err_code != 0 时存为 "<code>: <msg>"。
    /// @note order_id==0 或 code/msg 全空时忽略。
    static void note_order_reject(uint64_t order_id, int err_code, const char* err_msg) noexcept;
    /// @brief 消费并清除 order_id 对应的拒单原因；未命中写入空串。
    static void consume_order_reject_err(uint64_t order_id, char* out, size_t cap) noexcept;

    static void set_global(XtLiveRecorder* rec) noexcept;
    static XtLiveRecorder* global() noexcept;

private:
    /// @brief 将固定长度字符串字段安全 copy（保证 null-terminate，截断超长输入）。
    static void copy_fixed(char* dst, size_t dst_cap, const char* src) noexcept {
        XtCStringUtil::copy(dst, dst_cap, src);
    }

    /// @brief 后台线程主循环：drain 队列 → 序列化 → 写文件。
    void drain_loop();

    /// @brief drain 线程在批处理/空闲边界检查是否需要按大小或时间滚动当前文件。
    void maybe_rotate();

    /// @brief 滚动当前 live.jsonl → live.jsonl.<epoch>（并联动滚动 fbs event store），
    ///        重开新文件。drain 线程独占调用；rename/open 失败不致命（telemetry 降级）。
    /// @param reason   "size" / "time"，仅用于日志。
    /// @param now_sec  当前 wallclock epoch 秒（rename 后缀 + 时间窗复位）。
    void rotate_active_file(const char* reason, int64_t now_sec);

    /// @brief 序列化单条 Event 为 JSON Lines 格式并追加到文件流。
    void serialize_and_write(const Event& ev);

    /// @brief 尝试入队；失败则累加 drop_count 并静默丢弃。
    void push_or_drop(Event&& ev) noexcept;

    // ── serialize helpers per event type（drain 线程独占，冷路径）──
    // helper 复用 thread_local JSON buffer 与 drain 独占 FlatBufferBuilder。
    void serialize_md_tick(const Event& ev, std::string& os,
                           ct::data::IEventStore* es, void* fbb_ref);
    void serialize_strategy_decision(const Event& ev, std::string& os,
                                     ct::data::IEventStore* es, void* fbb_ref);
    void serialize_order_submit(const Event& ev, std::string& os,
                                ct::data::IEventStore* es, void* fbb_ref);
    void serialize_order_rtn(const Event& ev, std::string& os,
                             ct::data::IEventStore* es, void* fbb_ref);
    void serialize_trade_rtn(const Event& ev, std::string& os,
                             ct::data::IEventStore* es, void* fbb_ref);
    void serialize_conn_state(const Event& ev, std::string& os,
                              ct::data::IEventStore* es, void* fbb_ref);
    void serialize_strategy_metric(const Event& ev, std::string& os,
                                   ct::data::IEventStore* es, void* fbb_ref);
    // ── 新事件类型 serialize helpers ──────────────────────────────────────────
    void serialize_risk_reject(const Event& ev, std::string& os,
                               ct::data::IEventStore* es, void* fbb_ref);
    void serialize_param_update(const Event& ev, std::string& os,
                                ct::data::IEventStore* es, void* fbb_ref);
    void serialize_kill_switch(const Event& ev, std::string& os,
                               ct::data::IEventStore* es, void* fbb_ref);
    void serialize_position_lifecycle(const Event& ev, std::string& os,
                                      ct::data::IEventStore* es, void* fbb_ref);
    void serialize_intent_trace(const Event& ev, std::string& os,
                                ct::data::IEventStore* es, void* fbb_ref);

    Config                                  cfg_;
    std::unique_ptr<XtLockFreeQueue<Event>> queue_;
    std::thread                             worker_;
    std::atomic<bool>                       running_{false};

    std::atomic<uint64_t>                   emit_count_{0};
    std::atomic<uint64_t>                   drop_count_{0};
    std::atomic<uint64_t>                   written_count_{0};
    std::atomic<uint64_t>                   write_failed_count_{0};
    std::atomic<uint64_t>                   intent_trace_emit_count_{0};
    std::atomic<uint64_t>                   intent_trace_drop_count_{0};
    std::atomic<uint64_t>                   intent_trace_written_count_{0};
    std::atomic<uint64_t>                   intent_trace_write_failed_count_{0};

    // 后台线程独占（worker_ 持有），无需同步
    void*                                   ofs_handle_ = nullptr;  ///< 实为 std::ofstream*，前置隐藏
    std::atomic<ct::data::IEventStore*>     event_store_{nullptr}; ///< fbs sink。caller 写 release，drain 每事件读 acquire；生命周期由 caller 保证
    std::atomic<int8_t>                     venue_kind_{0};         ///< fbs SourceKind 默认 UNKNOWN；非 MD 事件用此 fallback。caller 写 release，drain 读 relaxed (不与其他状态联动)
    void*                                   fbb_handle_  = nullptr; ///< 实为 flatbuffers::FlatBufferBuilder*，drain 线程独占，Clear() 复用
    uint32_t                                current_trading_day_ = 0;  ///< drain 独占；MD_TICK 更新；非 MD 事件 fbs append 用

    // ── 运行时滚动状态（drain 线程独占，无需同步）──────────────────
    uint64_t                                current_file_bytes_ = 0;    ///< 当前文件已写字节，触发 rotate_bytes
    int64_t                                 file_opened_wall_sec_ = 0;  ///< 当前文件打开时 wallclock 秒，触发 rotate_interval_sec
    std::atomic<uint64_t>                   rotation_count_{0};         ///< 运行时滚动累计次数（跨线程读）
    uint64_t                                fbs_append_fail_count_ = 0; ///< fbs sink append 失败累计（drain 独占），rate-limited 告警
};
