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

namespace ct::data { class IEventStore; }

/// @brief 实盘事件录制器：producer 热路径入队，后台线程写 JSONL / fbs。
///
/// `emit_*` 只填充固定大小 Event 并 try-push 到 MPSC 队列；队列满或 recorder
/// 停止时丢弃并累加 `drop_count`。价格/金额字段统一使用 fixed-point ×1e6。
/// @warning start()/stop() 非线程安全，调用方必须串行化 lifecycle 操作。
class XtLiveRecorder {
public:
    struct Config {
        struct IntentTraceConfig {
            bool enabled = false;
            double sample_rate = 0.01;
            bool include_wire_debug = false;
        };

        bool enabled = false;
        std::string file_path;
        size_t queue_capacity = 8192;

        uint64_t rotate_bytes        = 1ull << 30;
        uint32_t rotate_interval_sec = 3600;
        IntentTraceConfig intent_trace;
    };

    enum EventType : uint8_t {
        kEvtMdTick            = 0,
        kEvtStrategyDecision  = 1,
        kEvtOrderSubmit       = 2,
        kEvtOrderRtn          = 3,
        kEvtTradeRtn          = 4,
        kEvtConnState         = 5,
        kEvtStrategyMetric    = 6,

        kEvtRiskReject        = 7,
        kEvtParamUpdate       = 8,
        kEvtKillSwitch        = 9,
        kEvtPositionLifecycle = 10,
        kEvtIntentTrace       = 11,
    };

    static constexpr int kMdDepth = 20;

    /// @brief 录制事件（trivially copyable）— 所有 payload 字段平铺，每条事件仅填充一种。
    struct Event {
        uint8_t  type        = 0;
        uint64_t ts_local_ns = 0;
        uint64_t ts_exch_ns  = 0;

        char     md_code[32]  = {};
        char     md_exchange_id[16] = {};
        uint32_t md_trading_date = 0;
        uint32_t md_action_date = 0;
        uint32_t md_action_time = 0;
        int64_t  md_bid_fp    = 0;
        int64_t  md_ask_fp    = 0;
        int64_t  md_last_fp   = 0;
        int64_t  md_volume    = 0;
        int64_t  md_bid_prices_fp[kMdDepth] = {};
        int64_t  md_ask_prices_fp[kMdDepth] = {};
        int64_t  md_bid_qty[kMdDepth]       = {};
        int64_t  md_ask_qty[kMdDepth]       = {};

        char     sd_strat_id[32]      = {};
        char     sd_code[32]          = {};
        char     sd_signal[8]         = {};
        int64_t  sd_target_pos        = 0;
        uint64_t sd_trigger_tick_ts   = 0;

        char     sd_book_id[32]       = {};
        char     sd_account_id[32]    = {};

        uint64_t os_logical_id        = 0;
        char     os_code[32]          = {};
        char     os_dir[8]            = {};
        char     os_offset[8]         = {};
        int64_t  os_price_fp          = 0;
        int64_t  os_qty               = 0;
        char     os_parent_strat[32]  = {};
        uint64_t os_decision_ts       = 0;
        uint64_t os_trigger_tick_ts   = 0;
        char     os_reason[48]        = {};

        char     os_strat_id[32]      = {};
        char     os_book_id[32]       = {};
        char     os_account_id[32]    = {};
        char     os_user_tag[32]      = {};
        uint64_t os_decision_mono_ns  = 0;

        uint64_t or_logical_id        = 0;
        uint64_t or_phys_id           = 0;
        char     or_status[16]        = {};
        int64_t  or_filled_qty        = 0;
        char     or_err_msg[64]       = {};
        char     or_cancel_initiator[16] = {};

        char     or_strat_id[32]      = {};
        char     or_book_id[32]       = {};
        char     or_account_id[32]    = {};

        uint64_t tr_trade_id          = 0;
        uint64_t tr_logical_id        = 0;
        uint32_t tr_trading_day       = 0;
        uint32_t tr_trade_action_date = 0;
        uint32_t tr_trade_action_time = 0;
        char     tr_code[32]          = {};
        char     tr_exchange_id[16]   = {};
        char     tr_side[8]           = {};
        int64_t  tr_price_fp          = 0;
        int64_t  tr_qty               = 0;
        bool     tr_is_maker          = false;

        char     tr_strat_id[32]      = {};
        char     tr_book_id[32]       = {};
        char     tr_account_id[32]    = {};
        int64_t  tr_fee_fp            = 0;
        int64_t  tr_realized_pnl_fp   = 0;
        bool     tr_has_realized      = false;
        bool     tr_is_close          = false;
        char     tr_route_status[20]  = {};

        char     cs_gateway[8]        = {};
        char     cs_state[16]         = {};
        char     cs_err_msg[64]       = {};

        char     cs_venue_id[16]      = {};

        char     sm_name[32]          = {};
        double   sm_value             = 0.0;

        char     sm_strat_id[32]      = {};
        char     sm_book_id[32]       = {};
        char     sm_account_id[32]    = {};

        char     rj_strat_id[32]      = {};
        char     rj_book_id[32]       = {};
        char     rj_account_id[32]    = {};
        char     rj_code[32]          = {};
        char     rj_dir[8]            = {};
        int64_t  rj_price_fp          = 0;
        int64_t  rj_qty               = 0;
        char     rj_reason[48]        = {};
        int64_t  rj_required_margin_fp = 0;
        int64_t  rj_available_margin_fp = 0;

        char     pu_actor[48]         = {};
        char     pu_strat_id[32]      = {};
        char     pu_key[64]           = {};
        char     pu_value_str[64]     = {};
        uint64_t pu_apply_mono_ns     = 0;

        char     ks_actor[48]         = {};
        char     ks_state[16]         = {};
        char     ks_reason[80]        = {};
        char     ks_venue_id[16]      = {};

        char     lc_strat_id[32]      = {};
        char     lc_book_id[32]       = {};
        char     lc_account_id[32]    = {};
        char     lc_code[32]          = {};
        char     lc_event_type[8]     = {};
        int64_t  lc_qty_fp            = 0;
        int64_t  lc_avg_price_fp      = 0;
        int64_t  lc_realized_pnl_fp   = 0;
        uint64_t lc_hold_duration_ms  = 0;
        uint64_t lc_cause_trade_id    = 0;

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

    void start();

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

    uint64_t rotation_count() const noexcept { return rotation_count_.load(std::memory_order_relaxed); }

    static bool sample_intent_trace(uint64_t trace_id, double sample_rate) noexcept;

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
                      const int64_t ask_qty[kMdDepth]);

    void set_event_store(ct::data::IEventStore* store) noexcept;

    void set_venue_kind(int8_t kind) noexcept;

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
        const char* dir              = nullptr;
        const char* offset           = nullptr;
        int64_t     price_fp         = 0;
        int64_t     qty              = 0;
        const char* parent_strat_id  = nullptr;
        uint64_t    decision_ts      = 0;
        uint64_t    trigger_tick_ts  = 0;
        const char* reason           = nullptr;
        const char* strat_id         = nullptr;
        const char* book_id          = nullptr;
        const char* account_id       = nullptr;
        const char* user_tag         = nullptr;
        uint64_t    decision_mono_ns = 0;
    };

    void emit_order_submit(const OrderSubmitParams& p);

    void emit_order_rtn(uint64_t ts_local_ns,
                        uint64_t logical_id,
                        uint64_t phys_id,
                        const char* status,
                        int64_t filled_qty,
                        const char* err_msg,
                        const char* cancel_initiator,
                        const char* strat_id    = nullptr,
                        const char* book_id     = nullptr,
                        const char* account_id  = nullptr);

    /// @brief 成交回报录制参数；使用 aggregate 避免长参数列表。
    struct TradeRtnParams {
        uint64_t    ts_local_ns        = 0;
        uint64_t    ts_exch_ns         = 0;
        uint32_t    trading_day        = 0;
        uint32_t    trade_action_date  = 0;
        uint32_t    trade_action_time  = 0;
        uint64_t    trade_id           = 0;
        uint64_t    logical_id         = 0;
        const char* code               = nullptr;
        const char* exchange_id        = nullptr;
        const char* side               = nullptr;
        int64_t     price_fp           = 0;
        int64_t     qty                = 0;
        bool        is_maker           = false;
        const char* strat_id           = nullptr;
        const char* book_id            = nullptr;
        const char* account_id         = nullptr;
        int64_t     fee_fp             = 0;
        int64_t     realized_pnl_fp    = 0;
        bool        has_realized       = false;
        bool        is_close           = false;
        const char* route_status       = nullptr;
    };

    void emit_trade_rtn(const TradeRtnParams& p);

    void emit_conn_state(uint64_t ts_local_ns,
                         const char* gateway,
                         const char* state,
                         const char* err_msg,
                         const char* venue_id = nullptr);

    void emit_strategy_metric(uint64_t ts_local_ns,
                              const char* name,
                              double value,
                              const char* strat_id    = nullptr,
                              const char* book_id     = nullptr,
                              const char* account_id  = nullptr);

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

    void emit_param_update(uint64_t ts_local_ns,
                           const char* actor,
                           const char* strat_id,
                           const char* key,
                           const char* value_str,
                           uint64_t apply_mono_ns);

    void emit_kill_switch(uint64_t ts_local_ns,
                          const char* actor,
                          const char* state,
                          const char* reason,
                          const char* venue_id);

    /// @brief 持仓 lifecycle 录制参数；由 per-(strategy_id, code) zero-cross 触发。
    /// @note sign-flip 拆成 close+open；panel 读取 open_time / hold_duration / realized。
    struct PositionLifecycleParams {
        uint64_t    ts_local_ns      = 0;
        uint64_t    ts_exch_ns       = 0;
        const char* strat_id         = nullptr;
        const char* book_id          = nullptr;
        const char* account_id       = nullptr;
        const char* code             = nullptr;
        const char* event_type       = nullptr;
        int64_t     qty_fp           = 0;
        int64_t     avg_price_fp     = 0;
        int64_t     realized_pnl_fp  = 0;
        uint64_t    hold_duration_ms = 0;
        uint64_t    cause_trade_id   = 0;
    };

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

    void emit_intent_trace(const IntentTraceParams& p);

    static void note_cancel_initiated(uint64_t order_id, const char* initiator) noexcept;

    static void consume_cancel_initiator(uint64_t order_id, char* out, size_t cap) noexcept;

    static void note_order_reject(uint64_t order_id, int err_code, const char* err_msg) noexcept;

    static void consume_order_reject_err(uint64_t order_id, char* out, size_t cap) noexcept;

    static void set_global(XtLiveRecorder* rec) noexcept;
    static XtLiveRecorder* global() noexcept;

private:

    static void copy_fixed(char* dst, size_t dst_cap, const char* src) noexcept {
        XtCStringUtil::copy(dst, dst_cap, src);
    }

    void drain_loop();

    void maybe_rotate();

    void rotate_active_file(const char* reason, int64_t now_sec);

    void serialize_and_write(const Event& ev);

    void push_or_drop(Event&& ev) noexcept;

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

    void*                                   ofs_handle_ = nullptr;
    std::atomic<ct::data::IEventStore*>     event_store_{nullptr};
    std::atomic<int8_t>                     venue_kind_{0};
    void*                                   fbb_handle_  = nullptr;
    uint32_t                                current_trading_day_ = 0;

    uint64_t                                current_file_bytes_ = 0;
    int64_t                                 file_opened_wall_sec_ = 0;
    std::atomic<uint64_t>                   rotation_count_{0};
    uint64_t                                fbs_append_fail_count_ = 0;
};
