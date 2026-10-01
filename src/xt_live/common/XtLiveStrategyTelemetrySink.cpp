#include "XtLiveStrategyTelemetrySink.h"

#include "xt_live/telemetry/recording/XtLiveRecorder.h"

namespace xt_live::common {

void XtLiveStrategyTelemetrySink::on_metric(uint64_t ts_local_ns, const char* name, double val,
                                            const char* strategy_id, const char* book_id) {
    if (auto* rec = XtLiveRecorder::global()) {
        rec->emit_strategy_metric(ts_local_ns, name, val, strategy_id, book_id);
    }
}

bool XtLiveStrategyTelemetrySink::intent_trace_enabled() const {
    auto* rec = XtLiveRecorder::global();
    return rec && rec->intent_trace_enabled();
}

bool XtLiveStrategyTelemetrySink::should_sample_intent_trace(uint64_t trace_id) const {
    auto* rec = XtLiveRecorder::global();
    return rec && rec->should_sample_intent_trace(trace_id);
}

void XtLiveStrategyTelemetrySink::emit_intent_trace(
    const xt_strategy_engine::StrategyIntentTrace& t) {
    auto* rec = XtLiveRecorder::global();
    if (!rec) return;
    // SDK POD → recorder wire 类型逐字段映射；ctx 不生产的深层字段（venue/account/
    // wire 价量/local_id/slice_*）留默认值，由 gateway 后续 stage 直填 recorder。
    XtLiveRecorder::IntentTraceParams p;
    p.trace_id           = t.trace_id;
    p.stage              = t.stage;
    p.strategy_id        = t.strategy_id;
    p.book_id            = t.book_id;
    p.code_raw           = t.code_raw;
    p.code_effective     = t.code_effective;
    p.dir                = t.dir;
    p.price_raw_fp       = t.price_raw_fp;
    p.price_effective_fp = t.price_effective_fp;
    p.qty_raw_fp         = t.qty_raw_fp;
    p.qty_effective_fp   = t.qty_effective_fp;
    p.tif_raw            = t.tif_raw;
    p.tif_effective      = t.tif_effective;
    p.offset_raw         = t.offset_raw;
    p.offset_effective   = t.offset_effective;
    p.reduce_only        = t.reduce_only;
    p.exec_flags         = t.exec_flags;
    p.trigger_tick_ts    = t.trigger_tick_ts;
    p.decision_mono_ns   = t.decision_mono_ns;
    p.event_mono_ns      = t.event_mono_ns;
    p.best_bid_fp        = t.best_bid_fp;
    p.best_ask_fp        = t.best_ask_fp;
    p.reject_scope       = t.reject_scope;
    p.reject_code        = t.reject_code;
    p.reject_reason      = t.reject_reason;
    p.user_tag_hash      = t.user_tag_hash;
    rec->emit_intent_trace(p);
}

XtLiveStrategyTelemetrySink& live_strategy_telemetry_sink() {
    static XtLiveStrategyTelemetrySink sink;
    return sink;
}

}  // namespace xt_live::common
