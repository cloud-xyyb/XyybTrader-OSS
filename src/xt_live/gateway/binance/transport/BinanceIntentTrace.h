#pragma once

#include <string>

#include "BinanceOrderParams.h"
#include "xt_live/telemetry/recording/XtLiveRecorder.h"
#include "xt_util/time/XtSteadyClock.h"

/// @file
/// @brief Binance TD wire 层 submit/reject 的 record 埋点
///
/// - 把 transport 准备发出或被拒绝的下单请求，连同归一化后的 wire 字段
///  （tif/offset/exec_flags/价格与数量）一起写进 XtLiveRecorder，
/// - 供事后按 trace_id 复原一笔委托从策略意图到交易所线上参数的每一步转换。

namespace binance_u {

inline const char* trace_tif_wire(XtTimeInForce tif) noexcept {
    switch (tif) {
    case XtTimeInForce::GTC: return "GTC";
    case XtTimeInForce::IOC: return "IOC";
    case XtTimeInForce::FOK: return "FOK";
    }
    return "unknown";
}

inline const char* trace_offset_wire(XtOffset offset) noexcept {
    switch (offset) {
    case XtOffset::Open:           return "open";
    case XtOffset::Close:          return "close";
    case XtOffset::CloseToday:     return "close_today";
    case XtOffset::CloseYesterday: return "close_yesterday";
    case XtOffset::Auto:           return "auto";
    }
    return "unknown";
}

inline std::string trace_exec_flags_wire(ExecFlags flags) {
    if (flags == exec_none()) return "none";
    std::string out;
    const auto add = [&out](const char* s) {
        if (!out.empty()) out += '|';
        out += s;
    };
    if (has_flag(flags, ExecFlag::ReduceOnly)) add("reduce_only");
    if (has_flag(flags, ExecFlag::PostOnly)) add("post_only");
    if (has_flag(flags, ExecFlag::Hidden)) add("hidden");
    if (has_flag(flags, ExecFlag::Market)) add("market");
    constexpr ExecFlags known = static_cast<uint32_t>(ExecFlag::ReduceOnly)
                              | static_cast<uint32_t>(ExecFlag::PostOnly)
                              | static_cast<uint32_t>(ExecFlag::Hidden)
                              | static_cast<uint32_t>(ExecFlag::Market);
    if ((flags & ~known) != 0) add("custom");
    return out.empty() ? "custom" : out;
}

inline void emit_wire_submit_trace(const TransportSubmitReq& req,
                                   const BinanceSubmitWireValues& wire) {
    if (req.trace_id == 0) return;
    auto* rec = XtLiveRecorder::global();
    if (!rec) return;
    const std::string exec_flags = trace_exec_flags_wire(req.exec_flags);
    rec->emit_intent_trace({
        .trace_id           = req.trace_id,
        .stage              = "WIRE_SUBMIT",
        .venue              = "binance_um",
        .code_raw           = req.symbol.c_str(),
        .code_effective     = req.symbol.c_str(),
        .dir                = req.is_buy ? "buy" : "sell",
        .price_raw_fp       = req.price_fp,
        .price_effective_fp = req.price_fp,
        .price_wire_fp      = wire.price_wire_fp,
        .qty_raw_fp         = req.qty_fp,
        .qty_effective_fp   = req.qty_fp,
        .qty_wire_fp        = wire.qty_wire_fp,
        .tif_raw            = trace_tif_wire(req.tif),
        .tif_effective      = trace_tif_wire(req.tif),
        .tif_wire           = wire.tif_wire.c_str(),
        .offset_raw         = trace_offset_wire(req.close_offset),
        .offset_effective   = trace_offset_wire(req.close_offset),
        .reduce_only        = wire.reduce_only,
        .exec_flags         = exec_flags.c_str(),
        .local_id           = req.local_id,
        .phys_id            = req.local_id,
        .event_mono_ns      = XtSteadyClock::steady_now_ns(),
        .wire_client_order_id_hash = wire.client_order_id_hash,
    });
}

inline void emit_wire_reject_trace(const TransportSubmitReq& req,
                                   const char* reject_code,
                                   const char* reject_reason) {
    if (req.trace_id == 0) return;
    auto* rec = XtLiveRecorder::global();
    if (!rec) return;
    const std::string exec_flags = trace_exec_flags_wire(req.exec_flags);
    rec->emit_intent_trace({
        .trace_id           = req.trace_id,
        .stage              = "WIRE_REJECT",
        .venue              = "binance_um",
        .code_raw           = req.symbol.c_str(),
        .code_effective     = req.symbol.c_str(),
        .dir                = req.is_buy ? "buy" : "sell",
        .price_raw_fp       = req.price_fp,
        .price_effective_fp = req.price_fp,
        .qty_raw_fp         = req.qty_fp,
        .qty_effective_fp   = req.qty_fp,
        .tif_raw            = trace_tif_wire(req.tif),
        .tif_effective      = trace_tif_wire(req.tif),
        .offset_raw         = trace_offset_wire(req.close_offset),
        .offset_effective   = trace_offset_wire(req.close_offset),
        .reduce_only        = binance_reduce_only(req.exec_flags, req.close_offset),
        .exec_flags         = exec_flags.c_str(),
        .local_id           = req.local_id,
        .phys_id            = req.local_id,
        .event_mono_ns      = XtSteadyClock::steady_now_ns(),
        .reject_scope       = "wire_pre_send",
        .reject_code        = reject_code,
        .reject_reason      = reject_reason,
        .wire_client_order_id_hash = binance_wire_hash(req.client_order_id),
    });
}

}
