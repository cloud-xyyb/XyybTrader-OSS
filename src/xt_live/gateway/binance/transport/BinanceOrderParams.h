#pragma once

#include <map>
#include <string>
#include <string_view>

#include "BinanceTdWireUtil.h"  
#include "refdata/BinanceExchangeInfo.h" 
#include "ITradeTransport.h" 
#include "xt_core/sync/XtTransparentStringHash.h"

/// @file BinanceOrderParams.h
/// @brief Binance submit-order wire 参数构造 helper
///
/// - REST transport 与 WS transport 都用这里生成下单公共字段；
/// - 签名、请求封装和线程模型仍归各自 transport；
/// - 集中 timeInForce、quantity/price 截断和 reduceOnly 映射，避免两条 TD 路径字段语义漂移。

namespace binance_u {

/// @brief 将内部 time-in-force + 执行标志映射为 Binance LIMIT 单 timeInForce 串。
/// @note PostOnly 优先映射 GTX；其余 GTC/IOC/FOK 直译，未知 tif 兜底 GTC。
inline const char* tif_to_binance(XtTimeInForce tif, ExecFlags flags) {
    if (has_flag(flags, ExecFlag::PostOnly)) return "GTX";
    switch (tif) {
    case XtTimeInForce::GTC: return "GTC";
    case XtTimeInForce::IOC: return "IOC";
    case XtTimeInForce::FOK: return "FOK";
    }
    return "GTC";
}

/// @brief 是否携带 reduceOnly：显式 ReduceOnly 标志或任意非 Open 平仓方向。
inline bool binance_reduce_only(ExecFlags flags, XtOffset close_offset) {
    return has_flag(flags, ExecFlag::ReduceOnly) || close_offset != XtOffset::Open;
}

/// @brief INTENT_TRACE 用的 normalized wire 字段；与实际 params 由同一 builder 生成。
struct BinanceSubmitWireValues {
    int64_t     price_wire_fp = 0;     ///< snap_price_fp 后的 wire 价格。
    int64_t     qty_wire_fp = 0;       ///< snap_qty_fp 后的 wire 数量。
    std::string tif_wire;              ///< Binance `timeInForce`。
    bool        reduce_only = false;   ///< 是否写入 reduceOnly=true。
    uint64_t    client_order_id_hash = 0; ///< newClientOrderId 的 FNV-1a hash。
};

/// @brief Binance submit 公共参数与同源 normalized 值。
struct BinanceSubmitOrderParams {
    std::map<std::string, std::string> params;
    BinanceSubmitWireValues wire;
};

inline uint64_t binance_wire_hash(std::string_view s) noexcept {
    if (s.empty()) return 0;
    return xt_core::sync::fnv1a_bytes(s.data(), s.size());
}

/// @brief 生成 Binance 下单公共参数，并返回同源 snap 后 wire 值。
inline BinanceSubmitOrderParams build_submit_order_params(const TransportSubmitReq& req) {
    BinanceSubmitOrderParams out;
    out.wire.price_wire_fp = snap_price_fp(req.price_fp, req.symbol);
    out.wire.qty_wire_fp = snap_qty_fp(req.qty_fp, req.symbol);
    out.wire.tif_wire = tif_to_binance(req.tif, req.exec_flags);
    out.wire.reduce_only = binance_reduce_only(req.exec_flags, req.close_offset);
    out.wire.client_order_id_hash = binance_wire_hash(req.client_order_id);

    out.params["symbol"] = req.symbol;
    out.params["side"] = req.is_buy ? "BUY" : "SELL";
    out.params["type"] = "LIMIT";
    out.params["timeInForce"] = out.wire.tif_wire;
    out.params["quantity"] = fp_to_str(out.wire.qty_wire_fp);
    out.params["price"] = fp_to_str(out.wire.price_wire_fp);
    out.params["newClientOrderId"] = req.client_order_id;
    out.params["newOrderRespType"] = "ACK";
    if (out.wire.reduce_only) {
        out.params["reduceOnly"] = "true";
    }
    return out;
}

}  // namespace binance_u
