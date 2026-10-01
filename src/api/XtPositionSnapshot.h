#pragma once

#include <string>

/// @brief 交易所持仓快照（provider-neutral position snapshot）。
///
/// 从 ITradeGateway 抽取的独立类型，供 xt_account::AccountStateStore 等低层模块使用，
/// 避免为获取此 POD 而引入完整的 ITradeGateway 头文件（及其传递依赖）。
struct XtPositionSnapshot {
    std::string code;                ///< 合约代码（instrument ID）
    double long_qty = 0.0;           ///< 多头总持仓量（long total qty）
    double long_today = 0.0;         ///< 多头今仓量（long today qty）
    double long_avg = 0.0;           ///< 多头均价（long avg open price）
    double short_qty = 0.0;          ///< 空头总持仓量（short total qty）
    double short_today = 0.0;        ///< 空头今仓量（short today qty）
    double short_avg = 0.0;          ///< 空头均价（short avg open price）
    // ---------------------------------------------------------------------
    // venue/broker-reported frozen quantity
    // ---------------------------------------------------------------------
    // CTP InvestorPosition.LongFrozen / ShortFrozen (总冻结,不分今/昨,在途平仓单)。
    // 用途：风控 close path 取 max(local_outstanding, broker_frozen) 双源取严
    //       防 local 漏减 / broker 滞后造成的超平窗口。
    // binance positionRisk 不暴露 frozen → 默认 0 → max 退化为 local outstanding。
    double long_frozen = 0.0;        ///< 多头冻结量（long frozen qty,总冻结）
    double short_frozen = 0.0;       ///< 空头冻结量（short frozen qty,总冻结）
    // ---------------------------------------------------------------------
    // broker_frozen staleness tracking
    // ---------------------------------------------------------------------
    // CTP TD push InvestorPosition 的本地接收 XtSteadyClock::steady_now_ms()。
    // 与 risk path now_ms 同源 (steady monotonic), 直接 delta 检测 staleness。
    // 检测 now_ms - broker_frozen_last_update_ms > 30000ms 时降级 local-only。
    // binance/XTP 不推 broker_frozen,字段保持 0 → risk path 自动 skip(无影响)。
    uint64_t broker_frozen_last_update_ms = 0; ///< broker_frozen 最近 push steady ms
};
