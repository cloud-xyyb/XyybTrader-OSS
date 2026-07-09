#pragma once

#include <string>

/// @brief 交易所持仓快照（provider-neutral position snapshot）。
///
/// 从 ITradeGateway 抽取的独立类型，供 xt_account::AccountStateStore 等低层模块使用，
/// 避免为获取此 POD 而引入完整的 ITradeGateway 头文件（及其传递依赖）。
struct XtPositionSnapshot {
    std::string code;
    double long_qty = 0.0;
    double long_today = 0.0;
    double long_avg = 0.0;
    double short_qty = 0.0;
    double short_today = 0.0;
    double short_avg = 0.0;

    double long_frozen = 0.0;
    double short_frozen = 0.0;

    uint64_t broker_frozen_last_update_ms = 0;
};
