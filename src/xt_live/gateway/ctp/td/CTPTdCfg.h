#pragma once

#include <string>

/// @brief CTP 交易网关配置。
///
/// 承载 CTP TD SDK 特有的 broker、flow 目录、结算确认和恢复查询参数；
///
/// @note 由 gateway factory variant 透传，或通过 TraderCTPGateway::configure_ctp() 显式注入。
struct CTPTdCfg {
    std::string broker;
    std::string flow_dir = "CTP_TD_FLOW";
    bool        confirm_settlement = true;
    int         settlement_confirm_timeout_ms = 8000;
    int         recovery_query_timeout_ms     = 8000;
    int         recovery_query_max_timeouts   = 6;
    int         recovery_rearm_backoff_ms     = 5000;
    int         recovery_rearm_alert_rounds   = 10;
};
