#pragma once

#include <cstdint>
#include <string>

/// @brief XTP 交易网关专属配置。
///
/// 承载 XTP TD SDK 特有的会话字段、软件鉴权字段和登录后恢复查询参数；
/// provider-neutral 的 front / user / password / reconnect_* 保留在 ITradeGateway::Config。
///
/// @note ITradeGateway::Config.front 建议使用 `tcp://<xtp-td-front>:<port>`，
///       TraderXTP 登录前拆分 host/port；Login 成功后 session_id 用于后续报撤单和查询。
struct XTPTdCfg {
    uint8_t     client_id = 1;
    std::string software_key;
    std::string software_version = "1.0.0";
    std::string account_key;

    uint32_t    heartbeat_interval_s = 15;
    uint32_t    buffer_size_mb = 128;
    std::string log_dir = "XTP_TD_LOG";
    int         log_level = 2;

    bool        confirm_init_query = true;
    int         init_query_timeout_ms = 8000;
    int         recovery_query_timeout_ms = 8000;
    int         recovery_query_max_timeouts = 6;

    int         position_refresh_ms = 60000;

    std::string local_id_instance;
};
