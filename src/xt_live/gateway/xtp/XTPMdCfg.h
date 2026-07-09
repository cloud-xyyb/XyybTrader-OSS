#pragma once

#include <cstdint>
#include <string>

/// @brief XTP 行情网关配置。
///
/// 承载 XTP MD SDK 特有的 client_id、software_key、protocol、heartbeat 和 log
/// 配置；provider-neutral 的 front / user / password / subs 保留在 IMdGateway::Config。
///
/// @note IMdGateway::Config.front 建议使用 `tcp://<xtp-md-front>:<port>`，由
///       ParserXTP 在 Login 前拆分 host/port。公网测试环境仅支持 TCP。
struct XTPMdCfg {

    enum class Protocol : uint8_t {
        kTcp = 1,
        kUdp = 2,
    };

    uint8_t     client_id = 1;
    std::string software_key;
    Protocol    protocol = Protocol::kTcp;
    uint32_t    heartbeat_interval_s = 15;
    uint32_t    buffer_size_mb = 128;
    std::string log_dir = "XTP_MD_LOG";
    int         log_level = 2;
};
