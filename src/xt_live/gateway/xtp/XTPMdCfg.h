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
    /// @brief 行情连接传输协议（XTP::API::QuoteApi::Login 第 5 参数 sock_type）。
    /// @note 公网 testnet 仅支持 kTcp；kUdp 仅在券商环境确认支持时启用。
    enum class Protocol : uint8_t {
        kTcp = 1,
        kUdp = 2,
    };

    uint8_t     client_id = 1;             ///< XTP 客户端编号（1-99，券商分配；与 TD 可同号）
    std::string software_key;              ///< 资讯端软件 key（券商分配，secrets.yaml 注入）
    Protocol    protocol = Protocol::kTcp;
    uint32_t    heartbeat_interval_s = 15; ///< XTP::API::QuoteApi::SetHeartBeatInterval
    uint32_t    buffer_size_mb = 128;      ///< XTP UDP buffer (TCP 下不生效但保留)
    std::string log_dir = "XTP_MD_LOG";    ///< XTP SDK 日志目录（CreateQuoteApi save_file_path）
    int         log_level = 2;             ///< XTP_LOG_LEVEL: 0=DEBUG/1=INFO/2=WARNING/3=ERROR/4=FATAL
};
