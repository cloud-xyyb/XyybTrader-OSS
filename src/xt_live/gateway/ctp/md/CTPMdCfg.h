#pragma once

#include <string>

/// @brief CTP 行情网关配置。
///
/// 承载 CTP MD SDK 特有的 broker 编码和 flow 文件目录；provider-neutral 的
/// front / user / password / subs 保留在 IMdGateway::Config。
///
/// @note 由 gateway factory variant 透传，或通过 MdCTPGateway::configure_ctp() 显式注入。
struct CTPMdCfg {
    std::string broker;
    std::string flow_dir = "CTP_MD_FLOW";
};
