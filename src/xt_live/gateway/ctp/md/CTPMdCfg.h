#pragma once

#include <string>

#include "ctp/md/CtpMdSessionGate.h"

/// @brief CTP 行情网关配置。
///
/// 承载 CTP MD SDK 特有的 broker 编码和 flow 文件目录；provider-neutral 的
/// front / user / password / subs 保留在 IMdGateway::Config。
///
/// @note 由 gateway factory variant 透传，或通过 MdCTPGateway::configure_ctp() 显式注入。
struct CTPMdCfg {
    std::string broker;                    ///< 经纪商代码（CTP broker ID）
    std::string flow_dir = "CTP_MD_FLOW";  ///< CTP SDK flow 状态目录；启动前需可写。

    /// @brief expected-tick 窗 gate（CTP-specific）。默认 gate_enabled=false（不门控，
    /// 保持 direct runner 原行为）；仅 xt_md_connector provider=ctp 时按 CLI/内置值填入。
    ctp_md_session::TradingWindowGate window_gate;
};
