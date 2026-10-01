#pragma once

#include <string>

/// @brief CTP 交易网关配置。
///
/// 承载 CTP TD SDK 特有的 broker、flow 目录、结算确认和恢复查询参数；
///
/// @note 由 gateway factory variant 透传，或通过 TraderCTPGateway::configure_ctp() 显式注入。
struct CTPTdCfg {
    std::string broker;                              ///< 经纪商代码（CTP broker ID）
    std::string flow_dir = "CTP_TD_FLOW";            ///< CTP SDK flow 状态目录；同时存放 OrderRef seed。
    bool        confirm_settlement = true;           ///< 登录后自动确认结算
    int         settlement_confirm_timeout_ms = 8000;  ///< 结算确认超时 ms
    int         recovery_query_timeout_ms     = 8000;  ///< 恢复查询超时 ms
    int         recovery_query_max_timeouts   = 6;     ///< 恢复查询最大连续超时次数
    int         recovery_rearm_backoff_ms     = 5000;  ///< 恢复查询 fail-closed 后重发的基准 backoff（指数退避封顶 60s）；<=0 关闭 re-arm（退回 fail-closed-forever）
    int         recovery_rearm_alert_rounds   = 10;    ///< re-arm 轮数 ≥ 此值置 td recovery wedged 告警（paging）；re-arm 仍继续重试不放弃
    int         refresh_query_timeout_ms      = 30000; ///< Ready 稳态周期 refresh 查询超时自愈 ms（无 bIsLast 回包超此值 → 解 stuck inflight 重发）；<=0 关闭（回滚开关）
    int         refresh_query_wedge_rounds    = 6;     ///< refresh 查询连续超时达此轮数置 refresh wedged 观测状态（page-then-continue，仍重发）
    bool        query_generation_fence        = true;  ///< 查询回包按 nRequestID 匹配在途轮（generation fence，过期轮整帧丢弃）；false=关闭（回滚开关，回退全收）
    bool        exec_ledger_query_fill_delivery = true; ///< query 重放成交行按来源规则交付（exactly-once 判据在交付投影）；false=回滚：重放行一律 absorbed 记账不下发（旧行为）
};
