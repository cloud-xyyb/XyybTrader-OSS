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
    uint8_t     client_id = 1;                  ///< XTP 客户端编号（1-99，券商分配）
    std::string software_key;                   ///< 软件 key（secrets.yaml 注入）
    std::string software_version = "1.0.0";     ///< 软件版本号（券商核验用）
    std::string account_key;                    ///< 资金账号 key（ITradeGateway::Config.user 别名）

    uint32_t    heartbeat_interval_s = 15;      ///< XTP::API::TraderApi::SetHeartBeatInterval
    uint32_t    buffer_size_mb = 128;           ///< XTP TD buffer 大小
    std::string log_dir = "XTP_TD_LOG";         ///< XTP SDK 日志目录（CreateTraderApi save_file_path）
    int         log_level = 2;                  ///< XTP_LOG_LEVEL（同 MD）

    /// @brief 登录后是否自动查询 asset / position，并 reverse-sync unfinished orders 后进入 ready。
    bool        confirm_init_query = true;
    int         init_query_timeout_ms = 8000;      ///< init asset/position 单步查询超时
    int         recovery_query_timeout_ms = 8000;  ///< 保留字段，XTP 侧未接线（reverse-sync 超时复用 init_query_timeout_ms）
    int         recovery_query_max_timeouts = 6;   ///< 保留字段，XTP 侧未接线（无超时预算计数）

    /// @brief ready 后周期 requery 间隔 ms（QueryAsset + QueryPosition）。
    /// XTP 只在 login/reconnect 查询一次，session 内成交会让本地快照陈旧；
    /// 周期 requery 让仓位/资金权威随 broker 收敛（周期对账兜底，validator `<=0` 拒启）。
    int         position_refresh_ms = 60000;

    /// resolved runtime.instance_id（config 装配层注入，非 yaml 直配）；非空时
    /// local_id 发号器走 per-instance tag 域（multibook 跨进程唯一），空 = legacy 共享域。
    /// @note tag 位在 bit 53+，client32（uint32 截断）域不受其保护，wrap 护栏
    ///       是进程内的——多实例 XTP 非支持形态。
    std::string local_id_instance;
};
