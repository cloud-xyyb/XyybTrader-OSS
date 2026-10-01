#pragma once

#include <string>

/// @brief Binance U 本位永续 TD 专属配置。
///
/// 只保存 Binance REST/UDS/WS trade 端点、认证和 transport 行为开关；
/// provider-neutral 字段仍由 `ITradeGateway::Config` 承载。
struct BinanceTdCfg {
    /// @brief 下单 transport 选择。
    enum class Transport { kRest, kWs };

    // ── REST / UDS 基础配置 ────────────────────────────────────────
    std::string base_rest_url;  ///< REST 基础 URL，如 `https://fapi.binance.com`。
    std::string base_ws_url;    ///< UDS WS 基础 URL；User Data Stream 拼 `/ws/<listenKey>`。
    std::string api_key;        ///< API key，通常经 secrets.yaml 注入。
    std::string api_secret;     ///< API secret，通常经 secrets.yaml 注入；用于 HMAC-SHA256。
    uint32_t listen_key_refresh_ms = 30 * 60 * 1000;  ///< listenKey 续期间隔 ms（默认 30min）
    /// resolved runtime.instance_id（config 装配层注入，非 yaml 直配）；非空时
    /// local_id 发号器走 per-instance tag 域（multibook 跨进程唯一），空 = legacy 共享域。
    std::string local_id_instance;
    bool     testnet = true;                           ///< 是否连接 testnet；仅用于日志/诊断，不改变 URL。
    int      recv_window_ms = 5000;                    ///< Binance 请求时间窗，单位 ms。
    uint32_t time_sync_interval_ms = 60 * 1000;        ///< server time 校准间隔 ms（默认 60s）

    // ── 下单 transport 配置 ────────────────────────────────────────
    Transport transport = Transport::kRest;             ///< 下单 transport，默认 REST。
    std::string base_ws_trade_url;                      ///< WS trade API URL，如 `wss://ws-fapi.binance.com/ws-fapi/v1`。
    uint32_t ws_submit_timeout_ms = 6000;               ///< WS submit timeout（>= recvWindow）
    uint32_t ws_stale_drop_ms = 1500;                   ///< WS 请求入队后超过该时间未发送则 stale，单位 ms。
    uint32_t ws_send_queue_cap = 256;                   ///< WS 未连接或背压时的发送队列上限。
    uint32_t ws_reconnect_min_ms = 500;                 ///< WS reconnect 最小退避 ms。
    uint32_t ws_reconnect_max_ms = 30000;               ///< WS reconnect 最大退避 ms。

    // ── Binance 专属行为开关 ───────────────────────────────────────
    bool     uds_first_effective_ack = false;
    ///< true 时 UDS NEW 与 transport ACK 谁先到先 emit alive，后到信号由订单状态机去重；
    ///  默认 false，保持 transport ACK 作为 alive 主路径。

    bool     close_reject_plain_fallback = false;
    ///< Binance-only fallback。true 时 CLOSE reduceOnly 被 `-2022` 拒绝后，先按最新
    ///  positionRisk 计算 flip-safe plain-close 数量，再仅重发一次不带 reduceOnly 的平仓单。
    ///  默认 false；该模式放弃 reduceOnly 的交易所级 no-flip 保护，只应在确认 venue
    ///  reduceOnly close path 异常时开启。
};
