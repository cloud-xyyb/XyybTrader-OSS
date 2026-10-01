#pragma once
#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "api/IMdGateway.h"
#include "ctp/md/CTPMdCfg.h"
#include "ctp/common/CtpExchangeInfer.h"
#include "ctp/common/CtpFrontDisconnectLogGate.h"
#include "ThostFtdcMdApi.h"

struct XtRawTick;
struct XtOrderQueue;
struct XtOrderDetail;
struct XtTransaction;

/// @brief CTP 行情通道的核心实现：连接、登录、订阅与行情解析。
///
/// - ParserCTP 实现 CTP MD SPI 回调，把交易所推来的深度行情转换成内部
///   `XtRawTick` 交给 `IMdGateway::Callbacks`；
/// - 行情断流由 check_health 周期检测并自动重建会话。
/// @warning callbacks 在 CTP SDK 回调线程执行，必须快速返回，
/// 不得反向调用 ParserCTP 生命周期接口或执行阻塞 I/O。
class ParserCTP : public CThostFtdcMdSpi {
public:
    // ── 回调类型 ──────────────────────────────────────────────
    using Callbacks = IMdGateway::Callbacks;

    // ── 生命周期 ──────────────────────────────────────────────
    ParserCTP();
    ~ParserCTP();

    /// @brief 注入 CTP 专属配置（broker / flow_dir）。
    /// @note 必须在 init() 之前调用；这是当前唯一配置路径。
    void configure_ctp(const CTPMdCfg& ctp_cfg);

    /// @brief 创建并配置 CTP MD API，保存订阅与重连参数；不启动网络连接。
    bool init(const IMdGateway::Config& cfg);

    /// @brief 启动 CTP 行情连接与登录。
    bool start();

    /// @brief 停止行情通道并释放 CTP MD API。
    void stop();

    // ── 健康检查 ──────────────────────────────────────────────
    /// @brief 健康检查：stale tick 检测 + self-reconnect（冷路径 ~1Hz）。
    bool check_health(uint32_t now_ms);

    // ── 回调绑定 ──────────────────────────────────────────────
    /// @brief 绑定行情、ready、front 状态等上层回调。
    void bind_callbacks(Callbacks callbacks);

    // ── CThostFtdcMdSpi 回调 ──────────────────────────────────────────────
    // 这些函数都在 CTP SDK 回调线程同步执行；实现负责 session 状态迁移和事件转发。

    /// @brief front 连上：开启新 session，清掉旧 login/subscribe/ready 状态并发起登录。
    void OnFrontConnected() override;

    /// @brief front 断开：当前 session 失效，清状态并通知上层 md_ready=false。
    /// @param nReason CTP SDK 原始断连原因码；详细值只用于 log/recorder。
    void OnFrontDisconnected(int nReason) override;

    /// @brief 登录应答：成功后标记 logged_in，并继续订阅行情。
    /// @param pRspUserLogin 登录成功时的 session 信息，可为空。
    /// @param pRspInfo CTP 错误信息；ErrorID 非 0 表示登录失败。
    void OnRspUserLogin(CThostFtdcRspUserLoginField* pRspUserLogin,
                        CThostFtdcRspInfoField* pRspInfo,
                        int nRequestID,
                        bool bIsLast) override;

    /// @brief 订阅应答：本批订阅收齐（bIsLast=true）后通知上层 ready。
    /// @param pSpecificInstrument 本次应答对应的合约，可为空。
    /// @param pRspInfo CTP 错误信息；ErrorID 非 0 表示订阅失败。
    /// @param bIsLast 是否为本批订阅应答的最后一条。
    void OnRspSubMarketData(CThostFtdcSpecificInstrumentField* pSpecificInstrument,
                            CThostFtdcRspInfoField* pRspInfo,
                            int nRequestID,
                            bool bIsLast) override;

    /// @brief 深度行情 tick：将 CTP 五档行情转换为内部 XtRawTick。
    /// @param pDepthMarketData CTP 原始五档行情；为空时直接忽略。
    void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField* pDepthMarketData) override;

private:
    // ── 生命周期内部步骤 ──────────────────────────────────────

    /// @brief init 内部实现，不自行加锁。
    /// @pre 调用方持有 mu_。
    bool init_locked(const IMdGateway::Config& cfg);

    /// @brief start 内部实现，不自行加锁。
    /// @pre 调用方持有 mu_。
    bool start_locked();

    // ── CTP 请求辅助 ──────────────────────────────────────────

    /// @brief 发起 MD 登录请求（login）。
    /// @pre 调用方持有 mu_。
    bool login();

    /// @brief 订阅配置中的行情合约列表（subscribe market data）。
    /// @pre 调用方持有 mu_。
    bool subscribe();

    // ── 回调辅助 ──────────────────────────────────────────────

    /// @brief 判定 CTP 返回是否包含错误。
    bool is_error(CThostFtdcRspInfoField* pRspInfo);

    /// @brief 每个 session 只通知一次 ready。
    /// @pre 调用方持有 mu_。
    void emit_ready();

    // ── 连接状态（mu_ 保护）─────────────────────────────────────────────
    mutable std::mutex mu_;  ///< 串行化控制线程 check_health/stop 与 SDK SPI 回调，保护 api_ 生命周期
    CThostFtdcMdApi* api_ = nullptr;  ///< 持有 CTP MD API 指针；stop/self-reconnect 中 Release 后置空
    std::string front_;     ///< CTP MD 前置地址；init_locked 从 Config 复制
    std::string broker_;    ///< BrokerID；configure_ctp 注入，login 请求使用
    std::string user_;      ///< UserID；init_locked 从 Config 复制，login 请求使用
    std::string pass_;      ///< 登录密码；init_locked 从 Config 复制，login 请求使用
    std::string flow_dir_;  ///< CTP flow 目录；CreateFtdcMdApi 使用
    std::vector<std::string> subs_;  ///< 订阅合约列表；subscribe() 转成 SDK char**
    int req_id_ = 0;                 ///< CTP 请求 ID 递增计数器，当前用于 ReqUserLogin
    bool connected_ = false;         ///< front 连接状态；OnFrontConnected/Disconnected 更新
    bool logged_in_ = false;         ///< 登录成功状态；OnRspUserLogin 成功后置 true
    bool subscribed_ = false;        ///< 本批订阅响应是否已收齐；bIsLast=true 后置 true
    bool ready_emitted_ = false;     ///< per-session ready 幂等标志；重连清零后允许再次通知
    CtpFrontDisconnectLogGate disc_log_gate_;  ///< OnFrontDisconnected 日志限频(mu_ 下访问)

    // ── 健康检查（stale tick self-reconnect + expected-tick window gate）─────────────────
    // 双锚协议：last_tick_steady_ms_ 仅 tick callback（SDK 线程）写；synthetic_arm_steady_ms_
    // 仅 check_health（控制线程）写（off→in 窗口进入 + 每次 stale rebuild 触发前）。staleness
    // 对两锚各算 wrap-safe age 取最小（见 CtpMdSessionGate::min_valid_age）。
    IMdGateway::Config saved_cfg_;                    ///< reconnect 使用的配置快照，init 时保存
    uint32_t stale_reconnect_ms_ = 0;                 ///< stale tick self-reconnect 阈值(ms)，0=禁用
    std::atomic<uint32_t> last_tick_steady_ms_{0};    ///< OnRtnDepthMarketData 在 connected_ gate 同锁内每 tick 落锚（tick-callback-only），check_health 跨线程读取（fast-path 无锁 / slow-path 锁内重读）
    ctp_md_session::TradingWindowGate window_gate_;   ///< expected-tick 窗 gate；gate_enabled=false=不门控(现行行为)
    uint32_t synthetic_arm_steady_ms_ = 0;            ///< 合成重连节流锚（仅 check_health 线程读写；0=未 arm）
    bool prev_in_window_ = false;                     ///< 上次 check_health 的窗内状态（仅 check_health 线程；用于 off→in 边沿检测）
    ctp_util::CtpExchangeWarnGate exchg_enrich_warn_gate_;  ///< ExchangeID 兜底推导未命中时按前缀 warn-once(仅 SDK MD 线程访问)

    // ── 外部回调 ──────────────────────────────────────────────
    std::function<void(const XtRawTick&, uint64_t)> on_tick_;  ///< tick 发布出口（IMdGateway::on_tick 型）；SDK tick 路径锁外调用
    std::function<void()> on_ready_;  ///< session ready 出口；emit_ready 持 mu_ 调用，handler 必须轻量
    std::function<void(bool, int)> on_front_;  ///< front 状态出口；持 mu_ 调用，参数为 connected/reason
};
