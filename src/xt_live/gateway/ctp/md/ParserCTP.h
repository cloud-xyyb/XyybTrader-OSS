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

    using Callbacks = IMdGateway::Callbacks;

    ParserCTP();
    ~ParserCTP();

    void configure_ctp(const CTPMdCfg& ctp_cfg);

    bool init(const IMdGateway::Config& cfg);

    bool start();

    void stop();

    bool check_health(uint32_t now_ms);

    void bind_callbacks(Callbacks callbacks);

    void OnFrontConnected() override;

    void OnFrontDisconnected(int nReason) override;

    void OnRspUserLogin(CThostFtdcRspUserLoginField* pRspUserLogin,
                        CThostFtdcRspInfoField* pRspInfo,
                        int nRequestID,
                        bool bIsLast) override;

    void OnRspSubMarketData(CThostFtdcSpecificInstrumentField* pSpecificInstrument,
                            CThostFtdcRspInfoField* pRspInfo,
                            int nRequestID,
                            bool bIsLast) override;

    void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField* pDepthMarketData) override;

private:

    bool init_locked(const IMdGateway::Config& cfg);

    bool start_locked();

    bool login();

    bool subscribe();

    bool is_error(CThostFtdcRspInfoField* pRspInfo);

    void emit_ready();

    mutable std::mutex mu_;
    CThostFtdcMdApi* api_ = nullptr;
    std::string front_;
    std::string broker_;
    std::string user_;
    std::string pass_;
    std::string flow_dir_;
    std::vector<std::string> subs_;
    int req_id_ = 0;
    bool connected_ = false;
    bool logged_in_ = false;
    bool subscribed_ = false;
    bool ready_emitted_ = false;
    CtpFrontDisconnectLogGate disc_log_gate_;

    IMdGateway::Config saved_cfg_;
    uint32_t stale_reconnect_ms_ = 0;
    std::atomic<uint32_t> last_tick_steady_ms_{0};
    ctp_md_session::TradingWindowGate window_gate_;
    uint32_t synthetic_arm_steady_ms_ = 0;
    bool prev_in_window_ = false;
    ctp_util::CtpExchangeWarnGate exchg_enrich_warn_gate_;

    std::function<void(const XtRawTick&, uint64_t)> on_tick_;
    std::function<void()> on_ready_;
    std::function<void(bool, int)> on_front_;
};
