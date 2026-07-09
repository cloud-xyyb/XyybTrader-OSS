#pragma once

#include <cstdint>

#include "xt_risk/engine/XtRiskMonitorState.h"

namespace xt_account { class AccountStateStore; }

namespace xt_risk {

/// @brief 盘中的主动风控监视器：日内回撤与行情停滞检测。
///
/// 控制环每帧喂入当前权益和行情时间：日内从峰值回撤超过阈值时先缩小下单量、再禁止开仓；
/// 行情停滞超阈值时标记 stale。结果写入 XtRiskMonitorState，
/// 由 submit 热路径读取。不创建线程；open halt 和 volume scale 都是
/// session 级状态，只有 on_session_begin() 会重置。
class XtRiskMonitor {
public:
    /// @brief Active risk 阈值配置。
    struct Config {
        bool enabled = false;

        int64_t idd_scale_threshold_bp = 200;
        int64_t idd_halt_threshold_bp  = 500;
        int64_t idd_scale_factor_fp    = 500000;

        uint32_t stale_quote_ms      = 0;
    };

    explicit XtRiskMonitor(Config cfg, XtRiskMonitorState* broadcast);

    void on_session_begin(uint32_t trading_date, int64_t current_equity_fp,
                          int64_t current_cashflow_fp = 0);

    void set_account_store(xt_account::AccountStateStore* store) noexcept;

    void on_frame(int64_t equity_fp, uint32_t last_md_tick_ms, uint32_t now_ms,
                  int64_t current_cashflow_fp = 0);

    void restore_latches(bool open_halted, int64_t halt_trigger_ms, int64_t peak_equity_fp);

    bool sanity_check_per_sid_state(std::size_t expected_size) const noexcept;

    const Config& config() const noexcept { return cfg_; }

    const XtRiskMonitorState* broadcast_state() const noexcept { return broadcast_; }

private:

    void update_drawdown(int64_t equity_fp, uint32_t now_ms);

    void update_stale_quote(uint32_t last_md_tick_ms, uint32_t now_ms);

    Config               cfg_;
    XtRiskMonitorState*  broadcast_;
    int64_t              peak_equity_fp_ = 0;
    int64_t              session_open_cashflow_fp_ = 0;
    uint32_t             last_session_date_ = 0;
    bool                 session_initialized_ = false;
    xt_account::AccountStateStore* account_store_ = nullptr;
};

}
