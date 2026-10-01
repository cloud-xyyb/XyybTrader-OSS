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
        bool enabled = false; ///< false 时 on_frame() no-op；session reset 仍可执行。

        // IDD 阈值，单位 basis points。
        int64_t idd_scale_threshold_bp = 200;    ///< 触发 volume_scale 的回撤阈值。
        int64_t idd_halt_threshold_bp  = 500;    ///< 触发 open_halted 的回撤阈值。
        int64_t idd_scale_factor_fp    = 500000; ///< scale 后的下单量系数；1e6 = 1.0x。

        /// @brief MD tick 停滞阈值，单位 ms；0 = 禁用。
        /// @note last_md_tick_ms 应来自 XtSteadyClock 单调时钟。
        uint32_t stale_quote_ms      = 0;
    };

    /// @brief 构造监控器；broadcast 为 non-owning，可为 nullptr。
    explicit XtRiskMonitor(Config cfg, XtRiskMonitorState* broadcast);

    /// @brief Session 开始时重置 peak/broadcast，并冻结 starting_balance。
    /// @note same trading_date 幂等；trading_date == 0 时按 first-call wins 处理。
    /// @param current_cashflow_fp session 开始时的累计外部出入金快照。
    void on_session_begin(uint32_t trading_date, int64_t current_equity_fp,
                          int64_t current_cashflow_fp = 0);

    /// @brief 注入 AccountStateStore，让 on_session_begin_hook() 冻结 per-sid daily-loss 基准。
    /// @note non-owning；nullptr 表示跳过 starting_balance freeze。
    void set_account_store(xt_account::AccountStateStore* store) noexcept;

    /// @brief Frame step：更新 cashflow-immune IDD、stale quote 和 last_update_ms。
    /// @param equity_fp 当前账户权益，含外部出入金。
    /// @param last_md_tick_ms 最近一笔 MD tick 的 steady ms；0 表示尚无 tick。
    /// @param now_ms 当前 steady ms。
    /// @param current_cashflow_fp 当前累计外部出入金；用于从 IDD 中扣除 session 内 cashflow。
    void on_frame(int64_t equity_fp, uint32_t last_md_tick_ms, uint32_t now_ms,
                  int64_t current_cashflow_fp = 0);

    /// @brief 启动期从 SafetyState 恢复 latched 状态（broadcast 唯一写入口）。
    /// @note open_halted 只 false→true latch；peak_equity 取 max；broadcast nullptr 时 no-op。
    ///   取代 caller 直接 poke broadcast，使 XtRiskMonitor 成为 broadcast decision 字段唯一 writer。
    void restore_latches(bool open_halted, int64_t halt_trigger_ms, int64_t peak_equity_fp);

    /// @brief 检查 per-sid runtime state 容量是否等于 sid_registry.sid_slot_count()。
    /// @param expected_size sid_registry.sid_slot_count()，包含 reserved sid 0。
    /// @return false 表示检测到 drift；函数只记 error log，不 abort。
    bool sanity_check_per_sid_state(std::size_t expected_size) const noexcept;

    const Config& config() const noexcept { return cfg_; }

    /// @brief 返回 non-owning broadcast state，供 lifecycle/persistence 层只读访问。
    /// @note decision 字段（open_halted/volume_scale/IDD）唯一写者是本类；
    ///       不提供可变指针，per_strategy 写入方持自己的 XtRiskMonitorState* 直接访问。
    const XtRiskMonitorState* broadcast_state() const noexcept { return broadcast_; }

private:
    /// @brief 更新 peak、drawdown bp、volume scale/open halt。
    void update_drawdown(int64_t equity_fp, uint32_t now_ms);

    /// @brief 检查 stale quote 并 latch open_halted。
    void update_stale_quote(uint32_t last_md_tick_ms, uint32_t now_ms);

    Config               cfg_;
    XtRiskMonitorState*  broadcast_; ///< non-owning；nullptr 时 on_frame() no-op。
    int64_t              peak_equity_fp_ = 0; ///< session 内 risk_equity 高水位。
    int64_t              session_open_cashflow_fp_ = 0; ///< session 起点累计外部出入金。
    uint32_t             last_session_date_ = 0;  ///< on_session_begin 去重（idempotent guard）
    bool                 session_initialized_ = false; ///< trading_date == 0 的 first-call sentinel。
    xt_account::AccountStateStore* account_store_ = nullptr; ///< non-owning；nullptr 跳过 freeze。
};

}  // namespace xt_risk
