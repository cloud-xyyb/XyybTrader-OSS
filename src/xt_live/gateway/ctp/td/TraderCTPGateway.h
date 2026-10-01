#pragma once

#include <functional>
#include <optional>
#include <utility>
#include <vector>

#include "api/ITradeGateway.h"
#include "api/IReconcilableGateway.h"
#include "ctp/td/TraderCTP.h"
#include "ctp/td/CTPTdCfg.h"

/// @brief CTP 交易网关的对外接口层，实现 ITradeGateway。
///
/// TraderCTPGateway 只负责把 provider-neutral 的调用给底层 TraderCTP
/// 例如：报撤单、回报、账户/持仓查询和恢复流程
///
/// @note reconcile_supported() 恒为 false：CTP 的一致性靠登录后的恢复查询流程，
/// 不走外部周期 reconcile。
class TraderCTPGateway : public ITradeGateway, public IReconcilableGateway {
public:
    /// @brief 构造底层 `TraderCTP`；不读取配置、不启动 CTP SDK。
    TraderCTPGateway() = default;

    /// @brief 释放底层 `TraderCTP`；CTP API 资源由 `TraderCTP` 析构处理。
    ~TraderCTPGateway() override = default;

    /// @brief 注入 CTP 专属配置；必须在 `init()` 前调用。
    void configure_ctp(const CTPTdCfg& ctp_cfg) {
        ctp_.configure_ctp(ctp_cfg);
    }

    /// @brief 初始化底层 `TraderCTP` 并创建 CTP API；不发起连接。
    /// @return false 表示配置、flow_dir、OrderRef 持久化或 API 创建失败。
    bool init(const Config& cfg) override {
        return ctp_.init(cfg);
    }

    /// @brief 启动 CTP TD 连接、登录和恢复流程。
    /// @return false 表示 `init()` 未成功或底层 API 不可用。
    /// @note readiness 通过已绑定 callbacks 异步交付。
    bool start() override { return ctp_.start(); }

    /// @brief 停止后台线程并释放 CTP TD API；不清空已绑定 callbacks。
    void stop() override { ctp_.stop(); }

    /// @brief 执行 TD stuck-disconnect 健康检查。
    /// @param now_ms `XtSteadyClock::steady_now_ms()` 时间源。
    /// @return false 表示本次触发 CTP API self-heal 重建。
    bool check_health(uint32_t now_ms) override { return ctp_.check_health(now_ms); }

    // ── ITradeGateway: 交易接口 ──────────────────────────────────────────────
    /// @brief 转发下单请求到 `TraderCTP`。
    /// @note CTP 无 reduceOnly/postOnly/hidden 语义，`exec_flags` 会被忽略。
    /// @return 本地订单号；0 表示校验失败或 CTP 请求未成功下发。
    uint64_t submit_order(const char* code,
                          bool is_buy,
                          int64_t price_fp,
                          int64_t qty_fp,
                          XtTimeInForce tif,
                          const char* user_tag,
                          XtOffset close_offset,
                          ExecFlags exec_flags) override {
        (void)exec_flags;  // CTP 忽略 ExecFlags（无对应柜台语义）
        return ctp_.submit_order(code, is_buy, price_fp, qty_fp, tif, user_tag, close_offset);
    }

    /// @brief 转发撤单请求到 `TraderCTP`。
    /// @return false 表示 API 不可用、无可撤目标或 CTP 请求未成功下发。
    bool cancel_order(uint64_t local_id) override { return ctp_.cancel_order(local_id); }

    /// @brief 转发 venue 终态查询给底层 TraderCTP（lost-terminal reconcile）。
    [[nodiscard]] bool is_order_terminal(uint64_t local_id) const override {
        return ctp_.is_local_terminal(local_id);
    }

    /// @brief 转发终态 payload 查询给底层 TraderCTP（交付投影 terminal_record）。
    [[nodiscard]] bool lookup_terminal(uint64_t local_id,
                                       XtOrderTerminalRecord& out) const override {
        return ctp_.lookup_terminal(local_id, out);
    }

    /// @brief 导出交付投影未终态订单 id（停机对账差集观测）。
    [[nodiscard]] bool collect_open_delivery_ids(std::vector<uint64_t>& out) const override {
        const auto ids = ctp_.exec_ledger().open_ids();
        out.insert(out.end(), ids.begin(), ids.end());
        return true;
    }

    // ── ITradeGateway: 恢复控制 ──────────────────────────────────────────────
    /// @brief 查询底层是否仍处于登录后恢复查询窗口。
    bool is_recovering() const override { return ctp_.is_recovering(); }
    /// @brief venue 权威交易日（登录应答 TradingDay）；未登录返 0。
    uint32_t trading_day() const override { return ctp_.trading_day(); }

    // ── ITradeGateway: 通道能力 ──────────────────────────────────────────────
    /// @brief 返回 CTP TD 通道能力。
    /// @note close_today 与 IOC/FOK 为 true；L2 orderflow 不是 TD 能力。
    Capabilities capabilities() const override {
        return Capabilities{
            true,
            false,
            true,
        };
    }

    /// @brief 导出 CTP 持仓查询控制统计。
    /// @note 仅用于 telemetry，不作为跨线程同步或恢复状态判断依据。
    QueryControlStats query_control_stats() const override {
        const TraderCTP::QueryControlStats in = ctp_.query_control_stats();
        QueryControlStats out;
        out.query_pos_throttled = in.query_pos_throttled;
        out.query_pos_inflight_skip = in.query_pos_inflight_skip;
        out.query_pos_dirty_merged = in.query_pos_dirty_merged;
        return out;
    }

    // ── IReconcilableGateway: opt-out (详见 class doc) ──────────────────────
    /// @brief 固定返回 false；CTP 当前不支持外部同步 reconcile 入口。
    bool reconcile_supported() const override { return false; }

    /// @brief 外部 position reconcile stub；固定返回 `std::nullopt`。
    std::optional<std::vector<XtPositionSnapshot>>
        fetch_positions_for_reconcile() override {
        return std::nullopt;
    }

    /// @brief 外部 ghost-order reconcile stub；固定返回 `std::nullopt`。
    std::optional<std::size_t>
        detect_and_resolve_ghost_orders_for_reconcile(uint32_t /*min_age_ms*/) override {
        return std::nullopt;
    }

    /// @brief 外部 resnap stub；当前 no-op。
    void request_resnap_for_reconcile() override {}

    // ── ITradeGateway: 回调绑定 ──────────────────────────────────────────────
    /// @brief 绑定统一 TD callbacks；必须在 `start()` 前调用。
    /// @warning 运行中 rebind 会与 SDK 回调线程或后台刷新线程并发。
    void bind_callbacks(Callbacks callbacks) override {
        ctp_.bind_callbacks(std::move(callbacks));
    }

private:
    // ── 底层 TD 实例 ──────────────────────────────────────────────
    TraderCTP ctp_;
};
