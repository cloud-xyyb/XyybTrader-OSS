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

    TraderCTPGateway() = default;

    ~TraderCTPGateway() override = default;

    void configure_ctp(const CTPTdCfg& ctp_cfg) {
        ctp_.configure_ctp(ctp_cfg);
    }

    bool init(const Config& cfg) override {
        return ctp_.init(cfg);
    }

    bool start() override { return ctp_.start(); }

    void stop() override { ctp_.stop(); }

    bool check_health(uint32_t now_ms) override { return ctp_.check_health(now_ms); }

    uint64_t submit_order(const char* code,
                          bool is_buy,
                          int64_t price_fp,
                          int64_t qty_fp,
                          XtTimeInForce tif,
                          const char* user_tag,
                          XtOffset close_offset,
                          ExecFlags exec_flags) override {
        (void)exec_flags;
        return ctp_.submit_order(code, is_buy, price_fp, qty_fp, tif, user_tag, close_offset);
    }

    bool cancel_order(uint64_t local_id) override { return ctp_.cancel_order(local_id); }

    [[nodiscard]] bool is_order_terminal(uint64_t local_id) const override {
        return ctp_.is_local_terminal(local_id);
    }

    [[nodiscard]] bool lookup_terminal(uint64_t local_id,
                                       XtOrderTerminalRecord& out) const override {
        return ctp_.lookup_terminal(local_id, out);
    }

    [[nodiscard]] bool collect_open_delivery_ids(std::vector<uint64_t>& out) const override {
        const auto ids = ctp_.exec_ledger().open_ids();
        out.insert(out.end(), ids.begin(), ids.end());
        return true;
    }

    bool is_recovering() const override { return ctp_.is_recovering(); }

    uint32_t trading_day() const override { return ctp_.trading_day(); }

    Capabilities capabilities() const override {
        return Capabilities{
            true,
            false,
            true,
        };
    }

    QueryControlStats query_control_stats() const override {
        const TraderCTP::QueryControlStats in = ctp_.query_control_stats();
        QueryControlStats out;
        out.query_pos_throttled = in.query_pos_throttled;
        out.query_pos_inflight_skip = in.query_pos_inflight_skip;
        out.query_pos_dirty_merged = in.query_pos_dirty_merged;
        return out;
    }

    bool reconcile_supported() const override { return false; }

    std::optional<std::vector<XtPositionSnapshot>>
        fetch_positions_for_reconcile() override {
        return std::nullopt;
    }

    std::optional<std::size_t>
        detect_and_resolve_ghost_orders_for_reconcile(uint32_t  ) override {
        return std::nullopt;
    }

    void request_resnap_for_reconcile() override {}

    void bind_callbacks(Callbacks callbacks) override {
        ctp_.bind_callbacks(std::move(callbacks));
    }

private:

    TraderCTP ctp_;
};
