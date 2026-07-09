#pragma once
#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ThostFtdcTraderApi.h"

/// @brief CTP 登录后恢复流程的状态机。
///
/// 进程重启或重连后本地是一张白纸，必须先把账户状态从柜台查回来才能交易。
/// CtpRecovery 按 持仓 → 资金 → 委托 → 成交 的顺序推进查询，全部完成后发布 ready
/// 查询的实际发起和结果落账都经 HostPorts 交回 TraderCTP。
/// login 和结算确认不归 CtpRecovery 管，属于外层 session 流程。
class CtpRecovery {
public:

    struct PositionSnapshot {
        std::string code;
        double long_qty          = 0.0;
        double long_today        = 0.0;
        double long_avg          = 0.0;
        double short_qty         = 0.0;
        double short_today       = 0.0;
        double short_avg         = 0.0;
        double long_frozen       = 0.0;
        double short_frozen      = 0.0;
    };

    using PositionsPublisher = std::function<void(const std::vector<PositionSnapshot>&)>;

    using ReadyPublisher = std::function<void()>;

    using QueryStarter = std::function<void()>;

    /// @brief Recovery 状态机依赖的宿主端口包。
    /// @note `start_*_query` 由 recovery 推进阶段时调用，最终回到宿主发起
    ///       CTP 查询；`publish_*` 在阶段完成后把聚合结果 / ready 交回宿主。
    /// @note 所有端口都在 recovery 锁外同步执行，允许查询入口反入
    ///       `TraderCTP::query_once(...)`。
    struct HostPorts {
        PositionsPublisher publish_positions;
        QueryStarter       start_position_query;
        QueryStarter       start_account_query;
        QueryStarter       start_order_query;
        QueryStarter       start_trade_query;
        ReadyPublisher     publish_ready;
    };

    explicit CtpRecovery(HostPorts ports);

    void set_recovery_enabled(bool enabled);

    bool recovery_enabled() const;

    bool is_recovering() const;

    void reset();

    void on_login();

    void on_position(const CThostFtdcInvestorPositionField* pPosition, bool bIsLast);

    void on_account_done(bool bIsLast);

    void on_order_done(bool bIsLast);

    void on_trade_done(bool bIsLast);

private:
    /// @brief on_position 阶段的持仓聚合中间状态。
    struct SideBucketAgg {
        double total          = 0.0;
        double today_by_date  = 0.0;
        double today_by_field = 0.0;
        double yd_sum         = 0.0;
        bool   saw_pos_date    = false;
        bool   saw_today_field = false;
        bool   saw_yd_field    = false;
    };
    struct PositionBucketAgg {
        SideBucketAgg long_side;
        SideBucketAgg short_side;
    };

    void merge_position_row(const CThostFtdcInvestorPositionField* row);

    std::vector<PositionSnapshot> collect_snapshots();

    static void accumulate_side(SideBucketAgg& side,
                                double qty, double today_field,
                                double yd, char pos_date);

    static double resolve_today_qty(const SideBucketAgg& side);

    void emit_ready();

    bool recovery_enabled_    = false;
    bool recovery_done_       = false;
    bool recovered_positions_ = false;
    bool recovered_account_   = false;
    bool recovered_orders_    = false;
    bool recovered_trades_    = false;
    bool ready_emitted_       = false;

    uint32_t reset_gen_ = 0;
    std::atomic<bool> recovering_{false};

    mutable std::mutex mu_;
    std::unordered_map<std::string, PositionSnapshot>  pos_cache_;
    std::unordered_map<std::string, PositionBucketAgg> pos_bucket_cache_;

    PositionsPublisher publish_positions_;
    QueryStarter       start_position_query_;
    QueryStarter       start_account_query_;
    QueryStarter       start_order_query_;
    QueryStarter       start_trade_query_;
    ReadyPublisher     publish_ready_;
};
