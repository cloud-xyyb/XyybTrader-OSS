#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "api/XtPositionSnapshot.h"
#include "xt_core/order/XtOrderTypes.h"

/**
 * @brief Gateway 提供给实盘 reconcile loop 的冷路径对账接口。
 *
 * @details
 * 这里的 reconcile 指“以 exchange/broker 返回的当前事实为准，周期性校验并修正
 * 本地 account store 与 in-flight order 状态”。它不在下单热路径运行，只用于处理
 * 网络丢包、UDS gap、进程重启或本地事件漏处理后产生的状态偏差。
 *
 * `fetch_positions_for_reconcile()` 拉取交易所当前 position 全量快照，调用方负责
 * 与本地持仓比对、累计 divergence、触发 resnap 或 kill_switch。
 * `detect_and_resolve_ghost_orders_for_reconcile()` 处理本地仍认为 in-flight、
 * 但交易所已不再认为是活单的 ghost order，并把交易所终态回灌到标准 TD 事件路径。
 * `request_resnap_for_reconcile()` 请求 gateway 重新拉取权威快照或重放 gap，用
 * exchange truth 覆盖本地状态。
 *
 * 不支持外部 reconcile 的 gateway 必须让 `reconcile_supported()` 返回 false；
 * 调用方必须先检查该开关再调用其他入口。
 *
 * @warning ghost order resolve 必须通过 gateway callback（如
 * `ITradeGateway::Callbacks::on_order_update`）发出，不得直接调用 engine。
 */
class IReconcilableGateway {
public:
    virtual ~IReconcilableGateway() = default;

    [[nodiscard]] virtual bool reconcile_supported() const = 0;

    [[nodiscard]] virtual std::optional<std::vector<XtPositionSnapshot>>
        fetch_positions_for_reconcile() = 0;

    [[nodiscard]] virtual std::optional<size_t>
        detect_and_resolve_ghost_orders_for_reconcile(uint32_t min_age_ms) = 0;

    virtual void request_resnap_for_reconcile() = 0;
};
