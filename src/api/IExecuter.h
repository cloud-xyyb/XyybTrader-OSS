#pragma once

#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"  // XtOrderExecUpdate
#include "api/IntentResult.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

namespace xt_runtime {

/// @brief 策略仓位 intent 的执行接口。
///
/// `XtCtaEngine` 将策略 intent 归一为单合约 signed delta，并在 filter /
/// risk scaling 后交给 executer。实现负责把可接受的 delta 转成
/// `IExecutionGateway::submit()` 调用，并通过订单/成交回报维护自己的执行状态。
///
/// direct-order 路径直接持有 `IExecutionGateway`，不经过本接口；HFT live/backtest
/// 属于 direct-order 路径。
///
/// @note owning engine serializes calls；本接口不承诺跨线程安全。
class IExecuter {
public:
    virtual ~IExecuter() = default;

    /// @brief executer 标识；用于诊断、日志和装配期识别。
    virtual const char* name() const noexcept = 0;

    /// @brief 批量设置全量目标仓位。
    ///
    /// 用于初始化 / session begin 时从持仓快照建立 target。主路径（策略
    /// 产生 intent）走 on_position_changed；不支持 bulk reset 的实现可留空。
    ///
    /// @param targets_fp  全量目标仓位映射 {code → target_pos_fp}
    virtual void set_position(const std::unordered_map<std::string, int64_t>& /*targets_fp*/) {}

    /// @brief 单合约目标仓位变化通知。
    ///
    /// `diff_pos_fp` 是 engine 已处理 filter / risk scaling 后的 signed delta；
    /// 正数增加 net position，负数减少 net position。调用方只传变化量；实现若
    /// 需要全量目标仓位，必须自行维护内部 target。
    ///
    /// @param strategy_id  发起 intent 的策略 id（OMS 多策略归属 / trade attribution 用）
    /// @param code         合约代码
    /// @param diff_pos_fp  本次变化量（scaled fp；正 = 买入/平空，负 = 卖出/平多）
    /// @param tag          intent 标签；可为空串
    virtual void on_position_changed(std::string_view strategy_id,
                                     const char* code,
                                     int64_t     diff_pos_fp,
                                     const char* tag) = 0;

    /// @brief 单合约目标仓位变化通知，并同步返回实际受理量。
    ///
    /// `accepted_delta_fp` 与请求 delta 同符号；部分受理时绝对值小于请求量。
    /// 调用方用该值修正 virtual position，避免 reject 后 logical position 漂移。
    ///
    /// 默认实现 forward 到 `on_position_changed`，并返回 100% accepted（保持
    /// mock executer / test stub 兼容）；会产生拒单回滚需求的实现必须 override。
    ///
    /// @param price_fp  策略限价（fp），透传到 gateway。
    /// @param tif       策略 tif，透传到 gateway。
    virtual IntentResult on_position_changed_with_result(
        std::string_view strategy_id,
        const char* code,
        int64_t     diff_pos_fp,
        int64_t     price_fp,
        XtTimeInForce tif,
        const char* tag) {
        (void)price_fp;
        (void)tif;
        on_position_changed(strategy_id, code, diff_pos_fp, tag);
        IntentResult r;
        r.accepted_delta_fp = diff_pos_fp;
        r.reject_reason = XtRejectCode::NoError;
        return r;
    }

    /// @brief 可选行情驱动入口。
    ///
    /// 默认空实现；需要行情推进执行状态的实现可 override。
    virtual void on_tick(const char* /*code*/, const XtTick& /*tick*/) {}

    /// @brief 订单回报（engine demux 后分发给 owning executer）。
    ///
    /// 实现据此结算 pending / submitted 等内部状态。默认空实现。
    virtual void on_order_update(const XtOrderExecUpdate& /*upd*/) {}

    /// @brief 成交回报（engine demux 后分发给 owning executer）。
    ///
    /// 默认空实现：同 on_order_update。
    virtual void on_fill(const XtTradeRecord& /*tr*/) {}

    /// @brief 内部状态快照（诊断 / test）。
    ///
    /// 默认空实现。derived class 可返回 JSON / 多行文本描述。
    virtual std::string report() const { return {}; }
};

}  // namespace xt_runtime
