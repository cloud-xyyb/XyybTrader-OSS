#pragma once

#include <cstdint>
#include <string_view>

#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOrderTypes.h"  // XtOrderExecUpdate
#include "api/ITradeGateway.h"  // AccountSnapshot
#include "xt_account/api/AccountSnapshot.h"

namespace xt_runtime {

/// @brief HFT engine 执行事件观察者（HFT engine execution event observer）。
///
/// Engine 在 domain 操作（order_book / account / bridge）完成后，遍历 observer
/// 链扇出事件。Observer 不修改 domain 状态，仅用于 out-of-band 观测。
///
/// 默认所有方法为空实现 — 实现类只需 override 需要的事件。
class IHftEngineObserver {
public:
    virtual ~IHftEngineObserver() = default;

    /// @brief 成交事件（trade observed）— 在 engine.on_trade 完成 domain 处理后调用。
    /// @param elig latency eligibility：kIneligible（延迟重放/合成）不喂 latency tracker。
    /// @param logical_order_id L-2 fill-link：engine build_strategy_fill_event_ 已算出的
    ///        本次 logical order id（非 0 时 latency observer 用其改写 tracker-only 副本的
    ///        order_id，与 submit track 键同域配对；0 = 无策略路由，其它 observer 忽略此参）。
    virtual void on_trade(const XtTradeRecord& /*tr*/,
                          XtLatencyEligibility /*elig*/ = XtLatencyEligibility::kEligible,
                          uint64_t /*logical_order_id*/ = 0) {}

    /// @brief 订单状态更新（order update observed）— 在 engine.on_order_update
    /// 完成 domain 处理后调用（参数已经过 IOrderIdTranslator 翻译）。
    /// @param elig latency eligibility：kIneligible（本地合成/延迟重放）不喂 latency tracker。
    virtual void on_order_update(const XtOrderExecUpdate& /*upd*/,
                                 XtLatencyEligibility /*elig*/ = XtLatencyEligibility::kEligible) {}

    /// @brief 订单提交事件（BT-only，live 不调；engine 不 fan-out，仅 BT gateway 调）。
    ///        Live observer 继承 no-op 零开销。
    ///        BT 用于 reporter 缓存 order_id→code 给 incident trace。
    virtual void on_order_submit(int64_t /*ts*/,
                                 uint64_t /*id*/,
                                 const char* /*code*/,
                                 bool /*is_buy*/,
                                 int64_t /*price_fp*/,
                                 int64_t /*qty_fp*/,
                                 std::string_view /*user_tag*/,
                                 bool /*is_close*/,
                                 bool /*is_today*/) {}

    // 持仓 / 账户快照 observer（live 快照事件的 out-of-band 广播）。
    // Backtest 不产生这些事件，默认空实现。Live combined adapter 内部转发到
    // runner_signals 的对应 atomic 字段。

    /// @brief 持仓快照处理开始 — engine.on_position_snapshot 进入时调用（在
    /// domain 处理之前），live 侧用于暂停策略 trading_ready 门闸。
    virtual void on_position_snapshot_begin(bool /*is_recovery*/) {}

    /// @brief 持仓快照处理完成 — engine.on_position_snapshot 结束前调用，
    /// live 侧用于更新 last_position_snapshot_ms 并刷新 trading_ready。
    /// @param position_applied true = engine 本次真正把快照灌进 account_store
    ///        (account_store_ && catalog_ 均非空)；false = hydration 被跳过。
    ///        readiness latch（position_snapshot_applied / td_position_ready）
    ///        只在 position_applied 时置位（诚实契约）；freshness witness 无条件更新。
    /// @param session_gen  TD session generation（gen fence）；observer 仅对 `session_gen ==
    ///        当前 signals gen` 置 readiness latch，旧 gen 的 stale snapshot 跳过。
    virtual void on_position_snapshot_end(bool /*is_recovery*/, bool /*position_applied*/,
                                          uint32_t /*session_gen*/) {}

    /// @brief 通道 ready 边沿 — engine.on_channel_ready 内 fan 到 observer（worker 线程）。
    /// live 侧用于把 readiness latch 的置位收成 worker 单写状态机（td 通道 up + apply 配对）。
    /// @note 与 IStrategyContext::on_channel_ready 同名但不同接口（此为 engine observer）。
    virtual void on_channel_ready(const char* /*channel*/) {}
    /// @brief 通道断开边沿 — engine.on_channel_lost 内 fan 到 observer（worker 线程）。
    /// live 侧用于在 worker 顺序清 readiness latch（排在 stale snapshot 之后，根治 re-latch）。
    virtual void on_channel_lost(const char* /*channel*/, int /*reason*/) {}

    /// @brief 账户快照处理完成 — engine.on_account_snapshot 末尾调用，
    /// live 侧用于广播 balance / close_profit / snapshot_ms。
    virtual void on_account_snapshot(const ITradeGateway::AccountSnapshot& /*snap*/,
                                      const xt_account::AccountSnapshot& /*metrics*/) {}
};

} // namespace xt_runtime
