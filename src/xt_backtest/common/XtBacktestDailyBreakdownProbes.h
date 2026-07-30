#pragma once

/// @file
/// @brief CTA 回测把 gateway 的提交 / 拒单事件按交易日计入 daily_breakdown 的探针。
///
/// bar / tick 两个 CTA runner 共用。探针只做「按当前交易日累加计数」，不触碰任何
/// domain 状态（account / order_book / 撮合），因此挂上它们后成交产物逐字节不变。

#include <cstdint>
#include <string_view>

#include "api/IHftEngineObserver.h"
#include "api/IRejectSink.h"
#include "api/XtRiskTypes.h"
#include "xt_backtest/reporter/XtBacktestDailyBreakdown.h"

namespace xt_core {
struct XtOrder;
}

namespace xt_backtest {

/// @brief 把 gateway 的 `on_order_submit` 事件按「当前交易日」计入 daily_breakdown。
///
/// runner 主循环在处理每根 bar / 每个 tick 前把 `*cur_trading_date` 更新为该事件的
/// 交易日；提交在策略回调里发生（gateway.submit 期），探针据此按交易日聚合，与 fill /
/// eod 的日轴口径一致。故意**不用** on_order_submit 携带的 ts —— 那是 action 轴，夜盘
/// 会与交易日分叉；fill / eod / tick 全部按交易日 key，提交也必须同轴。
struct DailyBreakdownSubmitProbe final : xt_runtime::IHftEngineObserver {
    XtBacktestDailyBreakdown* bd = nullptr;
    const uint32_t* cur_trading_date = nullptr;

    void on_order_submit(int64_t  , uint64_t  , const char*  ,
                         bool  , int64_t  , int64_t  ,
                         std::string_view  , bool  ,
                         bool  ) override {
        if (bd && cur_trading_date && *cur_trading_date != 0)
            bd->on_submit(*cur_trading_date);
    }
};

/// @brief 把 gateway 的风控拒单按「当前交易日」计入 daily_breakdown（含拒因分桶）。
///
/// 触发点与 `XtCtaBacktestOrderGateway::risk_reject_count_` 自增逐一同步（同在
/// `evaluate_risk_` 拒单分支），保证 daily_breakdown 逐日 risk_rejects 之和 ==
/// summary.risk_rejects —— 消费端 normalize 的 risk_rejects 口径不因新增 breakdown
/// 而回归成 0。CTA gateway 只调 `on_reject`（无 margin 专用路径），`on_margin_reject`
/// 一并覆盖以防其它 venue 装配复用本探针。
struct DailyBreakdownRejectProbe final : IRejectSink {
    XtBacktestDailyBreakdown* bd = nullptr;
    const uint32_t* cur_trading_date = nullptr;

    void on_margin_reject(const xt_core::XtOrder&  , const XtRiskResult& decision,
                          int64_t  , std::string_view  ) override {
        if (bd && cur_trading_date && *cur_trading_date != 0)
            bd->on_risk_reject(*cur_trading_date, decision.reason_text);
    }
    void on_reject(const xt_core::XtOrder&  , const XtRiskResult& decision,
                   std::string_view  ) override {
        if (bd && cur_trading_date && *cur_trading_date != 0)
            bd->on_risk_reject(*cur_trading_date, decision.reason_text);
    }
};

}
