#pragma once

/// @file ResyncFillReplayer.h
/// @brief 把暂存的 fill 按 venue 时间切成「快照前/快照后」两段的纯函数
///
/// 只做数据分区，不 emit、不持锁、不碰 gateway 状态；
/// 实际 emit 顺序由 ResyncFillGate::finish_with_snapshot() 执行。

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "api/ITradeGateway.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_live/gateway/binance/td/EffectiveSignalDedup.h"

namespace binance_u {

/// @brief 栅栏暂存的终态 order update + 其构造时 latency eligibility 与 dedup source。
/// @note flush 时透传构造时 eligibility（latency-terminal 批 L-1.2）。
struct ResyncTerminal {
    XtOrderExecUpdate            upd;
    EffectiveSignalDedup::Source src;
    XtLatencyEligibility         elig = XtLatencyEligibility::kEligible;
};

/// @brief 栅栏 flush 的有序 emit 计划（纯数据，caller 负责实际 emit + 持锁）。
///
/// 快照前的 fill 先 emit（记 PnL/fee，仓位效果被快照吸收不双计），
/// 快照后的 fill 在快照之后 emit 作正常增量；
/// 终态排在全部 fill 之后，保证策略先见 fill 后见终态。
/// 各段内部保持到达顺序。
struct ReplayPlan {
    std::vector<XtTradeRecord> pre_fills;
    std::vector<XtTradeRecord> post_fills;

    std::vector<ResyncTerminal> terminals;
    bool emit_position = false;
};

[[nodiscard]] inline ReplayPlan partition_resync_replay(
    std::vector<XtTradeRecord> fills,
    std::vector<ResyncTerminal> terminals,
    const std::unordered_map<std::string, int64_t>& fence_update_ms,
    int64_t fence_floor_ms,
    bool has_snapshot) {
    ReplayPlan plan;
    plan.terminals = std::move(terminals);

    if (!has_snapshot) {

        plan.pre_fills = std::move(fills);
        plan.emit_position = false;
        return plan;
    }

    plan.emit_position = true;

    const auto fence_of = [&](const std::string& code) -> int64_t {
        const auto it = fence_update_ms.find(code);
        if (it != fence_update_ms.end() && it->second > 0) return it->second;
        return fence_floor_ms;
    };
    const auto pre_snapshot = [&](const XtTradeRecord& tr) {
        return static_cast<int64_t>(tr.fill_ts_us / 1000) <= fence_of(tr.code);
    };

    for (auto& tr : fills) {
        if (pre_snapshot(tr)) {
            plan.pre_fills.push_back(std::move(tr));
        } else {
            plan.post_fills.push_back(std::move(tr));
        }
    }
    return plan;
}

}
