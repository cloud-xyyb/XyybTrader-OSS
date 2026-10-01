#pragma once
// -----------------------------------------------------------------------------
// SelWeightsCsvStrategy —  SEL 最简策略 demo
// -----------------------------------------------------------------------------
//
// [用途]
// backtest 闭环验证用最小 SEL 策略：从 weights.csv 文件读
//   预生成的 target_weights，按交易日推送到 ctx.set_target_weights()。
//
//   - 上游工具生成 weights → SEL backtest replay → 验证通路。
//   - 完整 lifecycle 不调 buy_fp / sell_fp（SEL 单接口约束）。
//   - 与 wt SelEngine 对齐：on_schedule 读 weights → set_position；这里是
//     csv 静态版（后续 ctpy / 因子动态版基于此架构）。
//
// [weights.csv schema]（表头定形态；必需三列 date,symbol,weight 固定在前，其后可带
//  任意子集的可选列，与 canonical A 股数据契约 + validator 对齐）
//   必需：date,symbol,weight
//   可选：signal_asof_date  信号可得日；只校验格式不消费其值（未来函数由 validator /
//                           walk-forward gate 判，引擎不据其决策）
//         effective_time    该行生效的 fire 时刻（HHMM）；空/缺列 = 不限时刻
//         task_uid          该行归属的 task 标识；空/缺列 = 不限 task
//
//   date,symbol,weight
//   20260109,600000.SH,0.30
//   20260116,600000.SH,0.50
//   ...
//
//   date,symbol,weight,effective_time,task_uid          # 同日两次 fire 各取其槽
//   20260109,600000.SH,0.30,1030,morning
//   20260109,600000.SH,0.55,1500,close
//
// [槽（slot）语义]
//   一行归属的槽 = (date, effective_time, task_uid)，两个可选维各自的空值是**通配**。
//   fire 时按「精确 → 通配」四级取**整槽**（不跨槽混权重）：
//     ① (date, curTime, 本策略 task_uid) → ② (date, curTime, 通配)
//     → ③ (date, 通配, 本策略 task_uid)  → ④ (date, 通配, 通配)
//   首个命中的槽整体作为本次目标权重。只有三列的老文件全部落在④，行为与加列前一致。
//
// [配置]
//   - weights_path: yaml strategy.weights_path（必需）
//   - normalize: bool — 自动归一化让 sum(|w|) = 1（默认 true）
//   - task_uid:  本策略实例的 task 标识（默认空 = 只吃通配槽）。**由 config 声明**，
//     不是 scheduler 分配的 id：策略侧 API（api/IStrategyContext）不暴露当前 task 身份，
//     而该 header 归实盘 lane 所有，本批不动它。同 sid 多 task 需要按 task 分权重时，
//     每个 task 配一份 strategy 实例并各自写死 task_uid。
//
// [信号逻辑]
//   on_init: 读 csv 解析 → date → 槽列表（每槽 map<symbol, weight>）
//   on_schedule(ctx, curDate, curTime, fireTime_ms):
//     - 按上面四级优先级取槽 → ctx.set_target_weights()
//     - 当日无任何命中槽 → no-op（保留之前 weights state）
// -----------------------------------------------------------------------------

#include "xt_strategy_engine/XtStrategyBase.h"
#include "api/IStrategyContext.h"
#include "xt_strategy_engine/XtStrategyInitConfig.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

class SelWeightsCsvStrategy : public XtStrategyBase {
public:
    void init(const XtStrategyInitConfig& cfg) override;
    void on_init(IStrategyContext* ctx) override;
    void on_schedule(IStrategyContext* ctx,
                     uint32_t curDate,
                     uint32_t curTime,
                     int64_t fireTime_ms) override;

    /// @brief normalize 幅度告警累计触发次数（每个 rebalance date 至多 +1）。
    /// @note 供测试观测；`ctx->log_warn` 真实告警不可断言时的计数器 fallback。
    [[nodiscard]] uint64_t normalize_drift_warn_count() const noexcept {
        return normalize_drift_warn_count_;
    }

private:
    /// @brief 同一 (date, effective_time, task_uid) 下的一组目标权重。
    struct WeightSlot {
        uint32_t    effective_time = 0;  ///< HHMM；0 = 通配（不限 fire 时刻）
        std::string task_uid;            ///< 空 = 通配（不限 task）
        std::unordered_map<std::string, double> weights;
    };

    /// @brief 解析 csv 到 date → 槽列表（strict，不跳坏行）。
    /// @param err 失败原因（含行号）；仅在返回 false 时写入。
    /// @return false = 文件打不开 / 任一坏行 / 重复 (date,time,task,symbol) / 零有效行。
    /// @note 表头定形态：前三列逐字 `date,symbol,weight`，其后可跟任意子集的可选列
    ///       （`signal_asof_date` / `effective_time` / `task_uid`，顺序不限、不得重复）；
    ///       列数由表头定死、数据行须逐行相符（多列不静默丢弃）；无表头时按 3 列办。
    ///       date 与 signal_asof_date 都须是真实日历日（月日按 Gregorian 校验，
    ///       `20260231` 拒）；as-of 只校验不消费（研究 provenance，引擎不据其决策）；
    ///       effective_time 须是真实钟面 HHMM；weight 须有限。
    bool load_weights_csv_(const std::string& path, std::string& err);

    /// @brief 按「精确 → 通配」四级取本次 fire 的槽；无命中返回 nullptr。
    [[nodiscard]] const WeightSlot* resolve_slot_(uint32_t date, uint32_t time) const;

    /// @brief 归一化 weights 让 sum(|w|) = 1（zero filter）。
    /// @note 求和按 symbol 字典序（浮点加法不可结合，遍历序会让结果跨平台漂移）。
    /// @return 归一化前的原始 gross（sum(|w|)）；供调用方判断缩放幅度。
    static double normalize_weights_(std::unordered_map<std::string, double>& w);

    std::string weights_path_;
    bool normalize_ = true;
    /// @brief 本策略实例的 task 标识（config 声明；空 = 只吃通配槽）。
    std::string task_uid_;

    /// @brief 全部 weights：date → 该日的槽列表（槽内 symbol → weight）。
    std::unordered_map<uint32_t, std::vector<WeightSlot>> slots_by_date_;

    /// @brief normalize 幅度告警累计触发次数（|gross-1|>0.5% 时 +1）。
    uint64_t normalize_drift_warn_count_ = 0;
};
