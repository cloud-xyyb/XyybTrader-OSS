#pragma once

/// @file
/// @brief SEL 回测事件流导出器（`backtest.trace_mode: full` 时产 events_full.csv）。
///
/// **列形态**与 HFT 侧 `XtEventStreamSink` 逐字相同——`ts,seq,event_type,payload`，
/// payload 为 `k=v,k=v,…`——viewer producer 的 `events_ingest.py` 因此零改动即可消费。
/// **事件语义**不同：SEL 没有 tick 级延迟腿（提交→ack→撮合→交付四段在日频口径下
/// 全部塌缩到同一天），本 sink 导出的是 SEL 原生门禁链：
///
///   `sel_fire` → `sel_target` → `sel_gate` → `order_submit` → `trade` → `sel_settle`
///
/// 其中 `order_submit` / `trade` 两型的 payload key 与 HFT 侧同名同义（order_id /
/// code / side / qty / price…），producer 据此重建 per-order 因果链；两者另带
/// `fire_seq`，把 order 域接回规划段（规划三型只有 fire_seq、order 域只有 order_id，
/// 没有这个字段就无法把同一 code 的两次 fire 各自归位）。`sel_*` 型是 SEL 专属
/// （fire 身份、规划期目标、门禁裁决、gateway 未受理、日终结算），producer 忽略其行
/// （无 order_id）而 viewer 可另行消费。
///
/// 门禁放行但 gateway 未受理的腿走 `sel_reject`（不是 `order_submit`）：order_id 是
/// producer 因果链的主键，未受理腿根本没有订单，发成 Submitted 会把一天里的多次
/// 拒绝合并成同一个虚构订单 #0。
///
/// **行序**：`sel_target` 先于同一次 fire 的 `sel_fire` 落行——engine 的 fire 汇总
/// 记录在 `on_target_weights` **退出时**才发（含各条早退路径都要发一条，见
/// `SelFireRecord` 契约），而逐 code 决策在过程中发。两者靠 `fire_seq` 关联，消费
/// 方按该字段归组即可，不要按文件行序推断因果。
///
/// **时间轴**：SEL 的成交口径是「T 日收盘价一次性成交」，日内没有真实时刻。ts 用
/// `exec_date + 日内序号` 合成：日基准时刻 = 该 run 的 fire 时刻（HHMM），每条事件
/// 在其上 +1ms，再编回 packed `YYYYMMDD*1e9 + HHMMSSmmm`。ts **不是延迟量纲**——
/// 不要拿它做时间差。
///
/// **排序契约**：`(ts, seq)` 复合键严格单调递增；`ts` 单独只保证**非降**。日内事件
/// 数顶到 23:59:59.999 后 ts 停在最后一毫秒（越日会让事件落到别的交易日，比丢时序
/// 精度危险得多），此后同 ts 的多行靠 `seq` 定序。消费方按 `(ts, seq)` 排序。
///
/// @note off 模式下 runner 仍构造本对象，但不调 `open()`：`is_open()` 恒 false，
///       全部 `emit` 早退，不创建文件——产物集合与未配置 trace 时逐位一致。

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "xt_util/path/PathUtil.h"

namespace xt_bt_sel {

/// @brief SEL 事件流写出器（流式，单线程）。
class XtSelTraceSink {
public:
    /// @brief 打开 `<report_dir>/events_full.csv` 并写表头。
    /// @param base_time HHMM 形式的日内基准时刻（run 的 fire 时刻）；越界值退 0。
    /// @return false = 打不开（调用方按产物写出失败处理）。
    [[nodiscard]] bool open(const std::string& report_dir, uint32_t base_time);

    [[nodiscard]] bool is_open() const noexcept { return af_ != nullptr; }

    /// @brief 切到新交易日：日内序号归零。
    void begin_day(uint32_t date) noexcept;

    /// @brief 追加一条事件行（payload 已格式化为 `k=v,…`）。
    /// @note payload 内的逗号是 key 分隔符而非 CSV 分隔符 → 整个 payload 走 CSV
    ///       引号转义写成**单个**字段，与 HFT sink 同规则（producer 用
    ///       `csv.reader` 反解）。
    void emit(const char* event_type, const std::string& payload);

    /// @return false = 未完整落盘（调用方按 rc=9 处理）。
    [[nodiscard]] bool commit();

    [[nodiscard]] size_t rows() const noexcept { return rows_; }

private:
    /// @brief 当前行的 packed ts（`YYYYMMDD*1e9 + HHMMSSmmm`）并推进日内序号。
    [[nodiscard]] int64_t next_ts_() noexcept;

    std::unique_ptr<PathUtil::AtomicOfstream> af_;
    std::string path_;
    uint32_t    date_          = 0;   ///< 当前交易日；0 = 尚未 begin_day
    uint32_t    base_ms_       = 0;   ///< 日基准时刻（ms since midnight）
    uint32_t    day_seq_       = 0;   ///< 日内序号（+1ms/条）
    uint64_t    seq_           = 0;   ///< 全局递增行号（seq 列）
    size_t      rows_          = 0;
};

}  // namespace xt_bt_sel
