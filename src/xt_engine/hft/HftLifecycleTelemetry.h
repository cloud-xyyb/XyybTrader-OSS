#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "xt_core/order/XtTradeRecord.h"

namespace xt_engine::hft {

/// @brief HftLifecycleTelemetry 输出的仓位 open/close/resume 事件。
///
/// open/close 只在 per-(strategy_id, code) 仓位从 0 到非 0、非 0 到 0 或反向穿越时产生；
/// 同向加减仓不产生事件。resume 表示启动或 recovery 快照中继承的持仓。
struct PositionLifecycleEvent {
    uint64_t    ts_local_ns      = 0;     ///< steady clock ns
    uint64_t    ts_exch_ns       = 0;     ///< exchange/replay wall-clock ns
    std::string strategy_id;
    std::string code;
    const char* event_type       = nullptr;  ///< "open" / "close" / "resume"
    int64_t     qty_fp           = 0;        ///< signed quantity
    int64_t     avg_price_fp     = 0;
    int64_t     realized_pnl_fp  = 0;        ///< close 事件字段
    uint64_t    hold_duration_ms = 0;        ///< close/resume 事件字段
    uint64_t    cause_trade_id   = 0;
};

/// @brief position lifecycle event sink；空 callback 表示禁用。
using PositionLifecycleCallback = std::function<void(const PositionLifecycleEvent&)>;

/// @brief Per-(sid, code) 持仓指标回调；空 callback 表示禁用。
using PositionMetricCallback = std::function<void(const std::string& sid,
                                                  const char* code,
                                                  int64_t qty_fp)>;

/// @brief 提供 HFT 持仓 lifecycle 事件与 position metric 输出。
///
/// `on_fill()` 接收同一 `(sid, code)` 的成交前/后仓位，识别从 0 建仓、
/// 回到 0 或反向穿零的边界，并输出 open/close 事件。
/// `on_resume()` 为启动或 recovery snapshot 中继承的非零持仓补发 resume 事件。
///
/// `init()` 必须在首笔 fill 或首个 position snapshot 前调用；open_ts 状态用于恢复
/// hold_duration。position metric 回调按 fill 后仓位同步。
class HftLifecycleTelemetry {
public:
    /// @brief 启动期 wiring；callback 为空表示禁用对应 sink。
    struct Init {
        std::string state_path;
        PositionLifecycleCallback lifecycle_sink;
        PositionMetricCallback metric_sink;
    };

    /// @brief on_resume() 输入条目；表示 snapshot 中已归因到 strategy 的非零持仓。
    ///
    /// 用于补发 resume 事件，并在缺失持久化 open_ts 时建立新的持仓起点。
    struct ResumeEntry {
        std::string sid;
        std::string code;
        int64_t     qty_fp = 0;
        int64_t     avg_price_fp = 0;
    };

    /// @brief 启动期一次性注入 open_ts 持久化路径和 telemetry sink。
    ///
    /// 必须在首笔 fill / 首个 position snapshot 前调用；空 state_path 禁用落盘。
    void init(Init init);

    /// @brief 是否已注入 lifecycle 或 position metric sink。
    bool any_enabled() const noexcept {
        return static_cast<bool>(emit_cb_) || static_cast<bool>(metric_cb_);
    }

    /// @brief 判断 snapshot 是否需要补发 RESUME；不修改内部状态。
    bool want_resume(bool is_recovery) const noexcept {
        return static_cast<bool>(emit_cb_) && (is_recovery || !first_snapshot_handled_);
    }
    /// @brief 标记已处理过 snapshot。
    void mark_snapshot_seen() noexcept { first_snapshot_handled_ = true; }

    /// @brief 根据 fill 前后仓位更新 lifecycle 与 position metric。
    /// @param effective_sid 已归因 sid；position metric 使用该值。
    void on_fill(const XtTradeRecord& tr, const std::string& effective_sid,
                 int64_t qty_before_fp, int64_t avg_before_fp,
                 int64_t qty_after_fp,  int64_t avg_after_fp);

    /// @brief 为 snapshot 继承持仓补发 RESUME。
    void on_resume(const std::vector<ResumeEntry>& entries);

private:
    void emit_lifecycle_(const XtTradeRecord& tr,
                         int64_t qty_before_fp, int64_t avg_before_fp,
                         int64_t qty_after_fp,  int64_t avg_after_fp);
    void save_() const;
    void load_();

    // key = "<sid>|<code>"，value = open wall ms。
    std::unordered_map<std::string, uint64_t> open_ts_map_;
    // 首个 snapshot 与 recovery snapshot 触发 RESUME 补发。
    bool first_snapshot_handled_ = false;
    // 空字符串表示禁用 open_ts 落盘。
    std::string state_path_;
    PositionLifecycleCallback emit_cb_;
    PositionMetricCallback    metric_cb_;
    bool initialized_ = false;
};

}  // namespace xt_engine::hft
