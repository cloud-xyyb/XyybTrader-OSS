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
    uint64_t    ts_local_ns      = 0;
    uint64_t    ts_exch_ns       = 0;
    std::string strategy_id;
    std::string code;
    const char* event_type       = nullptr;
    int64_t     qty_fp           = 0;
    int64_t     avg_price_fp     = 0;
    int64_t     realized_pnl_fp  = 0;
    uint64_t    hold_duration_ms = 0;
    uint64_t    cause_trade_id   = 0;
};

using PositionLifecycleCallback = std::function<void(const PositionLifecycleEvent&)>;

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

    void init(Init init);

    bool any_enabled() const noexcept {
        return static_cast<bool>(emit_cb_) || static_cast<bool>(metric_cb_);
    }

    bool want_resume(bool is_recovery) const noexcept {
        return static_cast<bool>(emit_cb_) && (is_recovery || !first_snapshot_handled_);
    }

    void mark_snapshot_seen() noexcept { first_snapshot_handled_ = true; }

    void on_fill(const XtTradeRecord& tr, const std::string& effective_sid,
                 int64_t qty_before_fp, int64_t avg_before_fp,
                 int64_t qty_after_fp,  int64_t avg_after_fp);

    void on_resume(const std::vector<ResumeEntry>& entries);

private:
    void emit_lifecycle_(const XtTradeRecord& tr,
                         int64_t qty_before_fp, int64_t avg_before_fp,
                         int64_t qty_after_fp,  int64_t avg_after_fp);
    void save_() const;
    void load_();

    std::unordered_map<std::string, uint64_t> open_ts_map_;

    bool first_snapshot_handled_ = false;

    std::string state_path_;
    PositionLifecycleCallback emit_cb_;
    PositionMetricCallback    metric_cb_;
    bool initialized_ = false;
};

}
