#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "api/IReconcilableGateway.h"
#include "xt_live/hft/reconcile/XtHftReconcileLoopCfg.h"

namespace xt_account { class AccountStateStore; }

namespace xt_runtime { namespace order { class XtOrderRuntimeBook; } }

namespace ct { namespace metrics { class XtMetricRegistry; } }

namespace xt_live { namespace control {

/// @brief 定期核对交易所真值与本地账本的后台线程。
///
/// XtHftReconcileLoop 每隔一个周期从交易所拉持仓/在途单快照跟本地对比：
/// 对不上先请求重同步，连续多轮仍对不上就触发 kill switch 停止交易。
/// 跑在独立冷路径线程，不占下单热路径；发现的幽灵单由 gateway 走正常
/// 回报路径注入，本线程不直接改写本地订单簿。
class XtHftReconcileLoop {
public:
    /// @brief Reconcile loop 配置（yaml: `runtime.td.reconcile.*`，见 XtHftReconcileLoopCfg）。
    using Config = XtHftReconcileLoopCfg;

    /// @brief 构造 reconcile loop；`last_pos_snapshot_ms` 可为空。
    ///
    /// @note ghost order 注入由 gateway 自己 emit 给 ITradeGateway::Callbacks，
    ///       继续走 td queue → event loop → engine 单写者路径；reconcile loop
    ///       不直接改写 XtOrderRuntimeBook。
    XtHftReconcileLoop(IReconcilableGateway& gw,
                    xt_account::AccountStateStore& account_store,
                    std::atomic<bool>& kill_switch,
                    std::atomic<uint64_t>* last_pos_snapshot_ms,
                    Config cfg);

    ~XtHftReconcileLoop();

    XtHftReconcileLoop(const XtHftReconcileLoop&)            = delete;
    XtHftReconcileLoop& operator=(const XtHftReconcileLoop&) = delete;

    /// @brief 启动 reconcile 线程；重复调用为 no-op。
    void start();

    /// @brief 请求线程停止并 join；析构会自动 stop()。
    void stop();

    /// @brief 跑一轮 reconcile，供单测调用；不依赖后台线程。
    void tick_for_test();

    /// @brief 注入 RESUME reset flag；tick 开头消费 true 并清空 divergence 状态。
    void set_reset_request_source(std::atomic<bool>& reset_req) {
        reset_request_external_ = &reset_req;
    }

    /// @brief 注入 Prometheus metric registry；nullptr 表示 no-op。
    ///
    /// 导出 `xt_state_divergence{field="reconcile_qty",symbol}` =
    /// |account_store - exchange positionRisk|。该口径使用 exchange REST 真值，
    /// 能覆盖 store-vs-broker 同步翻倍时 diff=0 的盲区。
    /// 另导出 `xt_reconcile_snapshot_age_ms`：fetch 成功写 0，失败写距上次成功
    /// 快照的毫秒数（持续增长 = loop fail-closed 空转，无比对覆盖）。
    void set_metric_registry(ct::metrics::XtMetricRegistry* reg) { metrics_ = reg; }

    /// @brief 注入 kill_switch_source atomic；divergence trip 时标 ReconcileDivergence，
    /// 重新对账一致自清时复位 None。空时不写 source（行为同旧）。
    void set_kill_switch_source(std::atomic<uint8_t>& src) { kill_switch_source_ = &src; }

    /// @brief 最近一次 position agree 的 steady-clock ms；有外部 atomic 时优先返回外部值。
    uint64_t last_position_verified_ms() const {
        if (last_pos_snapshot_ms_external_) {
            return last_pos_snapshot_ms_external_->load(std::memory_order_relaxed);
        }
        return last_verified_ms_.load(std::memory_order_relaxed);
    }

    /// @brief 当前连续 divergence 计数；仅供 test / metric snapshot，非线程安全。
    const std::unordered_map<std::string, uint8_t>& diverged_cycles_for_test() const {
        return diverged_;
    }

private:
    void run_loop();
    void tick_once();

    /// @brief Position diff + 行动阶梯。
    void reconcile_position();

    /// @brief Ghost order resolve；gateway 内部检出后回灌 engine 路径。
    void reconcile_orders();

    IReconcilableGateway&  gw_;
    xt_account::AccountStateStore&     account_store_;
    std::atomic<bool>&     kill_switch_;
    std::atomic<uint64_t>* last_pos_snapshot_ms_external_ = nullptr;  ///< 跟 UDS push 共写
    std::atomic<bool>*     reset_request_external_ = nullptr;          ///< control panel RESUME hook
    std::atomic<uint8_t>*  kill_switch_source_ = nullptr;             ///< trip 标 ReconcileDivergence / 自清复位 None（可空）
    ct::metrics::XtMetricRegistry* metrics_ = nullptr;                 ///< I1 divergence export; nullptr=no-op
    Config                 cfg_;

    /// per-code divergence 连续计数；agree 或外部 reset 时清零。
    std::unordered_map<std::string, uint8_t> diverged_;

    /// per-code 最近 resnap 请求时间，用于持续 divergence 下的节流重试。
    std::unordered_map<std::string, uint64_t> last_resync_ms_;

    /// 最近一次 fetch_positions 成功的 steady-clock ms；0 = 尚未成功过。
    uint64_t last_snapshot_ok_ms_ = 0;
    /// 首次 reconcile 尝试的 steady-clock ms；未成功过时作 snapshot age 基准。
    uint64_t first_attempt_ms_ = 0;

    /// 最近一次 reconcile 无 diff 的 steady-clock ms。
    std::atomic<uint64_t> last_verified_ms_{0};

    /// 标记 kill_switch 是否由本 loop 拉起；分歧消失时只自动清这个来源。
    std::atomic<bool> kill_set_by_reconcile_{false};

    /// 线程与唤醒控制。
    std::thread             thread_;
    std::atomic<bool>       running_{false};
    std::mutex              wake_mu_;
    std::condition_variable wake_cv_;
};

}}  // namespace xt_live::control
