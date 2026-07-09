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

    using Config = XtHftReconcileLoopCfg;

    XtHftReconcileLoop(IReconcilableGateway& gw,
                    xt_account::AccountStateStore& account_store,
                    std::atomic<bool>& kill_switch,
                    std::atomic<uint64_t>* last_pos_snapshot_ms,
                    Config cfg);

    ~XtHftReconcileLoop();

    XtHftReconcileLoop(const XtHftReconcileLoop&)            = delete;
    XtHftReconcileLoop& operator=(const XtHftReconcileLoop&) = delete;

    void start();

    void stop();

    void tick_for_test();

    void set_reset_request_source(std::atomic<bool>& reset_req) {
        reset_request_external_ = &reset_req;
    }

    void set_metric_registry(ct::metrics::XtMetricRegistry* reg) { metrics_ = reg; }

    void set_kill_switch_source(std::atomic<uint8_t>& src) { kill_switch_source_ = &src; }

    uint64_t last_position_verified_ms() const {
        if (last_pos_snapshot_ms_external_) {
            return last_pos_snapshot_ms_external_->load(std::memory_order_relaxed);
        }
        return last_verified_ms_.load(std::memory_order_relaxed);
    }

    const std::unordered_map<std::string, uint8_t>& diverged_cycles_for_test() const {
        return diverged_;
    }

private:
    void run_loop();
    void tick_once();

    void reconcile_position();

    void reconcile_orders();

    IReconcilableGateway&  gw_;
    xt_account::AccountStateStore&     account_store_;
    std::atomic<bool>&     kill_switch_;
    std::atomic<uint64_t>* last_pos_snapshot_ms_external_ = nullptr;
    std::atomic<bool>*     reset_request_external_ = nullptr;
    std::atomic<uint8_t>*  kill_switch_source_ = nullptr;
    ct::metrics::XtMetricRegistry* metrics_ = nullptr;
    Config                 cfg_;

    std::unordered_map<std::string, uint8_t> diverged_;

    std::unordered_map<std::string, uint64_t> last_resync_ms_;

    uint64_t last_snapshot_ok_ms_ = 0;

    uint64_t first_attempt_ms_ = 0;

    std::atomic<uint64_t> last_verified_ms_{0};

    std::atomic<bool> kill_set_by_reconcile_{false};

    std::thread             thread_;
    std::atomic<bool>       running_{false};
    std::mutex              wake_mu_;
    std::condition_variable wake_cv_;
};

}}
