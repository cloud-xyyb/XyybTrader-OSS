#pragma once

/// @file
/// @brief 从共享内存 ring 读行情的 IMdGateway 实现。
///
/// 把外部 connector 发布的 `XtShmRing<XtMdWireSlot>` 包成 `IMdGateway`：
/// reader 线程 attach/validate 共享内存段，按本进程订阅集过滤后触发 `on_tick`
/// （随 tick 传 connector stamp 的 capture_ns）；
/// 就绪、断连和健康检查复用 `IMdGateway` 的 on_ready/on_front/check_health 语义。
///
/// 线程归属：reader 线程是 `on_tick`/`on_ready`/`on_front` 的触发线程，
/// 也是下游 tick 队列的 producer；调用方必须按普通 venue gateway callback
/// 处理线程边界。边界：只读 ring（PROT_READ），只发布 tick，不下单，
/// 不访问 account/risk 或 worker-thread-owned 状态。

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <unordered_set>

#include "api/IMdGateway.h"

struct XtRawTick;

namespace xt_ipc {

template <class T>
class XtShmRing;

/// @brief 从 connector 广播的共享内存 ring 读行情的 MD gateway。
///
/// 实现 `IMdGateway` 生命周期：`start()` 起 reader 线程做 attach+validate→
/// `on_ready`→read loop；连接活性只看 ring head 是否推进，stale 或换段时
/// 触发 `on_front(false)`，新段可用后重新 `on_ready`。
///
/// @warning callback 在内部 reader 线程触发；调用方不得假设线程归属，callback 必须
/// 快速返回（同 venue gateway 语义）。
class XtShmMdGateway : public IMdGateway {
public:

    XtShmMdGateway(std::string ring_name,
                   const std::vector<std::string>& declared,
                   uint32_t stale_md_ms);
    ~XtShmMdGateway() override;

    XtShmMdGateway(const XtShmMdGateway&) = delete;
    XtShmMdGateway& operator=(const XtShmMdGateway&) = delete;

    bool init(const Config& cfg) override;

    bool start() override;

    void stop() override;

    Capabilities capabilities() const override;

    void bind_callbacks(Callbacks callbacks) override;

    void set_md_tap(std::function<void(const XtRawTick&)> tap);

    bool check_health(uint32_t now_ms) override;

    uint64_t subs_checks_total() const noexcept {
        return subs_checks_total_.load(std::memory_order_relaxed);
    }

    uint64_t subs_mismatch_total() const noexcept {
        return subs_mismatch_total_.load(std::memory_order_relaxed);
    }

    uint32_t attach_failing() const noexcept {
        return attach_failing_.load(std::memory_order_relaxed);
    }

    uint64_t attach_fail_count() const noexcept {
        return attach_fail_count_.load(std::memory_order_relaxed);
    }

    const char* attach_last_reason() const noexcept {
        const char* p = attach_last_reason_.load(std::memory_order_relaxed);
        return p ? p : "";
    }

    const std::string& ring_name() const noexcept { return ring_name_; }

private:

    void reader_loop();

    bool attach_segment();

    void detach_segment();

    bool segment_changed() const;

    void mark_disconnected(int reason);

    void mark_ready();

    void validate_declared_subset_();

    bool read_subs_sidecar_(std::unordered_set<std::string>& out) const;

    void note_attach_fail_(const char* reason);

    void note_attach_ok_();

    const std::string ring_name_;
    const std::unordered_set<std::string> declared_;
    const uint32_t stale_md_ms_;

    Callbacks cbs_;
    std::function<void(const XtRawTick&)> md_tap_;

    void* mmap_base_ = nullptr;
    uint64_t mapped_bytes_ = 0;
    uint64_t seg_ino_ = 0;
    uint64_t seg_dev_ = 0;
    uint64_t seg_epoch_ = 0;

    std::thread reader_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> attached_{false};

    std::atomic<uint32_t> last_head_advance_ms_{0};
    std::atomic<bool> health_disconnected_{false};

    std::atomic<uint64_t> subs_checks_total_{0};
    std::atomic<uint64_t> subs_mismatch_total_{0};
    bool subs_missing_warned_ = false;

    std::atomic<uint32_t> attach_failing_{0};
    std::atomic<uint64_t> attach_fail_count_{0};
    std::atomic<const char*> attach_last_reason_{nullptr};

    const char* attach_log_last_reason_ = nullptr;
    uint32_t attach_log_last_ms_ = 0;
    bool attach_logged_once_ = false;
};

}
