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
    /// @brief 构造 shm MD gateway。
    /// @param ring_name       共享内存 ring 段名（如 `/xt_md_ctp`），与 connector 一致。
    /// @param declared        本进程订阅合约集（per-symbol 过滤；空集 = 不过滤全放行）。
    /// @param stale_md_ms     head 停滞超过此 ms 判 stale → md_ready=false（默认 3000）。
    XtShmMdGateway(std::string ring_name,
                   const std::vector<std::string>& declared,
                   uint32_t stale_md_ms);
    ~XtShmMdGateway() override;

    XtShmMdGateway(const XtShmMdGateway&) = delete;
    XtShmMdGateway& operator=(const XtShmMdGateway&) = delete;

    // ── IMdGateway: 生命周期 ──────────────────────────────────────────────
    /// @brief 仅校验配置（ring_name 非空）；真正 attach 推迟到 start()。
    bool init(const Config& cfg) override;
    /// @brief 起 reader 线程：attach+validate→on_ready→read loop（含 re-attach + stale）。
    bool start() override;
    /// @brief 停 reader 线程并 munmap。
    void stop() override;

    // ── IMdGateway: 能力与回调 ──────────────────────────────────────────────
    /// @brief ring 只载 `XtRawTick`，不支持 L2 orderflow。
    Capabilities capabilities() const override;
    /// @brief 绑定行情回调；须在 start() 前完成。
    void bind_callbacks(Callbacks callbacks) override;

    // ── 泛型 pre-delivery tap（可选；非 IMdGateway 契约）─────────────────────
    /// @brief 安装 accepted tick 的 pre-delivery hook：reader 线程在 declared 过滤之后、
    ///        `on_tick` 之前对每笔 accepted tick 调用（非空才调）。
    /// @param tap 泛型消费者（如实盘录制 tap）；xt_ipc 层不知其语义，纯 std::function。
    /// @pre 须在 start() 前调用（reader 线程无锁读；与 bind_callbacks 同线程边界约定）。
    /// @note shm 部署下 per-gateway in-process tap（ParserCTP/MdBinance/ParserXTP）不在本
    ///       进程（connector 进程持有），二者互斥于部署形态 → 结构性无双录。
    void set_md_tap(std::function<void(const XtRawTick&)> tap);

    // ── IMdGateway: 健康检查（控制环 ~1Hz 调用）────────────────────────────
    /// @brief 返回 ring head 活性：距上次 head 推进 < stale_md_ms 为健康。
    /// @note 只判 head 停滞（writer 活性），不用 tick.action_time（交易所时间滞后）。
    /// @return true=健康；false=head 停滞达 stale 阈值或尚未 attach。
    bool check_health(uint32_t now_ms) override;

    // ── 诊断观测（测试/ops；非 IMdGateway 契约，跨线程 atomic 读）───────────
    /// @brief declared⊆subs sidecar 校验运行次数（每次 attach 一次；declared 空集不计）。
    uint64_t subs_checks_total() const noexcept {
        return subs_checks_total_.load(std::memory_order_relaxed);
    }
    /// @brief declared 含 connector-subs 集外合约的 attach 次数（诊断优先，初始 ready 门兜底）。
    uint64_t subs_mismatch_total() const noexcept {
        return subs_mismatch_total_.load(std::memory_order_relaxed);
    }

    // ── attach 失败可观测（T1.141①；跨线程 atomic 读，runner 层 emit prometheus）─────
    /// @brief 当前 attach 态：1=最近一次 attach 尝试失败且尚未成功恢复，0=已 attach 或从未失败。
    /// @note gauge 语义（0/1 latch）；runner frame-tail 每 1Hz emit，故序列非惰性、免
    ///       series-birth 盲区。connector 未起 / wire 错配 / 段截断都会置 1。
    uint32_t attach_failing() const noexcept {
        return attach_failing_.load(std::memory_order_relaxed);
    }
    /// @brief 累计 attach 失败次数（进程内单调）。
    uint64_t attach_fail_count() const noexcept {
        return attach_fail_count_.load(std::memory_order_relaxed);
    }
    /// @brief 最近一次 attach 失败原因（静态字面量：shm_open/fstat/mmap 或 validate_ring 返回值）。
    /// @return 空串=从未失败；否则指向静态字面量，仅日志/诊断，不进 metric label。
    const char* attach_last_reason() const noexcept {
        const char* p = attach_last_reason_.load(std::memory_order_relaxed);
        return p ? p : "";
    }
    /// @brief ring 段名（runner emit 时作 `ring` metric label；与 connector 一致）。
    const std::string& ring_name() const noexcept { return ring_name_; }

private:
    /// @brief reader 线程主体：attach + read loop + re-attach + stale 检测。
    void reader_loop();
    /// @brief shm_open+fstat+mmap+validate_ring；成功填 mmap_base_/mapped_bytes_/段身份。
    /// @return true=attach 成功且 ring 校验通过。
    bool attach_segment();
    /// @brief munmap 当前段并清 attach 状态（不动 reader 线程）。
    void detach_segment();
    /// @brief 探测 connector 换代（同 name 当前段 inode/dev 或 _pad1 epoch 变、或段消失）；变了返 true。
    /// @note inode/dev 作快速路径，epoch 作权威判据 —— inode 复用（unlink+立即重建）下
    ///       inode/dev 不变但 connector 每次 create 写入新单调 epoch，据此判换代。
    bool segment_changed() const;
    /// @brief 标记 stale/断连：md_ready=false（经 on_front(false)），幂等。
    void mark_disconnected(int reason);
    /// @brief 标记就绪：md_ready=true（经 on_ready），幂等。
    /// @note 调用方须已确认本代（attach generation）见过首个 accepted tick —— 初始 ready
    ///       门在 reader_loop 内以 initial_tick_seen 为前置，attach 时不直接 mark_ready。
    void mark_ready();
    /// @brief attach 后诊断校验 declared ⊆ connector-subs（读 `.subs` sidecar 文件）。
    /// @note 诊断优先、不阻塞 attach：缺失合约仅记 counter + error 日志（初始 ready 门已把
    ///       总错配 fail-closed 兜住）；sidecar 缺失/不可读 → warn 一次降级（向后兼容旧 connector）。
    ///       declared_ 空集（不过滤）→ 无可校验，直接跳过。
    void validate_declared_subset_();
    /// @brief 按候选目录探测并解析 connector 写的 `.subs` sidecar（每行一合约）。
    /// @param out 解析出的 subs 集合（空文件也算找到，out 为空）。
    /// @return true=某候选目录找到并解析；false=所有候选目录都无/不可读。
    bool read_subs_sidecar_(std::unordered_set<std::string>& out) const;
    /// @brief 记录一次 attach 失败：atomic 计数/态更新 + rate-limited loud 日志。仅 reader 线程调用。
    /// @param reason 静态字面量失败原因（shm_open/fstat/mmap 或 validate_ring 返回值）。
    /// @note 行为不变——attach 仍在 reader_loop 高频重试；本函数只补观测面。日志限频：
    ///       首次失败 / 原因变化 / 每 60s 一次（防高频重试逐次刷屏）。
    void note_attach_fail_(const char* reason);
    /// @brief 记录 attach 成功：清 attach_failing_ 与日志节流态（fail_count 单调不清）。仅 reader 线程调用。
    void note_attach_ok_();

    const std::string ring_name_;            ///< ring 段名
    const std::unordered_set<std::string> declared_; ///< per-symbol 过滤集（空=不过滤）
    const uint32_t stale_md_ms_;             ///< head 停滞 stale 阈值 ms

    Callbacks cbs_;                          ///< 行情回调（start 前由 bind 设定）
    std::function<void(const XtRawTick&)> md_tap_;  ///< 可选 pre-delivery tap（start 前由 set_md_tap 设定）

    // ── 段映射状态（仅 reader 线程读写，check_health 走 atomic 镜像）──────────
    void* mmap_base_ = nullptr;              ///< 当前 mmap 段基址（reader 线程拥有）
    uint64_t mapped_bytes_ = 0;              ///< 当前段字节数
    uint64_t seg_ino_ = 0;                   ///< 当前段 inode（换代快速路径）
    uint64_t seg_dev_ = 0;                   ///< 当前段 device（换代快速路径）
    uint64_t seg_epoch_ = 0;                 ///< 当前段 _pad1 epoch（换代权威判据，inode 复用下区分换代）

    // ── reader 线程 ↔ check_health（控制线程）共享活性镜像 ──────────────────
    std::thread reader_;                      ///< ring-reader 线程
    std::atomic<bool> stop_{false};           ///< 停止标志
    std::atomic<bool> attached_{false};       ///< 已 attach 且 ring 校验通过
    /// 上次 head 推进时刻，steady ms 低 32 位（与 check_health now_ms 同源同截断）。
    std::atomic<uint32_t> last_head_advance_ms_{0};
    std::atomic<bool> health_disconnected_{false};         ///< 已置 md_ready=false（去重 on_front）

    // ── sidecar declared⊆subs 诊断（reader 线程写 counter，控制/测试线程 atomic 读）──
    std::atomic<uint64_t> subs_checks_total_{0};    ///< sidecar 校验运行次数（每 attach 一次）
    std::atomic<uint64_t> subs_mismatch_total_{0};  ///< declared⊄subs 的 attach 次数
    bool subs_missing_warned_ = false;              ///< sidecar 缺失 warn 一次（仅 reader 线程访问）

    // ── attach 失败可观测（T1.141①：reader 线程写、控制/测试线程 atomic 读）──────────
    std::atomic<uint32_t> attach_failing_{0};              ///< 当前 attach 态（1=失败未恢复）
    std::atomic<uint64_t> attach_fail_count_{0};           ///< 累计 attach 失败次数（单调）
    std::atomic<const char*> attach_last_reason_{nullptr}; ///< 最近失败原因（静态字面量指针）
    // 日志节流状态（仅 reader 线程访问，无需 atomic）：首次 / 原因变化 / 每 60s 一次。
    const char* attach_log_last_reason_ = nullptr;  ///< 上次已刷日志的原因（原因变化再刷）
    uint32_t attach_log_last_ms_ = 0;               ///< 上次刷日志时刻（steady ms 低 32 位）
    bool attach_logged_once_ = false;               ///< 是否已刷过首条失败日志
};

}  // namespace xt_ipc
