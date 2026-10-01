#include "xt_ipc/XtShmMdGateway.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <unordered_set>

#include "xt_core/market/XtRawTick.h"
#include "xt_ipc/XtMdSidecarPath.h"
#include "xt_ipc/XtMdWireContract.h"
#include "xt_ipc/XtShmRing.h"
#include "xt_util/time/XtSteadyClock.h"

namespace xt_ipc {

namespace {
using Ring = XtShmRing<XtMdWireSlot>;

// 必须与控制线程 check_health 的 now_ms 同源同截断：RunnerFrameLoop 传
// static_cast<uint32_t>(XtSteadyClock::steady_now_ms())；XtSteadyClock 当前基于
// std::chrono::steady_clock，reader 也用同一入口取低 32 位，
// 用无符号回卷减法判停滞（delta < 2^32 ms ≈ 49 天，stale 阈值远小于此恒成立）。
uint32_t steady_ms_lo() {
    return static_cast<uint32_t>(XtSteadyClock::steady_now_ms());
}

// connector 每次 create 段把单调 epoch（启动 steady ns）写入 RingControlBlock._pad1
// 前 8 字节（见 xt_md_connector.cpp）。reader 据此判换代 —— offset 按结构体字段算
// （&ctrl->_pad1[0]），不硬编码。调用方须先 validate_ring 通过再读（保证 _pad1 在段内）。
uint64_t read_seg_epoch(const void* base) noexcept {
    const auto* ctrl = reinterpret_cast<const RingControlBlock*>(base);
    uint64_t epoch = 0;
    std::memcpy(&epoch, &ctrl->_pad1[0], sizeof(epoch));
    return epoch;
}

}  // namespace

XtShmMdGateway::XtShmMdGateway(std::string ring_name,
                               const std::vector<std::string>& declared,
                               uint32_t stale_md_ms)
    : ring_name_(std::move(ring_name)),
      declared_(declared.begin(), declared.end()),
      stale_md_ms_(stale_md_ms == 0 ? 3000u : stale_md_ms) {}

XtShmMdGateway::~XtShmMdGateway() { stop(); }

bool XtShmMdGateway::init(const Config& /*cfg*/) {
    // provider-neutral Config 不承载 ring 信息（由 ctor 注入）；此处仅校验段名非空。
    // 真正 attach 推迟到 start()（connector 可能晚于 runner 启动，attach 在 reader 线程轮询）。
    return !ring_name_.empty();
}

bool XtShmMdGateway::start() {
    if (reader_.joinable()) return false;  // 已启动
    stop_.store(false, std::memory_order_release);
    reader_ = std::thread([this] { reader_loop(); });
    return true;
}

void XtShmMdGateway::stop() {
    stop_.store(true, std::memory_order_release);
    if (reader_.joinable()) reader_.join();
    detach_segment();  // join 后 reader 不再访问段，munmap 安全
}

IMdGateway::Capabilities XtShmMdGateway::capabilities() const {
    Capabilities caps;
    caps.supports_l2_orderflow = false;  // ring 只载 XtRawTick
    return caps;
}

void XtShmMdGateway::bind_callbacks(Callbacks callbacks) { cbs_ = std::move(callbacks); }

void XtShmMdGateway::set_md_tap(std::function<void(const XtRawTick&)> tap) {
    md_tap_ = std::move(tap);
}

bool XtShmMdGateway::check_health(uint32_t now_ms) {
    if (!attached_.load(std::memory_order_acquire)) return false;
    const uint32_t last = last_head_advance_ms_.load(std::memory_order_acquire);
    // 无符号回卷减法（now_ms 与 last 同源同截断）：head 停滞达 stale_md_ms → 不健康。
    return static_cast<uint32_t>(now_ms - last) < stale_md_ms_;
}

void XtShmMdGateway::mark_ready() {
    // 幂等：从断连态恢复时才触发 on_ready（首次 attach 也走这条，起步态为断连）。
    if (health_disconnected_.exchange(false, std::memory_order_acq_rel)) {
        if (cbs_.on_ready) cbs_.on_ready();
    }
}

void XtShmMdGateway::mark_disconnected(int reason) {
    // 幂等：仅在首次进入断连态触发 on_front(false)，避免反复 publish md_ready=false。
    if (!health_disconnected_.exchange(true, std::memory_order_acq_rel)) {
        if (cbs_.on_front) cbs_.on_front(false, reason);
    }
}

bool XtShmMdGateway::read_subs_sidecar_(std::unordered_set<std::string>& out) const {
    // connector 的 lock 目录解析顺序：默认 /run/xyybtrader，不可写回退 /tmp（见
    // xt_md_connector.cpp resolve_sidecar_dir）；reader 无 connector 的 --lock-dir，按同优先级探测。
    static const char* const kCandidateDirs[] = {"/run/xyybtrader", "/tmp"};
    for (const char* dir : kCandidateDirs) {
        std::ifstream in(md_sidecar_path(dir, ring_name_, ".subs"));
        if (!in.is_open()) continue;
        std::string line;
        while (std::getline(in, line)) {
            const size_t b = line.find_first_not_of(" \t\r\n");
            if (b == std::string::npos) continue;  // 空/全空白行忽略
            const size_t e = line.find_last_not_of(" \t\r\n");
            out.insert(line.substr(b, e - b + 1));
        }
        return true;  // 找到并解析（空文件也算找到）
    }
    return false;  // 所有候选目录都无/不可读
}

void XtShmMdGateway::validate_declared_subset_() {
    if (declared_.empty()) return;  // 不过滤 → 无 declared 可校验，跳过

    subs_checks_total_.fetch_add(1, std::memory_order_relaxed);

    std::unordered_set<std::string> subs;
    if (!read_subs_sidecar_(subs)) {
        // sidecar 缺失/不可读（旧 connector 或滚动窗口）→ warn 一次降级，不阻塞 attach。
        if (!subs_missing_warned_) {
            subs_missing_warned_ = true;
            std::fprintf(stderr,
                         "xt_ipc XtShmMdGateway(%s): subs sidecar 缺失/不可读，跳过 declared⊆subs "
                         "校验（旧 connector 或滚动窗口；向后兼容不阻塞 attach）\n",
                         ring_name_.c_str());
        }
        return;
    }

    // declared ⊆ subs：列出不在 connector 订阅集内的 declared 合约。
    std::string missing;
    for (const auto& d : declared_) {
        if (subs.count(d) == 0) {
            if (!missing.empty()) missing += ',';
            missing += d;
        }
    }
    if (!missing.empty()) {
        subs_mismatch_total_.fetch_add(1, std::memory_order_relaxed);
        // 诊断优先：这些合约的 tick 将永不到达（connector 不订阅），初始 ready 门已把总错配
        // fail-closed 兜住；此处把部分错配的诊断提前到 attach 时刻，不做成硬 fail 启动。
        std::fprintf(stderr,
                     "xt_ipc XtShmMdGateway(%s): declared 合约不在 connector subs 集内: [%s]"
                     "（这些合约的 tick 将永不到达；初始 ready 门已 fail-closed 兜底）\n",
                     ring_name_.c_str(), missing.c_str());
    }
}

void XtShmMdGateway::note_attach_fail_(const char* reason) {
    attach_fail_count_.fetch_add(1, std::memory_order_relaxed);
    attach_failing_.store(1, std::memory_order_relaxed);
    attach_last_reason_.store(reason, std::memory_order_relaxed);
    // rate-limited loud 日志：attach 在 reader_loop 20ms/圈高频重试，逐次刷屏会淹没日志。
    // 刷条件：首次失败 / 原因变化 / 距上次 ≥60s。行为不变（仍重试），仅补 loud 观测面。
    const uint32_t now = steady_ms_lo();
    const bool reason_changed =
        (attach_log_last_reason_ == nullptr) ||
        (std::strcmp(attach_log_last_reason_, reason) != 0);
    if (!attach_logged_once_ || reason_changed ||
        static_cast<uint32_t>(now - attach_log_last_ms_) >= 60000u) {
        std::fprintf(stderr,
                     "xt_ipc XtShmMdGateway(%s): shm attach 失败 [%s]（reader 线程持续重试；"
                     "md_ready 初始门已 fail-closed 兜住交易）\n",
                     ring_name_.c_str(), reason);
        attach_logged_once_ = true;
        attach_log_last_reason_ = reason;
        attach_log_last_ms_ = now;
    }
}

void XtShmMdGateway::note_attach_ok_() {
    attach_failing_.store(0, std::memory_order_relaxed);  // 恢复（count 单调不清）
    attach_log_last_reason_ = nullptr;  // 下次失败重新首刷
    attach_logged_once_ = false;
}

bool XtShmMdGateway::attach_segment() {
    int fd = shm_open(ring_name_.c_str(), O_RDONLY, 0);
    if (fd < 0) {
        note_attach_fail_("shm_open");  // connector 未起 / 段名不符 (ENOENT 冷启常见，重试自愈)
        return false;
    }

    struct stat st {};
    if (fstat(fd, &st) != 0) {
        close(fd);
        note_attach_fail_("fstat");
        return false;
    }
    const uint64_t mapped = static_cast<uint64_t>(st.st_size);
    void* base = mmap(nullptr, mapped, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (base == MAP_FAILED) {
        note_attach_fail_("mmap");
        return false;
    }

    const auto* ctrl = reinterpret_cast<const RingControlBlock*>(base);
    if (const char* err = validate_ring(ctrl, mapped, Ring::kSlotStride, Ring::kElementSize);
        err != nullptr) {
        munmap(base, mapped);
        note_attach_fail_(err);  // 静态字面量（"wire_version mismatch"/"magic mismatch" 等）
        return false;            // 校验失败（截断段/换代中/wire 错配）→ fail-closed 不读
    }

    mmap_base_ = base;
    mapped_bytes_ = mapped;
    seg_ino_ = static_cast<uint64_t>(st.st_ino);
    seg_dev_ = static_cast<uint64_t>(st.st_dev);
    seg_epoch_ = read_seg_epoch(base);  // validate 已过 → _pad1 在段内，缓存 connector 换代判据
    note_attach_ok_();  // attach 成功 → 清 attach_failing_
    return true;
}

void XtShmMdGateway::detach_segment() {
    if (mmap_base_) {
        munmap(mmap_base_, mapped_bytes_);
        mmap_base_ = nullptr;
        mapped_bytes_ = 0;
    }
}

bool XtShmMdGateway::segment_changed() const {
    // 换代检测：connector 重启 unlink+recreate 后，inode/device 变是快速路径；但 tmpfs/
    // POSIX shm unlink+立即重建可复用同 inode（旧 mmap 仍指死页，magic/inode 都不变）。
    // 权威判据是 connector 每次 create 写入的单调 epoch（_pad1 前 8 字节）：读当前 mmap
    // 头的 epoch 与 attach 时缓存的 seg_epoch_ 比对，epoch 变即判换代。
    // 段消失（unlink 未重建窗口）或 fstat 失败也视为换代 → 触发 re-attach 轮询。
    int fd = shm_open(ring_name_.c_str(), O_RDONLY, 0);
    if (fd < 0) return true;  // 段消失（connector 重启窗口）→ 当作换代等重 attach
    struct stat st {};
    const bool ok = (fstat(fd, &st) == 0);
    close(fd);
    if (!ok) return true;
    if (static_cast<uint64_t>(st.st_ino) != seg_ino_ ||
        static_cast<uint64_t>(st.st_dev) != seg_dev_) {
        return true;  // 快速路径：inode/dev 变 → 换代
    }
    // inode 复用兜底：当前 mmap 头 epoch 与缓存比对（重建可能正在进行，magic 门 +
    // 这是保守 re-attach 决策，重 attach 内再 validate fail-closed，读到瞬态值也安全）。
    if (mmap_base_ == nullptr) return true;  // 无映射 → 当作换代等重 attach
    return read_seg_epoch(mmap_base_) != seg_epoch_;
}

void XtShmMdGateway::reader_loop() {
    // 起步态：未 attach 视为断连，md_ready 起步即 false（连不上即 HALT）。
    health_disconnected_.store(true, std::memory_order_release);

    Ring* ring = nullptr;
    Ring::Reader* reader = nullptr;
    uint32_t last_seg_check_ms = 0;  // 换代检测节流时间戳（steady ms 低 32 位）
    // 本代（attach generation）是否已见首个 accepted（declared 内）tick。初始 ready 门：
    // 首个 accepted tick 前 md_ready 恒 false（fail-closed，仅门初始 ready）；attach/换代重
    // attach 时复位 false → 新段同样要求首个 accepted tick 才恢复 ready。
    bool initial_tick_seen = false;

    auto release_segment = [&] {
        delete reader;
        reader = nullptr;
        delete ring;
        ring = nullptr;
        detach_segment();
    };

    XtMdWireSlot slot{};
    while (!stop_.load(std::memory_order_acquire)) {
        // ── 未 attach：轮询 attach（connector 可能晚启）──────────────────────
        if (!attached_.load(std::memory_order_acquire)) {
            if (attach_segment()) {
                ring = new Ring(mmap_base_);
                reader = new Ring::Reader(ring->make_reader());
                initial_tick_seen = false;  // 新代：初始 ready 门复位，等首个 accepted tick
                // 先写活性时戳再发布 attached_（release）：否则 check_health 控制线程可能
                // 在两 store 间读到 attached_=true 但 last=0 → 一次性 spurious unhealthy。
                last_head_advance_ms_.store(steady_ms_lo(), std::memory_order_release);
                attached_.store(true, std::memory_order_release);
                validate_declared_subset_();  // sidecar 诊断：declared ⊆ connector-subs
                // 不在此 mark_ready() —— 初始 ready 推迟到首个 accepted tick（下方 got_any 分支）。
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
        }

        // ── 已 attach：批量读 ring（上限 256 防饿死换代检测）────────────────────
        bool got_any = false;
        bool accepted_any = false;
        for (int i = 0; reader && i < 256 && reader->next(slot); ++i) {
            got_any = true;
            const XtRawTick& tick = slot.tick;
            // per-symbol 过滤：declared_ 空集 = 不过滤（任何 tick 即 accepted）；否则只喂本
            // 进程订阅合约。跨进程 wire 的 code 不保证 null-term → 有界构造 std::string 防越读。
            if (declared_.empty() ||
                declared_.count(std::string(tick.code,
                                            ::strnlen(tick.code, sizeof(tick.code)))) != 0) {
                accepted_any = true;
                // pre-delivery tap（如实盘录制）：declared 过滤之后、on_tick 之前，每笔
                // accepted tick 调一次。shm 部署下 per-gateway in-process tap 不在本进程
                // → tap 补齐「录制即数据集」的 MD_TICK 流，二者互斥无双录。
                if (md_tap_) md_tap_(tick);
                // capture_ns 是 connector publish 前 stamp 的跨进程可比 steady 时刻，随 tick
                // 一路传给 consumer（HFT binder → try_push_tick 起点选择器）；下游可 fallback 0。
                if (cbs_.on_tick) cbs_.on_tick(tick, static_cast<uint64_t>(slot.capture_ns));
            }
        }
        if (accepted_any) initial_tick_seen = true;  // 首个 declared 内 tick → 解锁初始 ready 门

        const uint32_t now = steady_ms_lo();
        if (got_any) {
            last_head_advance_ms_.store(now, std::memory_order_release);
            // 初始 ready 门：首个 accepted tick 前恒不 ready（含 out-of-scope 流量刷新 head 活性但
            // 不 arm）；initial_tick_seen 置位后 got_any→mark_ready 的在线自愈路径逐字保持。
            if (initial_tick_seen) mark_ready();
            continue;
        }

        // ── 无新数据：判 head 停滞 stale + 检测 connector 换代 ──────────────────
        const uint32_t last = last_head_advance_ms_.load(std::memory_order_acquire);
        if (static_cast<uint32_t>(now - last) >= stale_md_ms_) {
            mark_disconnected(/*reason=*/1);  // stale：head 停滞达阈值 → md_ready=false
        }
        // 换代检测做 shm_open+fstat 系统调用，节流到 ~200ms 一次（idle 路 200µs 自旋
        // 不必每圈探；正常行情 flow 走 got_any 分支根本不到此）。
        if (static_cast<uint32_t>(now - last_seg_check_ms) >= 200u) {
            last_seg_check_ms = now;
            if (segment_changed()) {
                // connector 重启换段（inode/epoch 变）→ munmap 重 attach 新段（cursor 从新 head）。
                attached_.store(false, std::memory_order_release);
                release_segment();
                mark_disconnected(/*reason=*/2);  // 换段窗口内 HALT，下轮重 attach
            }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

    release_segment();
}

}  // namespace xt_ipc
