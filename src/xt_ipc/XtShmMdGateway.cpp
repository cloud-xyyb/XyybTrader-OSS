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

uint32_t steady_ms_lo() {
    return static_cast<uint32_t>(XtSteadyClock::steady_now_ms());
}

uint64_t read_seg_epoch(const void* base) noexcept {
    const auto* ctrl = reinterpret_cast<const RingControlBlock*>(base);
    uint64_t epoch = 0;
    std::memcpy(&epoch, &ctrl->_pad1[0], sizeof(epoch));
    return epoch;
}

}

XtShmMdGateway::XtShmMdGateway(std::string ring_name,
                               const std::vector<std::string>& declared,
                               uint32_t stale_md_ms)
    : ring_name_(std::move(ring_name)),
      declared_(declared.begin(), declared.end()),
      stale_md_ms_(stale_md_ms == 0 ? 3000u : stale_md_ms) {}

XtShmMdGateway::~XtShmMdGateway() { stop(); }

bool XtShmMdGateway::init(const Config&  ) {

    return !ring_name_.empty();
}

bool XtShmMdGateway::start() {
    if (reader_.joinable()) return false;
    stop_.store(false, std::memory_order_release);
    reader_ = std::thread([this] { reader_loop(); });
    return true;
}

void XtShmMdGateway::stop() {
    stop_.store(true, std::memory_order_release);
    if (reader_.joinable()) reader_.join();
    detach_segment();
}

IMdGateway::Capabilities XtShmMdGateway::capabilities() const {
    Capabilities caps;
    caps.supports_l2_orderflow = false;
    return caps;
}

void XtShmMdGateway::bind_callbacks(Callbacks callbacks) { cbs_ = std::move(callbacks); }

void XtShmMdGateway::set_md_tap(std::function<void(const XtRawTick&)> tap) {
    md_tap_ = std::move(tap);
}

bool XtShmMdGateway::check_health(uint32_t now_ms) {
    if (!attached_.load(std::memory_order_acquire)) return false;
    const uint32_t last = last_head_advance_ms_.load(std::memory_order_acquire);

    return static_cast<uint32_t>(now_ms - last) < stale_md_ms_;
}

void XtShmMdGateway::mark_ready() {

    if (health_disconnected_.exchange(false, std::memory_order_acq_rel)) {
        if (cbs_.on_ready) cbs_.on_ready();
    }
}

void XtShmMdGateway::mark_disconnected(int reason) {

    if (!health_disconnected_.exchange(true, std::memory_order_acq_rel)) {
        if (cbs_.on_front) cbs_.on_front(false, reason);
    }
}

bool XtShmMdGateway::read_subs_sidecar_(std::unordered_set<std::string>& out) const {

    static const char* const kCandidateDirs[] = {"/run/xyybtrader", "/tmp"};
    for (const char* dir : kCandidateDirs) {
        std::ifstream in(md_sidecar_path(dir, ring_name_, ".subs"));
        if (!in.is_open()) continue;
        std::string line;
        while (std::getline(in, line)) {
            const size_t b = line.find_first_not_of(" \t\r\n");
            if (b == std::string::npos) continue;
            const size_t e = line.find_last_not_of(" \t\r\n");
            out.insert(line.substr(b, e - b + 1));
        }
        return true;
    }
    return false;
}

void XtShmMdGateway::validate_declared_subset_() {
    if (declared_.empty()) return;

    subs_checks_total_.fetch_add(1, std::memory_order_relaxed);

    std::unordered_set<std::string> subs;
    if (!read_subs_sidecar_(subs)) {

        if (!subs_missing_warned_) {
            subs_missing_warned_ = true;
            std::fprintf(stderr,
                         "xt_ipc XtShmMdGateway(%s): subs sidecar 缺失/不可读，跳过 declared⊆subs "
                         "校验（旧 connector 或滚动窗口；向后兼容不阻塞 attach）\n",
                         ring_name_.c_str());
        }
        return;
    }

    std::string missing;
    for (const auto& d : declared_) {
        if (subs.count(d) == 0) {
            if (!missing.empty()) missing += ',';
            missing += d;
        }
    }
    if (!missing.empty()) {
        subs_mismatch_total_.fetch_add(1, std::memory_order_relaxed);

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
    attach_failing_.store(0, std::memory_order_relaxed);
    attach_log_last_reason_ = nullptr;
    attach_logged_once_ = false;
}

bool XtShmMdGateway::attach_segment() {
    int fd = shm_open(ring_name_.c_str(), O_RDONLY, 0);
    if (fd < 0) {
        note_attach_fail_("shm_open");
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
        note_attach_fail_(err);
        return false;
    }

    mmap_base_ = base;
    mapped_bytes_ = mapped;
    seg_ino_ = static_cast<uint64_t>(st.st_ino);
    seg_dev_ = static_cast<uint64_t>(st.st_dev);
    seg_epoch_ = read_seg_epoch(base);
    note_attach_ok_();
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

    int fd = shm_open(ring_name_.c_str(), O_RDONLY, 0);
    if (fd < 0) return true;
    struct stat st {};
    const bool ok = (fstat(fd, &st) == 0);
    close(fd);
    if (!ok) return true;
    if (static_cast<uint64_t>(st.st_ino) != seg_ino_ ||
        static_cast<uint64_t>(st.st_dev) != seg_dev_) {
        return true;
    }

    if (mmap_base_ == nullptr) return true;
    return read_seg_epoch(mmap_base_) != seg_epoch_;
}

void XtShmMdGateway::reader_loop() {

    health_disconnected_.store(true, std::memory_order_release);

    Ring* ring = nullptr;
    Ring::Reader* reader = nullptr;
    uint32_t last_seg_check_ms = 0;

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

        if (!attached_.load(std::memory_order_acquire)) {
            if (attach_segment()) {
                ring = new Ring(mmap_base_);
                reader = new Ring::Reader(ring->make_reader());
                initial_tick_seen = false;

                last_head_advance_ms_.store(steady_ms_lo(), std::memory_order_release);
                attached_.store(true, std::memory_order_release);
                validate_declared_subset_();

            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
        }

        bool got_any = false;
        bool accepted_any = false;
        for (int i = 0; reader && i < 256 && reader->next(slot); ++i) {
            got_any = true;
            const XtRawTick& tick = slot.tick;

            if (declared_.empty() ||
                declared_.count(std::string(tick.code,
                                            ::strnlen(tick.code, sizeof(tick.code)))) != 0) {
                accepted_any = true;

                if (md_tap_) md_tap_(tick);

                if (cbs_.on_tick) cbs_.on_tick(tick, static_cast<uint64_t>(slot.capture_ns));
            }
        }
        if (accepted_any) initial_tick_seen = true;

        const uint32_t now = steady_ms_lo();
        if (got_any) {
            last_head_advance_ms_.store(now, std::memory_order_release);

            if (initial_tick_seen) mark_ready();
            continue;
        }

        const uint32_t last = last_head_advance_ms_.load(std::memory_order_acquire);
        if (static_cast<uint32_t>(now - last) >= stale_md_ms_) {
            mark_disconnected( 1);
        }

        if (static_cast<uint32_t>(now - last_seg_check_ms) >= 200u) {
            last_seg_check_ms = now;
            if (segment_changed()) {

                attached_.store(false, std::memory_order_release);
                release_segment();
                mark_disconnected( 2);
            }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

    release_segment();
}

}
