#include "xt_ipc/XtShmMdGateway.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstring>

#include "xt_core/market/XtRawTick.h"
#include "xt_ipc/XtMdWireContract.h"
#include "xt_ipc/XtShmRing.h"
#include "xt_util/time/XtSteadyClock.h"

namespace xt_ipc {

namespace {
using Ring = XtShmRing<XtRawTick>;

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

bool XtShmMdGateway::attach_segment() {
    int fd = shm_open(ring_name_.c_str(), O_RDONLY, 0);
    if (fd < 0) return false;

    struct stat st {};
    if (fstat(fd, &st) != 0) {
        close(fd);
        return false;
    }
    const uint64_t mapped = static_cast<uint64_t>(st.st_size);
    void* base = mmap(nullptr, mapped, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (base == MAP_FAILED) return false;

    const auto* ctrl = reinterpret_cast<const RingControlBlock*>(base);
    if (validate_ring(ctrl, mapped, Ring::kSlotStride) != nullptr) {
        munmap(base, mapped);
        return false;
    }

    mmap_base_ = base;
    mapped_bytes_ = mapped;
    seg_ino_ = static_cast<uint64_t>(st.st_ino);
    seg_dev_ = static_cast<uint64_t>(st.st_dev);
    seg_epoch_ = read_seg_epoch(base);
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

    auto release_segment = [&] {
        delete reader;
        reader = nullptr;
        delete ring;
        ring = nullptr;
        detach_segment();
    };

    XtRawTick tick{};
    while (!stop_.load(std::memory_order_acquire)) {

        if (!attached_.load(std::memory_order_acquire)) {
            if (attach_segment()) {
                ring = new Ring(mmap_base_);
                reader = new Ring::Reader(ring->make_reader());

                last_head_advance_ms_.store(steady_ms_lo(), std::memory_order_release);
                attached_.store(true, std::memory_order_release);
                mark_ready();
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
        }

        bool got_any = false;
        for (int i = 0; reader && i < 256 && reader->next(tick); ++i) {
            got_any = true;

            if (declared_.empty() ||
                declared_.count(std::string(tick.code,
                                            ::strnlen(tick.code, sizeof(tick.code)))) != 0) {
                if (cbs_.on_tick) cbs_.on_tick(tick);
            }
        }

        const uint32_t now = steady_ms_lo();
        if (got_any) {
            last_head_advance_ms_.store(now, std::memory_order_release);
            mark_ready();
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
