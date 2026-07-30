// xt_md_reader 是 MD fan-out 共享内存 ring 的只读诊断进程。
// 本文件只暴露可执行入口；CLI 解析、attach/map 校验、read-loop 统计都留在
// anonymous namespace。attach 走 fail-closed wire 校验，mmap 只用 PROT_READ。
// 落后被 writer lap 是设计行为，正确性由 received + gap 守恒和 synthetic 自校验判断。

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "xt_core/market/XtRawTick.h"
#include "xt_ipc/XtMdWireContract.h"
#include "xt_ipc/XtShmRing.h"

namespace {
using Ring = xt_ipc::XtShmRing<xt_ipc::XtMdWireSlot>;

struct ReaderOptions {
    std::string ring_name = "/xt_md_shadow";
    int64_t expect = 0;
    int64_t slow_ms = 0;
    int64_t timeout_ms = 10000;
    int64_t attach_timeout_ms = 5000;
    bool verify_synthetic = false;
    std::string dump_recv;
};

struct MappedRing {
    void* base = nullptr;
    uint64_t bytes = 0;
};

int64_t arg_i64(const char* s) { return std::strtoll(s, nullptr, 10); }

bool parse_options(int argc, char** argv, ReaderOptions& opt) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : ""; };
        if (a == "--ring-name") opt.ring_name = next();
        else if (a == "--expect") opt.expect = arg_i64(next());
        else if (a == "--slow-ms") opt.slow_ms = arg_i64(next());
        else if (a == "--timeout-ms") opt.timeout_ms = arg_i64(next());
        else if (a == "--attach-timeout-ms") opt.attach_timeout_ms = arg_i64(next());
        else if (a == "--verify-synthetic") opt.verify_synthetic = true;
        else if (a == "--dump-recv") opt.dump_recv = next();
        else {
            std::fprintf(stderr, "unknown arg: %s\n", a.c_str());
            return false;
        }
    }
    return true;
}

MappedRing attach_ring(const ReaderOptions& opt) {

    const auto t_attach0 = std::chrono::steady_clock::now();
    int fd = -1;
    for (;;) {
        fd = shm_open(opt.ring_name.c_str(), O_RDONLY, 0);
        if (fd >= 0) break;
        auto el = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t_attach0).count();
        if (el > opt.attach_timeout_ms) {
            std::fprintf(stderr, "xt_md_reader: 段 %s attach 超时（connector 未启?）\n",
                         opt.ring_name.c_str());
            return {};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // 取真实段大小（防 mmap 超过文件 → 越界 SIGBUS），mmap 全段后 fail-closed 校验。
    struct stat st{};
    if (fstat(fd, &st) != 0) {
        std::perror("fstat");
        close(fd);
        return {};
    }
    const uint64_t mapped = static_cast<uint64_t>(st.st_size);
    void* base = mmap(nullptr, mapped, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (base == MAP_FAILED) {
        std::perror("mmap");
        return {};
    }

    const auto* ctrl = reinterpret_cast<const xt_ipc::RingControlBlock*>(base);
    if (const char* err = xt_ipc::validate_ring(ctrl, mapped, Ring::kSlotStride, Ring::kElementSize)) {
        std::fprintf(stderr, "xt_md_reader: ring 校验失败（fail-closed）: %s\n", err);
        munmap(base, mapped);
        return {};
    }
    return {base, mapped};
}

int run_reader(const ReaderOptions& opt, const MappedRing& mapping) {
    Ring ring(mapping.base);
    auto rd = ring.make_reader();
    const uint64_t attach_cursor = rd.cursor();
    std::printf("xt_md_reader: attached ring=%s capacity=%llu attach_cursor=%llu mapped=%llu\n",
                opt.ring_name.c_str(), (unsigned long long)ring.capacity(),
                (unsigned long long)attach_cursor, (unsigned long long)mapping.bytes);
    std::fflush(stdout);

    std::FILE* dump = nullptr;
    if (!opt.dump_recv.empty()) {
        dump = std::fopen(opt.dump_recv.c_str(), "w");
        if (!dump) {
            std::perror("open --dump-recv");
            return 1;
        }
        std::fprintf(dump, "price,action_time,bid0,ask0\n");
    }

    int64_t received = 0;
    bool ordered = true, verify_ok = true;
    double last = -1.0, first = -1.0;
    const auto t0 = std::chrono::steady_clock::now();
    xt_ipc::XtMdWireSlot slot{};
    for (;;) {
        if (rd.next(slot)) {
            const XtRawTick& out = slot.tick;
            const uint64_t idx = rd.cursor() - 1;
            if (received == 0) first = out.price;
            if (received > 0 && !(out.price > last)) ordered = false;
            last = out.price;
            if (opt.verify_synthetic &&
                (out.price != static_cast<double>(idx) || out.action_time != static_cast<uint32_t>(idx)))
                verify_ok = false;
            if (dump) std::fprintf(dump, "%.6f,%u,%.6f,%.6f\n", out.price, out.action_time,
                                   out.bid_prices[0], out.ask_prices[0]);
            ++received;
            if (opt.slow_ms > 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(opt.slow_ms));
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }

        if (opt.expect > 0 && (received + static_cast<int64_t>(rd.gap_count()) >=
                               opt.expect - static_cast<int64_t>(attach_cursor)))
            break;
        auto el = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0).count();
        if (el > opt.timeout_ms) break;
    }

    const uint64_t gap = rd.gap_count();
    const int64_t expected_seen =
        (opt.expect > 0) ? (opt.expect - static_cast<int64_t>(attach_cursor)) : -1;
    const bool conserved =
        (opt.expect <= 0) || (received + static_cast<int64_t>(gap) == expected_seen);

    std::printf("xt_md_reader: SUMMARY recv=%lld gap=%llu first=%.0f last=%.0f ordered=%d verify=%d conserved=%d\n",
                (long long)received, (unsigned long long)gap, first, last,
                ordered ? 1 : 0, verify_ok ? 1 : 0, conserved ? 1 : 0);
    std::fflush(stdout);

    if (dump) std::fclose(dump);

    const bool pass = (!opt.verify_synthetic || ordered) && verify_ok && conserved && received > 0;
    return pass ? 0 : 1;
}

}

int main(int argc, char** argv) {
    ReaderOptions opt;
    if (!parse_options(argc, argv, opt)) return 2;

    const MappedRing mapping = attach_ring(opt);
    if (!mapping.base) return 1;

    const int rc = run_reader(opt, mapping);
    munmap(mapping.base, mapping.bytes);
    return rc;
}
