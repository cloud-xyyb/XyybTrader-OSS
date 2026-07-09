#include "xt_util/runtime/ThreadUtil.h"

#include <cstdio>
#include <pthread.h>
#include <sched.h>
#include <thread>
#if defined(__linux__) || defined(__APPLE__)
#include <sys/mman.h>
#endif
#if defined(__linux__)
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace ThreadUtil {

void set_current_thread_affinity(int cpu_id) {
#if defined(__linux__)
    if (cpu_id < 0) return;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#else
    (void)cpu_id;
#endif
}

void set_current_thread_priority(const std::string& level) {
#if defined(__linux__)
    if (level == "realtime") {
        sched_param sp;
        sp.sched_priority = 90;
        (void)sched_setscheduler(0, SCHED_FIFO, &sp);
    }
#else
    (void)level;
#endif
}

bool mlock_process_memory() {
#if defined(__linux__) || defined(__APPLE__)
    return ::mlockall(MCL_CURRENT | MCL_FUTURE) == 0;
#else
    return false;
#endif
}

bool bind_current_thread_numa(int cpu_id) {
#if defined(__linux__)
    if (cpu_id < 0) return true;

    char path[128];
    std::snprintf(path, sizeof(path),
                  "/sys/devices/system/cpu/cpu%d/topology/physical_package_id", cpu_id);
    FILE* f = std::fopen(path, "r");
    if (!f) return false;
    int node = -1;
    if (std::fscanf(f, "%d", &node) != 1) node = -1;
    std::fclose(f);
    if (node < 0) return false;

    constexpr int MPOL_BIND = 2;
    constexpr int MPOL_F_STATIC_NODES = (1 << 15);
    unsigned long nodemask = 1UL << node;
    long ret = ::syscall(238, MPOL_BIND | MPOL_F_STATIC_NODES, &nodemask, sizeof(nodemask) * 8 + 1);
    return ret == 0;
#else
    (void)cpu_id;
    return true;
#endif
}

void join_or_detach(std::thread& th) {
    if (!th.joinable()) return;
    if (th.get_id() != std::this_thread::get_id()) th.join();
    else th.detach();
}

}
