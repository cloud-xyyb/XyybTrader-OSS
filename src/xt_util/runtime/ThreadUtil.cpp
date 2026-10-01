#include "xt_util/runtime/ThreadUtil.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <sched.h>
#include <thread>
#if defined(__linux__) || defined(__APPLE__)
#include <sys/mman.h>
#endif
#if defined(__linux__)
#include <dirent.h>
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

int numa_node_of_cpu(int cpu_id, const char* cpu_sysfs_root) {
#if defined(__linux__)
    if (cpu_id < 0 || cpu_sysfs_root == nullptr) return -1;
    char dir_path[192];
    const int n =
        std::snprintf(dir_path, sizeof(dir_path), "%s/cpu%d", cpu_sysfs_root, cpu_id);
    if (n < 0 || n >= static_cast<int>(sizeof(dir_path))) return -1;  // 截断则拒探
    DIR* d = ::opendir(dir_path);
    if (!d) return -1;
    int node = -1;
    while (const dirent* e = ::readdir(d)) {
        // kernel 在 cpuN 目录下放 `node<M>` 条目标记所属 NUMA node；
        // topology/physical_package_id 是 socket id，NPS>1 拓扑下与 node 不同。
        if (std::strncmp(e->d_name, "node", 4) != 0) continue;
        char* endp = nullptr;
        const long v = std::strtol(e->d_name + 4, &endp, 10);
        if (endp == e->d_name + 4 || *endp != '\0' || v < 0) continue;
        node = static_cast<int>(v);
        break;
    }
    ::closedir(d);
    return node;
#else
    (void)cpu_id;
    (void)cpu_sysfs_root;
    return -1;
#endif
}

bool bind_current_thread_numa(int cpu_id) {
#if defined(__linux__)
    if (cpu_id < 0) return true;
    const int node = numa_node_of_cpu(cpu_id);
    // 单字 nodemask 承载上限 64 node；超界宁可不 bind 也不写错误掩码。
    if (node < 0 || node >= static_cast<int>(sizeof(unsigned long) * 8)) return false;

    // 直接 syscall，避免 xt_util 引入 libnuma 链接依赖。
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

}  // namespace ThreadUtil
