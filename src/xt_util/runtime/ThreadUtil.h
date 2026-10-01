#pragma once

#include <string>
#include <thread>

/// @file ThreadUtil.h
/// @brief 线程运行时调优工具。
namespace ThreadUtil {

/// @brief 将当前线程绑定到指定 CPU 核；`cpu_id < 0` 时 no-op。
void set_current_thread_affinity(int cpu_id);

/// @brief 设置当前线程调度优先级。
///
/// Linux 下仅 `realtime` label 尝试 SCHED_FIFO；其它 label 和非 Linux 平台为 no-op。
/// 调用不向上传递 `sched_setscheduler` 失败。
void set_current_thread_priority(const std::string& level);

/// @brief 尝试锁定进程地址空间（mlockall），成功返回 `true`。
bool mlock_process_memory();

/// @brief 从 sysfs 推导 `cpu_id` 所属 NUMA node id；失败返回 -1。
///
/// 读取 `<cpu_sysfs_root>/cpu<N>/` 目录下 kernel 提供的 `node<M>` 条目
/// （非 socket/package id——sub-NUMA / NPS>1 拓扑下两者不同）。
/// @param cpu_sysfs_root 测试 seam；生产使用默认值。
int numa_node_of_cpu(int cpu_id,
                     const char* cpu_sysfs_root = "/sys/devices/system/cpu");

/// @brief 将当前线程后续内存分配绑定到 `cpu_id` 所在 NUMA 节点。
///
/// 推荐在 `set_current_thread_affinity(cpu_id)` 之后调用。`cpu_id < 0` 时 no-op；
/// 非 Linux 平台返回 `true`。Linux 路径经 `numa_node_of_cpu` 推导节点，推导失败
/// （含 CONFIG_NUMA=n 无 node 条目的内核）返回 `false`，调用方应按性能调优失败
/// 处理，而不是按业务错误处理。
bool bind_current_thread_numa(int cpu_id);

/// @brief join 线程；若目标即当前线程则 detach，避免自我 join 死锁。
///
/// 回调/SPI 线程里触发 stop()/teardown 时，被 join 的目标可能就是当前线程
/// （如 CTP SDK 回调线程拆除自己启动的重连/刷新线程）。此时 join 自身会死锁，
/// 退化为 detach；非自身线程走正常 join；non-joinable 直接返回。
///
/// @warning detach 分支让线程脱离管理：若其捕获了即将析构的对象（如 `[this]`），
///          可能在对象析构后继续访问。调用方需保证 detach 路径下的生命周期安全。
void join_or_detach(std::thread& th);

}  // namespace ThreadUtil
