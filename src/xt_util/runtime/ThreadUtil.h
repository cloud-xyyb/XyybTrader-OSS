#pragma once

#include <string>
#include <thread>

/// @file ThreadUtil.h
/// @brief 线程运行时调优工具。
namespace ThreadUtil {

void set_current_thread_affinity(int cpu_id);

void set_current_thread_priority(const std::string& level);

bool mlock_process_memory();

int numa_node_of_cpu(int cpu_id,
                     const char* cpu_sysfs_root = "/sys/devices/system/cpu");

bool bind_current_thread_numa(int cpu_id);

void join_or_detach(std::thread& th);

}
