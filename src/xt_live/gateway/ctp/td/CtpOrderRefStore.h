#pragma once

#include <cstdint>
#include <mutex>
#include <string>

/// @brief CTP 订单号（OrderRef）分配器，带 crash-safe 持久化。
///
/// OrderRef 同时用作系统内的 local_id，不能重复。CtpOrderRefStore 把
/// 下次重启起点持久化到 flow_dir 下的 seed 文件，进程 crash 重启后
/// 只会跳号、不会复用已发出的单号。
class CtpOrderRefStore {
public:

    bool init(std::string flow_dir);

    uint64_t next();

private:

    std::string state_path() const;

    bool persist_next(uint64_t next_value) const;

    mutable std::mutex mu_;
    uint64_t next_{1};
    uint64_t reserved_until_{0};
    std::string flow_dir_;
};
