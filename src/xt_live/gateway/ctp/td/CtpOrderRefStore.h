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
    /// @brief 从 flow_dir/order_ref.next 恢复 OrderRef 序列，并原子写回当前 seed。
    /// @param flow_dir 流文件目录；须已存在且对当前进程可写。
    /// @return seed 读取与原子写回成功时返回 true。
    /// @note 启动时先只读旧 seed 再原子写回，避免 crash 后 seed 退回空文件或旧脏值。
    bool init(std::string flow_dir);

    /// @brief 分配下一个 OrderRef；预留块用尽时先持久化新上界再返回。
    /// @return 可用于本次报单的本地单号；序列耗尽或持久化失败时返回 0。
    /// @note 可被 worker 线程或内部重连线程调用；内部 mutex 串行化分配与扩块持久化。
    uint64_t next();

private:
    /// @brief 返回 OrderRef seed 文件路径（flow_dir/order_ref.next）。
    std::string state_path() const;
    /// @brief crash-safe 原子写回 seed 文件。
    bool persist_next(uint64_t next_value) const;

    mutable std::mutex mu_;       ///< 保护 OrderRef 序列与预留块持久化
    uint64_t next_{1};            ///< 下一个分配的 OrderRef
    uint64_t reserved_until_{0};  ///< 已持久化的预留块末尾，next_ 超过时先扩块
    std::string flow_dir_;        ///< 流文件目录（seed 文件所在）
};
