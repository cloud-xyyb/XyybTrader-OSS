#pragma once

#include <atomic>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>

#include "xt_report/core/IReportSink.h"

namespace xt_report {

/// @brief IReportSink 的 CSV 文件实现。
///
/// `on_*()` 接收已构造好的 Row，转换为 CSV 行后写入文件或内存缓冲。
/// async 模式由后台 I/O 线程周期落盘；`sync_flush()` 与后台落盘经 io_mu_
/// 串行，并负责最终 flush/fsync。
///
/// @pre 同步模式（`async_flush_ms == 0`）下 `on_*()` 直写 ofstream 无锁：
///      调用方必须单线程串行 on_*() 与 sync_flush（backtest 单线程路径即此形态）。
/// @note 领域事件转换和指标聚合由调用方完成，本类只负责 CSV sink。
/// @warning I/O 失败（async flush 或 sync 直写）会使当次及后续 `on_*()` 返回 false；
///          调用方必须检查返回值。
class XtCsvReportSink final : public IReportSink {
public:
    XtCsvReportSink() = default;
    ~XtCsvReportSink();

    XtCsvReportSink(const XtCsvReportSink&) = delete;
    XtCsvReportSink& operator=(const XtCsvReportSink&) = delete;

    /// @brief 创建报告目录、打开 CSV 文件并写入 header。
    ///
    /// `async_flush_ms > 0` 启动后台 I/O 线程；否则 `on_*()` 同步写入 ofstream。
    /// `fills_only=true` 只创建 fills.csv；未打开的 slot 对应 `on_*()` 返回 true
    /// 并跳过写入，用于只需要成交明细的调用路径。
    /// @note 非可重入（两模式统一）：二次 init 返 false——ofstream 对已开流 re-open
    ///       只会置 failbit（is_open() 仍 true），后续行会静默丢。
    bool init(const std::string& report_dir,
              const XtContractDims& dims,
              std::string* err = nullptr,
              int async_flush_ms = 50,
              bool fills_only = false);

    bool ready() const { return ready_.load(std::memory_order_acquire); }

    /// @brief I/O 写盘失败累计次数（async flush + sync 直写）；0 表示未观察到 I/O error。
    /// @note 生产链路经 sync_flush 返回值/on_*() false 消费错误态；本 getter 与
    ///       last_failed_slot 目前仅测试/诊断直接读。
    uint64_t async_fail_count() const noexcept { return async_fail_count_.load(std::memory_order_acquire); }
    /// @brief 最近一次写盘失败的 slot id；-1 表示暂无失败记录。
    int last_failed_slot() const noexcept { return last_failed_slot_.load(std::memory_order_acquire); }

    /// @note test-only seam：把 slot 流置 badbit 模拟写盘失败（slot 序同 kFileNames：
    ///       0=kpi_1s 1=fills 2=position 3=account_truth）；仅限 sync 模式 sink
    ///       （async 模式与 io_loop 竞争流 state）；生产代码禁调。
    void poison_slot_for_test(int slot);

    bool on_kpi_1s(const XtKpi1sRow& row) override;
    bool on_fill(const XtFillRow& row) override;
    bool on_position_snapshot(const XtPositionRow& row) override;
    bool on_account_truth(const XtAccountTruthRow& row) override;
    bool sync_flush() noexcept override;

private:
    // Slot 顺序同时约束 kFileNames、files_、paths_、active_buf_。
    enum Slot : int {
        kSlotKpi1s           = 0,
        kSlotFills           = 1,
        kSlotPositionSnap    = 2,
        kSlotAccountTruth    = 3,
        kSinkFileCount       = 4,  // 保持末尾, 扩 slot 时自动更新
    };
    // 文件名必须与 Slot enum 同序。
    static constexpr const char* kFileNames[kSinkFileCount] = {
        "kpi_1s.csv",
        "fills.csv",
        "position_snapshot_1s.csv",
        "account_truth_1s.csv",
    };

    /// @brief 追加一行；async 模式先接收入 buffer，写盘失败由 io_loop 记账；
    ///        sync 模式写失败当场记账并返 false；not-open slot = skip 返 true。
    bool append_buf(int slot, std::string&& line);
    void io_loop();
    /// @brief 持 io_mu_ 把「swap active_buf_ → 写盘」收成 FIFO 原子单元；
    /// io_loop 与 sync_flush 的唯一落盘入口（ofstream 不可并发写）。
    bool drain_and_flush(bool do_fsync) noexcept;
    /// @brief 将 work buffer 写入 ofstream；`do_fsync=true` 时同步到存储介质。
    /// @pre 调用方持有 io_mu_（经 drain_and_flush）或 io_thread 已退出。
    bool flush_to_disk(std::string bufs[kSinkFileCount], bool do_fsync) noexcept;
    /// @brief 对所有 backing files 执行 fsync；非 POSIX 平台退化为 no-op success。
    bool fsync_files() noexcept;

    std::string dims_prefix_;                        ///< 所有行共享的 CSV 维度前缀
    XtContractDims dims_;
    std::ofstream files_[kSinkFileCount];
    std::string   paths_[kSinkFileCount];            ///< fsync_files 需要 reopen 文件路径
    std::atomic<bool> ready_{false};

    // 跨线程错误通道：io_loop 不抛异常，只通过这两个 atomic 暴露 disk error。
    std::atomic<uint64_t> async_fail_count_{0};
    std::atomic<int>      last_failed_slot_{-1};

    std::mutex mu_;                                  ///< 保护 active_buf_（仅 buffer swap/append）
    std::mutex io_mu_;                               ///< 串行化落盘（io_loop vs sync_flush），swap→write FIFO
    std::string active_buf_[kSinkFileCount];         ///< producer append，I/O 线程 swap 后写盘
    std::condition_variable cv_;
    std::thread io_thread_;
    std::atomic<bool> stop_{false};
    int async_flush_ms_ = 0;                         ///< >0 启用后台刷盘，单位 ms
    bool init_done_ = false;                         ///< init 成功后置位；两模式统一的防重入闸
};

} // namespace xt_report
