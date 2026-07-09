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

    bool init(const std::string& report_dir,
              const XtContractDims& dims,
              std::string* err = nullptr,
              int async_flush_ms = 50,
              bool fills_only = false);

    bool ready() const { return ready_.load(std::memory_order_acquire); }

    uint64_t async_fail_count() const noexcept { return async_fail_count_.load(std::memory_order_acquire); }

    int last_failed_slot() const noexcept { return last_failed_slot_.load(std::memory_order_acquire); }

    void poison_slot_for_test(int slot);

    bool on_kpi_1s(const XtKpi1sRow& row) override;
    bool on_fill(const XtFillRow& row) override;
    bool on_position_snapshot(const XtPositionRow& row) override;
    bool on_account_truth(const XtAccountTruthRow& row) override;
    bool sync_flush() noexcept override;

private:

    enum Slot : int {
        kSlotKpi1s           = 0,
        kSlotFills           = 1,
        kSlotPositionSnap    = 2,
        kSlotAccountTruth    = 3,
        kSinkFileCount       = 4,
    };

    static constexpr const char* kFileNames[kSinkFileCount] = {
        "kpi_1s.csv",
        "fills.csv",
        "position_snapshot_1s.csv",
        "account_truth_1s.csv",
    };

    bool append_buf(int slot, std::string&& line);
    void io_loop();

    bool drain_and_flush(bool do_fsync) noexcept;

    bool flush_to_disk(std::string bufs[kSinkFileCount], bool do_fsync) noexcept;

    bool fsync_files() noexcept;

    std::string dims_prefix_;
    XtContractDims dims_;
    std::ofstream files_[kSinkFileCount];
    std::string   paths_[kSinkFileCount];
    std::atomic<bool> ready_{false};

    std::atomic<uint64_t> async_fail_count_{0};
    std::atomic<int>      last_failed_slot_{-1};

    std::mutex mu_;
    std::mutex io_mu_;
    std::string active_buf_[kSinkFileCount];
    std::condition_variable cv_;
    std::thread io_thread_;
    std::atomic<bool> stop_{false};
    int async_flush_ms_ = 0;
    bool init_done_ = false;
};

}
