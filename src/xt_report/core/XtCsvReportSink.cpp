#include "XtCsvReportSink.h"

#include <chrono>
#include <filesystem>

#if defined(__unix__) || defined(__APPLE__)
#  include <fcntl.h>
#  include <unistd.h>
#endif

#include "xt_report/util/XtCsvReportUtil.h"

namespace xt_report {

XtCsvReportSink::~XtCsvReportSink() {

    if (io_thread_.joinable()) {
        stop_.store(true, std::memory_order_relaxed);
        cv_.notify_all();
    }

    sync_flush();

    if (io_thread_.joinable()) {
        io_thread_.join();
    }

    sync_flush();
}

bool XtCsvReportSink::init(const std::string& report_dir,
                            const XtContractDims& dims,
                            std::string* err,
                            int async_flush_ms,
                            bool fills_only) {

    if (init_done_) {
        if (err) *err = "init called on initialized sink (re-init unsupported)";
        return false;
    }

    if (io_thread_.joinable()) {
        if (err) *err = "init called on running sink (re-init unsupported)";
        return false;
    }
    if (report_dir.empty()) {
        if (err) *err = "report_dir is empty";
        return false;
    }
    std::error_code ec;
    std::filesystem::create_directories(report_dir, ec);
    if (ec) {
        if (err) *err = std::string("create report_dir failed: ") + ec.message();
        return false;
    }
    async_flush_ms_ = async_flush_ms;
    ready_.store(false, std::memory_order_relaxed);
    dims_ = dims;
    dims_prefix_ = format_dims_prefix(dims_);

    for (int i = 0; i < kSinkFileCount; ++i) {
        if (fills_only && i != kSlotFills) continue;
        paths_[i] = report_dir + "/" + kFileNames[i];
        files_[i].open(paths_[i], std::ios::out | std::ios::trunc);
        if (!files_[i].is_open()) {
            if (err) *err = std::string("open csv file failed: ") + kFileNames[i];

            for (int j = 0; j < kSinkFileCount; ++j) {
                if (files_[j].is_open()) files_[j].close();
                paths_[j].clear();
            }
            return false;
        }
    }

    if (fills_only) {
        files_[kSlotFills] << csv_header_fill();
    } else {
        files_[kSlotKpi1s]            << csv_header_kpi_1s();
        files_[kSlotFills]            << csv_header_fill();
        files_[kSlotPositionSnap]     << csv_header_position_snapshot();
        files_[kSlotAccountTruth]     << csv_header_account_truth();
    }
    for (int i = 0; i < kSinkFileCount; ++i)
        if (files_[i].is_open()) files_[i].flush();
    ready_.store(true, std::memory_order_release);

    if (async_flush_ms_ > 0) {
        stop_.store(false, std::memory_order_relaxed);
        io_thread_ = std::thread(&XtCsvReportSink::io_loop, this);
    }
    init_done_ = true;
    return true;
}

void XtCsvReportSink::io_loop() {
    const auto interval = std::chrono::milliseconds(async_flush_ms_);
    while (!stop_.load(std::memory_order_relaxed)) {
        {
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait_for(lk, interval);
        }

        (void)drain_and_flush( false);
    }

    (void)drain_and_flush( false);
}

bool XtCsvReportSink::drain_and_flush(bool do_fsync) noexcept {

    std::lock_guard<std::mutex> io_lk(io_mu_);
    std::string work[kSinkFileCount];
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (int i = 0; i < kSinkFileCount; ++i) std::swap(work[i], active_buf_[i]);
    }
    return flush_to_disk(work, do_fsync);
}

bool XtCsvReportSink::flush_to_disk(std::string bufs[kSinkFileCount], bool do_fsync) noexcept {
    bool ok = true;
    for (int i = 0; i < kSinkFileCount; ++i) {
        if (!files_[i].is_open()) continue;
        if (!bufs[i].empty()) {
            files_[i].write(bufs[i].data(), static_cast<std::streamsize>(bufs[i].size()));
            bufs[i].clear();
        }

        files_[i].flush();

        if (!files_[i].good()) {
            ok = false;

            async_fail_count_.fetch_add(1, std::memory_order_release);
            last_failed_slot_.store(i, std::memory_order_release);
        }
    }
    if (do_fsync && !fsync_files()) {
        ok = false;
        async_fail_count_.fetch_add(1, std::memory_order_release);

    }
    return ok;
}

bool XtCsvReportSink::fsync_files() noexcept {
#if defined(__unix__) || defined(__APPLE__)

    auto sync_one = [](const std::string& path) noexcept -> bool {
        if (path.empty()) return true;
        const int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
        if (fd < 0) return false;
        const int rc = ::fsync(fd);
        ::close(fd);
        return rc == 0;
    };
    bool ok = true;
    for (int i = 0; i < kSinkFileCount; ++i) {
        if (!sync_one(paths_[i])) ok = false;
    }
    return ok;
#else

    return true;
#endif
}

bool XtCsvReportSink::sync_flush() noexcept {
    bool ok = true;
    try {

        if (!drain_and_flush( true)) ok = false;
        cv_.notify_all();
    } catch (...) {

        ok = false;
    }

    if (async_fail_count_.load(std::memory_order_acquire) > 0) ok = false;
    return ok;
}

bool XtCsvReportSink::append_buf(int slot, std::string&& line) {
    if (async_flush_ms_ > 0) {
        std::lock_guard<std::mutex> lk(mu_);
        active_buf_[slot] += line;
        return true;
    }

    if (!files_[slot].is_open()) return true;
    files_[slot].write(line.data(), static_cast<std::streamsize>(line.size()));

    if (!files_[slot].good()) {
        async_fail_count_.fetch_add(1, std::memory_order_release);
        last_failed_slot_.store(slot, std::memory_order_release);
        return false;
    }
    return true;
}

void XtCsvReportSink::poison_slot_for_test(int slot) {
    if (slot < 0 || slot >= kSinkFileCount) return;
    files_[slot].setstate(std::ios::badbit);
}

namespace {

bool XtCsvReportSink_check_health(std::atomic<bool>& ready,
                                  std::atomic<uint64_t>& async_fail) noexcept {
    if (!ready.load(std::memory_order_acquire)) return false;
    if (async_fail.load(std::memory_order_acquire) > 0) return false;
    return true;
}

}

bool XtCsvReportSink::on_kpi_1s(const XtKpi1sRow& row) {
    if (!XtCsvReportSink_check_health(ready_, async_fail_count_)) return false;
    return append_buf(kSlotKpi1s, to_csv_line(dims_prefix_, row));
}

bool XtCsvReportSink::on_fill(const XtFillRow& row) {
    if (!XtCsvReportSink_check_health(ready_, async_fail_count_)) return false;
    const std::string row_dims = (row.strategy_id.empty() && row.symbol.empty())
        ? dims_prefix_
        : format_dims_prefix(dims_, row.strategy_id, row.symbol);
    return append_buf(kSlotFills, to_csv_line(row_dims, row));
}

bool XtCsvReportSink::on_position_snapshot(const XtPositionRow& row) {
    if (!XtCsvReportSink_check_health(ready_, async_fail_count_)) return false;
    return append_buf(kSlotPositionSnap, to_csv_line(dims_prefix_, row));
}

bool XtCsvReportSink::on_account_truth(const XtAccountTruthRow& row) {
    if (!XtCsvReportSink_check_health(ready_, async_fail_count_)) return false;
    return append_buf(kSlotAccountTruth, to_csv_line(dims_prefix_, row));
}

}
