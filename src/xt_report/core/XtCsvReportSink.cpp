#include "XtCsvReportSink.h"

#include <chrono>
#include <filesystem>

#if defined(__unix__) || defined(__APPLE__)
#  include <fcntl.h>
#  include <unistd.h>
#endif

#include "xt_report/util/XtCsvReportUtil.h"

namespace xt_report {


// dtor 走 4 步：signal stop → main sync_flush + fsync → join io_thread →
// 兜底 sync_flush。所有落盘（io_loop 周期/末段 + sync_flush）都经
// drain_and_flush 持 io_mu_ 串行，ofstream 不会被两线程并发写；
// join 在 ofstream 析构前，无 mid-write 析构窗口。
XtCsvReportSink::~XtCsvReportSink() {
    // 1) 通知 io_thread 退出（让它 break out of cv_.wait_for）。
    if (io_thread_.joinable()) {
        stop_.store(true, std::memory_order_relaxed);
        cv_.notify_all();
    }
    // 2) 主线程 drain + fsync（与 io_thread 经 io_mu_ 串行）。
    sync_flush();
    // 3) join io_thread，让 io_loop 末段的 final drain 完整跑完。
    if (io_thread_.joinable()) {
        io_thread_.join();
    }
    // 4) join 后兜底再 sync_flush：对 io_loop 末段写入的内容做 fsync
    //    （io_loop 内 do_fsync=false 不 fsync）。此时主线程独占。
    sync_flush();
}

bool XtCsvReportSink::init(const std::string& report_dir,
                            const XtContractDims& dims,
                            std::string* err,
                            int async_flush_ms,
                            bool fills_only) {
    // 非可重入（两模式统一闸）：sync 模式无 io_thread，joinable 守卫不覆盖——
    // 二次 init 会对已开 ofstream re-open 置 failbit（is_open() 仍 true），后续行静默丢。
    if (init_done_) {
        if (err) *err = "init called on initialized sink (re-init unsupported)";
        return false;
    }
    // io_thread 已启动时再 init 还会 move-assign 到 joinable thread → std::terminate；
    // 保留作硬故障闸（init_done_ 抢先，正常路径不可达）。
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

    // fills_only: 只创建 fills.csv；未创建的 slot 由 flush_to_disk / fsync_files /
    // append_buf 的 is_open()/path.empty() guard 跳过。
    for (int i = 0; i < kSinkFileCount; ++i) {
        if (fills_only && i != kSlotFills) continue;
        paths_[i] = report_dir + "/" + kFileNames[i];
        files_[i].open(paths_[i], std::ios::out | std::ios::trunc);
        if (!files_[i].is_open()) {
            if (err) *err = std::string("open csv file failed: ") + kFileNames[i];
            // 失败即收拾：关掉已开文件，防 caller retry init 时 re-open 已开流置 failbit。
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
        // 周期 flush 不 fsync：每 50ms 一次 fsync 会 syscall overhead + page-cache
        // 抖动；只有 sync_flush() 显式调用才触 fsync。
        // 返回值忽略：周期 flush 失败不 propagate（io_thread 无 caller），最终
        // sync_flush 会重新 fsync + 暴露错误给 caller。
        (void)drain_and_flush(/*do_fsync=*/false);
    }
    // 退出前最后一次 drain 也只走 stream-flush；主线程 dtor 会再 sync_flush 收尾 fsync。
    (void)drain_and_flush(/*do_fsync=*/false);
}

bool XtCsvReportSink::drain_and_flush(bool do_fsync) noexcept {
    // io_mu_ 先于 mu_：谁先拿到落盘权谁先 swap，保 swap→write FIFO
    // （否则先 swap 的线程可能后写，CSV 行序颠倒）；
    // append_buf 只碰 mu_，热路径不被磁盘 I/O 阻塞。
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
        // 无条件 flush：同步模式（async_flush_ms=0）下 append_buf 直写 ofstream,
        // 数据在用户态 streambuf — 不 flush 则 fsync 只同步 page cache 已有字节,
        // sync_flush 的「已接收数据全部写出」契约被违反（crash 窗口丢数据）。
        files_[i].flush();
        // ofstream 任一 write/flush 失败会置 failbit,good() 转 false。
        if (!files_[i].good()) {
            ok = false;
            // Error contract: 记录 slot + 累计 fail_count，让后续 on_*() 看到
            // async I/O 失败并返回 false。io_loop noexcept 不抛，atomic 字段是
            // 唯一 cross-thread 错误通道。
            async_fail_count_.fetch_add(1, std::memory_order_release);
            last_failed_slot_.store(i, std::memory_order_release);
        }
    }
    if (do_fsync && !fsync_files()) {
        ok = false;
        async_fail_count_.fetch_add(1, std::memory_order_release);
        // fsync error 不归属单 slot, last_failed_slot 保持上一笔
    }
    return ok;
}

bool XtCsvReportSink::fsync_files() noexcept {
#if defined(__unix__) || defined(__APPLE__)
    // 每个 backing file 独立 ::open(O_WRONLY|O_APPEND) → ::fsync → ::close。
    // std::ofstream 不暴露 fd，故冷路径用 reopen + fsync；失败只影响 report sink。
    auto sync_one = [](const std::string& path) noexcept -> bool {
        if (path.empty()) return true;  // 未 init 的 slot 跳过
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
    // 非 POSIX 平台（如 MSVC）：no-op 但仍返回 true（ofstream::flush 已尽力）。
    return true;
#endif
}

bool XtCsvReportSink::sync_flush() noexcept {
    bool ok = true;
    try {
        // drain_and_flush 持 io_mu_：与 io_loop 的周期/末段落盘串行，
        // ofstream 不会被两线程并发写（session-end/dtor 窗口含在内）。
        if (!drain_and_flush(/*do_fsync=*/true)) ok = false;
        cv_.notify_all();
    } catch (...) {
        // noexcept 契约: 吞所有异常,转译成 ok=false。
        ok = false;
    }
    // Error contract: 把 async io_loop 记录的 I/O error 合入返回值；caller
    // 据此把 sink failure 继续上报给 Runner。
    if (async_fail_count_.load(std::memory_order_acquire) > 0) ok = false;
    return ok;
}

bool XtCsvReportSink::append_buf(int slot, std::string&& line) {
    if (async_flush_ms_ > 0) {
        std::lock_guard<std::mutex> lk(mu_);
        active_buf_[slot] += line;
        return true;  // buffered：写盘失败由 io_loop/flush_to_disk 记账
    }
    // fills_only 未创建的 slot 不算失败：skip 语义（backtest 仅写 fills）。
    if (!files_[slot].is_open()) return true;
    files_[slot].write(line.data(), static_cast<std::streamsize>(line.size()));
    // sync 直写无 io_loop 兜底：写失败当场记账并返 false 守 on_*() 错误契约；
    // 历史失败再由 check_health 拦后续行（末尾 sync_flush 可能对同一故障再计一次）。
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

// 接收新 row 前检查 sink 状态；返回 false 时 caller 应累计 drop 并上报。
//   ready=false -> init 未完成或失败
//   async_fail_count>0 -> 后台 I/O 已失败，继续接收只会扩大丢失窗口
bool XtCsvReportSink_check_health(std::atomic<bool>& ready,
                                  std::atomic<uint64_t>& async_fail) noexcept {
    if (!ready.load(std::memory_order_acquire)) return false;
    if (async_fail.load(std::memory_order_acquire) > 0) return false;
    return true;
}

} // namespace

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

} // namespace xt_report
