#include "xt_backtest/data/reader/XtBinTickReader.h"

#include "xt_util/path/PathUtil.h"

#include <cstdio>
#include <utility>

XtBinTickReader::XtBinTickReader(std::vector<std::string> paths)
    : paths_(std::move(paths)) {}

bool XtBinTickReader::init() {
    reset();
    // init 只做轻量存在性检查；header 校验延迟到 next_tick，避免初始化阶段打开全部文件。
    for (const auto& path : paths_) {
        if (PathUtil::file_exists(path)) return true;
    }
    return false;
}

void XtBinTickReader::reset() {
    idx_ = 0;
    in_.reset();
    fmt_ = XtTickBinFormat::DualV2;
    fmt_inited_ = false;
    io_error_ = false;
}

bool XtBinTickReader::next_tick(XtRawTick& out) {
    while (true) {
        if (!in_ && !open_next_file()) return false;
        if (!fmt_inited_) {
            // 每个文件单独探测 header；损坏/旧版 codec 文件跳过并告警（静默跳过会让
            // 显式 tick.files 输入在 kTickBinCodecVersion bump 后变成静默 0 tick）。
            if (!xt_tick_bin::detect_stream_format(*in_, fmt_)) {
                std::fprintf(stderr,
                             "[XtBinTickReader] skip %s: header/codec-version mismatch "
                             "(expect CTTKV%u); rebuild from source data\n",
                             idx_ > 0 && idx_ <= paths_.size() ? paths_[idx_ - 1].c_str()
                                                               : "<unknown>",
                             static_cast<unsigned>(xt_tick_bin::kTickBinCodecVersion));
                in_.reset();
                continue;
            }
            fmt_inited_ = true;
        }
        if (xt_tick_bin::read_next_tick(*in_, fmt_, out, nullptr)) {
            return true;
        }
        // 当前文件耗尽后不保留 EOF stream，下一轮尝试打开下一个文件。
        in_.reset();
    }
}

bool XtBinTickReader::open_next_file() {
    while (idx_ < paths_.size()) {
        // 三步 open：libstdc++ 的 filebuf setbuf 仅在 open 之前生效；构造即 open
        // 的写法会让 pubsetbuf 静默无效。放大到 1MB 后 ~1KB/record 的逐条读从
        // ~8 record/syscall 提到 ~1000 record/syscall。
        if (stream_buf_.empty()) stream_buf_.resize(kStreamBufBytes);
        in_ = std::make_unique<std::ifstream>();
        in_->rdbuf()->pubsetbuf(stream_buf_.data(),
                                static_cast<std::streamsize>(stream_buf_.size()));
        in_->open(paths_[idx_], std::ios::binary);
        ++idx_;
        if (in_->is_open()) {
            fmt_ = XtTickBinFormat::DualV2;
            fmt_inited_ = false;
            return true;
        }
        io_error_ = true;
        in_.reset();
    }
    return false;
}
