#include "xt_backtest/data/reader/XtBinTickReader.h"

#include "xt_util/path/PathUtil.h"

#include <utility>

XtBinTickReader::XtBinTickReader(std::vector<std::string> paths)
    : paths_(std::move(paths)) {}

bool XtBinTickReader::init() {
    reset();

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

            if (!xt_tick_bin::detect_stream_format(*in_, fmt_)) {
                in_.reset();
                continue;
            }
            fmt_inited_ = true;
        }
        if (xt_tick_bin::read_next_tick(*in_, fmt_, out, nullptr)) {
            return true;
        }

        in_.reset();
    }
}

bool XtBinTickReader::open_next_file() {
    while (idx_ < paths_.size()) {

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
