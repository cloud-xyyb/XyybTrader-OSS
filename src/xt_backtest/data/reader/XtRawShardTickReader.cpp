#include "xt_backtest/data/reader/XtRawShardTickReader.h"

#include <cstdio>
#include <utility>

XtRawShardTickReader::XtRawShardTickReader(std::vector<std::string> shard_paths)
    : shard_paths_(std::move(shard_paths)) {}

bool XtRawShardTickReader::init() {
    reset();
    return !shard_paths_.empty();
}

void XtRawShardTickReader::reset() {
    path_idx_ = 0;
    rec_idx_ = 0;
    records_.clear();
    records_.shrink_to_fit();
    failed_shard_.clear();
    failure_status_ = xt_raw_shard::XtRawShardStatus::Ok;
    io_error_ = false;
}

bool XtRawShardTickReader::next_tick(XtRawTick& out) {
    while (true) {
        if (rec_idx_ < records_.size()) {
            out = records_[rec_idx_++];
            return true;
        }
        if (!load_next_shard()) return false;
    }
}

bool XtRawShardTickReader::load_next_shard() {
    while (path_idx_ < shard_paths_.size()) {
        const std::string& path = shard_paths_[path_idx_++];
        xt_raw_shard::XtRawShardInfo info{};
        std::vector<XtRawTick> recs;
        const auto status = xt_raw_shard::load_shard(path, recs, info);
        if (status != xt_raw_shard::XtRawShardStatus::Ok) {

            failed_shard_ = path;
            failure_status_ = status;
            io_error_ = true;
            std::fprintf(stderr, "[XtRawShardTickReader] shard unusable (%s): %s\n",
                         xt_raw_shard::to_string(status), path.c_str());
            records_.clear();
            rec_idx_ = 0;
            return false;
        }
        records_.swap(recs);
        rec_idx_ = 0;
        if (!records_.empty()) return true;

    }
    return false;
}
