// CTP OrderRef 序列恢复、预留与 crash-safe 持久化。

#include "ctp/td/CtpOrderRefStore.h"

#include "XtLogger.h"
#include "xt_util/path/PathUtil.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <utility>

namespace {

constexpr uint64_t kOrderRefMax          = 999999999999ULL;

constexpr uint64_t kOrderRefReserveBlock = 10000ULL;

}

std::string CtpOrderRefStore::state_path() const {
    std::filesystem::path p(flow_dir_);
    p /= "order_ref.next";
    return p.string();
}

bool CtpOrderRefStore::persist_next(uint64_t next_value) const {
    char seed_buf[32];
    const int seed_len = std::snprintf(seed_buf, sizeof(seed_buf), "%llu\n",
                                       static_cast<unsigned long long>(next_value));
    const std::string path = state_path();
    if (seed_len <= 0 || static_cast<size_t>(seed_len) >= sizeof(seed_buf)) {
        XtLogger::error_m("TD", "order_ref seed format failed: {}", path);
        return false;
    }

    if (!PathUtil::write_atomic(path,
                                std::string(seed_buf, static_cast<size_t>(seed_len)),
                                0644)) {
        XtLogger::error_m("TD", "order_ref atomic persist failed: {}", path);
        return false;
    }
    return true;
}

bool CtpOrderRefStore::init(std::string flow_dir) {
    std::lock_guard<std::mutex> guard(mu_);
    flow_dir_ = std::move(flow_dir);

    uint64_t next_value = 1;
    const std::string path = state_path();

    {
        std::ifstream in(path);
        if (in) {
            uint64_t file_value = 0;
            in >> file_value;
            if (in.good() && file_value >= 1 && file_value <= (kOrderRefMax + 1ULL)) {
                next_value = file_value;
            } else {
                XtLogger::warn_m("TD", "order_ref seed invalid, fallback to 1 path={}", path);
            }
        }
    }
    next_           = next_value;

    reserved_until_ = next_value - 1ULL;

    return persist_next(next_value);
}

uint64_t CtpOrderRefStore::next() {

    std::lock_guard<std::mutex> guard(mu_);
    if (next_ > kOrderRefMax) {
        XtLogger::error_m("TD", "order_ref exhausted max={}", kOrderRefMax);
        return 0;
    }
    if (next_ > reserved_until_) {
        uint64_t next_reserved = next_ + kOrderRefReserveBlock;
        if (next_reserved > (kOrderRefMax + 1ULL)) next_reserved = kOrderRefMax + 1ULL;
        if (!persist_next(next_reserved)) return 0;
        reserved_until_ = next_reserved - 1ULL;
    }
    return next_++;
}
