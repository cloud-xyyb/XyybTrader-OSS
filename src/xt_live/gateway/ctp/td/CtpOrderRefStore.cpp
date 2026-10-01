// CTP OrderRef 序列恢复、预留与 crash-safe 持久化。

#include "ctp/td/CtpOrderRefStore.h"

#include "XtLogger.h"
#include "xt_util/path/PathUtil.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <utility>

namespace {

// CTP OrderRef 字段为数字字符串，当前保留 12 位空间。
constexpr uint64_t kOrderRefMax          = 999999999999ULL;
// 批量预留 10K 个号：crash 后最多跳过 10K 个未用 ref，但不会复用已发 ref。
constexpr uint64_t kOrderRefReserveBlock = 10000ULL;

}  // anonymous namespace

// ── Seed 文件与序列分配 ──────────────────────────
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
    // 扩块只发生在预留块耗尽时，直接使用 crash-safe atomic write，避免
    // 预打开 FILE* 的 inode 生命周期与 fsync 语义复杂化。
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
    // 启动时先只读旧 seed，再原子写回同一值，避免 crash 后 seed 文件退回空文件或旧脏值。
    uint64_t next_value = 1;
    const std::string path = state_path();
    // (1) 只读恢复 seed；此处绝不先 truncate。
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
    // reserved_until < next，让首笔下单先持久化新的预留块再返回 OrderRef。
    reserved_until_ = next_value - 1ULL;
    // (2) 原子写回 seed；即使写回后立刻 crash，下次也不会回落到 1 重发 OrderRef。
    return persist_next(next_value);
}

uint64_t CtpOrderRefStore::next() {
    // 稳态通常只有 worker 线程调用；mutex 主要保护 stop/init 与异常重入。
    // 扩块时先写入未来安全起点，再返回当前 OrderRef，确保 crash 后只跳号不复用。
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
