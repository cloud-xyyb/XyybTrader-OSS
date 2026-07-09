#pragma once

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <ios>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include "xt_util/path/PathUtil.h"

namespace xt_core::local_id {

/// @brief 为单个交易 venue 分配本地递增 order id。
///
/// 调用方为每个 venue 持有独立实例，submit 路径通过 next() 取号。
/// state_path 非空时启用 high-water 持久化，重启后继承时钟起点和已持久化上界。
/// 空 state_path 只提供当前进程内唯一递增语义，适合测试或无状态场景。
/// next() 支持多个 submit 线程并发调用；构造、析构和 test inspect 接口不提供并发同步。
///
/// @warning 不要在不同 venue 间复用实例或 state file，否则本地订单号域会混在一起。
class PerVenueLocalIdGenerator {
public:
    static constexpr uint64_t kDefaultReserveBlockSize = 1ULL << 16;

    static std::string default_state_path(std::string_view venue) {
        std::string v = venue.empty() ? std::string("default") : std::string(venue);
        return "/var/lib/xyybtrader/local_id_" + v + ".state";
    }

    PerVenueLocalIdGenerator()
        : PerVenueLocalIdGenerator( std::string{}) {}

    explicit PerVenueLocalIdGenerator(std::string state_path)
        : state_path_(std::move(state_path)) {
        initialize_from_raw_start(now_ms_shifted_start());
    }

    /// @brief 测试入口：直接指定 counter 起点，不读时钟、不读 state file。
    struct ExplicitStartTag {};
    PerVenueLocalIdGenerator(ExplicitStartTag, uint64_t start_value) {
        counter_.store(start_value, std::memory_order_relaxed);
    }

#ifdef XT_BUILD_TESTS
    /// @brief 测试入口：注入 raw_start、state path 和预留块大小。
    struct RawStartStateTag {};
    PerVenueLocalIdGenerator(RawStartStateTag, std::string state_path,
                             uint64_t raw_start,
                             uint64_t reserve_block = kDefaultReserveBlockSize)
        : state_path_(std::move(state_path)),
          reserve_block_(reserve_block == 0 ? 1 : reserve_block) {
        initialize_from_raw_start(raw_start);
    }
#endif

    ~PerVenueLocalIdGenerator() {

        if (!state_path_.empty()) {
            if (!persist_state_file_atomic(state_path_,
                    counter_.load(std::memory_order_relaxed))) {
                persist_fail_count_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    PerVenueLocalIdGenerator(const PerVenueLocalIdGenerator&) = delete;
    PerVenueLocalIdGenerator& operator=(const PerVenueLocalIdGenerator&) = delete;

    uint64_t next() noexcept {
        const uint64_t id = counter_.fetch_add(1, std::memory_order_relaxed);
        reserve_until_includes(id);
        return id;
    }

    uint64_t current_high_for_test() const noexcept {
        return counter_.load(std::memory_order_relaxed);
    }

    uint64_t reserved_until_for_test() const noexcept {
        return reserved_until_.load(std::memory_order_relaxed);
    }

    uint64_t persist_fail_count() const noexcept {
        return persist_fail_count_.load(std::memory_order_relaxed);
    }

private:
    static uint64_t now_ms_shifted_start() noexcept {
        const uint64_t now_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        return now_ms << 10;
    }

    void initialize_from_raw_start(uint64_t raw_start) {
        uint64_t start = raw_start;
        if (!state_path_.empty()) {
            const uint64_t saved_high = read_state_file_or_zero(state_path_);
            const uint64_t saved_next =
                saved_high == std::numeric_limits<uint64_t>::max()
                    ? saved_high
                    : saved_high + 1;
            start = std::max(start, saved_next);
        }
        counter_.store(start, std::memory_order_relaxed);
        if (state_path_.empty()) {
            reserved_until_.store(std::numeric_limits<uint64_t>::max(),
                                  std::memory_order_relaxed);
        } else {
            reserve_until_includes(start);
        }
    }

    static uint64_t read_state_file_or_zero(const std::string& path) noexcept {
        std::ifstream f(path, std::ios::in);
        if (!f.is_open()) return 0;
        std::string line;
        if (!std::getline(f, line)) return 0;
        try {
            return std::stoull(line);
        } catch (...) {
            return 0;
        }
    }

    static uint64_t add_saturating(uint64_t value, uint64_t delta) noexcept {
        const uint64_t max = std::numeric_limits<uint64_t>::max();
        return value > max - delta ? max : value + delta;
    }

    void reserve_until_includes(uint64_t id) noexcept {
        if (state_path_.empty()) return;
        if (id <= reserved_until_.load(std::memory_order_acquire)) return;

        std::lock_guard<std::mutex> guard(reserve_mu_);
        const uint64_t current = reserved_until_.load(std::memory_order_relaxed);
        if (id <= current) return;
        const uint64_t reserve_delta = reserve_block_ > 0 ? reserve_block_ - 1 : 0;
        const uint64_t next_reserved = add_saturating(id, reserve_delta);
        if (persist_state_file_atomic(state_path_, next_reserved)) {
            reserved_until_.store(next_reserved, std::memory_order_release);
        } else {

            persist_fail_count_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    static bool persist_state_file_atomic(const std::string& path,
                                          uint64_t value) noexcept {
        return PathUtil::write_atomic(path, std::to_string(value));
    }

    std::string state_path_;
    std::atomic<uint64_t> counter_{};
    std::atomic<uint64_t> reserved_until_{0};
    std::atomic<uint64_t> persist_fail_count_{0};
    uint64_t reserve_block_ = kDefaultReserveBlockSize;
    std::mutex reserve_mu_;
};

}
