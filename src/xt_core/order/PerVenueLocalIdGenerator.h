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

#include <stdexcept>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_util/path/PathUtil.h"

namespace xt_core::local_id {

class PerVenueLocalIdGenerator {
public:
    static constexpr uint64_t kDefaultReserveBlockSize = 1ULL << 16;

    static constexpr int      kInstanceTagShift = 53;
    static constexpr uint64_t kInstanceTagMax   = 255;

    /// @brief per-instance 构造身份；venue + resolved runtime.instance_id。
    struct InstanceIdentity {
        std::string venue;
        std::string instance;
        std::string state_dir = "/var/lib/xyybtrader";
    };

    static std::string default_state_path(std::string_view venue) {
        return legacy_state_path("/var/lib/xyybtrader", venue);
    }

    static std::string instance_state_path(const InstanceIdentity& ident) {
        return ident.state_dir + "/local_id_" + sanitize_component(ident.venue) +
               "__" + sanitize_component(ident.instance) + ".state";
    }

    PerVenueLocalIdGenerator()
        : PerVenueLocalIdGenerator( std::string{}) {}

    explicit PerVenueLocalIdGenerator(std::string state_path)
        : state_path_(std::move(state_path)) {
        initialize_from_raw_start(now_ms_shifted_start());
    }

    explicit PerVenueLocalIdGenerator(const InstanceIdentity& ident) {
        if (ident.instance.empty()) {
            state_path_ = legacy_state_path(ident.state_dir, ident.venue);
            initialize_from_raw_start(now_ms_shifted_start());
            return;
        }
        init_instance_identity(ident, now_ms_shifted_start());
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

    PerVenueLocalIdGenerator(RawStartStateTag, const InstanceIdentity& ident,
                             uint64_t raw_time_part,
                             uint64_t reserve_block = kDefaultReserveBlockSize)
        : reserve_block_(reserve_block == 0 ? 1 : reserve_block) {
        init_instance_identity(ident, raw_time_part);
    }

    static uint64_t tag_base_for_test(std::string_view venue,
                                      std::string_view instance) noexcept {
        return tag_base_from_hash(venue, instance);
    }

#define XT_LOCAL_ID_CLAIM_SYNC_SEAM 1

    static bool& claim_sync_fail_for_test() noexcept {
        static bool v = false;
        return v;
    }
#endif

    ~PerVenueLocalIdGenerator() {

        if (!state_path_.empty()) {
            const uint64_t high = std::min(
                counter_.load(std::memory_order_relaxed), domain_end_);
            if (!persist_state_file_atomic(state_path_, high)) {
                persist_fail_count_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        if (lock_fd_ >= 0) ::close(lock_fd_);
    }

    PerVenueLocalIdGenerator(const PerVenueLocalIdGenerator&) = delete;
    PerVenueLocalIdGenerator& operator=(const PerVenueLocalIdGenerator&) = delete;

    uint64_t next() noexcept {
        const uint64_t id = counter_.fetch_add(1, std::memory_order_relaxed);
        if (id > domain_end_) {
            std::fprintf(stderr,
                         "[FATAL] local_id domain exhausted: id=%llu > domain_end=%llu tag=%llu\n",
                         static_cast<unsigned long long>(id),
                         static_cast<unsigned long long>(domain_end_),
                         static_cast<unsigned long long>(instance_tag_));
            std::abort();
        }
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

    uint64_t instance_tag() const noexcept { return instance_tag_; }

private:
    static uint64_t now_ms_shifted_start() noexcept {
        const uint64_t now_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        return now_ms << 10;
    }

    void init_instance_identity(const InstanceIdentity& ident,
                                uint64_t raw_time_part) {
        validate_instance_or_throw(ident);
        state_path_       = instance_state_path(ident);
        legacy_seed_path_ = legacy_state_path(ident.state_dir, ident.venue);
        if (!PathUtil::ensure_directory(ident.state_dir)) {
            throw std::runtime_error(
                "local_id state dir unavailable: " + ident.state_dir);
        }
        acquire_instance_lock_or_throw(ident);
        try {
            instance_tag_ = claim_instance_tag_or_throw(ident);
            domain_end_   = ((instance_tag_ + 1) << kInstanceTagShift) - 1;
            initialize_from_raw_start(
                tagged_raw_start(instance_tag_, raw_time_part));
        } catch (...) {

            ::close(lock_fd_);
            lock_fd_ = -1;
            throw;
        }
    }

    void acquire_instance_lock_or_throw(const InstanceIdentity& ident) {
        const std::string lock_path = state_path_ + ".lock";
        const int fd = ::open(lock_path.c_str(),
                              O_CREAT | O_RDWR | O_CLOEXEC, 0644);
        if (fd < 0) {
            throw std::runtime_error(
                "local_id instance lock open failed: " + lock_path +
                " errno=" + std::to_string(errno));
        }
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            const int e = errno;
            ::close(fd);
            throw std::runtime_error(
                "local_id instance '" + ident.instance +
                "' already active (flock busy on " + lock_path +
                ", errno=" + std::to_string(e) +
                ") — two runners misconfigured with the same runtime.instance_id?");
        }
        lock_fd_ = fd;
    }

    void initialize_from_raw_start(uint64_t raw_start) {
        uint64_t start = raw_start;
        if (!state_path_.empty()) {
            uint64_t saved_high = read_state_file_or_zero(state_path_);
            if (instance_tag_ != 0) {
                if (saved_high >= domain_end_) {

                    throw std::runtime_error(
                        "local_id domain exhausted (saved_high=" +
                        std::to_string(saved_high) + " >= domain_end=" +
                        std::to_string(domain_end_) + ", tag=" +
                        std::to_string(instance_tag_) +
                        ") — refusing to reseed and reissue ids");
                }
                if ((saved_high >> kInstanceTagShift) != instance_tag_) {

                    saved_high = 0;
                }
            }
            if (saved_high == 0 && !legacy_seed_path_.empty()) {

                const uint64_t legacy_high =
                    read_state_file_or_zero(legacy_seed_path_);
                if ((legacy_high >> kInstanceTagShift) == 0) {
                    saved_high = legacy_high;
                }
            }
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

    static std::string legacy_state_path(const std::string& dir,
                                         std::string_view venue) {
        std::string v = venue.empty() ? std::string("default")
                                      : sanitize_component(venue);
        return dir + "/local_id_" + v + ".state";
    }

    static std::string sanitize_component(std::string_view s) {
        std::string out(s);
        for (char& c : out) {
            const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                            (c >= '0' && c <= '9') || c == '_' || c == '-';
            if (!ok) c = '_';
        }
        return out;
    }

    static uint64_t tagged_raw_start(uint64_t tag, uint64_t time_part) noexcept {
        const uint64_t cap = (1ULL << kInstanceTagShift) - 1;
        return (tag << kInstanceTagShift) | std::min(time_part, cap);
    }

    static uint64_t tag_base_from_hash(std::string_view venue,
                                       std::string_view instance) noexcept {
        uint64_t h = sync::fnv1a_update(sync::kFnv1a64OffsetBasis,
                                        venue.data(), venue.size());
        const char sep = '\x1f';
        h = sync::fnv1a_update(h, &sep, 1);
        h = sync::fnv1a_update(h, instance.data(), instance.size());
        return (h % kInstanceTagMax) + 1;
    }

    static std::string claim_file_path(const InstanceIdentity& ident,
                                       uint64_t tag) {
        return ident.state_dir + "/local_id_tag_" +
               sanitize_component(ident.venue) + "_" + std::to_string(tag) +
               ".owner";
    }

    static bool claim_write_durable(int fd, const std::string& line,
                                    const std::string& dir) noexcept {
        const char* buf = line.data();
        size_t remaining = line.size();
        bool ok = true;
        while (remaining > 0) {
            const ssize_t n = ::write(fd, buf, remaining);
            if (n > 0) {
                buf += n;
                remaining -= static_cast<size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            ok = false;
            break;
        }
        if (ok) ok = (::fsync(fd) == 0);
        if (::close(fd) != 0) ok = false;
        if (ok) ok = fsync_dir(dir);
#ifdef XT_BUILD_TESTS
        if (claim_sync_fail_for_test()) ok = false;
#endif
        return ok;
    }

    static bool fsync_dir(const std::string& dir) noexcept {
        const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd < 0) return false;
        bool ok = (::fsync(dfd) == 0);
        if (::close(dfd) != 0) ok = false;
        return ok;
    }

    enum class OwnerRead { NotPresent, Incomplete, Complete };

    static OwnerRead read_owner(const std::string& path,
                               std::string& out) noexcept {
        std::ifstream f(path, std::ios::in);
        if (!f.is_open()) return OwnerRead::NotPresent;
        out.clear();
        std::getline(f, out);

        if (f.eof() || f.fail()) return OwnerRead::Incomplete;

        if (f.peek() != std::char_traits<char>::eof()) {
            return OwnerRead::Incomplete;
        }
        return OwnerRead::Complete;
    }

    static void validate_instance_or_throw(const InstanceIdentity& ident) {
        for (const char c : ident.instance) {
            if (c == '\n' || c == '\r' || c == '\0') {
                throw std::runtime_error(
                    "local_id instance id contains control char (\\n/\\r/\\0), "
                    "breaks single-line owner invariant: '" + ident.instance +
                    "'");
            }
        }
    }

    static void refsync_reused_claim(const std::string& claim,
                                     const std::string& dir) noexcept {
        const int fd = ::open(claim.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd >= 0) {
            (void)::fsync(fd);
            ::close(fd);
        }
        (void)fsync_dir(dir);
    }

    static std::string parent_dir_of(const std::string& dir) noexcept {
        if (dir.empty()) return ".";
        size_t end = dir.size();
        while (end > 1 && dir[end - 1] == '/') --end;
        const size_t slash = dir.find_last_of('/', end - 1);
        if (slash == std::string::npos) return ".";
        if (slash == 0) return "/";
        return dir.substr(0, slash);
    }

    static uint64_t claim_instance_tag_or_throw(const InstanceIdentity& ident) {
        const uint64_t base = tag_base_from_hash(ident.venue, ident.instance);
        if (!PathUtil::ensure_directory(ident.state_dir)) {
            throw std::runtime_error(
                "local_id claim dir unavailable: " + ident.state_dir);
        }

        (void)fsync_dir(parent_dir_of(ident.state_dir));
        for (uint64_t k = 0; k < kInstanceTagMax; ++k) {
            const uint64_t tag = ((base - 1 + k) % kInstanceTagMax) + 1;
            const std::string claim = claim_file_path(ident, tag);
            std::string owner;
            const OwnerRead r = read_owner(claim, owner);
            if (r == OwnerRead::Complete) {
                if (owner == ident.instance) {
                    refsync_reused_claim(claim, ident.state_dir);
                    return tag;
                }
                continue;
            }
            if (r == OwnerRead::Incomplete) {
                continue;
            }

            const int fd = ::open(claim.c_str(),
                                  O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0644);
            if (fd >= 0) {

                if (!claim_write_durable(fd, ident.instance + "\n",
                                         ident.state_dir)) {

                    if (::unlink(claim.c_str()) != 0) {
                        throw std::runtime_error(
                            "local_id claim not durable AND unlink failed: " +
                            claim + " errno=" + std::to_string(errno));
                    }

                    (void)fsync_dir(ident.state_dir);
                    throw std::runtime_error(
                        "local_id claim not durable (write/fsync failed): " +
                        claim);
                }
                return tag;
            }
            if (errno == EEXIST) {

                const OwnerRead r2 = read_owner(claim, owner);
                if (r2 == OwnerRead::Complete && owner == ident.instance) {
                    refsync_reused_claim(claim, ident.state_dir);
                    return tag;
                }
                if (r2 != OwnerRead::NotPresent) {
                    continue;
                }

                throw std::runtime_error(
                    "local_id claim owner unreadable: " + claim);
            }
            throw std::runtime_error(
                "local_id claim create failed: " + claim +
                " errno=" + std::to_string(errno));
        }
        throw std::runtime_error(
            "local_id claim slots exhausted for venue '" + ident.venue +
            "' (255 instances per venue is not a supported topology)");
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
        uint64_t next_reserved = add_saturating(id, reserve_delta);

        if (next_reserved > domain_end_) next_reserved = domain_end_;
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
    std::string legacy_seed_path_;
    uint64_t instance_tag_ = 0;
    uint64_t domain_end_ =
        std::numeric_limits<uint64_t>::max();
    int lock_fd_ = -1;
    std::atomic<uint64_t> counter_{};
    std::atomic<uint64_t> reserved_until_{0};
    std::atomic<uint64_t> persist_fail_count_{0};
    uint64_t reserve_block_ = kDefaultReserveBlockSize;
    std::mutex reserve_mu_;
};

}
