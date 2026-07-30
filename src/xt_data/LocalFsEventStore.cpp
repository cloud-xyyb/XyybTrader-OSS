#include "LocalFsEventStore.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <optional>
#include <system_error>
#include <utility>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "flatbuffers/flatbuffers.h"

#include "xt_core/market/XtTickSortKey.h"

namespace ct::data {

namespace {

std::string format_trading_day(std::uint32_t day) {

    char buf[16];
    const std::uint32_t y = day / 10000;
    const std::uint32_t m = (day / 100) % 100;
    const std::uint32_t d = day % 100;
    std::snprintf(buf, sizeof(buf), "%04u-%02u-%02u", y, m, d);
    return std::string(buf);
}

std::optional<std::uint32_t> parse_trading_day(const std::string& name) {
    if (name.size() != 10 || name[4] != '-' || name[7] != '-') {
        return std::nullopt;
    }
    std::uint32_t y = 0, m = 0, d = 0;
    const char* p = name.data();
    if (std::from_chars(p, p + 4, y).ec != std::errc()) {
        return std::nullopt;
    }
    if (std::from_chars(p + 5, p + 7, m).ec != std::errc()) {
        return std::nullopt;
    }
    if (std::from_chars(p + 8, p + 10, d).ec != std::errc()) {
        return std::nullopt;
    }
    return y * 10000u + m * 100u + d;
}

std::string make_shard_key(std::uint32_t day,
                           std::string_view exch,
                           std::string_view sym) {
    std::string key;
    key.reserve(8 + 1 + exch.size() + 1 + sym.size());
    char day_buf[16];
    const int n = std::snprintf(day_buf, sizeof(day_buf), "%08u", day);
    if (n > 0) key.append(day_buf, static_cast<std::size_t>(n));
    key += '|';
    key.append(exch);
    key += '|';
    key.append(sym);
    return key;
}

std::size_t on_disk_size(std::FILE* fp) {
    const int fd = ::fileno(fp);
    if (fd < 0) {
        return 0;
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0 || st.st_size < 0) {
        return 0;
    }
    return static_cast<std::size_t>(st.st_size);
}

constexpr std::size_t kMaxRecordSize = 64u * 1024u;

bool is_path_component_safe(std::string_view s) {
    if (s.empty() || s == "." || s == "..") {
        return false;
    }
    for (const unsigned char c : s) {
        if (c < 0x20 || c == '/' || c == '\\') {
            return false;
        }
    }
    return true;
}

}

namespace {

/// @brief 多 shard forward-only iterator，按 trading_day/action_time 做 k-way merge。
///
/// 每个 FileState 持有一个当前 record buffer，MdTickView 指向该 buffer。
/// next() 在下一次调用开始时才推进上一个 shard，保证调用方拿到的 view 在
/// 本次 next() 返回后仍然有效，直到同一 iterator 的下一次 next()。
class LocalFsIterator : public IEventIterator {
public:
    explicit LocalFsIterator(std::vector<std::filesystem::path> files)
        : files_(std::move(files)) {
        prime_files_();
    }

    ~LocalFsIterator() override {
        for (auto& f : states_) {
            if (f.fp) {
                std::fclose(f.fp);
            }
        }
    }

    bool next(MdTickView& out) override {
        if (has_error_) return false;

        if (pending_advance_ != kNoFile) {
            const std::size_t idx = pending_advance_;
            pending_advance_ = kNoFile;
            if (read_next_record_(states_[idx])) {
                push_heap_(idx);
            }
            if (has_error_) {
                return false;
            }
        }
        if (heap_.empty()) {
            return false;
        }

        pop_heap_();
        const std::size_t idx = heap_.back();
        heap_.pop_back();

        FileState& f = states_[idx];
        out.tick      = schema::GetMdTick(f.buf.data());
        out.bytes_len = f.buf.size();
        pending_advance_ = idx;
        return true;
    }

    std::size_t bytes_read() const override { return bytes_read_; }
    bool has_error() const override { return has_error_; }

private:
    struct FileState {
        std::filesystem::path path;
        std::FILE* fp = nullptr;
        std::vector<uint8_t> buf;
        std::uint32_t trading_day = 0;
        std::uint32_t action_time = 0;
        std::uint64_t seq = 0;
        std::size_t file_order = 0;
    };

    static constexpr std::size_t kNoFile = static_cast<std::size_t>(-1);

    static void close_file_(FileState& f) {
        if (f.fp) {
            std::fclose(f.fp);
            f.fp = nullptr;
        }
    }

    void prime_files_() {
        states_.reserve(files_.size());
        for (std::size_t i = 0; i < files_.size(); ++i) {
            FileState f;
            f.path = files_[i];
            f.file_order = i;
            f.fp = std::fopen(f.path.c_str(), "rb");
            if (!f.fp) {
                continue;
            }
            states_.push_back(std::move(f));
            const std::size_t idx = states_.size() - 1;
            if (read_next_record_(states_[idx])) {
                push_heap_(idx);
            }
            if (has_error_) {
                return;
            }
        }
    }

    bool read_next_record_(FileState& f) {
        if (!f.fp) {
            return false;
        }

        std::uint32_t size_le = 0;
        const std::size_t nr = std::fread(&size_le, 1, sizeof(std::uint32_t), f.fp);
        bytes_read_ += nr;
        if (nr != sizeof(std::uint32_t)) {

            close_file_(f);
            return false;
        }

        const std::size_t size = static_cast<std::size_t>(size_le);
        if (size == 0 || size > kMaxRecordSize) {

            has_error_ = true;
            return false;
        }

        f.buf.resize(size);
        const std::size_t data_nr = std::fread(f.buf.data(), 1, size, f.fp);
        bytes_read_ += data_nr;
        if (data_nr != size) {
            close_file_(f);
            return false;
        }

        ::flatbuffers::Verifier v(f.buf.data(), size);
        if (!schema::VerifyMdTickBuffer(v)) {
            has_error_ = true;
            return false;
        }

        const auto* tick = schema::GetMdTick(f.buf.data());
        f.trading_day = tick->trading_day();
        f.action_time = tick->action_time();
        ++f.seq;
        return true;
    }

    bool heap_later_(std::size_t lhs, std::size_t rhs) const {
        const FileState& a = states_[lhs];
        const FileState& b = states_[rhs];

        const uint64_t ka = make_tick_sort_key(a.trading_day, a.action_time);
        const uint64_t kb = make_tick_sort_key(b.trading_day, b.action_time);
        if (ka != kb) {
            return ka > kb;
        }
        if (a.file_order != b.file_order) {
            return a.file_order > b.file_order;
        }
        return a.seq > b.seq;
    }

    void push_heap_(std::size_t idx) {
        heap_.push_back(idx);
        std::push_heap(heap_.begin(), heap_.end(),
                       [this](std::size_t a, std::size_t b) {
                           return heap_later_(a, b);
                       });
    }

    void pop_heap_() {
        std::pop_heap(heap_.begin(), heap_.end(),
                      [this](std::size_t a, std::size_t b) {
                          return heap_later_(a, b);
                      });
    }

    std::vector<std::filesystem::path> files_;
    std::vector<FileState> states_;
    std::vector<std::size_t> heap_;
    std::size_t pending_advance_ = kNoFile;
    std::size_t bytes_read_ = 0;
    bool has_error_ = false;
};

}

LocalFsEventStore::LocalFsEventStore(std::string root_path)
    : root_path_(std::move(root_path)) {

}

LocalFsEventStore::~LocalFsEventStore() {
    (void)flush();
}

std::filesystem::path
LocalFsEventStore::shard_path(std::uint32_t day,
                              std::string_view exch,
                              std::string_view sym) const {
    std::filesystem::path p(root_path_);
    p /= format_trading_day(day);
    p /= std::string(exch);
    p /= std::string(sym);
    p /= "tick.fbs";
    return p;
}

LocalFsEventStore::ShardHandle*
LocalFsEventStore::open_shard_(const std::string& key,
                              const std::filesystem::path& path) {

    if (poisoned_shards_.count(key) != 0) {
        return nullptr;
    }

    auto [it, inserted] = files_.try_emplace(key);
    if (!inserted && it->second.fp) {
        return &it->second;
    }

    std::error_code ec;
    std::filesystem::create_directories(path.parent_path(), ec);
    if (ec) {
        files_.erase(it);
        return nullptr;
    }
    std::FILE* fp = std::fopen(path.c_str(), "ab");
    if (!fp) {
        files_.erase(it);
        return nullptr;
    }
    it->second.fp = fp;

    it->second.clean_size = on_disk_size(fp);
    return &it->second;
}

std::filesystem::path LocalFsEventStore::events_ext_path(std::uint32_t day) const {
    std::filesystem::path p(root_path_);
    p /= format_trading_day(day);
    p /= "_events";
    p /= "events_ext.fbs";
    return p;
}

bool LocalFsEventStore::write_frame_(const std::string& key,
                                     const std::filesystem::path& path,
                                     ShardHandle& sh,
                                     const void* fbs_buf, std::size_t fbs_size) {
    const std::uint32_t size_le = static_cast<std::uint32_t>(fbs_size);
    write_scratch_.resize(sizeof(std::uint32_t) + fbs_size);
    std::memcpy(write_scratch_.data(), &size_le, sizeof(std::uint32_t));
    std::memcpy(write_scratch_.data() + sizeof(std::uint32_t), fbs_buf, fbs_size);
    const std::size_t total = write_scratch_.size();

    if (std::fwrite(write_scratch_.data(), 1, total, sh.fp) == total) {
        sh.clean_size += total;
        pending_bytes_ += total;
        return true;
    }

    const std::size_t clean = sh.clean_size;
    close_and_drop_shard_(key);
    bool rolled_back = false;
    const int tfd = ::open(path.c_str(), O_WRONLY);
    if (tfd >= 0) {
        struct stat st{};
        if (::fstat(tfd, &st) == 0 && st.st_size >= 0 &&
            static_cast<std::size_t>(st.st_size) >= clean &&
            ::ftruncate(tfd, static_cast<off_t>(clean)) == 0) {
            rolled_back = true;
        }
        ::close(tfd);
    }
    if (rolled_back) {
        torn_frame_rollback_count_.fetch_add(1, std::memory_order_relaxed);
    } else {
        poison_shard_(key);
    }
    return false;
}

void LocalFsEventStore::close_and_drop_shard_(const std::string& key) {
    auto it = files_.find(key);
    if (it == files_.end()) {
        return;
    }
    if (it->second.fp) {
        std::fclose(it->second.fp);
    }
    files_.erase(it);
}

void LocalFsEventStore::poison_shard_(const std::string& key) {
    if (poisoned_shards_.insert(key).second) {
        poisoned_shard_count_.fetch_add(1, std::memory_order_relaxed);

        std::fprintf(stderr,
            "[eventstore] shard poisoned fail-closed (torn frame unrecoverable): key=%s\n",
            key.c_str());
    }
}

bool LocalFsEventStore::append(std::uint32_t trading_day,
                                std::string_view exchange,
                                std::string_view symbol,
                                const void* fbs_buf,
                                std::size_t fbs_size) {
    if (!fbs_buf || fbs_size == 0) {
        return false;
    }
    if (fbs_size > kMaxRecordSize) {
        return false;
    }
    if (!is_path_component_safe(exchange) || !is_path_component_safe(symbol)) {
        return false;
    }

    const std::string key = make_shard_key(trading_day, exchange, symbol);
    const std::filesystem::path path = shard_path(trading_day, exchange, symbol);
    ShardHandle* sh = open_shard_(key, path);
    if (!sh) {
        return false;
    }
    return write_frame_(key, path, *sh, fbs_buf, fbs_size);
}

bool LocalFsEventStore::append_event_ext(std::uint32_t trading_day,
                                          const void* fbs_buf,
                                          std::size_t fbs_size) {
    if (!fbs_buf || fbs_size == 0) {
        return false;
    }
    if (fbs_size > kMaxRecordSize) {
        return false;
    }
    if (trading_day == 0) {
        return false;
    }

    const std::string key = make_shard_key(trading_day, "_events", "_events");
    const std::filesystem::path path = events_ext_path(trading_day);
    ShardHandle* sh = open_shard_(key, path);
    if (!sh) {
        return false;
    }
    return write_frame_(key, path, *sh, fbs_buf, fbs_size);
}

std::string LocalFsEventStore::rotate() {
    namespace fs = std::filesystem;
    std::error_code ec;

    const bool has_data = fs::exists(root_path_, ec) && !fs::is_empty(root_path_, ec);
    const bool flush_ok = flush();
    if (!has_data) return {};
    if (!flush_ok) {

        rotate_fail_count_.fetch_add(1, std::memory_order_relaxed);
        std::fprintf(stderr, "[eventstore] rotate skipped: flush persistence failed (root=%s)\n",
                     root_path_.c_str());
        return {};
    }

    const auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const fs::path src(root_path_);
    fs::path dst;
    long long sfx = static_cast<long long>(now);
    do {
        dst = src;
        dst += "." + std::to_string(sfx);
        ++sfx;
    } while (fs::exists(dst, ec));

    fs::rename(src, dst, ec);
    if (ec) {

        rotate_fail_count_.fetch_add(1, std::memory_order_relaxed);
        return {};
    }

    poisoned_shards_.clear();
    return dst.string();
}

std::unique_ptr<IEventIterator>
LocalFsEventStore::query(const QueryRange& range) {
    std::vector<std::filesystem::path> candidates;

    auto contains = [](const std::vector<std::string>& v, const std::string& x) {
        return v.empty() || std::find(v.begin(), v.end(), x) != v.end();
    };

    std::error_code ec;
    if (!std::filesystem::exists(root_path_, ec)) {
        return std::make_unique<LocalFsIterator>(std::vector<std::filesystem::path>{});
    }

    for (const auto& day_entry : std::filesystem::directory_iterator(root_path_, ec)) {
        if (ec) {
            break;
        }
        if (!day_entry.is_directory()) {
            continue;
        }

        const auto ymd_opt = parse_trading_day(day_entry.path().filename().string());
        if (!ymd_opt) {
            continue;
        }
        const std::uint32_t ymd = *ymd_opt;
        if (ymd < range.start_trading_day || ymd > range.end_trading_day) {
            continue;
        }

        for (const auto& exch_entry : std::filesystem::directory_iterator(day_entry.path(), ec)) {
            if (ec) {
                break;
            }
            if (!exch_entry.is_directory()) {
                continue;
            }
            const std::string exch_name = exch_entry.path().filename().string();
            if (!contains(range.exchanges, exch_name)) {
                continue;
            }

            for (const auto& sym_entry :
                    std::filesystem::directory_iterator(exch_entry.path(), ec)) {
                if (ec) {
                    break;
                }
                if (!sym_entry.is_directory()) {
                    continue;
                }
                const std::string sym_name = sym_entry.path().filename().string();
                if (!contains(range.symbols, sym_name)) {
                    continue;
                }

                const auto tick_path = sym_entry.path() / "tick.fbs";
                if (std::filesystem::exists(tick_path, ec)) {
                    candidates.push_back(tick_path);
                }
            }
        }
    }

    std::sort(candidates.begin(), candidates.end());
    return std::make_unique<LocalFsIterator>(std::move(candidates));
}

bool LocalFsEventStore::flush() {
    bool all_ok = true;
    for (auto& [key, sh] : files_) {
        if (!sh.fp) {
            continue;
        }
        bool ok = (std::fflush(sh.fp) == 0);
        const int fd = ::fileno(sh.fp);
        if (fd >= 0 && ::fsync(fd) != 0) {
            ok = false;
        }
        if (!ok) {

            poison_shard_(key);
        }

        if (std::fclose(sh.fp) != 0) {
            ok = false;
        }
        if (!ok) {
            all_ok = false;
        }
    }
    files_.clear();
    pending_bytes_ = 0;
    return all_ok;
}

}
