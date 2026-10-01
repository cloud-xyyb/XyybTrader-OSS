#include "XtLiveParamDomain.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>

#include <fcntl.h>
#include <semaphore.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

namespace {

constexpr uint32_t kShmMagic      = 0x4354504d; // "CTPM"
constexpr uint32_t kShmAbi        = 1;
constexpr mode_t   kShmObjectPerm = 0600;

std::string trim_copy(const std::string& in) {
    size_t b = 0;
    while (b < in.size() && std::isspace(static_cast<unsigned char>(in[b]))) ++b;
    size_t e = in.size();
    while (e > b && std::isspace(static_cast<unsigned char>(in[e - 1]))) --e;
    return in.substr(b, e - b);
}

std::string lower_copy(std::string in) {
    std::transform(in.begin(), in.end(), in.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return in;
}

std::string stable_hex(uint64_t v) {
    char buf[32] = {0};
    std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(v));
    return buf;
}

// POSIX shm/sem 名称要求 ≤ NAME_MAX 且不含特殊字符，
// 哈希方案保证固定 16 字符 hex，无需转义
std::string make_name_token(const std::string& domain) {
    const std::hash<std::string> hfn;
    const uint64_t hv = static_cast<uint64_t>(hfn(domain.empty() ? "xyybtrader_live" : domain));
    return stable_hex(hv);
}

std::string make_shm_name(const std::string& domain) {
    return std::string("/xt_param_") + make_name_token(domain);
}

std::string make_sem_name(const std::string& domain) {
    return std::string("/xt_param_ev_") + make_name_token(domain);
}

} // namespace

struct XtLiveParamDomain::ShmEntry {
    uint8_t used = 0;
    char    key[kShmKeyCap]   = {};
    char    value[kShmValueCap] = {};
};

struct XtLiveParamDomain::ShmState {
    uint32_t magic = 0;
    uint32_t abi   = 0;
    std::atomic<uint64_t> version{0};
    std::atomic<uint64_t> notify_seq{0};
    uint32_t entry_count = 0;
    uint32_t reserved    = 0;
    ShmEntry entries[kShmEntryCap];
};

struct XtLiveParamDomain::BackendShm {
    std::string domain;
    std::string shm_name;
    std::string sem_name;
    int         shm_fd     = -1;
    size_t      map_bytes  = sizeof(ShmState);
    ShmState*   state      = nullptr;
    sem_t*      notify_sem = SEM_FAILED;

    // 按 mmap → sem → fd 逆序释放，与初始化顺序严格相反
    ~BackendShm() {
        if (state && state != MAP_FAILED) {
            ::munmap(static_cast<void*>(state), map_bytes);
        }
        if (notify_sem != SEM_FAILED) {
            ::sem_close(notify_sem);
        }
        if (shm_fd >= 0) {
            ::close(shm_fd);
        }
    }
};

XtLiveParamDomain::XtLiveParamDomain() = default;

XtLiveParamDomain::~XtLiveParamDomain() {
    std::lock_guard<std::mutex> guard(mu_);
    close_shm_backend_locked();
}

bool XtLiveParamDomain::lock_shm_file_locked(int op) const {
    if (!shm_ || shm_->shm_fd < 0) return false;
    return ::flock(shm_->shm_fd, op) == 0;
}

bool XtLiveParamDomain::unlock_shm_file_locked() const {
    if (!shm_ || shm_->shm_fd < 0) return false;
    return ::flock(shm_->shm_fd, LOCK_UN) == 0;
}

void XtLiveParamDomain::close_shm_backend_locked() {
    shm_.reset();
}

bool XtLiveParamDomain::init_shm_backend_locked(const BackendConfig& cfg, std::string* err) {
    close_shm_backend_locked();
    // (1) 先退回 memory 安全态，防止后续任意步骤失败时
    //     backend_type_=Shm、shm_=null 的不一致状态
    backend_type_ = BackendType::Memory;
    backend_name_ = "memory";

    auto backend       = std::make_unique<BackendShm>();
    backend->domain    = cfg.domain.empty() ? "xyybtrader_live" : cfg.domain;
    backend->shm_name  = make_shm_name(backend->domain);
    backend->sem_name  = make_sem_name(backend->domain);

    // (2) 打开 / 创建 POSIX 共享内存对象
    backend->shm_fd = ::shm_open(backend->shm_name.c_str(), O_RDWR | O_CREAT, kShmObjectPerm);
    if (backend->shm_fd < 0) {
        if (err) *err = std::string("shm_open failed: ") + ::strerror(errno);
        return false;
    }
    // fchmod 补偿 umask 对 shm_open mode 的截断
    if (::fchmod(backend->shm_fd, kShmObjectPerm) != 0) {
        if (err) *err = std::string("fchmod failed: ") + ::strerror(errno);
        return false;
    }
    if (::ftruncate(backend->shm_fd, static_cast<off_t>(backend->map_bytes)) != 0) {
        if (err) *err = std::string("ftruncate failed: ") + ::strerror(errno);
        return false;
    }

    // (3) 映射到进程地址空间
    void* mapped = ::mmap(nullptr, backend->map_bytes,
                          PROT_READ | PROT_WRITE, MAP_SHARED, backend->shm_fd, 0);
    if (mapped == MAP_FAILED) {
        if (err) *err = std::string("mmap failed: ") + ::strerror(errno);
        return false;
    }
    backend->state = static_cast<ShmState*>(mapped);

    // (4) 打开通知信号量
    backend->notify_sem = ::sem_open(backend->sem_name.c_str(), O_CREAT, kShmObjectPerm, 0);
    if (backend->notify_sem == SEM_FAILED) {
        if (err) *err = std::string("sem_open failed: ") + ::strerror(errno);
        return false;
    }

    // (5) 首次创建时初始化 ShmState（加独占锁防多进程竞态）
    if (::flock(backend->shm_fd, LOCK_EX) != 0) {
        if (err) *err = std::string("flock(LOCK_EX) failed: ") + ::strerror(errno);
        return false;
    }
    ShmState* st = backend->state;
    if (st->magic != kShmMagic || st->abi != kShmAbi) {
        // entries 仅含 POD（uint8_t + char[]），memset 安全；
        // 原子字段通过 store 显式初始化，避免对含 std::atomic 的结构体整体 memset 产生 UB
        std::memset(st->entries, 0, sizeof(st->entries));
        st->magic       = kShmMagic;
        st->abi         = kShmAbi;
        st->entry_count = 0;
        st->reserved    = 0;
        st->version.store(0, std::memory_order_release);
        st->notify_seq.store(0, std::memory_order_release);
    }
    (void)::flock(backend->shm_fd, LOCK_UN);

    // (6) 所有资源就绪，提交状态
    // 必须先设置 backend_type_：refresh_from_shm_locked 开头检查 backend_type_ == Shm
    shm_ = std::move(backend);
    backend_name_ = "shm";
    backend_type_ = BackendType::Shm;
    if (!refresh_from_shm_locked(/*force=*/true)) {
        values_.clear();
        version_.store(0, std::memory_order_relaxed);
    }
    if (err) err->clear();
    return true;
}

bool XtLiveParamDomain::refresh_from_shm_locked(bool force) const {
    if (backend_type_ != BackendType::Shm || !shm_ || !shm_->state) return false;
    const uint64_t cur = version_.load(std::memory_order_relaxed);
    // 投机版本检查：在取 LOCK_SH 前先对比 shm 原子版本号（acquire 配对写端 release）。
    // 即便读到旧值也只是多取一次 flock，不影响正确性，仅损失 < 1µs
    const uint64_t shared = shm_->state->version.load(std::memory_order_acquire);
    if (!force && shared == cur) return false;
    if (!lock_shm_file_locked(LOCK_SH)) return false;

    std::unordered_map<std::string, std::string> snap;
    snap.reserve(kShmEntryCap);
    ShmState* st = shm_->state;
    if (st->magic == kShmMagic && st->abi == kShmAbi) {
        for (size_t i = 0; i < kShmEntryCap; ++i) {
            const ShmEntry& e = st->entries[i];
            if (!e.used) continue;
            if (e.key[0] == '\0') continue; // 防御 used=1 但 key 未写入的脏槽位
            snap[e.key] = e.value;
        }
    }
    const uint64_t sync_ver = st->version.load(std::memory_order_acquire);
    (void)unlock_shm_file_locked();

    values_ = std::move(snap);
    version_.store(sync_ver, std::memory_order_release);
    return true;
}

bool XtLiveParamDomain::apply_set_shm_locked(const std::string& key,
                                             const std::string& value,
                                             bool* out_changed,
                                             std::string* err) {
    if (out_changed) *out_changed = false;

    if (!shm_ || !shm_->state) {
        if (err) *err = "shm backend not initialized";
        return false;
    }
    if (key.empty()) {
        if (err) *err = "param key is empty";
        return false;
    }
    // >= 而非 >：保留 NUL 终止符空间
    if (key.size() >= kShmKeyCap) {
        if (err) *err = "param key too long";
        return false;
    }
    if (value.size() >= kShmValueCap) {
        if (err) *err = "param value too long";
        return false;
    }

    if (!lock_shm_file_locked(LOCK_EX)) {
        if (err) *err = std::string("flock(LOCK_EX) failed: ") + ::strerror(errno);
        return false;
    }

    // (1) 单次扫描：同时查找已有条目和首个空闲槽位
    ShmState* st        = shm_->state;
    ShmEntry* existing  = nullptr;
    ShmEntry* free_slot = nullptr;
    for (size_t i = 0; i < kShmEntryCap; ++i) {
        ShmEntry& e = st->entries[i];
        if (e.used) {
            if (std::strncmp(e.key, key.c_str(), kShmKeyCap) == 0) {
                existing = &e;
                break;
            }
        } else if (free_slot == nullptr) {
            free_slot = &e;
        }
    }

    // (2) 写入已有条目 / 插入新条目 / 容量满报错
    bool changed = false;
    if (existing) {
        if (std::strncmp(existing->value, value.c_str(), kShmValueCap) != 0) {
            std::memcpy(existing->value, value.c_str(), value.size() + 1);
            changed = true;
        }
    } else if (free_slot) {
        free_slot->used = 1;
        std::memcpy(free_slot->key,   key.c_str(),   key.size()   + 1);
        std::memcpy(free_slot->value, value.c_str(), value.size() + 1);
        ++st->entry_count;
        changed = true;
    } else {
        (void)unlock_shm_file_locked();
        if (err) *err = "params shm capacity reached";
        return false;
    }

    // (3) 递增版本号 + 通知序列号
    if (changed) {
        st->version.store(st->version.load(std::memory_order_relaxed) + 1,
                          std::memory_order_release);
        st->notify_seq.fetch_add(1, std::memory_order_relaxed);
    }
    (void)unlock_shm_file_locked();

    if (changed) {
        (void)refresh_from_shm_locked(/*force=*/true);
    }
    notify_param_changed_locked(changed);
    if (out_changed) *out_changed = changed;
    if (err) err->clear();
    return true;
}

bool XtLiveParamDomain::apply_erase_shm_locked(const std::string& key,
                                               bool* out_changed,
                                               std::string* err) {
    if (out_changed) *out_changed = false;
    if (!shm_ || !shm_->state) {
        if (err) *err = "shm backend not initialized";
        return false;
    }
    if (key.empty()) {
        if (err) *err = "param key is empty";
        return false;
    }
    if (!lock_shm_file_locked(LOCK_EX)) {
        if (err) *err = std::string("flock(LOCK_EX) failed: ") + ::strerror(errno);
        return false;
    }

    ShmState* st   = shm_->state;
    bool changed   = false;
    for (size_t i = 0; i < kShmEntryCap; ++i) {
        ShmEntry& e = st->entries[i];
        if (!e.used || std::strncmp(e.key, key.c_str(), kShmKeyCap) != 0) continue;
        e.used     = 0;
        e.key[0]   = '\0';
        e.value[0] = '\0';
        // 防御性下溢保护
        if (st->entry_count > 0) --st->entry_count;
        changed = true;
        break;
    }
    if (changed) {
        st->version.store(st->version.load(std::memory_order_relaxed) + 1,
                          std::memory_order_release);
        st->notify_seq.fetch_add(1, std::memory_order_relaxed);
    }
    (void)unlock_shm_file_locked();

    if (changed) {
        (void)refresh_from_shm_locked(/*force=*/true);
    }
    notify_param_changed_locked(changed);
    if (out_changed) *out_changed = changed;
    if (err) err->clear();
    return true;
}

// 批量写入：所有条目共享一次 flock，处理完毕后统一递增一次版本号。
// 容量满时静默跳过当前条目（partial write 语义），不回滚已写入条目
bool XtLiveParamDomain::apply_bulk_set_shm_locked(const std::vector<std::pair<std::string, std::string>>& flat) {
    if (!shm_ || !shm_->state || flat.empty()) return false;
    if (!lock_shm_file_locked(LOCK_EX)) return false;

    ShmState* st = shm_->state;
    bool changed = false;

    for (const auto& [key, value] : flat) {
        if (key.empty() || key.size() >= kShmKeyCap || value.size() >= kShmValueCap) continue;

        ShmEntry* existing  = nullptr;
        ShmEntry* free_slot = nullptr;
        for (size_t i = 0; i < kShmEntryCap; ++i) {
            ShmEntry& e = st->entries[i];
            if (e.used) {
                if (std::strncmp(e.key, key.c_str(), kShmKeyCap) == 0) {
                    existing = &e;
                    break;
                }
            } else if (free_slot == nullptr) {
                free_slot = &e;
            }
        }

        if (existing) {
            if (std::strncmp(existing->value, value.c_str(), kShmValueCap) == 0) continue;
            std::memcpy(existing->value, value.c_str(), value.size() + 1);
            changed = true;
        } else if (free_slot) {
            free_slot->used = 1;
            std::memcpy(free_slot->key,   key.c_str(),   key.size()   + 1);
            std::memcpy(free_slot->value, value.c_str(), value.size() + 1);
            ++st->entry_count;
            changed = true;
        }
        // 容量满时静默跳过
    }

    if (changed) {
        st->version.store(st->version.load(std::memory_order_relaxed) + 1,
                          std::memory_order_release);
        st->notify_seq.fetch_add(1, std::memory_order_relaxed);
    }
    (void)unlock_shm_file_locked();

    if (changed) {
        (void)refresh_from_shm_locked(/*force=*/true);
    }
    notify_param_changed_locked(changed);
    return changed;
}

void XtLiveParamDomain::notify_param_changed_locked(bool changed) {
    if (!changed) return;
    // cv_.notify_all() 必须无条件调用：wait_for_version_change 可能在 cv 上睡眠，
    // 而 backend 此后被切换为 Shm，sem_post 无法唤醒仍在 cv 上的线程
    cv_.notify_all();
    if (backend_type_ == BackendType::Shm && shm_ && shm_->notify_sem != SEM_FAILED) {
        (void)::sem_post(shm_->notify_sem);
    }
}

bool XtLiveParamDomain::configure_backend(const BackendConfig& cfg, std::string* err) {
    const std::string type = lower_copy(trim_copy(cfg.type));
    std::lock_guard<std::mutex> guard(mu_);
    if (type.empty() || type == "memory" || type == "inmemory") {
        close_shm_backend_locked();
        backend_name_ = "memory";
        backend_type_ = BackendType::Memory;
        if (err) err->clear();
        return true;
    }
    if (type == "shm" || type == "shared_memory") {
        if (!init_shm_backend_locked(cfg, err)) {
            return false;
        }
        return true;
    }
    if (err) {
        *err = "unsupported params backend: " + cfg.type;
    }
    return false;
}

std::string XtLiveParamDomain::backend_name() const {
    std::lock_guard<std::mutex> guard(mu_);
    return backend_name_;
}

bool XtLiveParamDomain::set(const std::string& key, const std::string& value) {
    bool changed = false;
    if (!set_checked(key, value, &changed, nullptr)) return false;
    return changed;
}

bool XtLiveParamDomain::set_checked(const std::string& key,
                                  const std::string& value,
                                  bool* out_changed,
                                  std::string* err) {
    if (out_changed) *out_changed = false;
    if (key.empty()) {
        if (err) *err = "param key is empty";
        return false;
    }
    std::lock_guard<std::mutex> guard(mu_);
    if (backend_type_ == BackendType::Shm) {
        return apply_set_shm_locked(key, value, out_changed, err);
    }
    // memory 后端：值相同时 no-op
    auto it = values_.find(key);
    if (it != values_.end() && it->second == value) {
        if (err) err->clear();
        return true;
    }
    values_[key] = value;
    version_.fetch_add(1, std::memory_order_relaxed);
    notify_param_changed_locked(true);
    if (out_changed) *out_changed = true;
    if (err) err->clear();
    return true;
}

bool XtLiveParamDomain::erase(const std::string& key) {
    if (key.empty()) return false;
    std::lock_guard<std::mutex> guard(mu_);
    if (backend_type_ == BackendType::Shm) {
        bool changed = false;
        if (!apply_erase_shm_locked(key, &changed, nullptr)) return false;
        return changed;
    }
    const size_t removed = values_.erase(key);
    if (removed > 0) {
        version_.fetch_add(1, std::memory_order_relaxed);
        notify_param_changed_locked(true);
        return true;
    }
    return false;
}

bool XtLiveParamDomain::get(const std::string& key, std::string& out) const {
    std::lock_guard<std::mutex> guard(mu_);
    if (backend_type_ == BackendType::Shm) {
        (void)refresh_from_shm_locked(false);
    }
    auto it = values_.find(key);
    if (it == values_.end()) return false;
    out = it->second;
    return true;
}

std::string XtLiveParamDomain::get_or(const std::string& key, const std::string& fallback) const {
    std::lock_guard<std::mutex> guard(mu_);
    if (backend_type_ == BackendType::Shm) {
        (void)refresh_from_shm_locked(false);
    }
    auto it = values_.find(key);
    return it != values_.end() ? it->second : fallback;
}

bool XtLiveParamDomain::parse_bool(const std::string& v, bool& out) {
    std::string s = trim_copy(v);
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (s == "1" || s == "true" || s == "yes" || s == "on") {
        out = true;
        return true;
    }
    if (s == "0" || s == "false" || s == "no" || s == "off") {
        out = false;
        return true;
    }
    return false;
}

bool XtLiveParamDomain::get_bool_or(const std::string& key, bool fallback) const {
    std::string v;
    if (!get(key, v)) return fallback;
    bool out = false;
    return parse_bool(v, out) ? out : fallback;
}

int64_t XtLiveParamDomain::get_int_or(const std::string& key, int64_t fallback) const {
    std::string v;
    if (!get(key, v)) return fallback;
    char* end = nullptr;
    const long long parsed = std::strtoll(v.c_str(), &end, 10);
    // 拒绝空字符串和含余项（如 "123abc"）的输入
    if (end == v.c_str() || (end && *end != '\0')) return fallback;
    return static_cast<int64_t>(parsed);
}

double XtLiveParamDomain::get_double_or(const std::string& key, double fallback) const {
    std::string v;
    if (!get(key, v)) return fallback;
    char* end = nullptr;
    const double parsed = std::strtod(v.c_str(), &end);
    if (end == v.c_str() || (end && *end != '\0')) return fallback;
    return parsed;
}

uint64_t XtLiveParamDomain::version() const {
    std::lock_guard<std::mutex> guard(mu_);
    if (backend_type_ == BackendType::Shm) {
        (void)refresh_from_shm_locked(false);
    }
    return version_.load(std::memory_order_relaxed);
}

size_t XtLiveParamDomain::size() const {
    std::lock_guard<std::mutex> guard(mu_);
    if (backend_type_ == BackendType::Shm) {
        (void)refresh_from_shm_locked(false);
    }
    return values_.size();
}

XtLiveParamDomain::Snapshot XtLiveParamDomain::snapshot() const {
    Snapshot snap;
    std::lock_guard<std::mutex> guard(mu_);
    if (backend_type_ == BackendType::Shm) {
        (void)refresh_from_shm_locked(false);
    }
    snap.version = version_.load(std::memory_order_relaxed);
    snap.values = values_;
    return snap;
}

// timeout_ms == 0 表示无限等待。EINTR 被透明重试。
// macOS 不支持 sem_timedwait，退化为 sem_trywait + 1ms 睡眠轮询
bool XtLiveParamDomain::sem_wait_timeout(sem_t* sem, uint32_t timeout_ms) {
    if (!sem) return false;
    if (timeout_ms == 0) {
        while (true) {
            if (::sem_wait(sem) == 0) return true;
            if (errno == EINTR) continue;
            return false;
        }
    }

#if defined(__APPLE__)
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true) {
        if (::sem_trywait(sem) == 0) return true;
        if (errno == EINTR) continue;
        if (errno != EAGAIN) return false; // 非"暂不可用"的真错误
        if (std::chrono::steady_clock::now() >= deadline) {
            errno = ETIMEDOUT;
            return false;
        }
        ::usleep(1000);
    }
#else
    // Linux：使用 CLOCK_REALTIME 绝对截止时间
    timespec ts{};
    if (::clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return false;
    }
    ts.tv_sec  += static_cast<time_t>(timeout_ms / 1000);
    ts.tv_nsec += static_cast<long>((timeout_ms % 1000) * 1000000UL);
    // 纳秒进位
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec  += 1;
        ts.tv_nsec -= 1000000000L;
    }

    while (true) {
        if (::sem_timedwait(sem, &ts) == 0) return true;
        if (errno == EINTR) continue;
        return false;
    }
#endif
}

bool XtLiveParamDomain::wait_for_version_change(uint64_t seen_version,
                                                uint32_t timeout_ms,
                                                uint64_t* out_version) {
    std::unique_lock<std::mutex> lock(mu_);

    if (backend_type_ == BackendType::Memory) {
        auto reached = [&]() {
            return version_.load(std::memory_order_relaxed) > seen_version;
        };
        if (!reached()) {
            if (timeout_ms == 0) {
                cv_.wait(lock, reached);
            } else {
                cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), reached);
            }
        }
        const uint64_t cur = version_.load(std::memory_order_relaxed);
        if (out_version) *out_version = cur;
        return cur > seen_version;
    }

    // shm 路径：先检查是否已有新版本，避免不必要的 sem_wait
    (void)refresh_from_shm_locked(false);
    const uint64_t cur = version_.load(std::memory_order_relaxed);
    if (cur > seen_version) {
        if (out_version) *out_version = cur;
        return true;
    }
    if (!shm_ || shm_->notify_sem == SEM_FAILED) {
        if (out_version) *out_version = cur;
        return false;
    }

    // 在释放锁前持有 shared_ptr 引用，防止 configure_backend 在 sem_wait 期间
    // 析构 BackendShm 导致 notify_sem 变悬空指针（UAF）
    std::shared_ptr<BackendShm> shm_ref = shm_;
    lock.unlock();

    (void)sem_wait_timeout(shm_ref->notify_sem, timeout_ms);
    shm_ref.reset(); // 先释放引用再重持锁，避免析构在锁内发生

    lock.lock();
    (void)refresh_from_shm_locked(false);
    const uint64_t final_ver = version_.load(std::memory_order_relaxed);
    if (out_version) *out_version = final_ver;
    return final_ver > seen_version;
}
