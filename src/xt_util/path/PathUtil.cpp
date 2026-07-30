#include "xt_util/path/PathUtil.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace PathUtil {

std::string parent_dir(const std::string& p) {
    if (p.empty()) return "";
    size_t pos = p.find_last_of("/");
    if (pos == std::string::npos) return "";
    return p.substr(0, pos);
}

bool has_path_separator(const std::string& p) {
    return p.find('/') != std::string::npos;
}

bool is_abs(const std::string& p) {
    if (p.empty()) return false;
    return p[0] == '/';
}

std::string current_executable_path() {
    char buf[1024] = {0};
    const ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (n <= 0) return std::string();
    buf[n] = '\0';
    return std::string(buf);
}

std::string join(const std::string& a, const std::string& b) {
    if (a.empty()) return b;
    if (b.empty()) return a;
    char last = a.back();
    if (last == '/') return a + b;
    return a + "/" + b;
}

std::string resolve_relative(const std::string& base_file, const std::string& rel_or_abs) {
    if (rel_or_abs.empty()) return rel_or_abs;
    if (is_abs(rel_or_abs)) return rel_or_abs;
    std::string base = parent_dir(base_file);
    if (base.empty()) return rel_or_abs;
    return join(base, rel_or_abs);
}

void ensure_dir(const std::string& path) {
    if (path.empty()) return;
    std::string dir = path;
    auto pos = dir.find_last_of("/");
    if (pos == std::string::npos) return;
    dir = dir.substr(0, pos);
    if (dir.empty()) return;
    std::string cur;
    for (size_t i = 0; i < dir.size(); ++i) {
        char c = dir[i];
        cur.push_back(c);
        if (c == '/' || i + 1 == dir.size()) {
            if (cur.size() == 0 || cur == "/") continue;
            if (mkdir(cur.c_str(), 0755) != 0) {
                if (errno != EEXIST) return;
            }
        }
    }
}

bool file_exists(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return f.good();
}

std::string strip_csv_ext(const std::string& s) {
    if (s.size() >= 4 && s.substr(s.size() - 4) == ".csv") {
        return s.substr(0, s.size() - 4);
    }
    return s;
}

std::string lower_ext(const std::string& path) {
    const auto sep = path.find_last_of("/\\");
    const auto name_begin = (sep == std::string::npos) ? 0 : sep + 1;
    const auto dot = path.find_last_of('.');
    if (dot == std::string::npos || dot < name_begin || dot == name_begin ||
        dot + 1 >= path.size()) {
        return {};
    }

    std::string ext = path.substr(dot + 1);
    for (char& ch : ext) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return ext;
}

std::string make_epoch_ms_suffixed_path(const std::string& base_path,
                                        const std::string& fallback_leaf) {
    std::string fallback = std::filesystem::path(fallback_leaf).filename().string();
    if (fallback.empty()) fallback = "path";

    std::filesystem::path p(base_path.empty() ? fallback : base_path);
    std::string leaf = p.filename().string();
    if (leaf.empty()) leaf = fallback;

    const int64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    p.replace_filename(leaf + "_" + std::to_string(ms));
    return p.lexically_normal().string();
}

namespace {

void set_path_error(std::string* err, const std::string& message) {
    if (err) *err = message;
}

std::string filesystem_error_message(const char* action, const std::error_code& ec) {
    return std::string(action) + ": " + ec.message();
}

void fsync_parent_dir(const std::string& target);

bool create_directories_with_parent_fsync(const std::filesystem::path& dir,
                                          std::string* err) {
    std::vector<std::filesystem::path> missing;
    std::filesystem::path cur = dir;
    std::error_code ec;
    while (!cur.empty() && !std::filesystem::exists(cur, ec) && !ec) {
        missing.push_back(cur);
        const auto parent = cur.parent_path();
        if (parent == cur) break;
        cur = parent;
    }
    if (ec) {
        set_path_error(err, filesystem_error_message("directory exists check failed", ec));
        return false;
    }
    for (auto it = missing.rbegin(); it != missing.rend(); ++it) {
        ec.clear();
        if (std::filesystem::create_directory(*it, ec)) {
            fsync_parent_dir(it->string());
        } else if (ec) {
            set_path_error(err, filesystem_error_message("create directory failed", ec));
            return false;
        }

    }
    return true;
}

}

bool ensure_directory(const std::string& dir, std::string* err) {
    if (err) err->clear();
    if (dir.empty()) {
        set_path_error(err, "directory path empty");
        return false;
    }

    const std::filesystem::path p(dir);
    std::error_code ec;
    const bool exists = std::filesystem::exists(p, ec);
    if (ec) {
        set_path_error(err, filesystem_error_message("directory exists check failed", ec));
        return false;
    }
    if (!exists) {
        if (!create_directories_with_parent_fsync(p, err)) return false;
    }

    ec.clear();
    const bool is_dir = std::filesystem::is_directory(p, ec);
    if (ec) {
        set_path_error(err, filesystem_error_message("directory type check failed", ec));
        return false;
    }
    if (!is_dir) {
        set_path_error(err, "path exists but is not a directory");
        return false;
    }
    return true;
}

bool ensure_writable_directory(const std::string& dir,
                               std::string* err,
                               const std::string& probe_name) {
    if (!ensure_directory(dir, err)) return false;

    const std::string probe_file = probe_name.empty()
        ? std::string(".xt_path_probe")
        : probe_name;
    if (has_path_separator(probe_file) || probe_file.find('\\') != std::string::npos) {
        set_path_error(err, "probe name must not contain path separator");
        return false;
    }

    const std::filesystem::path probe = std::filesystem::path(dir) / probe_file;
    std::ofstream out(probe.string(), std::ios::out | std::ios::app);
    if (!out) {
        set_path_error(err, "probe file open failed: " + probe.string());
        return false;
    }
    out.close();
    if (!out) {
        set_path_error(err, "probe file close failed: " + probe.string());
        return false;
    }

    std::error_code ec;
    std::filesystem::remove(probe, ec);
    return true;
}

namespace {

bool path_under_root_strict(const std::string& canonical, const std::string& root_canonical) {
    if (root_canonical.empty()) return false;
    if (canonical.size() < root_canonical.size()) return false;
    if (canonical.compare(0, root_canonical.size(), root_canonical) != 0) return false;
    if (canonical.size() == root_canonical.size()) return true;
    const char sep = canonical[root_canonical.size()];
    return sep == '/' || sep == '\\';
}

}

std::optional<std::string> resolve_under_root(const std::string& root,
                                              const std::string& rel_or_abs) {
    if (root.empty() || rel_or_abs.empty()) return std::nullopt;

    char root_buf[PATH_MAX] = {};
    if (!::realpath(root.c_str(), root_buf)) return std::nullopt;
    const std::string root_canonical(root_buf);

    char path_buf[PATH_MAX] = {};
    if (!::realpath(rel_or_abs.c_str(), path_buf)) return std::nullopt;
    const std::string path_canonical(path_buf);

    if (!path_under_root_strict(path_canonical, root_canonical)) return std::nullopt;
    return path_canonical;
}

namespace {

std::atomic<uint64_t> g_atomic_write_counter{0};

std::string make_tmp_path(const std::string& target) {
    const pid_t pid = ::getpid();
    const uint64_t cnt = g_atomic_write_counter.fetch_add(1, std::memory_order_relaxed);

    thread_local std::random_device rd;
    thread_local std::mt19937_64 rng(rd());
    const uint64_t r = rng();
    char suffix[64];
    std::snprintf(suffix, sizeof(suffix), ".tmp.%lld.%llu.%llx",
                  static_cast<long long>(pid),
                  static_cast<unsigned long long>(cnt),
                  static_cast<unsigned long long>(r));
    return target + suffix;
}

void fsync_parent_dir(const std::string& target) {
    if (target.empty()) return;
    std::string parent = parent_dir(target);
    if (parent.empty()) {

        parent = (target.front() == '/') ? "/" : ".";
    }
    int oflags = O_RDONLY;
#ifdef O_DIRECTORY
    oflags |= O_DIRECTORY;
#endif
#ifdef O_CLOEXEC
    oflags |= O_CLOEXEC;
#endif
    const int dfd = ::open(parent.c_str(), oflags);
    if (dfd < 0) return;
    (void)::fsync(dfd);
    ::close(dfd);
}

}

bool write_atomic(const std::string& target, const std::string& content, int mode) {
    if (target.empty()) return false;
    ensure_dir(target);
    const std::string tmp = make_tmp_path(target);
    int oflags = O_WRONLY | O_CREAT | O_TRUNC | O_EXCL;
#ifdef O_CLOEXEC
    oflags |= O_CLOEXEC;
#endif
    const int fd = ::open(tmp.c_str(), oflags, static_cast<mode_t>(mode));
    if (fd < 0) return false;
    const char* data = content.data();
    size_t remaining = content.size();
    while (remaining > 0) {
        const ssize_t n = ::write(fd, data, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            ::unlink(tmp.c_str());
            return false;
        }
        if (n == 0) {
            ::close(fd);
            ::unlink(tmp.c_str());
            return false;
        }
        data += n;
        remaining -= static_cast<size_t>(n);
    }
    if (::fsync(fd) != 0) {
        ::close(fd);
        ::unlink(tmp.c_str());
        return false;
    }
    if (::close(fd) != 0) {
        ::unlink(tmp.c_str());
        return false;
    }
    if (::rename(tmp.c_str(), target.c_str()) != 0) {
        ::unlink(tmp.c_str());
        return false;
    }
    fsync_parent_dir(target);
    return true;
}

AtomicOfstream::AtomicOfstream(const std::string& target, int mode)
    : target_(target),
      tmp_(make_tmp_path(target)),
      mode_(mode) {
    if (target_.empty()) return;
    ensure_dir(target_);
    ofs_.open(tmp_, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!ofs_.is_open()) return;
    (void)::chmod(tmp_.c_str(), static_cast<mode_t>(mode_));
}

AtomicOfstream::~AtomicOfstream() {
    if (!finished_) abort();
}

bool AtomicOfstream::commit() {
    if (finished_) return false;
    finished_ = true;

    if (!ofs_.is_open()) return false;
    ofs_.flush();
    if (!ofs_.good()) {
        ofs_.close();
        ::unlink(tmp_.c_str());
        return false;
    }

    ofs_.close();
    if (!ofs_.good() && !ofs_.eof()) {
        ::unlink(tmp_.c_str());
        return false;
    }

    int oflags = O_RDONLY;
#ifdef O_CLOEXEC
    oflags |= O_CLOEXEC;
#endif
    const int fd = ::open(tmp_.c_str(), oflags);
    if (fd < 0) {
        ::unlink(tmp_.c_str());
        return false;
    }
    if (::fsync(fd) != 0) {
        ::close(fd);
        ::unlink(tmp_.c_str());
        return false;
    }
    ::close(fd);

    if (::rename(tmp_.c_str(), target_.c_str()) != 0) {
        ::unlink(tmp_.c_str());
        return false;
    }

    fsync_parent_dir(target_);
    return true;
}

void AtomicOfstream::abort() {
    if (finished_) return;
    finished_ = true;
    if (ofs_.is_open()) ofs_.close();
    ::unlink(tmp_.c_str());
}

}
