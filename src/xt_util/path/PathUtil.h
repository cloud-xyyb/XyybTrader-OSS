#pragma once
#include <fstream>
#include <optional>
#include <string>

/// @file PathUtil.h
/// @brief 轻量路径工具
///
/// 本模块不负责业务日志和异常抛出。
namespace PathUtil {

std::string parent_dir(const std::string& p);

bool has_path_separator(const std::string& p);

bool is_abs(const std::string& p);

std::string current_executable_path();

std::string join(const std::string& a, const std::string& b);

std::string resolve_relative(const std::string& base_file, const std::string& rel_or_abs);

void ensure_dir(const std::string& path);

bool file_exists(const std::string& path);

std::string strip_csv_ext(const std::string& s);

std::string lower_ext(const std::string& path);

std::string make_epoch_ms_suffixed_path(const std::string& base_path,
                                        const std::string& fallback_leaf);

bool ensure_directory(const std::string& dir, std::string* err = nullptr);

bool ensure_writable_directory(const std::string& dir,
                               std::string* err = nullptr,
                               const std::string& probe_name = ".xt_path_probe");

std::optional<std::string> resolve_under_root(const std::string& root,
                                              const std::string& rel_or_abs);

bool write_atomic(const std::string& target, const std::string& content, int mode = 0644);

/// @brief 流式 RAII 原子写入器。
///
/// 用于大文件或二进制 record stream，避免在内存中聚合完整 buffer。
/// 未显式 `commit()` 或 `abort()` 时析构会自动 `abort()`。
///
/// 用法:
///   {
///       PathUtil::AtomicOfstream out(target);
///       if (!out.is_open()) return false;
///       out.stream() << "header\n";
///       out.stream().write(buf, n);
///       if (!out.commit()) return false;  // 显式 commit, 否则 dtor abort
///   }
class AtomicOfstream {
public:
    AtomicOfstream(const std::string& target, int mode = 0644);
    ~AtomicOfstream();

    AtomicOfstream(const AtomicOfstream&)            = delete;
    AtomicOfstream& operator=(const AtomicOfstream&) = delete;
    AtomicOfstream(AtomicOfstream&&)                 = delete;
    AtomicOfstream& operator=(AtomicOfstream&&)      = delete;

    bool is_open() const { return ofs_.is_open(); }

    std::ofstream& stream() { return ofs_; }

    bool commit();

    void abort();

private:
    std::string   target_;
    std::string   tmp_;
    std::ofstream ofs_;
    int           mode_;
    bool          finished_ = false;
};

}
