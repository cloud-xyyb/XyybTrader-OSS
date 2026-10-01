#pragma once
#include <fstream>
#include <optional>
#include <string>

/// @file PathUtil.h
/// @brief 轻量路径工具
///
/// 本模块不负责业务日志和异常抛出。
namespace PathUtil {

/// @brief 返回路径中的父目录；无目录分隔符时返回空串。
std::string parent_dir(const std::string& p);

/// @brief 判断路径中是否包含 POSIX 路径分隔符 `/`。
bool has_path_separator(const std::string& p);

/// @brief 判断路径是否为 POSIX 绝对路径。
bool is_abs(const std::string& p);

/// @brief 返回当前进程可执行文件路径；不可获取时返回空串。
std::string current_executable_path();

/// @brief 用单个 `/` 拼接路径片段；任一侧为空时返回另一侧。
std::string join(const std::string& a, const std::string& b);

/// @brief 将相对路径按 `base_file` 所在目录解析；绝对路径原样返回。
std::string resolve_relative(const std::string& base_file, const std::string& rel_or_abs);

/// @brief 确保文件路径的父目录存在；创建失败时静默返回。
void ensure_dir(const std::string& path);

/// @brief 判断文件是否可按二进制输入流打开。
bool file_exists(const std::string& path);

/// @brief 若字符串以 `.csv` 结尾则去掉扩展名；大小写敏感。
std::string strip_csv_ext(const std::string& s);

/// @brief 返回文件名扩展名的小写形式，不含前导点；无扩展名时返回空字符串。
std::string lower_ext(const std::string& path);

/// @brief 生成 `<base_path_leaf>_<epoch_ms>` 同级路径；只计算路径，不创建目录。
///
/// `base_path` 为空或以路径分隔符结尾时，使用 `fallback_leaf` 作为文件名部分。
/// `fallback_leaf` 必须表达文件名语义；为空或无法得到文件名时退化为 `"path"`。
/// 本函数只追加当前 `system_clock` epoch 毫秒后缀，不保证并发同毫秒调用绝对唯一。
std::string make_epoch_ms_suffixed_path(const std::string& base_path,
                                        const std::string& fallback_leaf);

/// @brief 确保 `dir` 本身存在且是目录。
///
/// 空路径、创建失败、已存在但不是目录均返回 false。`err` 可为空；非空时写入
/// 适合日志输出的失败原因。
bool ensure_directory(const std::string& dir, std::string* err = nullptr);

/// @brief 确保 `dir` 存在且当前进程可在其中创建文件。
///
/// 先调用 `ensure_directory()`，再用 `probe_name` 创建并关闭一个探针文件；探针
/// 删除失败只留下残留文件，不改变“目录可写”判定。`probe_name` 必须是不含路径
/// 分隔符的文件名。
bool ensure_writable_directory(const std::string& dir,
                               std::string* err = nullptr,
                               const std::string& probe_name = ".xt_path_probe");

/// @brief 验证并返回位于 `root` 之下的 canonical 绝对路径。
///
/// `root` 和 `rel_or_abs` 都通过 `realpath()` 解析，符号链接与 `..` 会被消解。
/// 返回有值时，结果物理路径等于 `root` 或严格位于 `root` 子树内；IO 失败、路径
/// 不存在或越界时返回 `std::nullopt`。
///
/// @param root         绝对根目录 (e.g. "/opt/xyybtrader/var/state");必须已存在。
/// @param rel_or_abs   要校验的路径 (绝对或相对);相对路径以 CWD 解析。
std::optional<std::string> resolve_under_root(const std::string& root,
                                              const std::string& rel_or_abs);

/// @brief 原子写文件：tmp + fsync(fd) + close + rename + fsync(parent)。
///
/// POSIX rename 保证读者只看到旧完整文件或新完整文件。失败时保留原 target，
/// 并尽力删除临时文件。函数会确保 target 父目录存在。
///
/// @param target  目标文件路径 (必须)
/// @param content 完整文件内容 (write 一次性写入)
/// @param mode    文件权限 (默认 0644)
/// @return        true=成功, false=失败 (已尝试清理 tmp)
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

    /// @brief 暴露底层 ofstream；调用方负责检查 stream 状态。
    std::ofstream& stream() { return ofs_; }

    /// @brief 显式提交；失败返回 false 并尽力删除 tmp。
    bool commit();

    /// @brief 显式放弃写入，关闭并删除 tmp。
    void abort();

private:
    std::string   target_;
    std::string   tmp_;
    std::ofstream ofs_;
    int           mode_;
    bool          finished_ = false;  ///< commit 或 abort 调用过则置 true
};

}  // namespace PathUtil
