#pragma once

#include <string>
#include <vector>

#include "StrategyABI.h"

/// @brief 策略动态库加载器。
///
/// @warning 非线程安全；`load` / `create` / `destroy` / `unload` 必须由调用方串行化。
///          同一 loader 创建的实例必须先 `destroy`，再调用 `unload` 或析构 loader。
/// @note 安全校验包含 plugin_root canonical prefix、sha256 sidecar 和 basename allowlist。
class XtStraLoader {
public:
    /**
     * @brief 加载器安全配置（loader security configuration）。
     *
     * @warning 默认值 fail-closed：未显式配置时要求 sha256 sidecar，且 basename
     *          必须命中 allowlist；缺签名或未列入 allowlist 的 plugin 会被拒绝加载。
     * @note `set_security_config()` 显式注入时优先于环境变量。
     */
    struct SecurityConfig {
        /// 插件根目录，必须是绝对路径；空字符串表示跳过 root prefix 检查。
        std::string plugin_root;

        /// 是否强制 sha256 sidecar 校验。
        /// true：必须存在 `<so>.sha256` 且 hash 与 .so 内容匹配，否则拒绝加载。
        /// false：sidecar 缺失允许加载（仅 dev / regression 显式 opt-out）。
        bool strict_signing = true;

        /// 是否启用 basename allowlist 检查（filename-only check，与 plugin_root 正交）。
        /// false：跳过 allowlist 检查（仅 dev / regression 显式 opt-out）。
        /// true：canonical path 的 basename 必须在 basename_allowlist 内；
        ///        空 list 等同于"拒绝所有 plugin"(fail-closed)。
        bool basename_allowlist_enforce = true;

        /// 允许加载的 .so basename 全集（完整 filename，含 `lib` 前缀与 `.so` 后缀）。
        /// 例：`{"libhft_tick_revert_strategy.so", "libcta_sma_xover_strategy.so"}`。
        /// 仅在 `basename_allowlist_enforce=true` 时生效。
        std::vector<std::string> basename_allowlist;

        /// 是否已显式注入（setter 调用过）。否则 load() 会从环境变量读取默认值。
        bool explicit_set = false;
    };

    XtStraLoader() = default;
    /// @brief 析构时 unload 动态库。@pre 本 loader 创建的实例已全部 destroy。
    ~XtStraLoader();
    // 禁拷贝：双持 handle_ 会导致 double-unload。move 转移句柄与符号，
    // source 置空（其后 unload/析构为 no-op）。
    XtStraLoader(const XtStraLoader&) = delete;
    XtStraLoader& operator=(const XtStraLoader&) = delete;
    XtStraLoader(XtStraLoader&& other) noexcept;
    XtStraLoader& operator=(XtStraLoader&& other) noexcept;

    /** @brief 显式注入安全配置（优先级高于环境变量）。 */
    void set_security_config(SecurityConfig cfg);

    /** @brief 加载动态库并校验 ABI 版本（load and validate ABI）。 */
    bool load(const char* path);
    /** @brief 卸载动态库并清空函数指针（unload library and reset symbols）。 */
    void unload();

    /** @brief 创建策略实例（call create_strategy）。
     *
     *  @return owning pointer；调用方必须用同一个 loader 的 destroy() 回收。
     */
    XtStrategyBase* create();
    /** @brief 销毁策略实例（call destroy_strategy）。
     *
     *  @warning 只能传入本 loader create() 返回且尚未销毁的实例。
     */
    void destroy(XtStrategyBase* stra);

    /** @brief 返回最近一次错误信息（last error message）。 */
    const std::string& last_error() const { return err_; }

private:
    using CreateFn = FnCreateStrategy;
    using DestroyFn = FnDestroyStrategy;
    using AbiFn = FnCtAbiVersion;
    using LayoutFn = FnCtAbiLayout;

    /** @brief 解析最终生效的安全配置（explicit setter 优先；否则读环境变量）。 */
    SecurityConfig resolve_effective_security_config() const;

    /** @brief canonicalize 后做 plugin_root / basename allowlist 检查。 */
    bool validate_path_security(const char* path,
                                const SecurityConfig& sec,
                                std::string& out_canonical);

    /** @brief sha256 sidecar 校验（hard-fail when strict_signing 或 sidecar 存在）。 */
    bool verify_sha256_sidecar(const std::string& canonical_path,
                               bool strict_signing);

    void* handle_ = nullptr;          ///< 动态库句柄（dlopen/LoadLibrary handle）。
    CreateFn create_ = nullptr;       ///< `create_strategy` 函数指针。
    DestroyFn destroy_ = nullptr;     ///< `destroy_strategy` 函数指针。
    AbiFn abi_ = nullptr;             ///< `xt_abi_version` 函数指针。
    LayoutFn order_exec_size_ = nullptr;  ///< `xt_sizeof_CtOrderExecUpdate` 函数指针。
    LayoutFn order_exec_align_ = nullptr; ///< `xt_alignof_CtOrderExecUpdate` 函数指针。
    LayoutFn tick_size_ = nullptr;        ///< `xt_sizeof_XtTick` 函数指针。
    std::string err_;                 ///< 最近一次错误描述（human-readable）。
    SecurityConfig security_;         ///< 安全配置（setter / 环境变量驱动）。
};
