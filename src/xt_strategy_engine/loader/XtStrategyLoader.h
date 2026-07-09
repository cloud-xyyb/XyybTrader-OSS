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

        std::string plugin_root;

        bool strict_signing = true;

        bool basename_allowlist_enforce = true;

        std::vector<std::string> basename_allowlist;

        bool explicit_set = false;
    };

    XtStraLoader() = default;

    ~XtStraLoader();

    XtStraLoader(const XtStraLoader&) = delete;
    XtStraLoader& operator=(const XtStraLoader&) = delete;
    XtStraLoader(XtStraLoader&& other) noexcept;
    XtStraLoader& operator=(XtStraLoader&& other) noexcept;

    void set_security_config(SecurityConfig cfg);

    bool load(const char* path);

    void unload();

    XtStrategyBase* create();

    void destroy(XtStrategyBase* stra);

    const std::string& last_error() const { return err_; }

private:
    using CreateFn = FnCreateStrategy;
    using DestroyFn = FnDestroyStrategy;
    using AbiFn = FnCtAbiVersion;
    using LayoutFn = FnCtAbiLayout;

    SecurityConfig resolve_effective_security_config() const;

    bool validate_path_security(const char* path,
                                const SecurityConfig& sec,
                                std::string& out_canonical);

    bool verify_sha256_sidecar(const std::string& canonical_path,
                               bool strict_signing);

    void* handle_ = nullptr;
    CreateFn create_ = nullptr;
    DestroyFn destroy_ = nullptr;
    AbiFn abi_ = nullptr;
    LayoutFn order_exec_size_ = nullptr;
    LayoutFn order_exec_align_ = nullptr;
    std::string err_;
    SecurityConfig security_;
};
