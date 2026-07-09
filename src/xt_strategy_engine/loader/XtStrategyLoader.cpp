#include "XtStrategyLoader.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#include <limits.h>
#include <stdlib.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#endif

namespace {

constexpr const char* kEnvPluginRoot = "CLOUDTRADER_PLUGIN_ROOT";
constexpr const char* kEnvStrictSigning = "CLOUDTRADER_PLUGIN_STRICT_SIGNING";
constexpr const char* kEnvAllowlistEnforce = "CLOUDTRADER_PLUGIN_ALLOWLIST_ENFORCE";
constexpr const char* kEnvAllowlist = "CLOUDTRADER_PLUGIN_ALLOWLIST";

bool parse_env_bool(const char* value, bool default_value) {
    if (!value || !*value) return default_value;
    std::string s(value);
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    if (s == "0" || s == "false" || s == "no" || s == "off") return false;
    return true;
}

std::vector<std::string> split_csv_allowlist(const char* csv) {
    std::vector<std::string> out;
    if (!csv || !*csv) return out;
    std::string_view s(csv);
    std::size_t start = 0;
    while (start <= s.size()) {
        std::size_t comma = s.find(',', start);
        if (comma == std::string_view::npos) comma = s.size();
        std::size_t lo = start;
        while (lo < comma && (s[lo] == ' ' || s[lo] == '\t')) ++lo;
        std::size_t hi = comma;
        while (hi > lo && (s[hi - 1] == ' ' || s[hi - 1] == '\t')) --hi;
        if (hi > lo) {
            out.emplace_back(s.substr(lo, hi - lo));
        }
        start = comma + 1;
    }
    return out;
}

bool path_under_root(const std::string& canonical, const std::string& root) {
    if (root.empty()) return true;
    std::string root_norm = root;
    while (!root_norm.empty() &&
           (root_norm.back() == '/' || root_norm.back() == '\\')) {
        root_norm.pop_back();
    }
    if (canonical.size() < root_norm.size()) return false;

    if (canonical.compare(0, root_norm.size(), root_norm) != 0) return false;
    if (canonical.size() == root_norm.size()) return true;
    const char sep = canonical[root_norm.size()];
    return sep == '/' || sep == '\\';
}

int hex_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
}

bool parse_sha256_sidecar(const std::string& content,
                          std::array<unsigned char, 32>& out_digest) {
    std::size_t i = 0;
    while (i < content.size() && (content[i] == ' ' || content[i] == '\t')) ++i;
    if (content.size() - i < 64) return false;
    for (std::size_t b = 0; b < 32; ++b) {
        const int hi = hex_value(content[i + b * 2]);
        const int lo = hex_value(content[i + b * 2 + 1]);
        if (hi < 0 || lo < 0) return false;
        out_digest[b] = static_cast<unsigned char>((hi << 4) | lo);
    }
    return true;
}

#ifndef _WIN32

bool compute_sha256_file(const std::string& path,
                         std::array<unsigned char, 32>& out_digest) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return false;

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) return false;
    bool ok = (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) == 1);
    if (ok) {
        std::array<char, 64 * 1024> buf{};
        while (ok && f.good()) {
            f.read(buf.data(), buf.size());
            const std::streamsize n = f.gcount();
            if (n > 0) {
                ok = (EVP_DigestUpdate(ctx, buf.data(),
                                       static_cast<std::size_t>(n)) == 1);
            }
            if (f.eof()) break;
        }
        if (f.bad()) ok = false;
    }
    if (ok) {
        unsigned int md_len = 0;
        ok = (EVP_DigestFinal_ex(ctx, out_digest.data(), &md_len) == 1) &&
             (md_len == 32);
    }
    EVP_MD_CTX_free(ctx);
    return ok;
}
#endif

}

void XtStraLoader::set_security_config(SecurityConfig cfg) {
    cfg.explicit_set = true;
    security_ = std::move(cfg);
}

XtStraLoader::SecurityConfig
XtStraLoader::resolve_effective_security_config() const {
    if (security_.explicit_set) return security_;
    SecurityConfig out;
    if (const char* root = std::getenv(kEnvPluginRoot); root && *root) {
        out.plugin_root = root;
    }
    out.strict_signing =
        parse_env_bool(std::getenv(kEnvStrictSigning), out.strict_signing);
    out.basename_allowlist_enforce =
        parse_env_bool(std::getenv(kEnvAllowlistEnforce), out.basename_allowlist_enforce);
    if (const char* csv = std::getenv(kEnvAllowlist); csv && *csv) {
        out.basename_allowlist = split_csv_allowlist(csv);
    }
    return out;
}

bool XtStraLoader::validate_path_security(const char* path,
                                          const SecurityConfig& sec,
                                          std::string& out_canonical) {

#ifdef _WIN32
    char canon_buf[MAX_PATH] = {};
    const DWORD n = GetFullPathNameA(path, MAX_PATH, canon_buf, nullptr);
    if (n == 0 || n >= MAX_PATH) {
        err_ = std::string("plugin path canonicalization failed: ") + path;
        return false;
    }
    out_canonical.assign(canon_buf, n);
#else
    char canon_buf[PATH_MAX] = {};
    if (!realpath(path, canon_buf)) {
        err_ = std::string("plugin path realpath() failed: ") + path +
               " (" + std::strerror(errno) + ")";
        return false;
    }
    out_canonical = canon_buf;
#endif

    if (!sec.plugin_root.empty()) {
        std::string root_canonical;
#ifdef _WIN32
        char root_buf[MAX_PATH] = {};
        const DWORD nr = GetFullPathNameA(sec.plugin_root.c_str(),
                                          MAX_PATH, root_buf, nullptr);
        if (nr == 0 || nr >= MAX_PATH) {
            err_ = "plugin_root canonicalization failed: " + sec.plugin_root;
            return false;
        }
        root_canonical.assign(root_buf, nr);
#else
        char root_buf[PATH_MAX] = {};
        if (!realpath(sec.plugin_root.c_str(), root_buf)) {
            err_ = "plugin_root realpath() failed: " + sec.plugin_root +
                   " (" + std::strerror(errno) + ")";
            return false;
        }
        root_canonical = root_buf;
#endif
        if (!path_under_root(out_canonical, root_canonical)) {
            err_ = "plugin path '" + out_canonical +
                   "' not under plugin_root '" + root_canonical + "'";
            return false;
        }
    }

    if (sec.basename_allowlist_enforce) {
        const std::string basename =
            std::filesystem::path(out_canonical).filename().string();
        const auto& wl = sec.basename_allowlist;
        if (std::find(wl.begin(), wl.end(), basename) == wl.end()) {
            err_ = "plugin basename '" + basename +
                   "' not in allowlist (size=" + std::to_string(wl.size()) +
                   "; set CLOUDTRADER_PLUGIN_ALLOWLIST env or call set_security_config)";
            return false;
        }
    }
    return true;
}

bool XtStraLoader::verify_sha256_sidecar(const std::string& canonical_path,
                                         bool strict_signing) {
#ifdef _WIN32

    if (strict_signing) {
        err_ = "strict signing requested but sha256 verification is "
               "POSIX-only in this build: " + canonical_path;
        return false;
    }
    return true;
#else
    const std::string sidecar_path = canonical_path + ".sha256";
    std::ifstream sidecar(sidecar_path);
    if (!sidecar.is_open()) {
        if (strict_signing) {
            err_ = "plugin sha256 sidecar missing: " + sidecar_path;
            return false;
        }
        return true;
    }

    std::string content((std::istreambuf_iterator<char>(sidecar)),
                        std::istreambuf_iterator<char>());
    sidecar.close();

    std::array<unsigned char, 32> expected{};
    if (!parse_sha256_sidecar(content, expected)) {
        err_ = "plugin sha256 sidecar malformed: " + sidecar_path;
        return false;
    }

    std::array<unsigned char, 32> actual{};
    if (!compute_sha256_file(canonical_path, actual)) {
        err_ = "plugin sha256 compute failed: " + canonical_path;
        return false;
    }

    if (CRYPTO_memcmp(expected.data(), actual.data(), expected.size()) != 0) {
        err_ = "plugin sha256 mismatch: " + canonical_path +
               " (manifest=" + sidecar_path + ")";
        return false;
    }
    return true;
#endif
}

bool XtStraLoader::load(const char* path) {
    unload();
    err_.clear();
    if (!path || path[0] == '\0') {
        err_ = "empty path";
        return false;
    }

    const SecurityConfig sec = resolve_effective_security_config();
    std::string canonical_path;
    if (!validate_path_security(path, sec, canonical_path)) {
        return false;
    }
    if (!verify_sha256_sidecar(canonical_path, sec.strict_signing)) {
        return false;
    }

    const char* const dlopen_path = canonical_path.c_str();

#ifdef _WIN32
    HMODULE h = LoadLibraryA(dlopen_path);
    if (!h) {
        err_ = "LoadLibrary failed: " + std::to_string(GetLastError());
        return false;
    }
    handle_ = static_cast<void*>(h);
    create_ = reinterpret_cast<CreateFn>(GetProcAddress(h, "create_strategy"));
    destroy_ = reinterpret_cast<DestroyFn>(GetProcAddress(h, "destroy_strategy"));
    abi_ = reinterpret_cast<AbiFn>(GetProcAddress(h, "xt_abi_version"));
    order_exec_size_ = reinterpret_cast<LayoutFn>(GetProcAddress(h, "xt_sizeof_CtOrderExecUpdate"));
    order_exec_align_ = reinterpret_cast<LayoutFn>(GetProcAddress(h, "xt_alignof_CtOrderExecUpdate"));
#else

    void* h = dlopen(dlopen_path, RTLD_NOW | RTLD_LOCAL);
    if (!h) {
        err_ = dlerror();
        return false;
    }
    handle_ = h;
    create_ = reinterpret_cast<CreateFn>(dlsym(h, "create_strategy"));
    destroy_ = reinterpret_cast<DestroyFn>(dlsym(h, "destroy_strategy"));
    abi_ = reinterpret_cast<AbiFn>(dlsym(h, "xt_abi_version"));
    order_exec_size_ = reinterpret_cast<LayoutFn>(dlsym(h, "xt_sizeof_CtOrderExecUpdate"));
    order_exec_align_ = reinterpret_cast<LayoutFn>(dlsym(h, "xt_alignof_CtOrderExecUpdate"));
#endif

    if (!create_ || !destroy_) {
        err_ = "missing create_strategy/destroy_strategy";
        unload();
        return false;
    }
    if (!abi_) {
        err_ = "missing xt_abi_version (plugin ABI mismatch)";
        unload();
        return false;
    }
    if (!order_exec_size_ || !order_exec_align_) {
        err_ = "missing XtOrderExecUpdate layout ABI symbols; rebuild strategy plugin";
        unload();
        return false;
    }

    const uint32_t ver = abi_();
    if (ver != XT_STRATEGY_ABI_VERSION) {
        err_ = "strategy ABI mismatch: plugin=" + std::to_string(ver) +
               " host=" + std::to_string(XT_STRATEGY_ABI_VERSION);
        unload();
        return false;
    }
    const uint64_t plugin_order_exec_size = order_exec_size_();
    const uint64_t plugin_order_exec_align = order_exec_align_();
    if (plugin_order_exec_size != XT_ABI_SIZEOF_CT_ORDER_EXEC_UPDATE ||
        plugin_order_exec_align != XT_ABI_ALIGNOF_CT_ORDER_EXEC_UPDATE) {
        err_ = "strategy ABI mismatch: XtOrderExecUpdate plugin=size " +
               std::to_string(plugin_order_exec_size) +
               " align " + std::to_string(plugin_order_exec_align) +
               " host=size " + std::to_string(XT_ABI_SIZEOF_CT_ORDER_EXEC_UPDATE) +
               " align " + std::to_string(XT_ABI_ALIGNOF_CT_ORDER_EXEC_UPDATE);
        unload();
        return false;
    }

    return true;
}

void XtStraLoader::unload() {
#ifdef _WIN32
    if (handle_) {
        FreeLibrary(static_cast<HMODULE>(handle_));
    }
#else
    if (handle_) {
        dlclose(handle_);
    }
#endif

    handle_ = nullptr;
    create_ = nullptr;
    destroy_ = nullptr;
    abi_ = nullptr;
    order_exec_size_ = nullptr;
    order_exec_align_ = nullptr;
}

XtStraLoader::~XtStraLoader() { unload(); }

XtStraLoader::XtStraLoader(XtStraLoader&& other) noexcept
    : handle_(other.handle_),
      create_(other.create_),
      destroy_(other.destroy_),
      abi_(other.abi_),
      order_exec_size_(other.order_exec_size_),
      order_exec_align_(other.order_exec_align_),
      err_(std::move(other.err_)),
      security_(std::move(other.security_)) {
    other.handle_ = nullptr;
    other.create_ = nullptr;
    other.destroy_ = nullptr;
    other.abi_ = nullptr;
    other.order_exec_size_ = nullptr;
    other.order_exec_align_ = nullptr;
}

XtStraLoader& XtStraLoader::operator=(XtStraLoader&& other) noexcept {
    if (this != &other) {
        unload();
        handle_ = other.handle_;
        create_ = other.create_;
        destroy_ = other.destroy_;
        abi_ = other.abi_;
        order_exec_size_ = other.order_exec_size_;
        order_exec_align_ = other.order_exec_align_;
        err_ = std::move(other.err_);
        security_ = std::move(other.security_);
        other.handle_ = nullptr;
        other.create_ = nullptr;
        other.destroy_ = nullptr;
        other.abi_ = nullptr;
        other.order_exec_size_ = nullptr;
        other.order_exec_align_ = nullptr;
    }
    return *this;
}

XtStrategyBase* XtStraLoader::create() {
    return create_ ? create_() : nullptr;
}

void XtStraLoader::destroy(XtStrategyBase* stra) {
    if (destroy_) {
        destroy_(stra);
    }
}
