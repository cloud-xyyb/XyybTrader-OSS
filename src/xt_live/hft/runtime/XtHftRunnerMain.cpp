#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include <dlfcn.h>

int run_hft_live(int argc, char** argv, std::atomic<bool>& shutdown_flag, std::atomic<int>& shutdown_sig);

// 全局 shutdown 标志，由信号处理函数设置（见 install_shutdown_signal_handlers）
static std::atomic<bool> g_shutdown{false};
static std::atomic<int> g_shutdown_sig{0};

// --probe-abi <so>：部署 gate 值精确探测——dlopen 插件、打印其内嵌
// xt_abi_version() 后退出，不进 runner 启动链（无 config/logger/网络）。
// 用 runner 自身宿主 dlopen 保证与真 loader 同符号解析环境；deploy [3.5/6]
// 在 orb VM 内调用并与 host XT_STRATEGY_ABI_VERSION 比对（mtime proxy 检不出
// 「mtime 新但内嵌旧 ABI」的 stale .so）。
// 退出码：0=已打印版本；2=dlopen 失败；3=符号缺失（非插件/超旧 ABI 宏前产物）。
static int probe_plugin_abi(const char* so_path) {
    void* handle = dlopen(so_path, RTLD_LOCAL | RTLD_NOW);
    if (!handle) {
        std::fprintf(stderr, "probe-abi: dlopen failed: %s\n", dlerror());
        return 2;
    }
    using AbiFn = uint32_t (*)();
    auto fn = reinterpret_cast<AbiFn>(dlsym(handle, "xt_abi_version"));
    if (!fn) {
        std::fprintf(stderr, "probe-abi: xt_abi_version symbol missing: %s\n", dlerror());
        dlclose(handle);
        return 3;
    }
    std::printf("%u\n", fn());
    dlclose(handle);
    return 0;
}

int main(int argc, char** argv) {
    if (argc == 3 && std::strcmp(argv[1], "--probe-abi") == 0) {
        return probe_plugin_abi(argv[2]);
    }
    return run_hft_live(argc, argv, g_shutdown, g_shutdown_sig);
}
