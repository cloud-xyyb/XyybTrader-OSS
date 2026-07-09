#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include <dlfcn.h>

int run_hft_live(int argc, char** argv, std::atomic<bool>& shutdown_flag, std::atomic<int>& shutdown_sig);

static std::atomic<bool> g_shutdown{false};
static std::atomic<int> g_shutdown_sig{0};

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
