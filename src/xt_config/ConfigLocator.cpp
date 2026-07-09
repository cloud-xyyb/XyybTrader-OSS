#include "ConfigLocator.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <unistd.h>

namespace {

bool file_exists(const std::string& p) {
    if (p.empty()) return false;
    std::ifstream f(p.c_str());
    return f.good();
}

std::string join_path(const std::string& a, const std::string& b) {
    if (a.empty()) return b;
    if (b.empty()) return a;
    char last = a.back();
    if (last == '/') return a + b;
    return a + "/" + b;
}

std::string parent_dir(const std::string& p) {
    if (p.empty()) return "";
    size_t pos = p.find_last_of("/");
    if (pos == std::string::npos) return "";
    return p.substr(0, pos);
}

std::string exe_dir() {
    char buf[1024] = {0};
    ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (len <= 0) return "";
    buf[len] = '\0';
    return parent_dir(std::string(buf));
}

bool looks_like_yaml_path(const char* arg) {
    const size_t n = std::strlen(arg);
    auto ends_with = [&](const char* suf) {
        const size_t m = std::strlen(suf);
        return n >= m && std::strcmp(arg + n - m, suf) == 0;
    };
    return ends_with(".yaml") || ends_with(".yml");
}

std::string parse_argv(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        const char* arg = argv[i];
        if (!arg) continue;
        if (std::strcmp(arg, "--config") == 0 || std::strcmp(arg, "-c") == 0) {
            if (i + 1 < argc && argv[i + 1]) return std::string(argv[i + 1]);
        }
        const char* prefix = "--config=";
        if (std::strncmp(arg, prefix, std::strlen(prefix)) == 0) {
            return std::string(arg + std::strlen(prefix));
        }
    }

    for (int i = 1; i < argc; ++i) {
        const char* arg = argv[i];
        if (!arg || arg[0] == '-') continue;
        if (looks_like_yaml_path(arg)) return std::string(arg);
        std::fprintf(stderr,
                     "xt_locate_config: ignoring unrecognized argument '%s' "
                     "(use --config <path>)\n",
                     arg);
    }
    return "";
}

std::string find_config_in_base(const std::string& base, const std::string& default_name) {
    std::string p = join_path(base, default_name);
    if (file_exists(p)) return p;
    p = join_path(join_path(base, "configs"), default_name);
    if (file_exists(p)) return p;
    return "";
}

}

std::string xt_locate_config(int argc, char** argv,
                             const char* env_name,
                             const char* default_name) {

    std::string p = parse_argv(argc, argv);
    if (!p.empty()) return p;

    if (env_name) {
        const char* env = std::getenv(env_name);
        if (env && env[0] != '\0') return std::string(env);
    }

    if (!default_name) default_name = "config.yaml";

    p = find_config_in_base(".", default_name);
    if (!p.empty()) return p;

    const std::string ed = exe_dir();
    if (!ed.empty()) {
        p = find_config_in_base(ed, default_name);
        if (!p.empty()) return p;

        std::string ed1 = parent_dir(ed);
        if (!ed1.empty()) {
            p = find_config_in_base(ed1, default_name);
            if (!p.empty()) return p;

            std::string ed2 = parent_dir(ed1);
            if (!ed2.empty()) {
                p = find_config_in_base(ed2, default_name);
                if (!p.empty()) return p;
            }
        }
    }

    return "";
}
