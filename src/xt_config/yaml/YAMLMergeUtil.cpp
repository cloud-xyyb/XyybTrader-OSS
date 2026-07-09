#include "xt_config/yaml/YAMLMergeUtil.h"

#include <fstream>

namespace {

bool file_exists(const std::string& path) {
    if (path.empty()) return false;
    std::ifstream ifs(path);
    return ifs.good();
}

std::string parent_dir(const std::string& path) {
    if (path.empty()) return std::string(".");
    const size_t pos = path.find_last_of("/\\");
    if (pos == std::string::npos) return std::string(".");
    if (pos == 0) return path.substr(0, 1);
    return path.substr(0, pos);
}

std::string join_path(const std::string& dir, const std::string& file) {
    if (dir.empty() || dir == ".") return file;
    const char last = dir.back();
    if (last == '/' || last == '\\') return dir + file;
    return dir + "/" + file;
}

YAML::Node clone_if_defined(const YAML::Node& node) {
    if (!node || !node.IsDefined()) return YAML::Node();
    return YAML::Clone(node);
}

}

namespace XtConfigYamlUtil {

YAML::Node deep_merge(const YAML::Node& base, const YAML::Node& overlay) {
    if (!base || !base.IsDefined() || base.IsNull()) return clone_if_defined(overlay);
    if (!overlay || !overlay.IsDefined() || overlay.IsNull()) return clone_if_defined(base);

    if (!base.IsMap() || !overlay.IsMap()) {
        return clone_if_defined(overlay);
    }

    YAML::Node out = YAML::Clone(base);
    for (auto it = overlay.begin(); it != overlay.end(); ++it) {
        if (!it->first || !it->first.IsScalar()) continue;
        const std::string key = it->first.as<std::string>();
        if (key.empty()) continue;

        const YAML::Node base_child = out[key];
        out[key] = deep_merge(base_child, it->second);
    }
    return out;
}

bool load_profile_with_common(const std::string& profile_path,
                              const std::string& common_file_name,
                              YAML::Node& out,
                              std::string* err) {
    out = YAML::Node();
    if (err) err->clear();
    if (!file_exists(profile_path)) {
        if (err) *err = "config not found: " + profile_path;
        return false;
    }

    try {
        YAML::Node profile = YAML::LoadFile(profile_path);
        const std::string common_path = join_path(parent_dir(profile_path), common_file_name);
        if (!file_exists(common_path)) {
            out = profile;
            return true;
        }

        YAML::Node common = YAML::LoadFile(common_path);
        out = deep_merge(common, profile);
        return true;
    } catch (const std::exception& e) {
        if (err) *err = e.what();
        return false;
    } catch (...) {
        if (err) *err = "unknown error";
        return false;
    }
}

bool load_profile_with_common_and_market(const std::string& profile_path,
                                         const std::string& common_file_name,
                                         const std::string& market_file_name,
                                         YAML::Node& out,
                                         std::string* err) {
    out = YAML::Node();
    if (err) err->clear();
    if (!file_exists(profile_path)) {
        if (err) *err = "config not found: " + profile_path;
        return false;
    }

    try {
        YAML::Node profile = YAML::LoadFile(profile_path);
        YAML::Node merged = profile;

        const std::string common_path = join_path(parent_dir(profile_path), common_file_name);
        if (file_exists(common_path)) {

            YAML::Node common = YAML::LoadFile(common_path);
            merged = deep_merge(common, merged);
        }

        const std::string market_path = join_path(parent_dir(profile_path), market_file_name);
        if (!file_exists(market_path)) {
            if (err) *err = "market config not found: " + market_path;
            return false;
        }
        YAML::Node market = YAML::LoadFile(market_path);

        YAML::Node result = deep_merge(merged, market);

        const std::string secrets_path = join_path(parent_dir(profile_path), "secrets.yaml");
        if (file_exists(secrets_path)) {
            YAML::Node secrets = YAML::LoadFile(secrets_path);
            result = deep_merge(result, secrets);
        }

        out = result;
        return true;
    } catch (const std::exception& e) {
        if (err) *err = e.what();
        return false;
    } catch (...) {
        if (err) *err = "unknown error";
        return false;
    }
}

}
