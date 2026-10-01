#pragma once

/// @file
/// @brief YAML 校验上下文与 typed-read helper。

#include <cstddef>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

/// @brief YAML 校验上下文与类型读取助手（validation context）。
///
/// 封装 `YAML::Node::as<T>()` 的异常处理，并把错误消息追加到共享 errors sink。
/// 业务约束仍由各模块 validator 判定；本类只负责读取辅助和错误收集。
/// 缺失或 null 节点按 optional field 处理，不追加错误；类型不匹配才报告。
class XtConfigValidationCtx {
public:
    explicit XtConfigValidationCtx(std::vector<std::string>& errors) : errors_(errors) {}

    void add(const std::string& msg) {
        errors_.push_back(msg);
    }

    void add_if(bool bad, const std::string& msg) {
        if (bad) add(msg);
    }

    // read_* helper 共享 optional-field 语义：缺失/null 返回 false 且不报错，
    // 类型或取值不合法返回 false 并追加中文错误。
    bool read_double(const YAML::Node& n, const std::string& path, double& out) {
        if (!n || n.IsNull()) return false;
        try {
            out = n.as<double>();
            return true;
        } catch (...) {
            add(path + " 必须是数字");
            return false;
        }
    }

    bool read_int(const YAML::Node& n, const std::string& path, int& out) {
        if (!n || n.IsNull()) return false;
        try {
            out = n.as<int>();
            return true;
        } catch (...) {
            add(path + " 必须是整数");
            return false;
        }
    }

    bool read_bool(const YAML::Node& n, const std::string& path, bool& out) {
        if (!n || n.IsNull()) return false;
        try {
            out = n.as<bool>();
            return true;
        } catch (...) {
            add(path + " 必须是布尔值");
            return false;
        }
    }

    bool read_string(const YAML::Node& n, const std::string& path, std::string& out) {
        if (!n || n.IsNull()) return false;
        try {
            out = n.as<std::string>();
            return true;
        } catch (...) {
            add(path + " 必须是字符串");
            return false;
        }
    }

    bool read_positive_size(const YAML::Node& n, const std::string& path, std::size_t& out) {
        if (!n || n.IsNull()) return false;
        try {
            const long long v = n.as<long long>();
            if (v <= 0) {
                add(path + " 必须 > 0");
                return false;
            }
            out = static_cast<std::size_t>(v);
            return true;
        } catch (...) {
            add(path + " 必须是正整数");
            return false;
        }
    }

private:
    std::vector<std::string>& errors_;
};
