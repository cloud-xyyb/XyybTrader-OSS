#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "XtStrategyFactoryCfg.h"
#include "XtStrategyLoader.h"

/// @brief 策略 plugin 工厂。
///
/// Factory 以 plugin id 管理 loader 和实例生命周期，调用方通过 id 注册动态库并创建
/// raw 或 managed 策略实例。
///
/// @warning 非线程安全；必须由调用方串行化。
/// @note `create_managed` 返回的 deleter 按创建时的 plugin id 调用对应 destroy；
///       factory 的生命周期必须长于策略实例。
class XtStrategyFactory {
public:

    using UniqueStra =
        std::unique_ptr<XtStrategyBase, std::function<void(XtStrategyBase*)>>;
    using Config = XtStrategyFactoryCfg;

    void set_base_path(const std::string& base_path);

    bool register_plugin(const std::string& id, const std::string& so_path);

    XtStrategyBase* create(const std::string& id);

    UniqueStra create_managed(const std::string& id);

    void destroy(const std::string& id, XtStrategyBase* stra);

    const std::string& last_error() const { return err_; }

private:
    /** @brief 已注册插件条目（registered plugin entry）。 */
    struct Entry {
        std::string so_path;
        XtStraLoader loader;
    };

    std::string base_path_;
    std::unordered_map<std::string, Entry> entries_;
    std::string err_;
};
