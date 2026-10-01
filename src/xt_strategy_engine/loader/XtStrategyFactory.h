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
    /// 托管策略实例，析构时按 plugin id 自动调用对应 destroy。
    using UniqueStra =
        std::unique_ptr<XtStrategyBase, std::function<void(XtStrategyBase*)>>;
    using Config = XtStrategyFactoryCfg;

    /** @brief 设置插件相对路径解析基准目录（plugin base path）。 */
    void set_base_path(const std::string& base_path);
    /** @brief 注册插件并完成 ABI 校验（register plugin）。 */
    bool register_plugin(const std::string& id, const std::string& so_path);

    /** @brief 创建裸指针实例；调用方必须用 destroy(id, ptr) 回收。 */
    XtStrategyBase* create(const std::string& id);
    /** @brief 创建托管实例（managed create）。 */
    UniqueStra create_managed(const std::string& id);
    /** @brief 销毁策略实例（destroy by plugin id）。 */
    void destroy(const std::string& id, XtStrategyBase* stra);

    /** @brief 返回最近一次错误信息（last error message）。 */
    const std::string& last_error() const { return err_; }

private:
    /** @brief 已注册插件条目（registered plugin entry）。 */
    struct Entry {
        std::string so_path;                            ///< 解析后的动态库路径。
        XtStraLoader loader;                            ///< 底层加载器对象。
    };

    std::string base_path_;                           ///< 插件基准路径（for relative so path）。
    std::unordered_map<std::string, Entry> entries_; ///< 已注册插件表（id -> entry）。
    std::string err_;                                 ///< 最近一次错误描述。
};
