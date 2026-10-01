#pragma once

#include <string>

#include "XtStrategyBase.h"
#include "XtStrategyLoader.h"

/// @brief 策略 plugin RAII holder。
///
/// 持有 loader 和由该 loader 创建的策略实例；reset()/析构时先 destroy instance，
/// 再 unload 动态库。非线程安全，调用方必须串行化 load/reset/get。
struct XtPluginHolder {
    XtStraLoader loader;              ///< 动态库加载器；负责 ABI/layout/security 校验。
    XtStrategyBase* instance = nullptr; ///< owning pointer；必须用 loader.destroy 回收。
    std::string err;                  ///< 最近一次 load/create 错误。

    XtPluginHolder() = default;
    ~XtPluginHolder() { reset(); }
    XtPluginHolder(const XtPluginHolder&) = delete;
    XtPluginHolder& operator=(const XtPluginHolder&) = delete;

    /// @brief 加载动态库并创建策略实例。
    ///
    /// 失败时对象回到未加载状态，err 保存 human-readable 原因。
    bool load(const std::string& so_path) {
        reset();
        if (so_path.empty()) {
            err = "empty so_path";
            return false;
        }
        if (!loader.load(so_path.c_str())) {
            err = loader.last_error();
            return false;
        }
        instance = loader.create();
        if (!instance) {
            err = "create_strategy returned null";
            reset();
            return false;
        }
        return true;
    }

    /// @brief 返回当前策略实例；未加载或创建失败时为 nullptr。
    XtStrategyBase* get() const { return instance; }

    /// @brief 销毁实例并卸载动态库；可重复调用。
    void reset() {
        if (instance) {
            loader.destroy(instance);
            instance = nullptr;
        }
        loader.unload();
    }
};
