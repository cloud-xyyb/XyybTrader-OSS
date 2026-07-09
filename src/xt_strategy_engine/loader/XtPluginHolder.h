#pragma once

#include <string>

#include "XtStrategyBase.h"
#include "XtStrategyLoader.h"

/// @brief 策略 plugin RAII holder。
///
/// 持有 loader 和由该 loader 创建的策略实例；reset()/析构时先 destroy instance，
/// 再 unload 动态库。非线程安全，调用方必须串行化 load/reset/get。
struct XtPluginHolder {
    XtStraLoader loader;
    XtStrategyBase* instance = nullptr;
    std::string err;

    XtPluginHolder() = default;
    ~XtPluginHolder() { reset(); }
    XtPluginHolder(const XtPluginHolder&) = delete;
    XtPluginHolder& operator=(const XtPluginHolder&) = delete;

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

    XtStrategyBase* get() const { return instance; }

    void reset() {
        if (instance) {
            loader.destroy(instance);
            instance = nullptr;
        }
        loader.unload();
    }
};
