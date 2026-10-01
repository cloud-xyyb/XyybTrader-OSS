#pragma once

#include <memory>
#include <string>

#include "XtPluginHolder.h"

namespace xt_strategy_engine {

/// @brief 策略装配单元：plugin holder + 策略 id。
///
/// runner 装配期必须保证 plugin 非空；id 必须与同索引的 config/context/sink
/// 装配元组一致，由 StrategyAssemblyContract 校验。
struct StrategyInstance {
    std::unique_ptr<XtPluginHolder> plugin; ///< owning plugin holder；装配成功后必须非空。
    std::string id;                         ///< 策略唯一 id；参与 multi-strategy 路由。
};

}  // namespace xt_strategy_engine
