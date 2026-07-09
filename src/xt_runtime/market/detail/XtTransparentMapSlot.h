#pragma once

/// @file
/// @brief market cache 内部用的 map slot helper
///
/// 本文件服务 `xt_runtime::market` 内部热路径
/// 按合约代码找缓存槽；没有就创建一个默认槽，然后返回引用

namespace xt_runtime::market::detail {

template <typename Map>
typename Map::mapped_type& slot_for(Map& m, const char* code) {
    auto it = m.find(code);
    if (it == m.end()) it = m.emplace(code, typename Map::mapped_type{}).first;
    return it->second;
}

}
