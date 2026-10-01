#pragma once

/// @file
/// @brief market cache 内部用的 map slot helper
///
/// 本文件服务 `xt_runtime::market` 内部热路径
/// 按合约代码找缓存槽；没有就创建一个默认槽，然后返回引用

namespace xt_runtime::market::detail {

/// @brief 取出 code 对应的值；没有这个 code 时插入默认值。
///
/// 只给 market 的 string-key 热路径使用；调用方必须在进入本 helper 前完成
/// 空 code 过滤。
///
/// @note C++17 下 const char* 查找仍可能临时构造 std::string；C++20 transparent
///       lookup 生效后，已存在 code 的查找才不需要构造 std::string。
/// @pre code 不能为 nullptr。
template <typename Map>
typename Map::mapped_type& slot_for(Map& m, const char* code) {
    auto it = m.find(code);
    if (it == m.end()) it = m.emplace(code, typename Map::mapped_type{}).first;
    return it->second;
}

}  // namespace xt_runtime::market::detail
