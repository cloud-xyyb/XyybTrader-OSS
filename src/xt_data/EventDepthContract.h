#pragma once

/// @file
/// @brief EventStore MdTick depth 契约常量。
///
/// FlatBuffers MdTick 的盘口档数可变（contract v2）：写方按 venue 真实档数
/// 写入 1..kEventStoreMdDepth 档并以 `depth_levels_real` 记录，读方接受该
/// 区间并校验四个数组长度 == depth_levels_real（旧 v1 固定 5 档文件天然落
/// 在区间内，向后兼容）。

namespace ct::data {

inline constexpr int kEventStoreMdDepth = 20;

}
