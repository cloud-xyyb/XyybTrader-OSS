#pragma once

/// @file
/// @brief EventStore MdTick depth 契约常量。
///
/// FlatBuffers schema v1 的 MdTick 固定保存 L1-L5。
/// 写方、读方和测试共用该常量作为 depth 上限。

namespace ct::data {

inline constexpr int kEventStoreMdDepth = 5;

}
