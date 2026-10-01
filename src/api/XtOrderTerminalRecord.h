#pragma once

#include <cstdint>

#include "xt_core/order/XtOrderTypes.h"

/// @brief venue 终态记录的按值出参 payload（`ITradeGateway::lookup_terminal` 出参）。
///
/// gateway 侧终态记录的最小只读投影，供引擎周期一致性检查按真实终态派生收尾
/// 事件（Filled 带累计量、Rejected 保原状态），取代一律合成 Canceled。
///
/// 字段契约：
/// - `status` 只承载终态值（Filled/Canceled/Rejected）；提供方必须在非终态时
///   返回 false 而不是填非终态值。
/// - `filled_fp` < 0 表示 venue 侧不提供累计成交量，调用方保留本地视图。
///
/// 按值出参（不含内部引用），跨 gateway 锁边界安全。
struct XtOrderTerminalRecord {
    XtOrderStatus status = XtOrderStatus::Canceled;  ///< 终态；仅终态值合法
    int64_t filled_fp = -1;  ///< 终态时刻累计成交量（fp）；<0 = 不可得
};
