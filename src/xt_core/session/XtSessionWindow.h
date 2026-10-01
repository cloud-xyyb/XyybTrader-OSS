#pragma once

#include <utility>
#include <vector>

/// @brief Closeout 的禁开仓与强平 HHMM 窗口。
///
/// `no_open_hhmm` 是开始禁止新开仓的分钟，`force_hhmm` 是触发强平的分钟。
/// no-open 判断按 `[no_open_hhmm, force_hhmm]` 闭区间执行，包含 force 分钟。
struct XtSessionWindow {
    int no_open_hhmm = 0;  ///< 禁开仓起始 HHMM；0 表示未配置或由上层默认值覆盖。
    int force_hhmm = 0;    ///< 强平起始 HHMM；0 表示未配置或由上层默认值覆盖。
};
