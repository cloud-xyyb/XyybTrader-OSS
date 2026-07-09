#pragma once

#include "xt_report/core/XtReportTypes.h"

namespace xt_report {

/// @brief 报表行的写出接口。
///
/// reporter 产出的每一行（KPI、成交、持仓、账户快照）数据的写出口，
/// 去向由实现决定（如 CSV 文件）。
/// 所有写入方法返回 false 表示排队/写入/落盘已失败，调用方必须检查；
/// sync_flush() 强制落盘并 fsync，实现必须 noexcept 并把 I/O 异常转译为 false。
class IReportSink {
public:
    virtual ~IReportSink() = default;

    [[nodiscard]] virtual bool on_kpi_1s(const XtKpi1sRow& row) = 0;

    [[nodiscard]] virtual bool on_fill(const XtFillRow& row) = 0;

    [[nodiscard]] virtual bool on_position_snapshot(const XtPositionRow&  ) { return true; }

    [[nodiscard]] virtual bool on_account_truth(const XtAccountTruthRow&  ) { return true; }

    virtual bool sync_flush() noexcept = 0;
};

}
