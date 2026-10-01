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

    /// @brief 写入 1 秒 KPI 行（每秒或按窗口聚合的性能指标快照）。
    [[nodiscard]] virtual bool on_kpi_1s(const XtKpi1sRow& row) = 0;
    /// @brief 写入成交明细行。
    [[nodiscard]] virtual bool on_fill(const XtFillRow& row) = 0;
    /// @brief 写入每秒多合约持仓快照行（包含 bid/ask 价格字段）。
    [[nodiscard]] virtual bool on_position_snapshot(const XtPositionRow& /*row*/) { return true; }
    /// @brief 写入每秒账户真值快照行。默认空实现向后兼容。
    [[nodiscard]] virtual bool on_account_truth(const XtAccountTruthRow& /*row*/) { return true; }

    /// @brief 强制将缓冲区内容立即落盘并下发 fsync（force flush + fsync to disk）。
    ///
    /// 返回 `true` 表示已接收数据全部成功写出并同步；返回 `false` 表示至少
    /// 一个写出、flush 或 sync 步骤失败。实现必须捕获所有 I/O 异常，转译成
    /// false 返回值。
    virtual bool sync_flush() noexcept = 0;
};

} // namespace xt_report
