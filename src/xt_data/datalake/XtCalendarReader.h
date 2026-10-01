#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "XtDatalakeRoot.h"

namespace xt_data {
namespace datalake {

/// @brief 数据湖 A 股交易日历的内存查询器。
///
/// A 股按交易日历运作（周末与法定节假日停市），策略和回测都要判断某日是不是交易日、找相邻
/// 交易日、或取一段区间内的交易日。本类从 `XtDatalakeRoot` 把日历 `load()` 进内存，之后
/// 提供这些查询。
///
/// 该对象不做并发保护；不要让 `load()` 与查询方法并发执行。
class XtCalendarReader {
public:
    /// @param datalake non-owning root resolver；调用方保证其生命周期长于 reader。
    explicit XtCalendarReader(const XtDatalakeRoot& datalake) : datalake_(datalake) {}

    /// @brief 加载 calendar.parquet 全表到内存。
    /// @param exchange 过滤 `exchange` 列；空字符串表示不过滤，默认 `"SSE"`。
    /// @return true 表示成功加载；false 表示文件不存在、读取失败或缺少必需字段。
    bool load(const std::string& exchange = "SSE");

    /// @brief 当前是否为交易日（YYYYMMDD）。
    [[nodiscard]] bool is_trading_day(uint32_t yyyymmdd) const noexcept {
        return trading_dates_set_.count(yyyymmdd) > 0;
    }

    /// @brief 已加载的全部交易日，升序排列；load() 会替换已有内容。
    [[nodiscard]] const std::vector<uint32_t>& trading_dates() const noexcept {
        return trading_dates_;
    }

    /// @brief 返回大于等于 cur 的首个交易日。
    /// @return 0 表示 cur 已超过 calendar 边界。
    [[nodiscard]] uint32_t next_trading_date(uint32_t cur) const;

    /// @brief 返回严格小于 cur 的上个交易日；不存在时返回 0。
    [[nodiscard]] uint32_t prev_trading_date(uint32_t cur) const;

    /// @brief 返回闭区间 [from, to] 内所有交易日；调用方应保证 from <= to。
    [[nodiscard]] std::vector<uint32_t> trading_dates_in_range(uint32_t from,
                                                                uint32_t to) const;

private:
    const XtDatalakeRoot& datalake_;
    std::vector<uint32_t> trading_dates_;            ///< 升序交易日。
    std::unordered_set<uint32_t> trading_dates_set_; ///< is_trading_day() O(1) lookup。
};

}  // namespace datalake
}  // namespace xt_data
