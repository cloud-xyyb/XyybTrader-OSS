#pragma once
//
// SEL backtest 数据加载层。
// CSV trading_dates 视为交易日历；datalake calendar.parquet 作为备选来源。
// loader 返回过滤后的 trading_dates 与 source，空结果表示没有可用日历。
//

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "xt_data/datalake/XtDatalakeRoot.h"
#include "xt_data/datalake/XtCalendarReader.h"
#include "xt_engine/sel/SelTaskScheduler.h"  // IHolidayProvider

namespace xt_bt_sel {

/// @brief CSV holiday provider：`trading_dates_` 内的日期视为交易日。
///
/// `is_holiday()` 对不在集合内的日期返回 true；`next_trading_date()` 返回严格大于
/// 当前日的下一交易日，若不存在则返回 0。
class XtCsvCalendarHolidayProvider : public xt_engine_sel::IHolidayProvider {
public:
    explicit XtCsvCalendarHolidayProvider(std::vector<uint32_t> trading_dates);

    [[nodiscard]] bool is_holiday(const std::string& /*trdtpl*/,
                                  uint32_t date,
                                  bool /*ifWeekend*/) const override;

    [[nodiscard]] uint32_t next_trading_date(const std::string& /*trdtpl*/,
                                             uint32_t curDate) const override;

    [[nodiscard]] const std::vector<uint32_t>& trading_dates() const noexcept {
        return trading_dates_;
    }

private:
    std::vector<uint32_t> trading_dates_;       ///< sorted asc
};

/// @brief datalake XtCalendarReader 到 `xt_engine_sel::IHolidayProvider` 的适配器。
///
/// 让 SelTaskScheduler 直接用 calendar.parquet 数据判定节假日 + 顺延（holiday
/// rollover）。与 XtCsvCalendarHolidayProvider 对称：CSV 源走前者，datalake
/// parquet 源走本类。
class XtCalendarHolidayProvider : public xt_engine_sel::IHolidayProvider {
public:
    explicit XtCalendarHolidayProvider(
        std::shared_ptr<xt_data::datalake::XtCalendarReader> calendar)
        : calendar_(std::move(calendar)) {}

    /// @brief 节假日判定：当日**不**在交易日历中即视为 holiday。
    /// @note ifWeekend 参数当前忽略（A 股 calendar 本身已包含周末过滤）。
    [[nodiscard]] bool is_holiday(const std::string& /*trdtpl*/,
                                  uint32_t date,
                                  bool /*ifWeekend*/) const override {
        return calendar_ ? !calendar_->is_trading_day(date) : false;
    }

    [[nodiscard]] uint32_t next_trading_date(const std::string& /*trdtpl*/,
                                             uint32_t curDate) const override {
        return calendar_ ? calendar_->next_trading_date(curDate) : (curDate + 1);
    }

private:
    std::shared_ptr<xt_data::datalake::XtCalendarReader> calendar_;
};

/// @brief SEL data loader：一次性解析 trading dates/source 供主循环使用。
///
/// 解析顺序固定为 CSV 优先、datalake parquet 次之。成功时返回闭区间过滤后的
/// `trading_dates`、可选 holiday provider 与 `source`；空结果表示无可用日历。
class XtSelDataLoader {
public:
    /// @brief SEL 回测数据加载结果。
    struct LoadResult {
        std::vector<uint32_t> trading_dates;            ///< 闭区间过滤后
        std::unique_ptr<xt_engine_sel::IHolidayProvider> holiday_provider;
        std::string source;                              ///< "csv:..." / "parquet:..." / "empty"
        // ── consumed manifest 归因（载入前口径）─────────────────────────
        size_t raw_date_count   = 0;  ///< 窗过滤前的（去重）交易日总数 = rows_loaded。
        size_t calendar_bad_rows = 0; ///< CSV 日历坏行数（非日期 / 解析失败，跳过表头）。
    };

    /// @param csv_path        trading_dates.csv 路径（empty → skip csv）
    /// @param datalake_root   数据湖根（empty → skip parquet）
    /// @param date_from / date_to  闭区间过滤；0 = 不过滤
    [[nodiscard]] static LoadResult load(const std::string& csv_path,
                                          const std::string& datalake_root,
                                          uint32_t date_from,
                                          uint32_t date_to);
};

}  // namespace xt_bt_sel
