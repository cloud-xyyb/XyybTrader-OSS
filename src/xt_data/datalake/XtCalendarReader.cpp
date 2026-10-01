#include "XtCalendarReader.h"
#include "schema_v1.h"
#include "XtLogger.h"

#include <algorithm>
#include <memory>

#include <arrow/api.h>

#include "XtArrowReadUtil.h"

namespace xt_data {
namespace datalake {

using namespace xt_arrow;  // parquet 读取 helper 只在 reader 实现层使用。

bool XtCalendarReader::load(const std::string& exchange) {
    trading_dates_.clear();
    trading_dates_set_.clear();

    const std::string path = datalake_.resolve_path(TableKind::Calendar, {});
    std::shared_ptr<arrow::Table> table;
    if (!read_parquet_table(path, table)) {
        XtLogger::warn_m("DataLake", "XtCalendarReader: cannot read {}", path);
        return false;
    }

    const auto date_arr = col_array(table, schema_v1::calendar::kColDate);
    const auto trading_arr = col_array(table, schema_v1::calendar::kColIsTradingDay);
    const auto exchg_arr = col_array(table, schema_v1::calendar::kColExchange);
    if (!date_arr || !trading_arr) {
        XtLogger::error_m("DataLake", "XtCalendarReader: missing required columns");
        return false;
    }

    // datalake schema 允许 date=Int32/Int64、is_trading_day=Bool/Int8；
    // reader 在此收敛为 YYYYMMDD + bool。
    const auto date_int32 = std::dynamic_pointer_cast<arrow::Int32Array>(date_arr);
    const auto date_int64 = std::dynamic_pointer_cast<arrow::Int64Array>(date_arr);
    const auto trading_bool = std::dynamic_pointer_cast<arrow::BooleanArray>(trading_arr);
    const auto trading_int8 = std::dynamic_pointer_cast<arrow::Int8Array>(trading_arr);
    const auto exchg_str = std::dynamic_pointer_cast<arrow::StringArray>(exchg_arr);

    const int64_t n = table->num_rows();
    trading_dates_.reserve(static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) {
        // exchange 列缺失时保持单市场 snapshot 兼容，不因无法过滤而拒绝整表。
        if (!exchange.empty() && exchg_str) {
            if (exchg_str->IsNull(i)) {
                continue;
            }
            if (exchg_str->GetString(i) != exchange) {
                continue;
            }
        }

        bool is_trading = false;
        if (trading_bool) {
            is_trading = !trading_bool->IsNull(i) && trading_bool->Value(i);
        } else if (trading_int8) {
            is_trading = !trading_int8->IsNull(i) && trading_int8->Value(i) != 0;
        }
        if (!is_trading) {
            continue;
        }

        uint32_t d = 0;
        if (date_int32) {
            d = static_cast<uint32_t>(date_int32->Value(i));
        } else if (date_int64) {
            d = static_cast<uint32_t>(date_int64->Value(i));
        } else {
            continue;
        }
        if (d == 0) {
            continue;
        }
        trading_dates_.push_back(d);
    }
    std::sort(trading_dates_.begin(), trading_dates_.end());
    trading_dates_set_.reserve(trading_dates_.size());
    for (auto d : trading_dates_) {
        trading_dates_set_.insert(d);
    }

    XtLogger::info_m("DataLake", "XtCalendarReader: loaded {} trading dates from {}",
                     trading_dates_.size(), path);
    return true;
}

uint32_t XtCalendarReader::next_trading_date(uint32_t cur) const {
    auto it = std::lower_bound(trading_dates_.begin(), trading_dates_.end(), cur);
    if (it == trading_dates_.end()) {
        return 0;
    }
    return *it;
}

uint32_t XtCalendarReader::prev_trading_date(uint32_t cur) const {
    auto it = std::lower_bound(trading_dates_.begin(), trading_dates_.end(), cur);
    if (it == trading_dates_.begin()) {
        return 0;
    }
    --it;
    return *it;
}

std::vector<uint32_t> XtCalendarReader::trading_dates_in_range(uint32_t from,
                                                                  uint32_t to) const {
    std::vector<uint32_t> out;
    auto lo = std::lower_bound(trading_dates_.begin(), trading_dates_.end(), from);
    auto hi = std::upper_bound(trading_dates_.begin(), trading_dates_.end(), to);
    out.assign(lo, hi);
    return out;
}

}  // namespace datalake
}  // namespace xt_data
