#include "XtSelTraceSink.h"

#include <cstdio>
#include <filesystem>

#include "../common/XtBacktestReport.h"   // finish_report_stream + csv_escape_field
#include "xt_logger/XtLogger.h"

namespace xt_bt_sel {

namespace {

/// @brief 一天的毫秒数上界；日内序号顶到此值即 clamp（ts 必须留在当日）。
constexpr uint32_t kMsPerDay = 24u * 3600u * 1000u;

/// @brief HHMM → ms since midnight；越界（HH>23 / MM>59）退 0。
[[nodiscard]] uint32_t hhmm_to_ms(uint32_t hhmm) noexcept {
    const uint32_t hh = hhmm / 100;
    const uint32_t mm = hhmm % 100;
    if (hh > 23 || mm > 59) return 0;
    return hh * 3600u * 1000u + mm * 60u * 1000u;
}

/// @brief ms since midnight → packed HHMMSSmmm。
[[nodiscard]] uint32_t ms_to_packed_time(uint32_t ms) noexcept {
    const uint32_t hh = ms / 3600000u;
    uint32_t rem      = ms % 3600000u;
    const uint32_t mm = rem / 60000u;
    rem %= 60000u;
    const uint32_t ss  = rem / 1000u;
    const uint32_t mil = rem % 1000u;
    return hh * 10'000'000u + mm * 100'000u + ss * 1000u + mil;
}

}  // namespace

bool XtSelTraceSink::open(const std::string& report_dir, uint32_t base_time) {
    std::filesystem::create_directories(report_dir);
    path_    = (std::filesystem::path(report_dir) / "events_full.csv").string();
    base_ms_ = hhmm_to_ms(base_time);
    af_      = std::make_unique<PathUtil::AtomicOfstream>(path_);
    if (!af_->is_open()) {
        XtLogger::error_m("SEL_BT", "events_full.csv open failed: {}", path_);
        af_.reset();
        return false;
    }
    // 表头与 HFT XtEventStreamSink 逐字相同（producer 按此列序解析）。
    af_->stream() << "ts,seq,event_type,payload\n";
    return true;
}

void XtSelTraceSink::begin_day(uint32_t date) noexcept {
    date_    = date;
    day_seq_ = 0;
}

int64_t XtSelTraceSink::next_ts_() noexcept {
    // 日内每条 +1ms；顶到日末则停在最后一毫秒（ts 越日会让事件落到别的交易日，
    // 比丢时序精度危险得多）。此后同 ts 的多行靠 seq 定序 —— 排序契约是
    // `(ts, seq)` 严格单调，ts 单独只保证非降（见 header）。
    uint32_t ms = base_ms_ + day_seq_;
    if (ms >= kMsPerDay) ms = kMsPerDay - 1;
    else ++day_seq_;
    return static_cast<int64_t>(date_) * 1'000'000'000LL +
           static_cast<int64_t>(ms_to_packed_time(ms));
}

void XtSelTraceSink::emit(const char* event_type, const std::string& payload) {
    if (!af_) return;
    auto& out = af_->stream();
    char buf[64];
    const int n = std::snprintf(buf, sizeof(buf), "%lld,%llu,",
                                static_cast<long long>(next_ts_()),
                                static_cast<unsigned long long>(++seq_));
    if (n > 0) out.write(buf, n);
    out << (event_type ? event_type : "") << ',' << csv_escape_field(payload) << '\n';
    ++rows_;
}

bool XtSelTraceSink::commit() {
    if (!af_) return true;   // 未开启 = 无产物要落盘，不是失败
    const bool ok = finish_report_stream(*af_, path_, "SEL_BT", "events_full.csv", rows_);
    af_.reset();
    return ok;
}

}  // namespace xt_bt_sel
