#include "XtBacktestDailyBreakdown.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <system_error>

#include "XtLogger.h"

namespace {

// JSON 字符串最小转义：reason 文本来自 risk 层（静态文案为主），只处理会破坏
// JSON 语法的字符，不追求完整 escaper。
std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (const char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

}  // namespace

void XtBacktestDailyBreakdown::on_tick(uint32_t trading_date) noexcept {
    if (trading_date != 0) ++daily_stats_[trading_date].tick_count;
}

void XtBacktestDailyBreakdown::on_submit(uint32_t trading_date) noexcept {
    if (trading_date != 0) ++daily_stats_[trading_date].submit_count;
}

void XtBacktestDailyBreakdown::on_fill(uint32_t trading_date) noexcept {
    if (trading_date != 0) ++daily_stats_[trading_date].fill_count;
}

void XtBacktestDailyBreakdown::on_risk_reject(uint32_t trading_date, std::string_view reason) {
    if (trading_date == 0) return;
    auto& st = daily_stats_[trading_date];
    ++st.risk_reject_count;
    if (reason.empty()) return;
    // 分桶封顶：reason 文本理论上可携带动态数字（风控层多为静态文案），封顶
    // 防止单日 map 无界增长；已存在的桶不受上限影响继续累加。
    auto it = st.rejects_by_reason.find(std::string(reason));
    if (it != st.rejects_by_reason.end()) {
        ++it->second;
        return;
    }
    if (st.rejects_by_reason.size() >= kMaxReasonBuckets) {
        ++st.rejects_by_reason["(other)"];
        return;
    }
    ++st.rejects_by_reason[std::string(reason)];
}

void XtBacktestDailyBreakdown::on_day_account(uint32_t trading_date, double equity,
                                              double cash, double frozen) {
    if (trading_date == 0) return;
    auto& st = daily_stats_[trading_date];
    st.has_account = true;
    st.eod_equity = equity;
    st.eod_cash = cash;
    st.eod_frozen = frozen;
}

bool XtBacktestDailyBreakdown::write_json(const std::string& out_path,
                                          uint32_t cfg_start_date,
                                          uint32_t cfg_end_date,
                                          bool shared_execution_counters) const {
    if (daily_stats_.empty()) return true;  // 没数据不写文件 (vs failure)

    std::error_code ec;
    std::filesystem::create_directories(
        std::filesystem::path(out_path).parent_path(), ec);

    std::ofstream out(out_path);
    if (!out.good()) {
        XtLogger::error_m("REPORT", "daily_breakdown::write_json: open fail path={}", out_path);
        return false;
    }

    out << "{\n";
    out << "  \"schema_version\": 2,\n";
    out << "  \"start_date\": " << cfg_start_date << ",\n";
    out << "  \"end_date\": " << cfg_end_date << ",\n";
    out << "  \"days\": " << daily_stats_.size() << ",\n";
    // F8:仅共享计数路径落该标志（默认缺席 → 单策略 / HFT 逐字节不变）。
    if (shared_execution_counters) {
        out << "  \"shared_execution_counters\": true,\n";
    }
    out << "  \"breakdown\": [\n";

    size_t i = 0;
    for (const auto& [date, st] : daily_stats_) {
        out << "    {";
        out << "\"date\": " << date;
        out << ", \"ticks\": " << st.tick_count;
        out << ", \"submits\": " << st.submit_count;
        out << ", \"fills\": " << st.fill_count;
        out << ", \"risk_rejects\": " << st.risk_reject_count;
        if (!st.rejects_by_reason.empty()) {
            out << ", \"risk_rejects_by_reason\": {";
            size_t j = 0;
            for (const auto& [reason, cnt] : st.rejects_by_reason) {
                out << "\"" << json_escape(reason) << "\": " << cnt;
                if (++j < st.rejects_by_reason.size()) out << ", ";
            }
            out << "}";
        }
        if (st.has_account) {
            char eq[32], ca[32], fr[32];
            std::snprintf(eq, sizeof(eq), "%.2f", st.eod_equity);
            std::snprintf(ca, sizeof(ca), "%.2f", st.eod_cash);
            std::snprintf(fr, sizeof(fr), "%.2f", st.eod_frozen);
            out << ", \"eod_equity\": " << eq
                << ", \"eod_cash\": " << ca
                << ", \"eod_frozen\": " << fr;
        }
        out << "}";
        if (++i < daily_stats_.size()) out << ",";
        out << "\n";
    }

    out << "  ]\n";
    out << "}\n";
    out.close();
    XtLogger::info_m("REPORT", "daily_breakdown written: {} days → {}",
                     daily_stats_.size(), out_path);
    return true;
}
