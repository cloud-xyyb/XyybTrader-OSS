#include "XtSelTradability.h"

#include <cctype>
#include <fstream>
#include <vector>

#include "xt_logger/XtLogger.h"

namespace xt_bt_sel {

namespace {

/// @brief 去除字段首尾空白(空格/Tab)。
[[nodiscard]] std::string trim_field(const std::string& s) {
    size_t b = 0;
    size_t e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

/// @brief 严格 0/1 解析;其它值返回 false(该行 skip,宁缺勿错)。
[[nodiscard]] bool parse_bool01(const std::string& s, bool& out) {
    if (s == "0") { out = false; return true; }
    if (s == "1") { out = true;  return true; }
    return false;
}

}  // namespace

TradabilityVerdict check_tradability(const TradabilityFlags* flags, bool is_buy) noexcept {
    if (!flags) return TradabilityVerdict::Ok;  // 缺行 = 完全可交易(fail-open)
    if (!flags->is_open) return TradabilityVerdict::Suspended;
    if (is_buy && flags->is_limit_up) return TradabilityVerdict::LimitUpBuy;
    if (!is_buy && flags->is_limit_down) return TradabilityVerdict::LimitDownSell;
    return TradabilityVerdict::Ok;
}

bool TradabilityCalendar::load(const std::string& csv_path, std::string& err) {
    by_date_.clear();
    total_rows_ = 0;

    std::ifstream ifs(csv_path);
    if (!ifs) {
        err = "cannot open tradability csv: " + csv_path;
        return false;
    }

    std::string line;
    bool header_seen = false;
    size_t skipped = 0;
    size_t line_no = 0;
    while (std::getline(ifs, line)) {
        ++line_no;
        // trim CR/LF(CRLF 容忍,镜像 read_close_prices_csv)
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) line.pop_back();
        if (line.empty()) continue;

        // 按逗号全量切分;真严格恰好 5 列(整行恰 4 个逗号)——缺列/多列一律
        // skip + warn(宁缺勿错,与 flag 非法值同路径计入 skipped)。
        std::vector<std::string> fields;
        size_t start = 0;
        while (true) {
            const size_t comma = line.find(',', start);
            if (comma == std::string::npos) {
                fields.push_back(line.substr(start));
                break;
            }
            fields.push_back(line.substr(start, comma - start));
            start = comma + 1;
        }

        const std::string date_str = trim_field(fields[0]);
        if (!header_seen) {
            header_seen = true;
            // header 容忍:首列非数字视为 header 行。
            if (date_str.empty() ||
                !std::isdigit(static_cast<unsigned char>(date_str[0]))) {
                continue;
            }
        }

        bool ok = (fields.size() == 5);
        uint32_t date = 0;
        std::string code;
        TradabilityFlags fl;
        if (ok) {
            // date 先验全数字且 ≤8 位(YYYYMMDD 域):挡尾垃圾("2026O113")、
            // 负号 wrap、与 >2^32 源值截断伪造合法键,防 stoul 宽松解析静默入库。
            bool all_digits = !date_str.empty() && date_str.size() <= 8;
            for (const char c : date_str) {
                if (!std::isdigit(static_cast<unsigned char>(c))) {
                    all_digits = false;
                    break;
                }
            }
            if (all_digits) {
                try {
                    date = static_cast<uint32_t>(std::stoul(date_str));
                } catch (...) {
                    ok = false;  // 全数字但超范围(stoul overflow)
                }
            } else {
                ok = false;
            }
            code = trim_field(fields[1]);
            if (date == 0 || code.empty()) ok = false;
        }
        if (ok) {
            ok = parse_bool01(trim_field(fields[2]), fl.is_open) &&
                 parse_bool01(trim_field(fields[3]), fl.is_limit_up) &&
                 parse_bool01(trim_field(fields[4]), fl.is_limit_down);
        }
        if (!ok) {
            ++skipped;
            XtLogger::warn_m("SEL_BT",
                             "tradability csv line {} 非法(需恰 5 列: date,code + 严格 0/1 ×3),skip: '{}'",
                             line_no, line);
            continue;
        }

        by_date_[date][code] = fl;  // 重复 (date,code) 后行覆盖前行
        ++total_rows_;
    }

    if (total_rows_ == 0) {
        err = "tradability csv has 0 valid data rows: " + csv_path;
        if (skipped > 0) err += " (skipped " + std::to_string(skipped) + " invalid lines)";
        return false;
    }
    return true;
}

const TradabilityFlags* TradabilityCalendar::find(uint32_t date,
                                                  const std::string& code) const {
    const auto dit = by_date_.find(date);
    if (dit == by_date_.end()) return nullptr;
    const auto cit = dit->second.find(code);
    return (cit == dit->second.end()) ? nullptr : &cit->second;
}

const TradabilityCalendar::CodeFlagsMap*
TradabilityCalendar::find_date(uint32_t date) const {
    const auto dit = by_date_.find(date);
    return (dit == by_date_.end()) ? nullptr : &dit->second;
}

}  // namespace xt_bt_sel
