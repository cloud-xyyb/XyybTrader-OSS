#include "XtSelCorporateAction.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <unordered_set>

#include "xt_logger/XtLogger.h"

namespace xt_bt_sel {

namespace {

/// @brief 去除字段首尾空白（空格/Tab）。
[[nodiscard]] std::string trim_field(const std::string& s) {
    size_t b = 0;
    size_t e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

/// @brief 该年该月的天数（Gregorian，含闰年规则）。
[[nodiscard]] int days_in_month(uint32_t year, uint32_t month) {
    static constexpr int kDays[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month < 1 || month > 12) return 0;
    if (month == 2) {
        const bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        return leap ? 29 : 28;
    }
    return kDays[month];
}

/// @brief YYYYMMDD 严格解析：恰 8 位数字 + 真实日历域（月 1-12、日 ≤ 当月天数）。
///
/// 空串或全 0（"0" / "00000000"）= 「未提供」→ 0 且 ok（pay_date /
/// shares_list_date 的缺省写法）。除此之外一律 fail-fast：短位数（`2026011`）会被
/// 后续窗口判据当成窗前事件静默吞掉整笔分红，非法月日（`20261399`）让应收/红股
/// 永不到期——两者都是「回测成功但账本失真」，比整跑失败更难追溯。
/// @return false = 非法，调用方 fail-fast。
[[nodiscard]] bool parse_opt_date(const std::string& s, uint32_t& out) {
    out = 0;
    if (s.empty()) return true;
    bool all_zero = true;
    for (const char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
        if (c != '0') all_zero = false;
    }
    if (all_zero) return true;  // "0" / "00000000" = 未提供
    if (s.size() != 8) return false;
    uint32_t v = 0;
    try {
        v = static_cast<uint32_t>(std::stoul(s));
    } catch (...) {
        return false;
    }
    const uint32_t month = (v / 100) % 100;
    const uint32_t day   = v % 100;
    const uint32_t year  = v / 10000;
    if (day == 0 || static_cast<int>(day) > days_in_month(year, month)) return false;
    out = v;
    return true;
}

/// @brief 非负有限 double 解析（费率/比例列）；空串 = 0。
[[nodiscard]] bool parse_nonneg(const std::string& s, double& out) {
    out = 0.0;
    if (s.empty()) return true;
    try {
        size_t pos = 0;
        out = std::stod(s, &pos);
        if (pos != s.size()) return false;  // 尾垃圾（"1.0x"）不放行
    } catch (...) {
        return false;
    }
    return std::isfinite(out) && out >= 0.0;
}

/// @brief 严格 0/1 解析。
[[nodiscard]] bool parse_bool01(const std::string& s, bool& out) {
    if (s == "0") { out = false; return true; }
    if (s == "1") { out = true;  return true; }
    return false;
}

/// @brief 按逗号全量切分（不处理引号；CA 字段无逗号语义）。
[[nodiscard]] std::vector<std::string> split_fields(const std::string& line) {
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
    return fields;
}

/// @brief CSV 列名与列序的唯一真相（数据行按下标取值，见 `load`）。
constexpr const char* kCaColumnNames[13] = {
    "code",          "record_date",   "ex_date",           "pay_date",
    "cash_per_share","bonus_transfer_per_share",           "rights_per_share",
    "rights_price",  "is_delist",     "shares_list_date",  "conversion_to",
    "conversion_ratio", "cash_in_lieu"};

/// @brief 窗内非交易日归日：返回 ≤ date 的最后一个交易日；不存在返回 0。
[[nodiscard]] uint32_t last_trading_date_le(const std::vector<uint32_t>& dates, uint32_t date) {
    const auto it = std::upper_bound(dates.begin(), dates.end(), date);
    if (it == dates.begin()) return 0;
    return *(it - 1);
}

}  // namespace

const char* rights_policy_name(RightsPolicy p) noexcept {
    switch (p) {
        case RightsPolicy::kSubscribe: return "subscribe";
        case RightsPolicy::kForfeit:   return "forfeit";
    }
    return "unknown";
}

bool parse_rights_policy(const std::string& s, RightsPolicy& out) noexcept {
    if (s == "subscribe") { out = RightsPolicy::kSubscribe; return true; }
    if (s == "forfeit")   { out = RightsPolicy::kForfeit;   return true; }
    return false;
}

CaLoadStatus CorporateActionCalendar::load(const std::string& csv_path,
                                           bool allow_skip_rights,
                                           std::string& err) {
    rows_.clear();
    by_ex_date_.clear();
    rights_skipped_ = 0;

    std::ifstream ifs(csv_path);
    if (!ifs) {
        err = "cannot open corporate_actions csv: " + csv_path;
        return CaLoadStatus::kIoError;
    }

    // (code, ex_date) 唯一：同键两行会把同一笔分红/送转应用两次，是账本级错误。
    std::unordered_set<std::string> seen;
    std::string line;
    size_t line_no = 0;
    bool first_line = true;
    while (std::getline(ifs, line)) {
        ++line_no;
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) line.pop_back();
        if (line.empty()) continue;

        auto fields = split_fields(line);
        if (first_line) {
            first_line = false;
            // header 判据：首列字面 "code"（A 股 code 以数字起头，不能用「非数字」判据）。
            if (!fields.empty() && trim_field(fields[0]) == "code") {
                // 列名与列序逐列校验。只认首列会放行 `…,conversion_to,cash_in_lieu,
                // conversion_ratio` 这类错序表头，之后数据行按下标取值 —— 换股比例被
                // 读成零股现金替代价（比例静默放大整仓），账本失真且全程无报错。
                if (fields.size() < 9 || fields.size() > 13) {
                    err = "corporate_actions csv header: expected 9 to 13 columns, got " +
                          std::to_string(fields.size());
                    return CaLoadStatus::kBadRow;
                }
                for (size_t i = 0; i < fields.size(); ++i) {
                    const std::string name = trim_field(fields[i]);
                    if (name != kCaColumnNames[i]) {
                        err = "corporate_actions csv header column " +
                              std::to_string(i + 1) + ": expected '" +
                              kCaColumnNames[i] + "', got '" + name + "'";
                        return CaLoadStatus::kBadRow;
                    }
                }
                continue;
            }
        }
        if (fields.size() < 9 || fields.size() > 13) {
            err = "corporate_actions csv line " + std::to_string(line_no) +
                  ": expected 9 to 13 columns (code,record_date,ex_date,pay_date,"
                  "cash_per_share,bonus_transfer_per_share,rights_per_share,rights_price,"
                  "is_delist[,shares_list_date[,conversion_to[,conversion_ratio"
                  "[,cash_in_lieu]]]]), got " + std::to_string(fields.size());
            return CaLoadStatus::kBadRow;
        }

        SelCorporateAction ev;
        ev.code = trim_field(fields[0]);
        const bool parsed =
            !ev.code.empty() &&
            parse_opt_date(trim_field(fields[1]), ev.record_date) &&
            parse_opt_date(trim_field(fields[2]), ev.ex_date) &&
            parse_opt_date(trim_field(fields[3]), ev.pay_date) &&
            parse_nonneg(trim_field(fields[4]), ev.cash_per_share) &&
            parse_nonneg(trim_field(fields[5]), ev.bonus_transfer_per_share) &&
            parse_nonneg(trim_field(fields[6]), ev.rights_per_share) &&
            parse_nonneg(trim_field(fields[7]), ev.rights_price) &&
            parse_bool01(trim_field(fields[8]), ev.is_delist) &&
            (fields.size() < 10 ||
             parse_opt_date(trim_field(fields[9]), ev.shares_list_date)) &&
            (fields.size() < 12 ||
             parse_nonneg(trim_field(fields[11]), ev.conversion_ratio)) &&
            (fields.size() < 13 ||
             parse_nonneg(trim_field(fields[12]), ev.cash_in_lieu));
        if (fields.size() >= 11) ev.conversion_to = trim_field(fields[10]);
        if (!parsed || ev.ex_date == 0) {
            err = "corporate_actions csv line " + std::to_string(line_no) +
                  " has malformed fields (code/date/rate): '" + line + "'";
            return CaLoadStatus::kBadRow;
        }

        // 日期序：record ≤ ex ≤ pay（缺省者跳过该比较）。
        if (ev.record_date != 0 && ev.record_date > ev.ex_date) {
            err = "corporate_actions csv line " + std::to_string(line_no) +
                  ": record_date must be <= ex_date";
            return CaLoadStatus::kBadRow;
        }
        if (ev.pay_date != 0 && ev.pay_date < ev.ex_date) {
            err = "corporate_actions csv line " + std::to_string(line_no) +
                  ": pay_date must be >= ex_date";
            return CaLoadStatus::kBadRow;
        }
        if (ev.shares_list_date != 0 && ev.shares_list_date < ev.ex_date) {
            err = "corporate_actions csv line " + std::to_string(line_no) +
                  ": shares_list_date must be >= ex_date";
            return CaLoadStatus::kBadRow;
        }
        // 事件型互斥：终止上市 / 换股各自独占一行，不承载分红/送转/配股语义
        //（validator 侧同规则）。混填意味着这行的施加序无从定义。
        const bool has_value_col =
            ev.cash_per_share != 0.0 || ev.bonus_transfer_per_share != 0.0 ||
            ev.rights_per_share != 0.0 || ev.rights_price != 0.0;
        if (ev.is_delist && (has_value_col || !ev.conversion_to.empty() ||
                             ev.conversion_ratio != 0.0 || ev.cash_in_lieu != 0.0)) {
            err = "corporate_actions csv line " + std::to_string(line_no) +
                  ": is_delist=1 requires all value columns to be 0 and conversion_to empty";
            return CaLoadStatus::kBadRow;
        }
        if (!ev.conversion_to.empty()) {
            if (has_value_col) {
                err = "corporate_actions csv line " + std::to_string(line_no) +
                      ": conversion_to requires all dividend/bonus/rights columns to be 0";
                return CaLoadStatus::kBadRow;
            }
            if (!(ev.conversion_ratio > 0.0)) {
                err = "corporate_actions csv line " + std::to_string(line_no) +
                      ": conversion_to requires conversion_ratio > 0";
                return CaLoadStatus::kBadRow;
            }
            if (ev.conversion_to == ev.code) {
                err = "corporate_actions csv line " + std::to_string(line_no) +
                      ": conversion_to must differ from code";
                return CaLoadStatus::kBadRow;
            }
        } else if (ev.conversion_ratio != 0.0 || ev.cash_in_lieu != 0.0) {
            err = "corporate_actions csv line " + std::to_string(line_no) +
                  ": conversion_ratio / cash_in_lieu require a non-empty conversion_to";
            return CaLoadStatus::kBadRow;
        }
        // 配股比例与配股价同生共死：只给比例 = 缴款额无从算（会被当成免费送股），
        // 只给价格 = 价格挂在没有配股的行上（数据错位）。两者都是静默失真源。
        if ((ev.rights_per_share > 0.0) != (ev.rights_price > 0.0)) {
            err = "corporate_actions csv line " + std::to_string(line_no) +
                  ": rights_per_share and rights_price must both be > 0 or both be 0";
            return CaLoadStatus::kBadRow;
        }
        if (ev.rights_per_share > 0.0 && allow_skip_rights) {
            // 整行跳过（不除权、不认购、不摊薄）：研究可显式接受这份失真，
            // 代价是该标的自此与真实历史分叉。
            ev.rights_skipped = true;
            ++rights_skipped_;
        }

        ev.raw_ex_date = ev.ex_date;  // 归日会改写 ex_date；施加序按原始日
        const std::string key = ev.code + '|' + std::to_string(ev.ex_date);
        if (!seen.insert(key).second) {
            err = "corporate_actions csv line " + std::to_string(line_no) +
                  ": duplicate (code, ex_date) = " + key;
            return CaLoadStatus::kBadRow;
        }
        rows_.push_back(std::move(ev));
    }

    if (rows_.empty()) {
        err = "corporate_actions csv has 0 valid data rows: " + csv_path;
        return CaLoadStatus::kBadRow;
    }
    reindex_();
    return CaLoadStatus::kOk;
}

void CorporateActionCalendar::align_to_trading_dates(
    const std::vector<uint32_t>& trading_dates, AlignStats& stats) {
    stats = AlignStats{};
    if (trading_dates.empty()) {
        rows_.clear();
        by_ex_date_.clear();
        return;
    }
    const uint32_t first = trading_dates.front();
    const uint32_t last  = trading_dates.back();

    std::vector<SelCorporateAction> kept;
    kept.reserve(rows_.size());
    for (auto& ev : rows_) {
        if (ev.ex_date > last) {
            // 窗末之后的事件归日到窗内 = 把未来事实搬进回测（future leak）→ 整条忽略。
            ++stats.ignored_after_window;
            XtLogger::warn_m("SEL_BT",
                             "corporate action ignored (after backtest window): code={} "
                             "ex_date={} > last trading date {}", ev.code, ev.ex_date, last);
            continue;
        }
        if (ev.ex_date < first) {
            // 窗首之前：事件已发生在回测起点之前，价格/持仓基线已含之。
            ++stats.ignored_before_window;
            XtLogger::warn_m("SEL_BT",
                             "corporate action ignored (before backtest window): code={} "
                             "ex_date={} < first trading date {}", ev.code, ev.ex_date, first);
            continue;
        }
        const uint32_t aligned = last_trading_date_le(trading_dates, ev.ex_date);
        if (aligned != ev.ex_date) {
            ++stats.rebased_to_prev_trading_date;
            XtLogger::warn_m("SEL_BT",
                             "corporate action ex_date {} is not a trading date — rebased to "
                             "{} (code={})", ev.ex_date, aligned, ev.code);
            ev.ex_date = aligned;
        }
        // 新股到账日：CSV 给了就用原值（其非交易日形态由 `list_date <= date` 的到期
        // 判据自然落到其后第一个交易日）；未给则取 **ex 日的下一个交易日**——A 股送转/
        // 配股股份的上市至少滞后一个交易日，取 ex 日即时到账等于把不可卖窗口当零。
        // 窗末已无下一交易日 → 哨兵 `kListDateBeyondWindow`：整跑不到账（留在在途账），
        // 而不是退化成 ex 日即时——后者凭空多出一批当日可卖股，方向乐观。
        if (ev.shares_list_date != 0) {
            ev.effective_list_date = ev.shares_list_date;
            // CSV 表达的是「ex 日即到账」，但 ex 被归日前移 → 到账实际变成滞后。
            // 这是归日的副作用而非数据事实，单独计数披露。
            ev.list_date_lagged_by_rebase =
                (ev.shares_list_date == ev.raw_ex_date && ev.ex_date != ev.raw_ex_date);
        } else {
            const auto next = std::upper_bound(trading_dates.begin(),
                                               trading_dates.end(), ev.ex_date);
            ev.effective_list_date =
                (next != trading_dates.end()) ? *next : kListDateBeyondWindow;
            ev.list_date_defaulted = true;
        }
        // record_date 同样归到交易日；早于窗首时置 0 并**标记**（施加点退化为
        // 「ex 日初持仓」，与「未提供」同算法但风险不同——见 record_before_window）。
        if (ev.record_date != 0) {
            const uint32_t rec = last_trading_date_le(trading_dates, ev.record_date);
            if (rec >= first) {
                ev.record_date = rec;
            } else {
                ev.record_date = 0;
                ev.record_before_window = true;
            }
        }
        kept.push_back(std::move(ev));
    }
    rows_ = std::move(kept);
    reindex_();
}

const std::vector<SelCorporateAction>*
CorporateActionCalendar::find_ex_date(uint32_t date) const {
    const auto it = by_ex_date_.find(date);
    return (it == by_ex_date_.end()) ? nullptr : &it->second;
}

void CorporateActionCalendar::reindex_() {
    by_ex_date_.clear();
    for (const auto& ev : rows_) by_ex_date_[ev.ex_date].push_back(ev);
    // 施加序 = 原始 ex_date 升序（同 raw 日内保持 CSV 行序）：归日把同 code 的多个
    // 非交易日事件并到同一交易日时，复合公式不可交换，必须按原时间序逐条施加。
    for (auto& kv : by_ex_date_) {
        std::stable_sort(kv.second.begin(), kv.second.end(),
                         [](const SelCorporateAction& a, const SelCorporateAction& b) {
                             return a.raw_ex_date < b.raw_ex_date;
                         });
    }
}

// ── CaReceivableLedger ──────────────────────────────────────────────────────

void CaReceivableLedger::accrue_cash(const std::string& code, uint32_t pay_date,
                                    int64_t amount_fp) {
    if (amount_fp == 0) return;
    pending_cash_.push_back(PendingCash{code, pay_date, amount_fp});
    outstanding_cash_fp_ += amount_fp;
}

std::vector<CaReceivableLedger::PendingCash>
CaReceivableLedger::take_due_cash(uint32_t date) {
    std::vector<PendingCash> due;
    size_t w = 0;
    for (size_t i = 0; i < pending_cash_.size(); ++i) {
        if (pending_cash_[i].pay_date <= date) {
            outstanding_cash_fp_ -= pending_cash_[i].amount_fp;
            due.push_back(pending_cash_[i]);
            continue;
        }
        pending_cash_[w++] = pending_cash_[i];
    }
    pending_cash_.resize(w);
    return due;
}

void CaReceivableLedger::accrue_shares(const std::string& code, uint32_t list_date,
                                       int64_t qty_fp, int64_t cost_fp) {
    if (qty_fp <= 0) return;
    pending_shares_.push_back(PendingBonus{code, list_date, qty_fp, cost_fp});
}

std::vector<CaReceivableLedger::PendingBonus>
CaReceivableLedger::take_pending_shares(const std::string& code) {
    std::vector<PendingBonus> taken;
    size_t w = 0;
    for (size_t i = 0; i < pending_shares_.size(); ++i) {
        if (pending_shares_[i].code == code) {
            taken.push_back(pending_shares_[i]);
            continue;
        }
        pending_shares_[w++] = pending_shares_[i];
    }
    pending_shares_.resize(w);
    return taken;
}

std::vector<CaReceivableLedger::PendingBonus>
CaReceivableLedger::take_due_shares(uint32_t date) {
    std::vector<PendingBonus> due;
    size_t w = 0;
    for (size_t i = 0; i < pending_shares_.size(); ++i) {
        if (pending_shares_[i].list_date <= date) {
            due.push_back(pending_shares_[i]);
            continue;
        }
        pending_shares_[w++] = pending_shares_[i];
    }
    pending_shares_.resize(w);
    return due;
}

}  // namespace xt_bt_sel
