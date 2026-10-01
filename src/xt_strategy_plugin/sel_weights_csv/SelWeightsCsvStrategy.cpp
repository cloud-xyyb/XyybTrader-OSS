#include "SelWeightsCsvStrategy.h"

#include <algorithm>
#include <cctype>   // std::isspace / std::isdigit（split/parse 逐字符判据）
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

/// @brief normalize 幅度告警阈值：|gross(sum|w|) - 1| 超过此值即认为发生了
///        non-trivial 缩放（而非浮点噪声），触发一次性告警。
constexpr double kNormalizeDriftWarnThreshold = 0.005;  // 0.5%

/// @brief 简单 csv 行解析：split by comma，trim whitespace。
std::vector<std::string> split_csv_line(const std::string& line) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : line) {
        if (c == ',') {
            out.push_back(cur);
            cur.clear();
        } else if (c != '\r' && c != '\n') {
            cur.push_back(c);
        }
    }
    out.push_back(cur);
    // trim spaces
    for (auto& s : out) {
        size_t l = 0;
        while (l < s.size() && std::isspace(static_cast<unsigned char>(s[l]))) ++l;
        size_t r = s.size();
        while (r > l && std::isspace(static_cast<unsigned char>(s[r - 1]))) --r;
        s = s.substr(l, r - l);
    }
    return out;
}

/// @brief 该年该月的天数（Gregorian，含闰年规则）；月越界返回 0。
/// @note 与 SEL 回测侧 corporate action CSV 解析同一判据；插件是独立编译的 .so
///       （只吃 api/ 头，不链 xt_backtest），故此处自带一份。
[[nodiscard]] int days_in_month(uint32_t year, uint32_t month) {
    static constexpr int kDays[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month < 1 || month > 12) return 0;
    if (month == 2) {
        const bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        return leap ? 29 : 28;
    }
    return kDays[month];
}

/// @brief 严格 YYYYMMDD 解析：恰 8 位数字 + [19000101, 29991231] + 真实日历域。
/// @param out 仅在返回 nullptr 时写入。
/// @return nullptr = 合法；否则为失败原因（静态字面量，直接进 err 消息）。
/// @note 显式 8 位约束不能省：`stoul` 接受任意长度再窄化到 uint32_t，
///       `429496729601` 会 wrap 进合法域混过范围检查。
[[nodiscard]] const char* parse_calendar_date(const std::string& s, uint32_t& out) {
    out = 0;
    if (s.size() != 8) return "date must be 8 digits (YYYYMMDD)";
    for (const char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return "date is not an integer";
    }
    const auto v = static_cast<uint32_t>(std::stoul(s));  // 8 位纯数字 → 不会抛
    if (v < 19000101u || v > 29991231u) return "date out of range";
    const uint32_t month = (v / 100) % 100;
    const uint32_t day   = v % 100;
    if (day == 0 || static_cast<int>(day) > days_in_month(v / 10000, month)) {
        return "date is not a real calendar day (YYYYMMDD)";
    }
    out = v;
    return nullptr;
}

/// @brief 钟面 HHMM 解析（1-4 位数字；`930` = 09:30）；空串 = 0（通配槽）。
/// @param out 仅在返回 nullptr 时写入。
/// @return nullptr = 合法；否则为失败原因（静态字面量，直接进 err 消息）。
/// @note 校验真实钟面（时 ≤ 23、分 ≤ 59）：`1075` 这类值永远不等于任何 fire 时刻，
///       该槽于是永不命中——跑出来是「少一次调仓」而不是错误，与 date 同判据。
/// @note 显式 `0000` 与空值同为 0，即落进通配槽。SEL 的 task 时刻是盘中钟面，
///       00:00 fire 不在可达域，故这一重合无实际歧义。
[[nodiscard]] const char* parse_clock_hhmm(const std::string& s, uint32_t& out) {
    out = 0;
    if (s.empty()) return nullptr;   // 空 = 通配
    if (s.size() > 4) return "time must be HHMM (at most 4 digits)";
    for (const char c : s) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return "time is not an integer";
    }
    const auto v = static_cast<uint32_t>(std::stoul(s));
    if (v / 100 > 23 || v % 100 > 59) return "time is not a real clock time (HHMM)";
    out = v;
    return nullptr;
}

}  // namespace

void SelWeightsCsvStrategy::init(const XtStrategyInitConfig& cfg) {
    weights_path_ = cfg.get_string("weights_path", "");
    normalize_    = cfg.get_bool("normalize", true);
    task_uid_     = cfg.get_string("task_uid", "");
}

void SelWeightsCsvStrategy::on_init(IStrategyContext* ctx) {
    if (weights_path_.empty()) {
        ctx->log_warn("SelWeightsCsvStrategy: weights_path is empty (no-op)");
        return;
    }
    // fail-closed：配置了 weights_path 就必须完整读到有效权重。
    // 静默跳过坏行/重复键会让「策略跑了但目标权重是残缺的」变成一次看似成功的
    // 回测——研究结论建立在无声丢失的数据上，比直接报错危险得多。
    // 抛异常 = runner 装配期 fail-fast（SEL BT rc=5）/ live per-sid fail-closed。
    std::string err;
    if (!load_weights_csv_(weights_path_, err)) {
        char buf[512];
        std::snprintf(buf, sizeof(buf),
                      "SelWeightsCsvStrategy: weights csv '%s' 不可用 — %s",
                      weights_path_.c_str(), err.c_str());
        ctx->log_warn(buf);
        throw std::runtime_error(buf);
    }
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "SelWeightsCsvStrategy: loaded weights csv '%s' (%zu dates, task_uid='%s')",
                  weights_path_.c_str(), slots_by_date_.size(), task_uid_.c_str());
    ctx->log_info(buf);
}

const SelWeightsCsvStrategy::WeightSlot*
SelWeightsCsvStrategy::resolve_slot_(uint32_t date, uint32_t time) const {
    const auto it = slots_by_date_.find(date);
    if (it == slots_by_date_.end()) return nullptr;
    // 「精确 → 通配」四级，取首个命中的**整槽**：跨槽合并权重会把两次 fire 的目标
    // 拼成一个从未被任何一行表达过的组合。
    const auto pick = [&](uint32_t want_time, const std::string& want_uid)
        -> const WeightSlot* {
        for (const auto& s : it->second) {
            if (s.effective_time == want_time && s.task_uid == want_uid) return &s;
        }
        return nullptr;
    };
    static const std::string kAny;
    if (!task_uid_.empty()) {
        if (const auto* s = pick(time, task_uid_)) return s;
    }
    if (const auto* s = pick(time, kAny)) return s;
    if (!task_uid_.empty()) {
        if (const auto* s = pick(0, task_uid_)) return s;
    }
    return pick(0, kAny);
}

void SelWeightsCsvStrategy::on_schedule(IStrategyContext* ctx,
                                          uint32_t curDate,
                                          uint32_t curTime,
                                          int64_t /*fireTime_ms*/) {
    const WeightSlot* slot = resolve_slot_(curDate, curTime);
    if (slot == nullptr) {
        // 本次 fire 无对应槽 — no-op（保留之前的 weights state）。
        return;
    }
    auto weights = slot->weights;  // copy 出来 normalize（不动 cache）
    if (normalize_) {
        const double gross = normalize_weights_(weights);
        if (gross > 0.0 && std::fabs(gross - 1.0) > kNormalizeDriftWarnThreshold) {
            ++normalize_drift_warn_count_;
            char wbuf[224];
            std::snprintf(wbuf, sizeof(wbuf),
                          "SelWeightsCsvStrategy: normalize 把 gross sum|w|=%.3f 缩放到 1.0"
                          "——若 0.95 类 sub-unity gross 是刻意的现金 buffer，请设 normalize:false",
                          gross);
            ctx->log_warn(wbuf);
        }
    }

    char buf[224];
    std::snprintf(buf, sizeof(buf),
                  "SelWeightsCsvStrategy: rebalance date=%u time=%u slot(time=%u,"
                  "task='%s') n_symbols=%zu",
                  curDate, curTime, slot->effective_time, slot->task_uid.c_str(),
                  weights.size());
    ctx->log_info(buf);

    ctx->set_target_weights(weights);
}

bool SelWeightsCsvStrategy::load_weights_csv_(const std::string& path, std::string& err) {
    slots_by_date_.clear();
    std::ifstream ifs(path);
    if (!ifs) {
        err = "cannot open file";
        return false;
    }

    auto fail_at = [&err](size_t lineno, const char* why) {
        char buf[192];
        std::snprintf(buf, sizeof(buf), "line %zu: %s", lineno, why);
        err = buf;
        return false;
    };

    std::unordered_set<std::string> seen_keys;  // "date|time|task|symbol" 去重键
    std::string line;
    size_t lineno = 0;
    bool first = true;
    size_t valid_rows = 0;
    // 可选列的列位（SIZE_MAX = 该列不在本文件里）；由表头定死，数据行按位取值。
    // 无表头（首行即数据行）时三者皆缺，按 canonical 3 列办。
    constexpr size_t kAbsent = static_cast<size_t>(-1);
    size_t col_asof = kAbsent, col_time = kAbsent, col_task = kAbsent;
    size_t expected_cols = 3;
    while (std::getline(ifs, line)) {
        ++lineno;
        // 空行 / 纯空白行（无分隔符）跳过——与 validator 的空行规则对齐；
        // 有分隔符的空字段行仍走下面的列数/取值校验。
        if (line.find_first_not_of(" \t\r\n") == std::string::npos) continue;
        if (first) {
            first = false;
            // 首行的合法形态：`date,symbol,weight` 打头的表头（其后可跟任意子集的
            // 可选列），或直接是数据行。旧的「首列非数字即当 header 跳过」会把坏行
            // （`BROKEN,foo,bar`）静默吞掉，与 fail-closed 契约相反，故表头名逐字比对。
            const auto cols = split_csv_line(line);
            const bool is_header = cols.size() >= 3 && cols[0] == "date" &&
                                   cols[1] == "symbol" && cols[2] == "weight";
            if (is_header) {
                for (size_t i = 3; i < cols.size(); ++i) {
                    size_t* slot = nullptr;
                    if      (cols[i] == "signal_asof_date") slot = &col_asof;
                    else if (cols[i] == "effective_time")   slot = &col_time;
                    else if (cols[i] == "task_uid")         slot = &col_task;
                    else {
                        // 未知列名：这份文件的 schema 跟本解析器认的不是一回事，
                        // 静默忽略等于放行一份没被理解的输入。
                        return fail_at(lineno,
                            "unknown optional column (allowed: signal_asof_date, "
                            "effective_time, task_uid)");
                    }
                    if (*slot != kAbsent) return fail_at(lineno, "duplicate column name");
                    *slot = i;
                }
                expected_cols = cols.size();
                continue;
            }
            if (cols.empty() || cols[0].empty() ||
                !std::isdigit(static_cast<unsigned char>(cols[0][0]))) {
                return fail_at(lineno,
                    "first line must be either a 'date,symbol,weight[,...]' header "
                    "or a data row");
            }
            // 首行是数据行 — fallthrough 处理
        }
        const auto parts = split_csv_line(line);
        // 列数严判（按表头定死的形态）：多出来的列此前被静默丢弃，而「多一列」通常
        // 意味着这份文件的 schema 跟本解析器认的不是一回事（错位一列 = symbol 读成
        // weight 也可能恰好过后面的取值校验）。少列同样拒。
        if (parts.size() != expected_cols) {
            char msg[128];
            std::snprintf(msg, sizeof(msg),
                          "expected exactly %zu columns (per header)", expected_cols);
            return fail_at(lineno, msg);
        }
        uint32_t date = 0;
        if (const char* why = parse_calendar_date(parts[0], date)) {
            // 含日历域（月 1-12、日 ≤ 当月天数，含闰年）：`20260231` 这类日期永远不
            // 是交易日，该行权重于是永不触发 —— 跑出来的是「少一次调仓」的另一个
            // 实验，而不是错误。判据与 CA CSV 解析一致。
            return fail_at(lineno, why);
        }
        // as-of 列族只校验不消费：它是研究 provenance（这行权重用到哪天为止的信息），
        // 由 validator / walk-forward gate 判未来函数，引擎不拿它做任何决策。校验仍
        // 要做——坏的 as-of 意味着这份 provenance 本身不可信，放行等于让下游的未来
        // 函数防线建立在没解析过的字段上。
        if (col_asof != kAbsent) {
            uint32_t asof = 0;
            if (const char* why = parse_calendar_date(parts[col_asof], asof)) {
                const std::string msg = std::string("signal_asof_date: ") + why;
                return fail_at(lineno, msg.c_str());
            }
        }
        // effective_time / task_uid：本行归属的槽坐标；空值 = 通配（见 header 注释）。
        uint32_t eff_time = 0;
        if (col_time != kAbsent) {
            if (const char* why = parse_clock_hhmm(parts[col_time], eff_time)) {
                const std::string msg = std::string("effective_time: ") + why;
                return fail_at(lineno, msg.c_str());
            }
        }
        const std::string task_uid = col_task != kAbsent ? parts[col_task] : std::string();
        const std::string& sym = parts[1];
        if (sym.empty()) return fail_at(lineno, "empty symbol");
        double w = 0.0;
        try {
            size_t consumed = 0;
            w = std::stod(parts[2], &consumed);
            if (consumed != parts[2].size()) return fail_at(lineno, "trailing junk in weight");
        } catch (...) {
            return fail_at(lineno, "weight is not a number");
        }
        if (!std::isfinite(w)) return fail_at(lineno, "weight is not finite");
        // 重复 (date,time,task,symbol)：last-wins 会静默丢掉一条真实意图，无法判断
        // 哪条才是对的。去重键含槽坐标——同日不同槽的同 symbol 是合法形态。
        if (!seen_keys.insert(std::to_string(date) + '|' + std::to_string(eff_time) +
                              '|' + task_uid + '|' + sym).second) {
            return fail_at(lineno, "duplicate (date,effective_time,task_uid,symbol) key");
        }
        auto& day = slots_by_date_[date];
        auto sit = std::find_if(day.begin(), day.end(), [&](const WeightSlot& s) {
            return s.effective_time == eff_time && s.task_uid == task_uid;
        });
        if (sit == day.end()) {
            day.push_back(WeightSlot{eff_time, task_uid, {}});
            sit = day.end() - 1;
        }
        sit->weights[sym] = w;
        ++valid_rows;
    }
    // 读循环靠 getline 返回 false 结束，而这既可能是 EOF 也可能是 I/O 错误：
    // 截断的文件在 badbit 下会被当成「读完了」，静默少几天权重。
    if (ifs.bad()) {
        err = "I/O error while reading (stream badbit set) — file may be truncated";
        return false;
    }
    if (valid_rows == 0) {
        err = "no valid weight rows";
        return false;
    }
    return true;
}

double SelWeightsCsvStrategy::normalize_weights_(std::unordered_map<std::string, double>& w) {
    // 按 symbol 排序后求和：unordered_map 遍历序随 hash seed / 实现变化，浮点加法
    // 不可结合 → 同一份 weights 在不同平台可归一出不同的 1e-16 级尾差，进而在
    // min_lot 截断的临界点上翻出整手差异。排序求和让归一化结果平台确定。
    std::vector<const std::pair<const std::string, double>*> sorted;
    sorted.reserve(w.size());
    for (const auto& kv : w) sorted.push_back(&kv);
    std::sort(sorted.begin(), sorted.end(),
              [](const auto* a, const auto* b) { return a->first < b->first; });

    double total = 0.0;
    for (const auto* kv : sorted) total += std::fabs(kv->second);
    if (total <= 0.0) return total;
    for (const auto* kv : sorted) {
        w[kv->first] /= total;
    }
    return total;
}

// ── plugin export（当前统一 5 符号 ABI，与 cta_sma_xover 同模式） ────────────
#include "xt_strategy_engine/StrategyABI.h"
XT_STRATEGY_ABI_EXPORTS(SelWeightsCsvStrategy)
