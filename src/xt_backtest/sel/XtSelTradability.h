#pragma once

/// @file
/// @brief SEL 回测 tradability(停牌/涨跌停)输入契约:CSV 日历 + 纯函数判定。
///
/// 输入 CSV schema: `date,code,is_open,is_limit_up,is_limit_down`
///   - date: YYYYMMDD(与 trading dates 同历)
///   - code: 与引擎 code **逐字符精确匹配**(如 `600000.SH`,不走 catalog normalize_key)
///   - 三个 flag 严格 0/1;非法值该行 skip + warn(正确性输入宁缺勿错)
/// 缺行 = 完全可交易(per-row fail-open,研究侧生成全 panel)。

#include <cstdint>
#include <string>
#include <unordered_map>

namespace xt_bt_sel {

/// @brief 单 (date,code) 的可交易性 flags;默认值 = 完全可交易。
struct TradabilityFlags {
    bool is_open       = true;   ///< 0=停牌,当日不成交
    bool is_limit_up   = false;  ///< 1=涨停,拒 BUY
    bool is_limit_down = false;  ///< 1=跌停,拒 SELL
};

/// @brief tradability gate 判定结果;非 Ok 一律整笔拒(SEL close 撮合无盘口深度概念)。
enum class TradabilityVerdict { Ok, Suspended, LimitUpBuy, LimitDownSell };

/// @brief 纯函数判定:缺行(nullptr flags)= 完全可交易。
/// @note 停牌优先于涨跌停(is_open=0 时 BUY/SELL 全拒,不再看 limit flags)。
[[nodiscard]] TradabilityVerdict check_tradability(const TradabilityFlags* flags,
                                                   bool is_buy) noexcept;

/// @brief tradability CSV 日历: date,code,is_open,is_limit_up,is_limit_down。
/// @note 配置了路径但文件不可读/0 有效数据行 → load 返回 false(fail-fast,调用方终止)。
/// @note 内存 O(CSV 有效行数)全量驻留;全 A 股全 panel(~5000 code × 250 日/年
///       ≈ 每年百万行)多年回测达千万行量级,注意规模。
class TradabilityCalendar {
public:
    using CodeFlagsMap = std::unordered_map<std::string, TradabilityFlags>;

    /// @brief 读 CSV;header 容忍(首列非数字行 skip)、CRLF 容忍、非法行 skip + warn。
    /// @return false = 文件不可读或 0 有效数据行,err 填原因。
    bool load(const std::string& csv_path, std::string& err);

    /// @brief 查 (date,code);缺行返回 nullptr(= 完全可交易)。
    /// @note 测试/观测用途;runner 热查询走 find_date + 当日 map。
    [[nodiscard]] const TradabilityFlags* find(uint32_t date, const std::string& code) const;

    /// @brief 查某日全部 code 的 flags map;该日无任何行返回 nullptr。
    [[nodiscard]] const CodeFlagsMap* find_date(uint32_t date) const;

    [[nodiscard]] size_t size() const { return total_rows_; }         ///< 有效数据行数(日志用)
    [[nodiscard]] size_t date_count() const { return by_date_.size(); }  ///< 覆盖 date 数(覆盖度日志用)

private:
    std::unordered_map<uint32_t, CodeFlagsMap> by_date_;
    size_t total_rows_ = 0;
};

}  // namespace xt_bt_sel
