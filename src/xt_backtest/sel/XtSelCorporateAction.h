#pragma once

/// @file
/// @brief SEL 回测 corporate-action（除权除息 / 配股 / 换股 / 退市）输入契约：
///        三日期 CSV 日历 + BT-only 应收账（现金分红 pay-date 应收 + 在途新股）。
///
/// 输入 CSV schema（9–13 列，第 10 列起可选，逐列可截断）：
/// `code,record_date,ex_date,pay_date,cash_per_share,bonus_transfer_per_share,`
/// `rights_per_share,rights_price,is_delist`
/// `[,shares_list_date[,conversion_to[,conversion_ratio[,cash_in_lieu]]]]`
///   - 日期均为 **严格 YYYYMMDD**：恰 8 位数字 + 真实日历域（月 1-12、日 ≤ 当月
///     天数，含闰年）；`pay_date` / `shares_list_date` 允许空或全 0（= 未提供）。
///     短位数（`2026011`）与非法月日（`20261399`）一律 fail-fast——前者会被窗口
///     判据当成窗前事件吞掉整笔分红，后者让应收/新股永不到期
///   - 首行若以字面 `code` 开头即视为表头，其**全部列名与列序**必须与上面的序列
///     逐列精确一致（数据行按下标取值，错序表头会让 `conversion_ratio` 读成
///     `cash_in_lieu` 之类，比例静默放大且无任何报错）；不匹配 fail-fast
///   - `code` 与引擎 code 逐字符精确匹配（不走 catalog normalize_key）
///   - `is_delist=1` 行的其余数值列必须为 0、`conversion_to` 必须为空（终止上市日，
///     无分红/送转/配股/换股语义）
///   - `rights_per_share` 与 `rights_price` 同为正或同为 0（缺一即配股无法定价，
///     或配股价挂在没有配股的行上——两者都是数据错位）
///   - `conversion_to` 非空 = 换股行（吸收合并 / 证券变更）：`conversion_ratio` 必须
///     为正、目标 code 不得等于本 code，且其余数值列必须为 0（换股是独立事件型）；
///     `conversion_to` 为空时 `conversion_ratio` / `cash_in_lieu` 必须为 0
///
/// 与 tradability CSV 的 per-row fail-open 不同：CA 直接改钱与持仓，坏行一律
/// fail-fast（静默跳过一笔分红 = 账本静默失真），(code,ex_date) 重复亦拒。
///
/// @note 内存 O(有效行数) 全量驻留；A 股全 panel 多年（每年 ~万级事件）无压力。

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace xt_bt_sel {

/// @brief 配股（rights issue）的处置口径。
///
/// 两者的除权参考价用**同一个全式** `(prev − d + r·p_r) / (1 + b + r)`：配股是否认购
/// 不改变市场的除权基准，弃权的损失正是「参考价按摊薄后算、但没拿到便宜的新股」这
/// 一差额，如实进权益。
enum class RightsPolicy {
    kSubscribe = 0,  ///< 缴款认购（现金不足时按可缴整股截断并计数）
    kForfeit,        ///< 弃权：不缴款、不获配，除权损失如实体现
};

/// @brief `RightsPolicy` 的稳定字符串名（summary.json 字段值 / yaml 取值）。
[[nodiscard]] const char* rights_policy_name(RightsPolicy p) noexcept;

/// @brief 解析 yaml `selector.rights_policy` 取值。
/// @return false = 非法取值（调用方 fail-fast）。
[[nodiscard]] bool parse_rights_policy(const std::string& s, RightsPolicy& out) noexcept;

/// @brief 「到账日在回测窗之外」哨兵：`effective_list_date` 取此值时新股永不到期，
///        整跑留在在途账（收尾 `receivable_outstanding_bonus` 可见）。
///
/// 窗末 ex 日已无下一交易日时用它，而不是退化成「ex 日即时到账」：后者方向乐观
/// （凭空多出一批当日即可卖的股），把模型的不可卖窗口从「至少一个交易日」压成零。
constexpr uint32_t kListDateBeyondWindow = 0xFFFF'FFFFu;

/// @brief 单条 corporate action（CSV 一行）。
struct SelCorporateAction {
    std::string code;
    uint32_t record_date = 0;        ///< 股权登记日；0 = 未提供（退化取 ex 日初持仓）
    uint32_t ex_date     = 0;        ///< 除权除息日 / 终止上市日（施加点，归日后= 交易日）
    uint32_t raw_ex_date = 0;        ///< CSV 原始 ex_date（归日前）；同日多事件的施加序基准
    uint32_t pay_date    = 0;        ///< 派息日；0 = 未提供（现金分红 ex 日直接到账）
    uint32_t shares_list_date = 0;   ///< CSV 原值；0 = 未提供（缺省见 effective_list_date）
    /// 新股（送转 + 配股）实际到账日：CSV 给了就用它，未给则取 **ex 日的下一个交易日**
    /// （A 股送转/配股股份的上市日至少滞后一个交易日；ex 日即可卖是把不可卖窗口当零）。
    /// `kListDateBeyondWindow` = 窗末已无下一交易日 → 整跑不到账（计数披露）。
    uint32_t effective_list_date = 0;
    double   cash_per_share = 0.0;   ///< 每股现金分红（税前）
    double   bonus_transfer_per_share = 0.0;  ///< 每股送转股数（送股+转增合并口径）
    double   rights_per_share = 0.0; ///< 每股配股数（配股比例）
    double   rights_price     = 0.0; ///< 配股价（每股缴款额）
    std::string conversion_to;       ///< 换股目标 code；空 = 非换股行
    double   conversion_ratio = 0.0; ///< 每旧股换得的新股数（换股行必须 > 0）
    double   cash_in_lieu     = 0.0; ///< 阀下零股的每股现金替代价（0 = 零股直接抹去）
    bool     is_delist = false;      ///< 终止上市日：持仓强制清算
    bool     rights_skipped = false; ///< allow_skip_rights 下整行跳过（只发 events 行）
    /// CSV 未给 shares_list_date，到账日取的是缺省值（ex 日下一交易日）。
    bool     list_date_defaulted = false;
    /// CSV 显式给的 shares_list_date 等于**原始** ex_date（= 数据表达「ex 日即到账」），
    /// 但 ex_date 被归日前移到更早的交易日 → 到账日实际变成滞后。归日的副作用而非
    /// 数据事实，单独计数披露。
    bool     list_date_lagged_by_rebase = false;
    /// CSV 提供了 record_date 但它早于回测窗首交易日（归日后被置 0）。确权基数退化
    /// 为 ex 日初持仓，与「未提供」不同：窗首后建的仓会拿到不该拿的分红（过度计提），
    /// 故两种退化分开计数披露。
    bool     record_before_window = false;
};

/// @brief CA 日历加载结果分类；调用方据此选退出码。
enum class CaLoadStatus {
    kOk,       ///< 加载成功
    kIoError,  ///< 文件不可读
    kBadRow,   ///< 结构/取值非法（列数、日期序、事件型混填、重复键、0 有效行）
};

/// @brief corporate action 日历：按 ex_date 索引。
///
/// 载入后必须调用 `align_to_trading_dates()`：把窗内非交易日事件归到 ≤ 该日的
/// 最后一个交易日（绝不静默丢事件），窗外事件整条忽略并计数（归日到窗内会造成
/// future leak——把未来才发生的退市/分红搬进回测窗）。
class CorporateActionCalendar {
public:
    /// @brief 归日/忽略统计（summary 披露用）。
    struct AlignStats {
        size_t rebased_to_prev_trading_date = 0;  ///< 窗内非交易日 → 前一交易日
        size_t ignored_before_window        = 0;  ///< 事件日早于窗首交易日
        size_t ignored_after_window         = 0;  ///< 事件日晚于窗末交易日
    };

    /// @param allow_skip_rights true 时 rights 行整条跳过（标 rights_skipped）而非 fail-fast。
    /// @param err 失败原因（kOk 时不写）。
    [[nodiscard]] CaLoadStatus load(const std::string& csv_path, bool allow_skip_rights,
                                   std::string& err);

    /// @brief 把 ex_date / record_date 对齐到交易日历；窗外事件整条剔除；同时定出
    ///        `effective_list_date`（CSV 未给 shares_list_date 时取 ex 日的下一交易日）。
    /// @param trading_dates 升序交易日（回测窗内，已过滤）。
    void align_to_trading_dates(const std::vector<uint32_t>& trading_dates,
                                AlignStats& stats);

    /// @brief 查某 ex_date 的全部事件；该日无事件返回 nullptr。
    /// @note 行序 = 原始 `raw_ex_date` 升序，同 raw 日内保持 CSV 行序。归日会把同一
    ///       code 的多个非交易日事件并到同一交易日（如连休期内的两笔），此时施加序
    ///       必须是原时间序——复合公式（参考价、送转因子）不可交换，逆序结果不同。
    [[nodiscard]] const std::vector<SelCorporateAction>* find_ex_date(uint32_t date) const;

    /// @brief 全部事件（align 之后 = 窗内事件）；record-date 仓位快照排程用。
    [[nodiscard]] const std::vector<SelCorporateAction>& rows() const noexcept { return rows_; }

    [[nodiscard]] size_t size() const noexcept { return rows_.size(); }
    [[nodiscard]] size_t rights_skipped_count() const noexcept { return rights_skipped_; }

private:
    void reindex_();

    std::vector<SelCorporateAction> rows_;
    std::unordered_map<uint32_t, std::vector<SelCorporateAction>> by_ex_date_;
    size_t rights_skipped_ = 0;
};

/// @brief BT-only 应收账：现金分红 pay-date 应收 + 在途新股（ex→list 窗口的送转股与
///        配股认购股）。
///
/// 不进 account——account 没有 receivable 原语，且应收不可用于买入。portfolio
/// 权益口径 = account equity + 应收现金 + 在途新股市值，五个消费点（daily_equity /
/// trades.csv equity_after / summary final_equity / engine sizing 分母 /
/// daily_positions）统一消费之，否则 ex→pay 窗口权益先凹后跳。
class CaReceivableLedger {
public:
    /// @brief 一笔在途新股（ex 日确权/缴款、list 日到账）。
    ///
    /// ex 日**不摊** avg（qty 没变，摊了总成本就凭空缩水，窗口内卖出的 realized 会
    /// 错），到账时按「总成本 = 旧总成本 + 本笔取得成本」重算
    /// avg = (旧 qty×旧 avg + cost) / (旧 qty + qty)。送转股 cost=0，公式退化为
    /// 「总成本不变」；配股股 cost = 缴款额（现金已在 ex 日扣）。窗口内卖出因此按
    /// 未摊 avg 结账（逐笔归因偏早，累计 realized 正确）。
    struct PendingBonus {
        std::string code;
        uint32_t list_date = 0;
        int64_t  qty_fp    = 0;   ///< 待到账股数（fp，整股）
        int64_t  cost_fp   = 0;   ///< 本笔取得成本（fp）；送转 0，配股 = 缴款额
    };

    /// @brief 一笔未到账现金分红（ex 日确权、pay_date 到账）。
    struct PendingCash {
        std::string code;
        uint32_t pay_date  = 0;
        int64_t  amount_fp = 0;   ///< 税后净额（fp）
    };

    /// @brief 记一笔现金分红应收（pay_date 到达日转现金）。
    void accrue_cash(const std::string& code, uint32_t pay_date, int64_t amount_fp);

    /// @brief 取出并清除 pay_date ≤ date 的到期应收。
    /// @return 到期明细（逐笔带 code，供 events 行归因）。
    [[nodiscard]] std::vector<PendingCash> take_due_cash(uint32_t date);

    /// @brief 记一笔在途新股。
    /// @param cost_fp 本笔取得成本（送转 0，配股 = 缴款额）；到账日摊入 avg。
    void accrue_shares(const std::string& code, uint32_t list_date, int64_t qty_fp,
                       int64_t cost_fp = 0);

    /// @brief 取出并清除 list_date ≤ date 的到期在途新股。
    [[nodiscard]] std::vector<PendingBonus> take_due_shares(uint32_t date);

    /// @brief 取出并清除某 code 的全部在途新股（未到 list_date 的也取）。
    /// @note 退市/换股终止态用：ex→list 窗口内该 code 终止时在途新股必须一并了结，
    ///       否则其市值一直计在权益里、list 日又加回已消失的 symbol（「终止后复活仓」）。
    [[nodiscard]] std::vector<PendingBonus> take_pending_shares(const std::string& code);

    /// @brief 未到账现金应收合计（fp）。
    [[nodiscard]] int64_t outstanding_cash_fp() const noexcept { return outstanding_cash_fp_; }

    /// @brief 在途新股（未到账）；调用方按当日 mark 逐笔重估市值。
    [[nodiscard]] const std::vector<PendingBonus>& pending_shares() const noexcept {
        return pending_shares_;
    }

private:
    std::vector<PendingCash>  pending_cash_;
    std::vector<PendingBonus> pending_shares_;
    int64_t outstanding_cash_fp_ = 0;
};

}  // namespace xt_bt_sel
