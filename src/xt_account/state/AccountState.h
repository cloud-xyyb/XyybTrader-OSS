#pragma once

#include <string>
#include <unordered_map>
#include <cstdint>
#include <memory>
#include "xt_config/catalog/XtCommodityCatalog.h"

struct XtAccountEvent;

/// @brief reducer 背后的可变账户账本对象。
///
/// 保存账户资金、持仓表和订单冻结 ledger；不负责线程同步、snapshot 发布或
/// mark price 管理。写入口只暴露给 `apply_event()`，调用方通过
/// `AccountStateStore` 提交事件并读取账户/持仓/风控快照。
///
/// @note 非线程安全；live 路径由 AccountStateStore worker 线程串行写入，再发布
///       immutable snapshot 给读侧。
class AccountState {
public:
    /// @brief AccountState 构造配置。
    struct Config {
        double initial_cash = 1000000.0;  ///< 初始资金；构造时转换为 fixed-point。
        bool t1_enabled = false;          ///< A 股 T+1 可平口径；开启后 total 可平量扣除当日新增持仓。
        bool futures_mode = false;        ///< 期货模式；开启后 PnL / 保证金按合约乘数计算。

        /// 品种目录（non-owning, nullable）；期货模式下用于查 vol_scale。
        const XtCommodityCatalog* catalog = nullptr;
    };

    /// @brief 单订单冻结资金 ledger entry，用于维护账户级 frozen_cash 不变量。
    ///
    /// 该结构不表达订单生命周期；订单状态归 XtOrderRuntimeBook。这里仅记录
    /// AccountState 释放冻结保证金所需的剩余冻结额、已处理成交量和每手保证金。
    struct FrozenOrderSlot {
        int64_t frozen   = 0;   ///< 当前剩余冻结保证金（fixed-point）。
        int64_t filled   = 0;   ///< 已处理的累计成交手数（fixed-point）。
        int64_t per_unit = 0;   ///< 每手保证金（fixed-point）。
    };

    // ── 生命周期与会话 ──────────────────────────────────────────────
    /// @brief 按配置初始化账户状态。
    /// @note `Config::catalog` 只保存 non-owning 指针，不延长品种目录生命周期。
    /// @throws std::invalid_argument 当 futures_mode && catalog==nullptr（fail-fast 取代运行期静默 1× 退化）。
    explicit AccountState(const Config& cfg);

    // ── 账户与持仓查询 ──────────────────────────────────────────────
    /// @brief 账户现金余额（fixed-point）。
    [[nodiscard]] int64_t cash() const;
    /// @brief 当前冻结保证金总额（fixed-point），与 `order_slots()` 同源维护。
    [[nodiscard]] int64_t frozen_cash() const;
    /// @brief 订单冻结 ledger 的影子总额（fixed-point），恒等于 Σ order_slots()[*].frozen
    /// （在 slot 变更同点维护，含重复 accepted 覆盖退旧值）。与 frozen_cash() 分属
    /// 两条独立维护路径：两者分歧即 frozen audit 要暴露的账本不一致信号。
    [[nodiscard]] int64_t frozen_by_orders() const;
    /// @brief 当前可用现金（fixed-point），等于 `cash() - frozen_cash()`。
    [[nodiscard]] int64_t available_cash() const;
    /// @brief 累计已实现盈亏（fixed-point）。
    [[nodiscard]] int64_t realized_pnl() const;
    /// @brief 累计手续费（fixed-point）。
    [[nodiscard]] int64_t fees() const;
    /// @brief 是否按期货账户语义处理成交、PnL 和保证金。
    [[nodiscard]] bool futures_mode() const;

    /// @brief 返回构造时注入的品种目录；可能为 nullptr。
    /// @note AccountState 不拥有该对象；调用方必须保证生命周期覆盖本状态对象。
    [[nodiscard]] const XtCommodityCatalog* commodity_catalog() const;

    /// @brief 返回冻结资金 ledger 只读视图。
    /// @warning 仅供 AccountStateStore 审计 frozen_cash 不变量；不是订单生命周期 API。
    const std::unordered_map<uint64_t, FrozenOrderSlot>& order_slots() const;

    /// @brief 当前状态是否维护今昨仓拆分；T+1 或 futures mode 下为 true。
    [[nodiscard]] bool has_today_split() const;

    /// @brief position_readout() 返回的单合约持仓只读视图。
    ///
    /// 包含内部 Position 的数量、均价、broker frozen 原始字段，也带出净持仓、
    /// 净方向均价和今昨可平量等派生口径。调用方拿到的是值拷贝，不持有内部
    /// Position 指针；未知 code 返回全默认值。
    struct PositionReadout {
        int64_t qty = 0;                          ///< 净持仓 long - short
        int64_t avg_price = 0;                    ///< 净方向均价；对锁/无仓为 0
        int64_t long_qty = 0;
        int64_t long_avg_price = 0;
        int64_t short_qty = 0;
        int64_t short_avg_price = 0;
        int64_t long_today_qty = 0;
        int64_t short_today_qty = 0;
        int64_t broker_long_frozen_fp = 0;
        int64_t broker_short_frozen_fp = 0;
        int64_t broker_frozen_last_update_ms = 0;
        int64_t available_sell_today_qty = 0;
        int64_t available_sell_total_qty = 0;
        int64_t available_cover_today_qty = 0;
        int64_t available_cover_total_qty = 0;
    };
    /// @brief 一次查找并返回指定 code 的完整持仓视图。
    ///
    /// 用于 snapshot 构建路径，避免同一 code 多次查持仓表；未知 code 返回全默认值。
    [[nodiscard]] PositionReadout position_readout(const char* code) const;

    /// @brief 遍历持仓 map 的全部 code（含零仓项）；账户级聚合的唯一迭代源。
    /// @note 迭代序不确定（unordered_map）；消费方只做可交换的 int64 求和，序无关。
    ///       key 引用仅回调期间有效；不暴露 Position 本体（读侧走 position_readout）。
    /// @note 仅 writer 线程可调（与其它直读口同约）：pos_ 无锁，跨线程迭代是 race。
    template <class Fn>
    void for_each_position_code(Fn&& fn) const {
        for (const auto& kv : pos_) {
            fn(kv.first);
        }
    }

private:
    // AccountState 的唯一写入口：reducer 将事件翻译为状态变更。
    friend void apply_event(AccountState& account, const XtAccountEvent& ev);

    // ── 持仓状态模型 ──────────────────────────────────────────────
    /// @brief 单合约持仓状态；数量、均价、broker frozen 均为 fixed-point。
    ///
    /// long_today / short_today 只在 T+1 或平今语义启用时参与可平量计算。
    /// broker frozen 是 venue 回报的总冻结量，用于 close 风控与本地 outstanding
    /// 取严；非 CTP venue 默认为 0。
    struct Position {
        int64_t long_qty       = 0;   ///< 多头持仓手数（fp）
        int64_t long_today     = 0;   ///< 多头今日开仓手数（fp，用于平今/T+1）
        int64_t long_avg_price = 0;   ///< 多头均价（fp）
        int64_t short_qty      = 0;   ///< 空头持仓手数（fp）
        int64_t short_today    = 0;   ///< 空头今日开仓手数（fp）
        int64_t short_avg_price= 0;   ///< 空头均价（fp）
        int64_t broker_long_frozen_fp  = 0; ///< broker 侧多头冻结总量（CTP push / XTP 由 sellable 派生）；无源 venue 为 0。
        int64_t broker_short_frozen_fp = 0; ///< broker 侧空头冻结总量（同上）；无源 venue 为 0。
        int64_t broker_frozen_last_update_ms = 0; ///< broker frozen 最近 push steady ms；0 表示缺失。
    };

    // ── 内部工具函数 ──────────────────────────────────────────────
    /** @brief 获取或创建合约持仓槽（写路径）。 */
    Position& get_pos(const char* code);

    /**
     * @brief 查找合约持仓槽（只读路径）。
     * @return 指向 Position 的指针，若合约无持仓则返回 nullptr。
     */
    const Position* find_pos(const char* code) const;

    /// @brief 计算净方向均价；无仓或完全对锁时返回 0。
    static int64_t avg_price_of_(const Position& p);
    /// @brief 计算多头可平量；is_today=true 取今仓，T+1 total 口径扣除今仓。
    static int64_t available_sell_of_(const Position& p, bool is_today, bool t1_enabled);
    /// @brief 计算空头可平量；is_today=true 取今仓，T+1 total 口径扣除今仓。
    static int64_t available_cover_of_(const Position& p, bool is_today, bool t1_enabled);

    // ── reducer 写入口 ────────────────────────────────────────────
    /// @brief 进入新 session；启用今昨仓拆分时清零 today 计数。
    void on_session_begin();
    /// @brief 仓位域 recovery 擦除：清空持仓、冻结资金 ledger（pos_ / order_slots_ / frozen_cash_）。
    /// @note 保留 cash / realized_pnl / fees 账务域：仓位 recovery 快照不携带资金真值，
    ///       账务域清零会让 available_cash 归零且无人再锚定；cash 由 venue account
    ///       snapshot（SetCash）单独覆盖。
    void reset_positions();
    /// @brief 绝对覆盖现金余额（fixed-point），用于 broker/recovery 账户快照。
    void set_cash(int64_t cash);
    /// @brief 用 broker/recovery 持仓快照覆盖单合约持仓。
    /// @param code 合约代码；写入路径会为缺失合约创建持仓槽。
    /// @param long_qty 多头持仓手数（fixed-point）。
    /// @param long_avg_price 多头持仓均价（fixed-point）。
    /// @param short_qty 空头持仓手数（fixed-point）。
    /// @param short_avg_price 空头持仓均价（fixed-point）。
    /// @param long_today 多头今仓手数（fixed-point），写入时 clamp 到 `[0, long_qty]`。
    /// @param short_today 空头今仓手数（fixed-point），写入时 clamp 到 `[0, short_qty]`。
    /// @param broker_long_frozen_fp broker-side 多头冻结总量；非 CTP venue 传 0。
    /// @param broker_short_frozen_fp broker-side 空头冻结总量；非 CTP venue 传 0。
    /// @param broker_frozen_last_update_ms broker frozen push steady ms；0 表示缺失。
    void set_position(const char* code,
                      int64_t long_qty,
                      int64_t long_avg_price,
                      int64_t short_qty,
                      int64_t short_avg_price,
                      int64_t long_today = 0,
                      int64_t short_today = 0,
                      int64_t broker_long_frozen_fp  = 0,
                      int64_t broker_short_frozen_fp = 0,
                      int64_t broker_frozen_last_update_ms = 0);
    /// @brief 接收订单 accepted 事件并冻结保证金；无效 order_id 或非正保证金会被忽略。
    /// @param required_margin 本订单需要冻结的保证金（fixed-point）。
    /// @param per_unit_margin 每手保证金（fixed-point），用于按成交增量释放冻结。
    void on_order_accepted(uint64_t order_id, int64_t required_margin, int64_t per_unit_margin);
    /// @brief 接收订单累计成交/终态事件，按成交增量和终态释放冻结保证金。
    /// @note `filled` 是累计成交量；`is_final=true` 时释放剩余冻结并删除订单槽。
    void on_order_update(uint64_t order_id, int64_t filled, bool is_final);
    /// @brief 应用日终结算增量；realized PnL 加入现金，settlement fee 从现金扣除。
    /// @param realized_delta 本次结算确认的已实现盈亏增量（fixed-point）。
    /// @param settlement_fee 本次结算手续费（fixed-point）。
    void on_daily_settlement(int64_t realized_delta, int64_t settlement_fee);
    /// @brief 应用出入金增量；正数入金，负数出金。
    /// @param amount 出入金金额（fixed-point）。
    void on_cash_transfer(int64_t amount);
    /// @brief 应用成交事件，更新持仓、现金、手续费和 realized PnL。
    /// @param qty 成交手数（fixed-point）。
    /// @param price 成交价格（fixed-point）。
    /// @param fee 成交手续费（fixed-point）。
    /// @param is_buy true 表示买入，false 表示卖出。
    /// @param is_close true 表示按平仓语义处理，false 允许剩余量开仓。
    /// @param is_today true 表示平今/今仓语义；仅在启用今昨拆分时影响 today 计数。
    void on_trade_ex(const char* code,
                     bool is_buy,
                     int64_t qty,
                     int64_t price,
                     int64_t fee,
                     bool is_close,
                     bool is_today);

    // ── 账户级账务与模式状态 ──────────────────────────────────────────────
    int64_t cash_ = 0;            ///< 账户总资金（fp）。
    int64_t frozen_cash_ = 0;     ///< 账户级冻结保证金（fp，frozen margin）。
    int64_t frozen_by_orders_ = 0;///< 订单冻结 ledger 影子总额（fp，≡ Σ order_slots_[*].frozen）。
    int64_t realized_pnl_ = 0;    ///< 账户累计已实现盈亏（fp，realized P&L）。
    int64_t fees_ = 0;            ///< 账户累计手续费（fp）。
    bool t1_enabled_ = false;     ///< A 股 T+1 可平口径开关；total 可平量扣除 today。
    bool futures_mode_ = false;   ///< 期货账户模式开关。

    /// @brief 字符串 key 的持仓 map；get_pos()/find_pos() 的唯一存储。
    std::unordered_map<std::string, Position> pos_;

    /// 品种目录（non-owning, nullable）；期货模式下查 vol_scale 用于 PnL 计算。
    const XtCommodityCatalog* catalog_ = nullptr;

    /// @brief 账户冻结资金 ledger：order_id → FrozenOrderSlot。
    ///
    /// 与 frozen_cash_ 同源维护；成交、撤单、拒单只通过该表释放剩余冻结保证金。
    std::unordered_map<uint64_t, FrozenOrderSlot> order_slots_;
};
