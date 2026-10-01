#pragma once
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "api/ICtaGateway.h"
#include "xt_account/state/AccountStateStore.h"
#include "xt_risk/config/XtRiskCfg.h"
#include "xt_risk/gateway/XtRiskGateway.h"
#include "xt_risk/backtest/XtBacktestRiskGuardContext.h"
#include "xt_config/catalog/XtCommodityCatalog.h"
#include "xt_config/catalog/XtFeeRuleBook.h"
#include "xt_cost/XtTradeCostConfig.h"
#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtOrder.h"
#include "api/IHftEngineObserver.h"
#include "xt_cost/XtFillCostApplier.h"

class IRejectSink;
#include "xt_runtime/order/XtOrderRuntimeBook.h"
#include "xt_account/api/AccountSnapshot.h"
#include "xt_engine/sim/XtCtaMatchEngine.h"
#include "xt_core/session/XtSessionPhase.h"
#include "xt_util/time/XtTime.h"

/// @brief CTA 回测执行链的订单 gateway。
///
/// 实现 `ICtaGateway`，持有 `XtCtaMatchEngine` 并串接 risk、cost、offset 推断和
/// fill/update 暂存。order book 与 account 的 domain 更新由 `XtCtaEngine` drain 后完成；
/// 本类通过 non-owning 指针引用这些 Runner-owned 组件。
class XtCtaBacktestOrderGateway final : public xt_runtime::ICtaGateway {
public:
    /// @brief CTA 回测基础设施的装配配置。
    ///
    /// `build_backtest_infra()` 直接消费本结构；新增字段需要同步该装配入口。
    struct Config {
        XtRiskCfg               risk;
        XtTradeCostConfig          cost;
        XtCommodityCatalog::Config commodities;
        XtFeeRuleBook::Config      fees;
        AccountState::Config       account;
    };

    XtCtaBacktestOrderGateway() = default;

    // ── 依赖绑定（Runner 构造后调用一次） ─────────────────────────────

    /// 绑定全部执行链依赖；内部同步绑定 MatchEngine。
    /// @note 所有指针为非拥有；调用方保证生命周期覆盖 XtCtaBacktestOrderGateway。
    /// @note CTA 走 intent-driven 模型：apply_fills_ 只做
    ///       cost/finalize/sidecar 后入队；engine drain 后完成 order_book/account
    ///       domain。策略层不触发 on_fill，下一 bar 通过 view 读取聚合持仓。
    /// @note catalog 可选（null = offset 路由 fence 不启用，Open 直通）；接入后
    ///       close_first 品种的 Open 意图走 XtOffsetRouter（T2.OFFSET-2）。
    void bind(xt_account::AccountStateStore*                        account,
              XtRiskGateway*                             risk_gw,
              XtBacktestRiskGuardContext*                risk_guard_ctx,
              xt_fill_cost::FillCostApplier*             cost,
              xt_runtime::order::XtOrderRuntimeBook*     order_book,
              const XtCommodityCatalog*                  catalog = nullptr);

    /// 添加 observer (report observer / trade capture)，按 add 顺序 fan-out。
    /// 非 owning; 调用方保证 observer 生命周期 >= gateway。
    void add_observer(xt_runtime::IHftEngineObserver* obs) {
        if (obs) observers_.push_back(obs);
    }
    void clear_observers() { observers_.clear(); }

    /// 绑定 reject sink；reject path 只调用 IRejectSink::on_reject()。
    void bind_reject_sink(IRejectSink* sink) { reject_sink_ = sink; }

    /// 当前 packed timestamp (YYYYMMDD*1e9 + HHMMSSmmm), 供 observer on_order_submit
    /// fan-out 与 reject path 使用。
    int64_t current_ts() const { return xt_time::packed_ts(last_date_, last_time_ms_); }

    // ── IExecutionGateway ──────────────────────────────────────────

    /// submit: 风控检查（模式 A）→ 入队 MatchEngine →
    ///         冻结保证金（order_accepted）→ order_book 登记。
    /// strategy_id 写入 `order_strategy_` sidecar，apply_fills_ 回写 trade.strategy_id。
    xt_runtime::SubmitResult submit(const char* code, bool is_buy, int64_t price_fp,
                                     int64_t qty_fp, XtTimeInForce tif,
                                     std::string_view user_tag, XtOffset offset,
                                     ExecFlags exec_flags,
                                     std::string_view strategy_id = "") override;

    /// 撤销指定订单，委托给 XtCtaMatchEngine::cancel（推 Canceled update 并移除挂单）
    bool cancel(uint64_t order_id) override;
    void stop(std::chrono::milliseconds timeout = std::chrono::seconds(5)) noexcept override {
        (void)timeout;
    }

    // ── 回测专用接口 ───────────────────────────────────────────────

    /// 更新市场价格并驱动挂单撮合（Engine.on_tick/on_bar 调用此方法）。
    /// 内部调用 MatchEngine.update_price + process_pending；
    /// 对每笔成交执行数据源加工：cost/finalize/sidecar → fills_buf_；
    /// domain 由 XtCtaEngine drain 后完成。
    /// @return 撮合成交笔数。
    int update_market_price(const char* code, int64_t price_fp,
                            uint32_t date, uint32_t time_ms) override;

    /// @brief bar 模式专用：以 next bar 的 open 价成交该合约所有 pending 单。
    ///
    /// 跳过限价检查，直接以 open_fp 成交 pending 中该合约的订单。
    /// 语义：bar[t] 产生的订单在 bar[t+1].open 成交。
    ///
    /// 调用时机（Runner 主循环）：
    ///   1. fill_pending_at_bar_open(bar[t].open)  ← 成交 bar[t-1] 的 pending
    ///   2. account mark_price ← bar[t].close
    ///   3. engine.feed_bar(bar[t])  ← 策略 on_bar 产生新 submit → pending
    ///
    /// @note 只由 bar-mode runner 主循环调用；tick 模式和实盘不使用此方法。
    int fill_pending_at_bar_open(const char* code, int64_t open_fp,
                                 uint32_t date, uint32_t time_ms);

    /// 立即成交（用于止损/bar-close，不经 pending 队列）。
    /// @param skip_risk true 时跳过风控（止损强制平仓专用）；
    ///                  跳过时亦不冻结保证金（无风控结果提供 margin 数据）
    bool fill_immediate(const char* code, bool is_buy, int64_t qty_fp,
                        int64_t price_fp, const char* tag, XtOffset offset,
                        uint32_t date, uint32_t time_ms,
                        bool skip_risk = false);

    /// 日结（ICtaGateway 接口实现）：委托 flush_session_end。
    int flush_session(uint32_t date, uint32_t time_ms,
                      bool allow_market_fill = true) override;

    // ICtaGateway drain overrides（pull 模式）
    //
    // 对齐 HFT backtest 分层：gateway 只产出 fills/updates 入 queue，engine
    // drain 后完成 domain（order_book / account / exec sink）。data-source
    // 层职责保留在 gateway：cost enrichment + finalize_fill_offsets（is_close
    // 判断需 account 读视图）+ listener tap（报告层）。
    //
    // Engine 在 on_tick / on_bar / on_clock 末尾调这些 drain，取出对应事件
    // 转发给 engine.on_order_update / on_trade 完成 domain 处理。

    /// @brief Drain 已到期的 fill 到 out。
    size_t drain_fills(uint32_t date, uint32_t time_ms,
                       std::vector<XtTradeRecord>& out) override;

    /// @brief 尚未被 drain 的回报数（fill + order update）。
    ///
    /// run 收尾时应为 0：非零意味着有回报从未送达 observer/engine —— 撤单的 frozen
    /// 释放与 order-book slot 清理都挂在 order update 的派发上，漏 drain 即静默泄漏。
    /// 供 runner 在收尾把该不变量写进 machine-readable 报告。
    [[nodiscard]] size_t pending_report_count() const noexcept {
        return fills_buf_.size() + updates_buf_.size();
    }

    /// @brief Drain 已产生的 order_update 到 out。
    size_t drain_order_updates(std::vector<XtOrderExecUpdate>& out) override;

    /// @brief 时钟推进入口(IReplayRuntime → XtCtaEngine::on_clock 转发)。
    ///
    /// 转发给 match_engine_.advance_clock 后,drain 释放出来的 visible trades
    /// 与 visible updates,完成回报链分发。回测稀疏 tick / 跨多个 tick 的延迟
    /// 事件由此入口推进可见性。
    void advance_clock(uint32_t date, uint32_t time_ms) override;

    /// 日结：市价单用尾价成交，限价单发 Canceled 后清空 pending。
    /// @param allow_market_fill 传入时间戳是否为 in-session 合法时刻；false 时市价单
    ///        改发 Canceled（见 XtCtaMatchEngine::flush_session_end）。
    /// @return 日结成交笔数
    int flush_session_end(uint32_t date, uint32_t time_ms, bool allow_market_fill = true);

    // ── 配置接口 ───────────────────────────────────────────────────

    /// @brief 配置内部 XtCtaMatchEngine 的物理时间参数。
    void configure_match_engine(const XtCtaMatchEngine::Config& cfg);

    /// @brief 设置 venue 类别用于集合竞价 phase 推断。
    /// @details 默认 Continuous24h（期货 / 加密始终 Continuous → 行为不变 bit-exact）；
    ///          A 股 SEL backtest 装配时需 set AShare 让 evaluate_risk_ 推断
    ///          eval_ctx.is_in_call_auction → 集合竞价时段拒非 GTC 单。
    void set_venue_kind(xt_core::XtVenueKind kind) noexcept { venue_kind_ = kind; }

    size_t pending_count()     const { return match_engine_.pending_count(); }
    size_t active_orders()     const override { return match_engine_.pending_count(); }
    int    risk_reject_count() const { return risk_reject_count_; }
    /// @brief sidecar 当前 entry 数（契约测试观测点：reject 不残留 / terminal 归零）。
    size_t order_strategy_size() const { return order_strategy_.size(); }

private:
    XtRiskResult evaluate_risk_(const char* code, bool is_buy, int64_t price_fp,
                                int64_t qty_fp, XtTimeInForce tif, XtOffset offset,
                                uint64_t order_id);
    void drain_and_dispatch_order_updates_();
    void finalize_fill_offsets_(std::vector<XtTradeRecord>& trades);
    void apply_fills_();

    // ── 成员 ─────────────────────────────────────────────────────────
    XtCtaMatchEngine               match_engine_;              ///< 撮合引擎（Gateway 持有所有权）
    std::vector<XtTradeRecord>     trades_scratch_;            ///< drain filled trades 临时缓冲（复用减少分配）
    std::vector<XtCtaMatchEngine::PendingOrderUpdate> order_update_scratch_; ///< drain order update 临时缓冲（复用减少分配）

    // 已 finalize + cost-enriched 的 fill/update 暂存，等待 engine pull drain。
    std::vector<XtTradeRecord>     fills_buf_;                 ///< 待 engine drain 的 fills
    std::vector<XtOrderExecUpdate> updates_buf_;               ///< 待 engine drain 的 order updates
    XtRiskGateway*                 risk_gw_         = nullptr; ///< 风控入口（非拥有）
    XtBacktestRiskGuardContext*    risk_guard_ctx_  = nullptr; ///< 风控上下文（非拥有）
    xt_account::AccountStateStore*            account_ref_     = nullptr; ///< 账户（非拥有）
    // observers_ 负责执行事件 fan-out；reject_sink_ 只负责拒单落地。
    std::vector<xt_runtime::IHftEngineObserver*> observers_;            ///< 报告 observer fan-out 链
    IRejectSink*                   reject_sink_      = nullptr;  ///< 拒单落地接口
    xt_fill_cost::FillCostApplier* cost_            = nullptr; ///< 成本模型（非拥有）
    xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr; ///< 订单簿（非拥有）
    const XtCommodityCatalog*      catalog_         = nullptr; ///< 商品元数据（offset 路由 policy；非拥有，可 null）
    int                            risk_reject_count_ = 0;     ///< 累计风控拒单数
    uint32_t                       last_date_         = 0;     ///< 最新市场时间（submit 风控上下文）
    uint32_t                       last_time_ms_      = 0;     ///< 最新市场时间（submit 风控上下文）
    std::unordered_map<std::string, int64_t> last_market_price_fp_; ///< 各合约最新价（市价单风控估价）
    /// strategy_id sidecar —— 风控通过后写入；apply_fills_ 读出回写 tr.strategy_id
    ///（部分成交保留 entry），终态 update drain 时 erase，防无限增长。
    std::unordered_map<uint64_t, std::string> order_strategy_;
    /// venue 类别（A 股 → 集合竞价 phase 推断；其他 venue 始终 Continuous）。
    xt_core::XtVenueKind venue_kind_ = xt_core::XtVenueKind::Continuous24h;
};
