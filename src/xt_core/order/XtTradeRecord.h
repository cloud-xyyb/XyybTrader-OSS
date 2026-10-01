#pragma once

#include <cstdint>
#include <string>

#include "XtFixedPoint.h"
#include "XtFillEvent.h"
#include "XtOffset.h"
#include "XtOrderTypes.h"

/**
 * @brief 完整成交审计记录（full fill audit record）。
 *
 * 冷路径结构，承载 broker 回报 ID、自然日/交易日、成本与本地 trace 字段。
 * 策略回调和仓位更新使用 `to_fill_event()` 提取出的 `XtFillEvent`。
 */
struct XtTradeRecord {
    // ── core: 成交核心数据 ──────────────────────────────────────────────

    /** 内部订单号，与 XtFillEvent::order_id 及 XtOrderExecUpdate::id 一致。 */
    uint64_t order_id = 0;

    /** 合约码，如 "IF2403"。 */
    std::string code;

    /** true = 买入，false = 卖出。 */
    bool is_buy = true;

    /**
     * 成交量定点整数（fixed-point，与 XtFillEvent::qty_fp 口径一致）。
     * 报告层读取时用 order_exec_from_fp() 转换。
     */
    int64_t qty_fp = 0;

    /** 报单价（order price，不含 slippage）。 */
    double price = 0.0;

    /** 实际成交价（exec price = 报单价 + slippage）。 */
    double exec_price = 0.0;

    /** 报单日期（YYYYMMDD 整型）；无 broker 日期时由 adapter 填本地自然日。 */
    uint32_t order_date = 0;

    /** 报单时间（ms since midnight）；无 broker 时间时由 adapter 填本地时间。 */
    uint32_t order_time = 0;

    /** 成交日期（YYYYMMDD）。自然日语义（不等于交易日，夜盘会跨午夜）。 */
    uint32_t trade_date = 0;

    /** 成交时间（ms since midnight）。 */
    uint32_t trade_time = 0;

    /**
     * 交易日（trading day，YYYYMMDD）。
     *
     * 期货语义：夜盘归次日交易日（CTP broker `TradingDay` 字段权威）。
     * 回测/无 broker 场景：可由 live_ops/trading_day.py 规则推导。
     * 与 `trade_date`（自然日）区分：聚合 PnL / fills 时用此字段，与
     * exchange 结算口径一致（跨市场：期货 / 股票 / 加密语义统一）。
     *
     * 字段缺失（== 0）时下游视为"等同 trade_date"。
     */
    uint32_t trading_day = 0;

    /**
     * 成交标准时间戳（us epoch）。
     * 回测：合成时间（replay synthetic）；实盘：本地回调时间（local_cb_us）。
     * 与 XtFillEvent::fill_ts_us 口径完全一致，是跨模块关联的时间基准。
     */
    uint64_t fill_ts_us = 0;

    /** 是否为平仓方向。 */
    bool is_close = false;

    /** true = 平今仓，false = 平昨仓或开仓。 */
    bool is_today = false;

    /** 开平语义，详见 XtOffset。 */
    XtOffset close_offset = XtOffset::Open;

    // ── broker: 交易所/经纪商回报 ID（仅实盘有效，回测留空） ──────────────────────────────────────────────

    /** 交易所 ID（如 "SHFE"）；无交易所字段的 venue 可留空。 */
    std::string exchange_id;

    /** 成交编号；实盘 venue 能提供时应填交易所或 broker 侧唯一 ID。 */
    std::string trade_id;

    /** 报单引用；通常是本地会话内唯一 ID。 */
    std::string order_ref;

    /** 交易所/经纪商报单号；无对应字段时留空。 */
    std::string order_sys_id;

    // ── cost: 执行成本（由 FillCostApplier 在成交后填入） ──────────────────────────────────────────────

    /**
     * 被动成交标记（maker/taker）。
     * true  = maker：挂单被动成交，cost pipeline 会从标准费率中扣减 maker rebate。
     * false = taker：主动吃单成交，按标准费率计算（无返佣）。
     * 回测由撮合层判断，实盘由 venue adapter 或保守默认值填入。
     */
    bool is_passive = false;

    /** 手续费（fee），负数表示返佣。单位：元。 */
    double fee = 0.0;

    /** 滑点成本（slippage = exec_price - order_price）。单位：元/每手价格变动。 */
    double slippage = 0.0;

    // ── trace: 本地时延追踪（由 live/backtest adapter 填入） ──────────────────────────────────────────────

    /** 策略自定义标签（透传，不参与成交逻辑）。 */
    std::string user_tag;

    /// 策略归属标识；多策略回测按此字段分发成交，单策略 live 场景可留空。
    std::string strategy_id;

    /** 请求链路追踪 ID（分布式 trace，可选）。 */
    std::string trace_id;

    /** 报单提交开始时间戳（us），对应 send() 调用前。 */
    uint64_t local_submit_begin_us = 0;

    /** 报单提交完成时间戳（us），对应 send() 返回后。 */
    uint64_t local_submit_end_us = 0;

    /** 成交回报到达本地的时间戳（us），即 venue trade callback 进入时刻。 */
    uint64_t local_cb_us = 0;

    /** 成交事件进入策略回调的时间戳（us），用于度量 td→strategy 时延。 */
    uint64_t local_strategy_us = 0;

    // ── realized: engine on_trade 写回的 per-fill 已实现盈亏 ──────────────────────────
    /**
     * 该笔成交的已实现盈亏（realized PnL，×1e6 定点）。
     * 平仓单非零，开仓/加仓为 0；report-contract fills.csv 与 GetRecentFills RPC
     * 使用该字段作为下游 realized PnL 口径。字段位于末尾以保持既有字段 offset。
     */
    int64_t realized_pnl_fp = 0;

    // ── attribution: gateway 预 stamp 的策略归因 intern key ──────────────────────────
    /**
     * 策略归因 intern key（StrategySidRegistry sid_id，0 = 未归因）。
     * 生产点强制契约：sid_id 与 strategy_id 必须同源（同一次 OrderAttributionStore
     * lookup 派生，miss 时两键一致为 0/空），禁止只写单键 —— engine 恢复链仅由
     * 空 string 触发，若出现 sid_id!=0 且 string 空的分歧记录，恢复链会用 code
     * 派生值覆盖 stamp。engine fill 路由优先按本字段分发，0 时回退 strategy_id
     * 解析与恢复链。字段位于末尾以保持既有字段 offset。
     */
    uint32_t sid_id = 0;

    /// sim-only 瞬态：撮合引擎在 submit 时与 entry 相邻配对采样的 response latency（ns），
    /// 随成交盖回给 BT gateway 计算 fill 可见时刻；-1 = 非 sim 路径/无 latency model。
    /// 不进 JSONL/fbs wire（两序列化器均显式逐字段）。
    int64_t sim_response_latency_ns = -1;

    /// sim-only 瞬态：BT gateway 按延迟模型算出的成交「模型可见时刻」（自然日 epoch ns，
    /// match event ns + response latency；与 pending 释放 gate 同值）；-1 = 非 sim 路径。
    /// 不进 JSONL/fbs wire（两序列化器均显式逐字段）。
    int64_t sim_visible_at_ns = -1;

    // ── 转换 ──────────────────────────────────────────────

    /// @brief 提取热路径成交事件；执行一次 double→fixed-point 转换和 code 拷贝。
    XtFillEvent to_fill_event() const {
        XtFillEvent f;
        f.order_id       = order_id;
        // 策略可见财务值用 round（::to_fp_round，与账户 reducer 的 AccountEventCodec
        // xt_fp::to_fp 同口径）；trunc 会与账户侧产生 1 fp-unit 系统性向下分歧。
        f.exec_price_fp  = ::to_fp_round(exec_price);
        f.qty_fp         = qty_fp;
        f.fee_fp         = ::to_fp_round(fee);
        f.slippage_fp    = ::to_fp_round(slippage);
        f.close_offset   = close_offset;
        f.is_buy         = is_buy;
        f.is_today       = is_today;
        fill_event_set_code(f, code.c_str());
        return f;
    }
};
