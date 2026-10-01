#pragma once

#include <cstdint>
#include <deque>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "xt_core/market/XtRawTick.h"
#include "XtSimOrder.h"
#include "XtOrderBook.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_core/order/XtOffset.h"
#include "IQueueModel.h"

namespace YAML {
class Node;
}

/// @brief HFT 回测订单状态机与本地订单簿适配层。
///
/// `submit()` / `cancel()` 接收订单请求，`advance_clock()` 推进订单生命周期，
/// `set_depth_snapshot_fp()` 写入盘口，`match_tick()` 输出成交，
/// `drain_updates()` 输出订单状态增量。
///
/// 调用方按 replay worker 串行推进同一实例；本类不提供并发保护。
class XtOrderManager {
public:
    /// @brief 撮合状态机运行参数。
    struct RuntimeConfig {
        double queue_cancel_ratio = 0.0;  ///< 价格不变且盘口缩量时的 queue_ahead 侵蚀比例。
        uint64_t ack_delay_ticks = 0;     ///< ACK 延迟 tick 数；逐单/ms/us 延迟均为 0 时使用。
        uint64_t ttl_ticks = 0;           ///< 订单 TTL tick 数；ttl_us 为 0 时使用。

        uint64_t ack_delay_us = 0;   ///< ACK 延迟，单位 µs；0 表示关闭时间维度 ACK 延迟。
        uint64_t ttl_us = 0;         ///< 订单 TTL，单位 µs；0 表示关闭时间维度 TTL。

        double ofi_queue_scale = 0.0; ///< 最优档 OFI 对 queue_ahead 的额外侵蚀系数。

        bool market_impact_enabled = false; ///< 是否把我方成交量带入下一 tick 的盘口深度。
        double market_impact_decay = 0.5;   ///< market impact carry 每 tick 衰减系数，期望范围 [0,1]。
    };

    /// @brief 应用运行参数并预留热路径缓冲；不清空在途订单，不重置订单号。
    void configure_runtime(const RuntimeConfig& cfg);

    /// @brief 注入 queue_ahead 模型；指针 non-owning，生命周期必须覆盖本对象。
    ///
    /// `nullptr` 表示所有新被动单的 `queue_ahead` 视为 0。
    /// 预期在 bootstrap 装配阶段注入一次。
    void set_queue_model(const IQueueModel* m) noexcept { queue_model_ = m; }

    /// @brief 分配下一个本地订单 ID；调用方随后传给 submit()。
    uint64_t alloc_order_id() { return next_id_++; }

    /// @brief 推进撮合时钟（双轴），并释放到期 ACK、fill eligibility 与 TTL。
    ///
    /// `event_now_ns` 是 event-axis（自然日 epoch ns），一切时长/到期只与它比较；
    /// `trading_date` + `action_time_ms`（HHMMSSmmm）仅供回报戳（local_cb_ns 合成）。
    /// event_now_ns 回退时在入口 clamp 到当前值（脉冲计数 event_clock_clamp_count）：
    /// 时间在撮合域内永不倒流；脏数据源（legacy simtick 夜盘 action_date=trading）
    /// 降级为冻结推进而非 kill run。
    /// 到期状态迁移会追加订单回报到内部更新缓冲，调用方通过 drain_updates() 读取。
    void advance_clock(uint64_t tick, int64_t event_now_ns,
                       uint32_t trading_date, uint32_t action_time_ms);

    /// @brief 单轴便捷入口（action==trading 的日盘场景/测试）：event = to_ns(date, time_ms)。
    void advance_clock(uint64_t tick, uint32_t date, uint32_t time_ms);

    /// @brief 以 event ns 推进撮合时钟（双轴显式参数，禁止从 ns 反解冒充 trading）。
    ///
    /// `event_now_ns < earliest_due_event_ns_` 时只刷新当前时钟（fast path）；
    /// 否则进入完整 advance_clock() 路径。回退同样走入口 clamp。
    void advance_clock_to(int64_t event_now_ns,
                          uint32_t trading_date, uint32_t action_time_ms);

    /// @brief 单参便捷入口（测试/单轴场景）：slow path 时由 from_ns 派生回报戳。
    void advance_clock_to(int64_t event_now_ns);

    /// @brief 返回下一次必须进入完整时钟推进的精确 event-ns 下界。
    /// @return `INT64_MAX` 表示当前没有 event 域 pending（tick 域 pending 不影响
    ///         fast path：它们只能由 market tick 推进，时间推进对其无效）。
    int64_t peek_earliest_due_ns() const noexcept { return earliest_due_event_ns_; }

    /// @brief event 时钟入口 clamp 触发次数（倒流脏数据诊断）。
    uint64_t event_clock_clamp_count() const noexcept { return event_clock_clamp_count_; }

    /// @brief 提交订单到本地状态机。
    ///
    /// `code` 必须非空，`qty_fp` 与 `price_fp` 必须大于 0；市价意图应在上游
    /// 解析为带真实价格的 aggressive IOC 后再进入 HFT sim。
    /// `order_date/order_time_ms` 透传到成交记录，时间口径为 `YYYYMMDD` 与
    /// `HHMMSSmmm`。`ack_delay_ms_sampled`、`fill_latency_ms_sampled` 与
    /// `response_latency_ns_sampled` 为逐单采样结果；前两者非零时覆盖对应的固定延迟
    /// 参数，response 随单存进 `XtSimOrder::response_latency_ns`（-1 = 无 latency model）。
    ///
    /// @param order_id 调用方分配的本地订单号；通常来自 `alloc_order_id()`。
    /// @param user_tag 策略侧透传标记，进入成交记录，不参与撮合。
    /// @param close_offset 开平标记，透传给成交记录和订单回报。
    /// @param is_resubmit 取消重报标记，仅传给 queue model 估算初始 queue_ahead。
    /// @return 成功返回本地订单号；输入非法或容量不足返回 0，并递增 engine_drops()。
    uint64_t submit(uint64_t order_id,
                    const char* code,
                    bool is_buy,
                    int64_t price_fp,
                    int64_t qty_fp,
                    XtTimeInForce tif = XtTimeInForce::GTC,
                    uint32_t order_date = 0,
                    uint32_t order_time_ms = 0,
                    std::string_view user_tag = "",
                    XtOffset close_offset = XtOffset::Open,
                    bool is_resubmit = false,
                    uint32_t ack_delay_ms_sampled = 0,
                    uint32_t fill_latency_ms_sampled = 0,
                    int64_t response_latency_ns_sampled = -1);

    /// @brief 按本地订单号撤单。
    /// @param cancel_delay_ms_sampled 撤单请求到达撮合的采样延迟（ms）；0 = 立即生效。
    ///        非零时订单进入 pending-cancel，到期前继续参与撮合（cancel-fill race）；
    ///        到期时若已终态则撤单落空（不补发回报）。
    /// @return 订单存在且尚未终态时返回 true（延迟模式下表示请求已受理）；
    ///         未知或已终态返回 false。对已有在途撤单请求的订单幂等返回 true。
    bool cancel(uint64_t id, uint32_t cancel_delay_ms_sampled = 0);

    /// @brief 为从未进入 manager 的 in-transit 单（engine delay wheel 内被撤 /
    ///        EOF 丢弃）合成 Canceled 终态回报。上游 gateway 在 submit 时已
    ///        冻结 margin、注册 book slot，销账链靠终态；无终态 = frozen 永泄。
    void emit_in_transit_canceled(uint64_t order_id, const char* code, bool is_buy,
                                  int64_t price_fp, int64_t qty_fp,
                                  XtTimeInForce tif, std::string_view user_tag,
                                  XtOffset close_offset);

    /// @brief 按本地订单号拒单。
    /// @return 订单存在且尚未终态时返回 true；未知或已终态返回 false。
    bool reject(uint64_t id, const char* reason_code = nullptr);

    /// @brief EOF 收尾纯清理：把所有仍 alive 的在途单从 orders_/book 归档，不 emit
    ///        终态回报。撮合 EOF 后永不再推进；delivered 终态回报由 gateway 按
    ///        delivered 口径另行合成派发，此处只收 sim 撮合簿（使 live_orders()==0）。
    /// @note 与 `discard_in_transit_orders`（delay-wheel 单）对称，覆盖已入撮合簿的单。
    void discard_eof_live_orders();

    // ── Diagnostics ─────────────────────────────────────────────────

    /// @brief submit/book 容量与输入校验拒单计数。
    struct EngineDrops {
        uint64_t capacity_max_concurrent = 0;  ///< orders_.size() >= kMaxConcurrentOrders
        uint64_t blank_code              = 0;  ///< code 为空指针 / 空串
        uint64_t zero_qty                = 0;  ///< qty_fp <= 0
        uint64_t nonpos_price            = 0;  ///< price_fp <= 0；HFT sim 不接收市价 sentinel
        uint64_t orders_per_level        = 0;  ///< PriceLevel::push 容量满 (XtOrderBook)
        uint64_t price_levels_full       = 0;  ///< get_or_insert 价位数满 (XtOrderBook)
    };

    /// @brief 返回 drop counter 当前值；回测结束后可用于断言策略没有被本地撮合层拒单。
    const EngineDrops& engine_drops() const noexcept { return engine_drops_; }

    /// @brief XtOrderBook 在单价位订单数达到上限时回调。
    void note_orders_per_level_drop() noexcept { ++engine_drops_.orders_per_level; }

    /// @brief XtOrderBook 在价格层级容量达到上限时回调。
    void note_price_levels_full_drop() noexcept { ++engine_drops_.price_levels_full; }

    // ── Market and matching ─────────────────────────────────────────

    /// @brief 写入 `code` 的本 tick 盘口快照。
    ///
    /// 快照按 symbol 隔离（per-code slot）：只滚动该 symbol 自己的 prev/last 与
    /// market impact carry，供 maker/taker 判定、queue model、OFI erosion 使用。
    /// 价格和数量均为 `kOrderExecFpScale` fixed-point；内部只把数量转换为 double。
    /// 空数组按 0 处理，`depth` 会 clamp 到 `[0, XtRawTick::DEPTH]`；`code` 空则 no-op。
    void set_depth_snapshot_fp(const char* code,
                               const int64_t* bid_px_fp, const int64_t* ask_px_fp,
                               const int64_t* bid_qty_fp, const int64_t* ask_qty_fp,
                               int depth);

    /// @brief 设置当前 tick 的涨跌停价（fixed-point；0 = 无限制 fail-open）。
    ///
    /// 板价被动单免疫 queue erosion/重估（板上排队几乎不前进），并透传给
    /// XtOrderBook 做触板整侧冻结。由上层每 tick 与盘口快照同刻刷新。
    /// @note 涨跌停保持全局单值不进 per-code slot：全部读点都在 match_tick 动态
    ///       范围内，且引擎每 tick 与该 tick 的 code/盘口同刻刷新（同源无 skew）；
    ///       refresh/erosion 已按 ord.code 过滤，不会用本 tick 限价判定他合约订单。
    void set_price_limits_fp(int64_t upper_fp, int64_t lower_fp) noexcept {
        upper_limit_fp_ = upper_fp;
        lower_limit_fp_ = lower_fp;
    }

    /// @brief 基于当前盘口撮合在途订单，并追加成交记录。
    ///
    /// `code` 为空时直接返回。`trades` 使用 append 语义；订单状态更新进入内部
    /// update buffer，需由 drain_updates() 导出。
    /// @param max_fill_qty 当前 symbol 单 tick 最大成交量；<0 不限制，0 = 本 tick
    ///        无可成交量。由上层 volume cap 决定。
    void match_tick(const char* code,
                    const int64_t* bid_px_fp,
                    const int64_t* ask_px_fp,
                    const int64_t* bid_qty_fp,
                    const int64_t* ask_qty_fp,
                    int depth,
                    double max_fill_qty,
                    std::vector<XtTradeRecord>& trades);

    // ── Queries and drains ──────────────────────────────────────────

    /// @brief 从在途与已归档订单中按 id 查询；不存在返回 nullptr。
    const XtSimOrder* find(uint64_t id) const;

    /// @brief 当前在途订单视图；引用在下一次 submit/archive 后可能失效。
    const std::vector<XtSimOrder>& live_orders() const;

    /// @brief 将增量订单回报 append 到 out，并清空内部 update buffer。
    void drain_updates(std::vector<XtOrderExecUpdate>& out);

private:
    /// @brief 单 tick 内按订单累计成交量；供 IOC 剩余撤单判断使用。
    struct FillRecord {
        uint64_t id = 0;
        int64_t qty_fp = 0;   ///< 本 tick 累计成交量 fixed-point
    };
    static constexpr int kMaxFillsPerTick = 256;

    /// @brief 单 symbol 盘口快照与派生状态（queue model / OFI / impact carry）。
    ///
    /// 多 symbol 回放下 last/prev/carry 必须按 code 隔离：任意 symbol 的 tick
    /// 只滚动自己的 prev/last，否则 maker/taker 判定、queue erosion 与
    /// market impact carry 会被「最近任意 symbol」的盘口跨合约污染。
    struct DepthSnapshot {
        int64_t last_bid_px_fp[XtRawTick::DEPTH] = {0};
        int64_t last_ask_px_fp[XtRawTick::DEPTH] = {0};
        double  last_bid_qty[XtRawTick::DEPTH]   = {0};
        double  last_ask_qty[XtRawTick::DEPTH]   = {0};
        int64_t prev_bid_px_fp[XtRawTick::DEPTH] = {0};
        int64_t prev_ask_px_fp[XtRawTick::DEPTH] = {0};
        double  prev_bid_qty[XtRawTick::DEPTH]   = {0};
        double  prev_ask_qty[XtRawTick::DEPTH]   = {0};
        int     last_depth = 0;  ///< 最近一次快照的有效盘口深度

        // market impact carry：下一 tick 同价位深度先扣除上一 tick 我方消耗残留。
        double consumed_ask_carry[XtRawTick::DEPTH] = {0};  ///< 买单消耗卖档后的 ask side carry
        double consumed_bid_carry[XtRawTick::DEPTH] = {0};  ///< 卖单消耗买档后的 bid side carry
    };

    /// @brief per-code 快照槽；线性小表（同 XtHftMatchEngine::VolumeEntry 惯例），
    ///        warm path 无 hash，槽一经建立身份不变、永不复用。
    struct DepthSlot {
        char code[32] = {};  ///< 与 XtTick::code 同宽；超长截断
        DepthSnapshot snap;
    };

    /// @brief 只查在途订单；已归档或未知 id 返回 nullptr。
    XtSimOrder* find_live(uint64_t id) noexcept {
        auto it = id_to_slot_.find(id);
        return (it != id_to_slot_.end()) ? &orders_[it->second] : nullptr;
    }
    const XtSimOrder* find_live(uint64_t id) const noexcept {
        auto it = id_to_slot_.find(id);
        return (it != id_to_slot_.end()) ? &orders_[it->second] : nullptr;
    }

    /// @brief 查找 code 的快照；miss 返回 nullptr（= 该 symbol 尚无盘口）。
    const DepthSnapshot* find_depth_(const char* code) const noexcept;
    /// @brief 查找或插入 code 的快照槽；新 symbol 首见 append（冷路径分配）。
    DepthSnapshot& find_or_insert_depth_(const char* code);

    /// @brief 用 `code` 最新盘口重估其被动单 queue_ahead，并应用最优档 OFI erosion；
    ///        只作用于 `ord.code == code` 的挂单。
    void refresh_queue_ahead(const char* code, const DepthSnapshot& snap, int depth);
    /// @brief 价格不变但盘口数量减少时，按配置削减 `code` 同价位被动单 queue_ahead。
    void apply_queue_cancel_erosion(const char* code, const DepthSnapshot& snap, int depth);
    /// @brief 订单价恰为涨/跌停板价（erosion 免疫判定；limit 0 = 无限制）。
    bool is_board_price_(int64_t px_fp) const noexcept {
        return (upper_limit_fp_ > 0 && px_fp == upper_limit_fp_)
            || (lower_limit_fp_ > 0 && px_fp == lower_limit_fp_);
    }

    /// @brief 应用订单簿成交结果，更新订单状态并 append 成交记录。
    void apply_fill_updates(const std::vector<XtOrderBook::Fill>& fills,
                            std::vector<XtTradeRecord>& trades,
                            FillRecord* filled_buf, int& filled_count);
    /// @brief 归档订单簿主动撤销的订单，并补发 Canceled 回报。
    void apply_canceled_ids(const std::vector<uint64_t>& ids);
    /// @brief 归档完全成交订单；最后一笔 fill 已发布状态时不重复 emit。
    void apply_done_ids(const std::vector<uint64_t>& ids);
    /// @brief 在 tick 末撤销 IOC 剩余量；只检查本 tick 有成交的 IOC 订单。
    void cancel_ioc_remainders(const FillRecord* filled_buf, int filled_count);
    /// @brief 立即撤单（pending-cancel 到期与 delay=0 共用路径）。
    bool cancel_now_(uint64_t id);

    /// @brief 将本 tick 成交量按价位写入 `snap`（当前 tick symbol）的 market-impact carry。
    void accumulate_market_impact(const std::vector<XtOrderBook::Fill>& fills,
                                  DepthSnapshot& snap);

    /// @brief 将在途订单迁移到 done_；使用 swap-with-last，调用后 orders_ 引用失效。
    void archive_order(uint64_t id,
                       bool remove_from_book,
                       bool emit_update,
                       int64_t last_fill_fp);

    /// @brief 将订单状态快照写入增量回报缓冲；`last_fill_fp` 只描述本次新增成交。
    void push_update(const XtSimOrder& ord, int64_t last_fill_fp);

    /// @brief event 时钟入口 clamp：回退值冻结到当前（计数 + 首次 warn），保撮合域单调。
    int64_t clamp_event_now_(int64_t event_now_ns) noexcept;

    /// @brief 若 `due_event_ns` 早于当前下界则下调（arm 点增量维护，只降不升）。
    void note_event_deadline_(int64_t due_event_ns) noexcept {
        if (due_event_ns < earliest_due_event_ns_) earliest_due_event_ns_ = due_event_ns;
    }

    /// @brief deadline 基准：首个时钟前（INT64_MIN）视为 0（首次推进即到期，同旧语义）。
    int64_t event_base_() const noexcept {
        return cur_event_ns_ != INT64_MIN ? cur_event_ns_ : 0;
    }

    /// @brief 重算 event 域精确到期下界（ACK/fill/cancel deadline + TTL(us)）。
    ///
    /// 扫 pending 列表与（仅 ttl_us 启用时）live orders 取最小 event-ns deadline；
    /// 无 event 域 pending 时设 `INT64_MAX`。tick 域 pending（ack_due_tick/ttl_ticks）
    /// 不参与：它们只能由 market tick 推进，时间 fast path 跳过它们是正确的。
    void recompute_earliest_due_() noexcept;

    // 时钟与主键（clock / ids）。
    uint64_t next_id_ = 1;    ///< 单实例递增的本地订单号
    uint64_t cur_tick_ = 0;   ///< 当前 tick 序号
    uint32_t cur_date_ = 0;   ///< 当前交易日 YYYYMMDD（回报戳轴，不入时长运算）
    uint32_t cur_time_ = 0;   ///< 当前时刻 ms since midnight；由 HHMMSSmmm 转换得到

    // 运行参数。
    uint64_t ack_delay_ticks_ = 0;               ///< tick 维度 ACK 延迟
    uint64_t ttl_ticks_ = 0;                     ///< tick 维度订单 TTL
    uint64_t ack_delay_us_ = 0;                  ///< 时间维度 ACK 延迟（µs，0=disabled）
    uint64_t ttl_us_ = 0;                        ///< 时间维度 TTL（µs，0=disabled）
    // 以下仿真参数与 L2 数量同域运算，保持 double。
    /// Queue model 是 queue_ahead 计算来源；nullptr 时按 0 queue_ahead 处理。
    const IQueueModel* queue_model_ = nullptr;

    double queue_cancel_ratio_ = 0.0;            ///< 盘口缩量侵蚀比例（queue erosion）
    double ofi_queue_scale_ = 0.0;               ///< OFI 排队侵蚀系数（OFI erosion scale）
    bool   market_impact_enabled_ = false;       ///< 盘口冲击传导开关（market impact carry）
    double market_impact_decay_ = 0.5;            ///< 冲击衰减系数（impact decay per tick）
    int64_t upper_limit_fp_ = 0;                 ///< 当前 tick 涨停价（0 = 无限制）
    int64_t lower_limit_fp_ = 0;                 ///< 当前 tick 跌停价（0 = 无限制）

    // ── Order state ─────────────────────────────────────────────────

    /// 在途订单容量上限；超过时 submit() 返回 0 并递增 drop counter。
    static constexpr int kMaxConcurrentOrders = 8192;
    std::vector<XtSimOrder> orders_;   ///< 在途订单；顺序不稳定，archive 使用 swap-with-last
    EngineDrops engine_drops_;

    /// id -> orders_ slot index；与 orders_ 同步维护。
    std::unordered_map<uint64_t, size_t> id_to_slot_;

    /// 已归档订单有界 FIFO cache；超过 kDoneOrderCacheCap 从头部淘汰，防长回测无界增长。
    /// find() 对已淘汰 id 返回 nullptr（冷路径诊断/测试语义，无 prod caller 依赖全量历史）。
    static constexpr size_t kDoneOrderCacheCap = 8192;
    std::deque<XtSimOrder> done_;
    std::vector<XtOrderExecUpdate> updates_; ///< 增量状态更新缓冲（drain 后清空）。
    std::vector<uint64_t> pending_acks_;   ///< Submitted 且等待 ACK 到期的订单 ID。
    std::vector<uint64_t> pending_fills_;  ///< Accepted 但尚未到达撮合资格时刻的订单 ID。
    std::vector<uint64_t> pending_cancels_; ///< 撤单请求在途（cancel entry latency）的订单 ID。

    /// advance_clock 三段循环共用的暂存缓冲；clear() 不释放 capacity。
    std::vector<uint64_t> clock_scratch_;

    // ── Clock fast-path cache ────────────────────────────────────────

    int64_t earliest_due_event_ns_ = INT64_MAX;  ///< event 域精确到期下界（fast-path skip）
    int64_t cur_event_ns_ = INT64_MIN;  ///< 最近推进到的 event-axis ns（入口 clamp 保单调）
    uint64_t event_clock_clamp_count_ = 0;  ///< event 时钟倒流 clamp 次数（脏数据诊断）

    // ── Queue model snapshots（per-code 隔离） ──────────────────────

    /// per-code 盘口快照表；线性扫描，容量 = distinct symbol 数（回测宇宙内有界）。
    std::vector<DepthSlot> depth_table_;

    XtOrderBook order_book_;  ///< 模拟订单簿（owning，价格层级撮合引擎）
};

namespace engine_config {

XtOrderManager::RuntimeConfig parse_order_manager_runtime_config(const YAML::Node& node);

} // namespace engine_config
