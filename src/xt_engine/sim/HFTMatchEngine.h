#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "xt_core/market/XtTick.h"
#include "XtOrderManager.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtOffset.h"
#include "HFTMatchEngineCfg.h"
#include "ILatencyModel.h"
#include "XtLatencyArith.h"
#include "IQueueModel.h"

namespace YAML {
class Node;
}

/// @brief HFT 回测撮合门面。
///
/// 调用方注入配置、latency model 和 queue model 后，通过 `advance_clock()`、
/// `submit()`、`cancel()` 与 `on_tick()` 推进订单生命周期。
/// 新增成交写入调用方提供的 trades，订单状态经 `drain_updates()` 输出。
///
/// 同一实例由 replay loop 串行调用；本类不提供并发保护。
class XtHftMatchEngine {
public:
    using Config = XtHftMatchEngineCfg;

    XtHftMatchEngine() = default;
    explicit XtHftMatchEngine(const Config& cfg) { set_config(cfg); }

    /// @brief 应用回测撮合配置，并转发 runtime 参数到 XtOrderManager。
    void set_config(const Config& cfg);

    /// @brief 清空 per-symbol total_volume 缓存；不动配置、模型注入和在途订单。
    void reset_volume_cache();

    /// @brief 按 tick index 推进订单 ACK、撮合资格和 TTL（双轴）。
    /// @param event_now_ns   event-axis 时刻（一切时长/到期只与它比较）
    /// @param trading_date   交易日（仅供成交/回报戳）
    /// @param action_time_ms HHMMSSmmm
    void advance_clock(uint64_t tick_idx, int64_t event_now_ns,
                       uint32_t trading_date, uint32_t action_time_ms);

    /// @brief 单轴便捷入口（action==trading 日盘场景/测试）：event = to_ns(date, time_ms)。
    void advance_clock(uint64_t tick_idx, uint32_t date, uint32_t time_ms);

    /// @brief 按时间推进订单生命周期，不消耗 tick index（双轴；命名镜像
    /// OrderManager::advance_clock_to，避免与 tick 版 3 参重载对 int 字面量二义）。
    void advance_clock_to(int64_t event_now_ns,
                          uint32_t trading_date, uint32_t action_time_ms);

    /// @brief 单轴便捷入口（测试）：event = to_ns(date, time_ms)。
    void advance_clock(uint32_t date, uint32_t time_ms);

    /// @brief 标定下一次 on_tick 生成的成交时间戳。
    ///
    /// 延迟撮合模式下，订单可在后续 tick 盘口成交，但成交记录应归属到策略
    /// 接收信号的 dispatch tick。0 表示未覆盖，回退到当前撮合时钟。
    void set_dispatch_clock(uint32_t date, uint32_t time_ms) noexcept {
        dispatch_date_ = date;
        dispatch_time_ms_ = time_ms;
    }

    /// @brief 注入 latency model；nullptr 表示 entry/response 延迟均为 0。
    /// @note bootstrap 一次性注入，不支持运行期热切换。
    void set_latency_model(std::unique_ptr<ILatencyModel> model) noexcept {
        latency_model_ = std::move(model);
    }

    /// @brief 是否已注入 latency model。
    bool has_latency_model() const noexcept { return latency_model_ != nullptr; }

    /// @brief 抽样 fill 到策略可见的 response latency；无模型时返回 0。
    /// @param event_ts_ns 采样时刻（event 轴）；越界输出在消费点抛错（非 noexcept）。
    int64_t sample_response_latency_ns(int64_t event_ts_ns) const {
        return latency_model_
            ? xt_latency::validate_latency_ns(
                  latency_model_->response_latency_ns(event_ts_ns), "response")
            : 0;
    }

    /// @brief 注入 queue model；nullptr 时新单 queue_ahead 按 0 处理。
    /// @note bootstrap 一次性注入，不支持运行期热切换。
    void set_queue_model(std::unique_ptr<IQueueModel> model) noexcept {
        queue_model_ = std::move(model);
        order_manager_.set_queue_model(queue_model_.get());
    }

    /// @brief 是否已注入 queue model。
    bool has_queue_model() const noexcept { return queue_model_ != nullptr; }

    /// @brief 分配本地订单 ID；Gateway 在 submit 前调用。
    uint64_t alloc_order_id() { return order_manager_.alloc_order_id(); }

    /// @brief 提交 fixed-point 订单；失败返回 0。
    ///
    /// order_id 必须来自 alloc_order_id()。price_fp/qty_fp 直接透传到底层
    /// XtOrderManager，不做 double 往返转换。
    /// @param order_time_ms HHMMSSmmm
    /// @param is_resubmit true 表示撤单后重新入队
    uint64_t submit(uint64_t order_id,
                    const char* code,
                    bool is_buy,
                    int64_t price_fp,
                    int64_t qty_fp,
                    XtTimeInForce tif,
                    uint32_t order_date,
                    uint32_t order_time_ms,
                    std::string_view user_tag,
                    XtOffset close_offset,
                    bool is_resubmit = false);

    /// @brief 按订单 ID 撤单；返回请求是否被受理。
    ///
    /// `cancel_latency_enabled` 且已注入 latency model 时，撤单经 entry latency
    /// 延迟到达撮合，在途期间订单仍可成交（cancel-fill race）；否则立即生效。
    bool cancel(uint64_t id);

    /// @brief 输入 tick 盘口并输出本 tick 新增成交。
    ///
    /// 调用方应先推进 clock，再调用 on_tick。trades 只 append，不清空调用方已有内容。
    void on_tick(const XtTick& tick, std::vector<XtTradeRecord>& trades);

    /// @brief 将内部订单更新追加到 out，并清空内部 update buffer。
    /// @param out 追加输出；调用前已有内容会保留。
    void drain_updates(std::vector<XtOrderExecUpdate>& out);

    /// @brief 当前在途订单数。
    size_t live_orders() const;

    /// @brief 当前在途订单只读视图；下次状态迁移后引用可能失效。
    const std::vector<XtSimOrder>& live_orders_view() const;

    /// @brief 按订单号查询，包含已归档订单。
    const XtSimOrder* find(uint64_t id) const;

    /// @brief 设置 submit 到进入 OrderManager 的 tick 延迟；0 关闭 delay wheel。
    /// @warning 仅 bootstrap 装配阶段调用；运行期调用会清空 delay wheel，
    ///          丢弃尚未提交的 deferred order。
    void set_order_delay_ticks(uint32_t n);

    /// @brief 当前 delay wheel 内尚未提交的订单数。
    std::size_t delay_wheel_pending_count() const noexcept;

    /// @brief 收集 delay wheel 内在途（尚未提交到 OrderManager）的订单 id，
    ///        append 到 out。强平/批量撤单需覆盖 wheel 单（live_orders_view
    ///        不含它们；漏撤的 delayed Open 会在强平后 flush+成交 → 超平）。
    void collect_in_transit_order_ids(std::vector<uint64_t>& out) const;

    /// @brief EOF 丢弃的 in-transit order 累计数；set_order_delay_ticks 不重置。
    std::uint64_t lost_in_transit_count() const noexcept { return lost_in_transit_count_; }

    /// @brief EOF 收尾时丢弃 delay wheel 内尚未提交的订单。
    /// @return 本次丢弃的订单数；order_delay_ticks_ == 0 时返回 0
    std::size_t discard_in_transit_orders() noexcept;

    /// @brief EOF 收尾纯清理：把撮合簿内仍 alive（resting/pending）的在途单归档，
    ///        不 emit 终态。委托 XtOrderManager；delivered 终态由 gateway 另行合成。
    void discard_eof_live_orders();

private:
    /// @brief volume_limit 的 per-symbol 累计成交量缓存槽。
    struct VolumeEntry {
        char   code[32] = {};
        double volume   = 0.0; ///< 上一笔 total_volume_fp 折算后的 lot 数量。
    };

    /// @brief order delay wheel 中等待进入 XtOrderManager 的 submit 参数快照。
    ///
    /// submit() 入队时复制全部参数；到期后 submit_immediate_() 使用该快照补采样
    /// ack/fill latency 并提交。code/user_tag 使用固定缓冲，超长输入会被截断。
    struct DeferredOrder {
        uint64_t order_id;
        char code[32];
        bool is_buy;
        int64_t price_fp;
        int64_t qty_fp;
        XtTimeInForce tif;
        uint32_t order_date;
        uint32_t order_time_ms; ///< HHMMSSmmm。
        char user_tag[32];
        XtOffset close_offset;
        bool is_resubmit;
        /// 入队（submit）时刻的 event ns；flush 时采样用本原始 lookup 时刻
        ///（不用 flush 时刻），而 entry/ACK deadline base 取 flush 时 event——
        /// order_delay_ticks 与 entry latency 两段严格串联，不折成 max。
        int64_t lookup_event_ns;
    };

    /// @brief 生成 [0,1] 均匀随机数，用于 error/reject 注入。
    double next_rand01();

    /// @brief 计算本 tick 可用于撮合的成交量上限。
    /// @return <0 不限制（数据源无成交量信息）；0 = 本 tick 禁止成交；>0 = cap。
    double calc_max_fill_qty(const XtTick& tick);

    /// @brief 采样本次报单 ACK 延迟，返回向上取整后的 ms。
    uint32_t sample_ack_delay_ms();

    /// @brief 采样 fill 资格延迟（submit→可撮合 fill-eligibility，走 entry latency），
    ///        返回向上取整后的 ms。fill 可见性延迟由 gateway 用 response latency 处理。
    /// @param event_ts_ns 采样时刻（event 轴）。
    uint32_t sample_fill_latency_ms(int64_t event_ts_ns);

    /// @brief 采样撤单请求到达撮合的延迟（entry latency），返回向上取整 ms；
    ///        开关关闭或无 latency model 时返回 0 = 立即生效。
    /// @param event_ts_ns 采样时刻（event 轴）。
    uint32_t sample_cancel_delay_ms(int64_t event_ts_ns);

    /// @brief 采样用 event 时刻；首 event 前且模型 requires_event_timestamp 时 fail-fast
    ///（静默以 ts=0 取首样本正是 T2.SIM-1 的病）；否则 INT64_MIN 视为 0。
    int64_t query_event_ts_() const;

    /// @brief 查找或插入合约 total_volume 缓存槽；warm path 线性扫描，新 symbol 首见 append。
    double& find_or_insert_volume(const char* code);

    /// @brief 推进 delay wheel，并提交当前槽中到期的 deferred order。
    void advance_wheel_and_flush_();

    /// @brief 按 error_rate_ 随机撤销部分在途订单，保持 RNG stream 消耗顺序。
    void inject_cancel_errors_();

    /// @brief 更新本 tick depth 视图并撮合订单，新增成交 append 到 trades。
    void match_orders_on_tick_depth_(const XtTick& tick, std::vector<XtTradeRecord>& trades);

    /// @brief 为本轮新增成交写入 dispatch/current 时间，并消费 dispatch clock。
    void stamp_new_trade_times_(size_t first_new_trade, std::vector<XtTradeRecord>& trades);

    /// @brief 按 split_fills_ 配置拆分本轮新增成交。
    void split_new_fills_(size_t first_new_trade, std::vector<XtTradeRecord>& trades);

    /// @brief 绕过 delay wheel 直接提交已捕获参数；返回 0 表示底层拒单。
    uint64_t submit_immediate_(const DeferredOrder& d);

    // 核心状态
    XtOrderManager order_manager_;
    double error_rate_ = 0.0;
    double reject_rate_ = 0.0;
    bool split_fills_ = false;
    // split 参数以 fixed-point 存储；set_config 归一化（tick<=0→1 手、min<=0→tick、
    // max<min→min）并校验 finite 后一次转换。
    int64_t split_min_fp_  = 1'000'000;
    int64_t split_max_fp_  = 100'000'000;
    int64_t split_tick_fp_ = 1'000'000;
    bool volume_limit_ = true;
    bool cancel_latency_enabled_ = false;  ///< 撤单 entry 延迟开关；默认即时撤单
    bool price_limit_enabled_ = false;     ///< 涨跌停 limit-lock 开关；默认撮合不读 limit

    // 撮合时钟
    uint32_t current_date_ = 0;
    uint32_t current_time_ms_ = 0;
    int64_t  cur_event_ns_ = INT64_MIN;  ///< 最近推进到的 event ns（采样/deferred 戳源）。
    uint32_t prev_date_ = 0;
    uint32_t prev_time_ms_ = 0;
    // 下一次 on_tick 新增成交使用的 dispatch 时间；on_tick 末尾消耗并清零。
    uint32_t dispatch_date_ = 0;
    uint32_t dispatch_time_ms_ = 0;

    // 随机源与 ACK 延迟
    std::mt19937 rng_;
    std::uniform_real_distribution<double> uniform_dist_{0.0, 1.0};
    uint64_t ack_delay_us_ = 0;         ///< ACK 延迟均值，单位 us
    uint64_t ack_delay_stddev_us_ = 0;  ///< ACK 延迟标准差，单位 us；0 表示固定延迟
    std::normal_distribution<double> latency_dist_{0.0, 1.0};

    // 外部模型注入
    std::unique_ptr<ILatencyModel> latency_model_;
    std::unique_ptr<IQueueModel> queue_model_;

    // 每合约 total_volume 缓存；线性小表避免回测热路径 hash/rehash，
    // 容量无上限（新 symbol 仅首见时 append，旧槽永不复用）。
    std::vector<VolumeEntry> volume_table_;

    // split fill scratch buffer
    std::vector<int64_t> parts_buf_;
    std::vector<XtTradeRecord> expanded_buf_;

    // order delay wheel
    uint32_t order_delay_ticks_ = 0;
    std::vector<std::vector<DeferredOrder>> delay_wheel_;
    uint32_t wheel_cursor_ = 0;

    /// EOF 丢弃的 delay wheel 订单累计值；回测单线程访问。
    std::uint64_t lost_in_transit_count_ = 0;
};

namespace engine_config {

XtHftMatchEngine::Config parse_match_engine_config(const YAML::Node& node);

} // namespace engine_config
