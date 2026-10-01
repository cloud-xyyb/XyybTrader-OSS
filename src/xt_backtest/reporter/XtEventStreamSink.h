#pragma once
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>

#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "XtEventStreamSinkCfg.h"

struct XtRawTick;

/// @brief 回测事件流导出器。
///
/// 本类写出 tick、订单、成交和系统状态事件，并从订单生命周期派生 latency 事件。
/// Full 模式写入 `events_full.csv`；Off 模式下所有事件入口直接返回。
/// 所有时间参数使用 packed YYYYMMDD*1e9 + HHMMSSmmm。
class XtEventStreamSink {
public:
    using Config = XtEventStreamSinkCfg;

    // ── 初始化与配置 ──────────────────────────────────────────────────────
    XtEventStreamSink() = delete;
    /// @param report_dir 输出目录（caller resolve 后传入，单一 truth；相对路径
    ///        按 base_dir 解析）。yaml backtest.report_dir 由 XtReportCfg 单点解析。
    explicit XtEventStreamSink(const Config& cfg, std::string base_dir = "",
                               std::string report_dir = "reports");
    /** @brief 是否启用事件记录（trace mode != Off）。 */
    bool enabled() const { return trace_mode_ != Config::TraceMode::Off; }

    // ── 事件写入接口 ──────────────────────────────────────────────────────
    /** @brief 记录 tick 事件（仅 export_tick_=true 时有效）。 */
    void on_tick(int64_t ts, const XtRawTick& t);
    /** @brief 记录订单状态更新事件（order update）。 */
    void on_order_update(int64_t ts, const XtOrderExecUpdate& u);
    /** @brief 记录成交事件（trade fill）。 */
    void on_trade(int64_t ts, const XtTradeRecord& tr);
    /** @brief 记录下单提交事件（order submit）。 */
    void on_order_submit(int64_t ts,
                         uint64_t order_id,
                         const char* code,
                         bool is_buy,
                         int64_t price_fp,
                         int64_t qty_fp,
                         std::string_view user_tag,
                         bool is_close,
                         bool is_today);
    /** @brief 记录系统状态快照（在途订单数、丢包、重连）。
     *
     * 采样按 `on_sys_stats()` 调用次数计数，而不是对 packed timestamp 取模；
     * packed ts 不是均匀递增的 tick 序号。
     */
    void on_sys_stats(int64_t ts, size_t live_orders, int drops = 0, int reconnects = 0);

    // ── 刷盘 ──────────────────────────────────────────────────────────────
    /** @brief 将缓冲区内容落盘（batch flush）。 */
    void flush();

private:
    // ── 延迟链路跟踪结构 ──────────────────────────────────────────────────
    /** @brief 单笔订单延迟链路状态（latency tracking per order）。
     *
     * `*_ts == 0` 表示该链路阶段尚未到达；非零值均为 packed timestamp
     * (YYYYMMDD*1e9 + HHMMSSmmm)。订单终态时聚合成一行 `latency` 事件写入 full 流。
     *
     * 六腿时间语义（latency legs）：
     *   - signal_ts ：策略发信号时刻（本实现 == order_ts，submit tick 的 packed ts）。
     *   - order_ts  ：下单提交 tick 的 packed ts（on_order_submit 设置）。
     *   - ack_ts    ：首个 Accepted update 交付 tick 的 packed ts（on_order_update）。
     *   - match_ts  ：撮合钟面 packed（取 XtTradeRecord.trade_date/trade_time；BT 路径
     *                 trade_date 由撮合引擎用交易日灌入，夜盘与自然日轴分叉，仅作钟面参考）。
     *   - model_visible_ts：延迟模型可见时刻 packed（on_trade 从
     *                 XtTradeRecord.sim_visible_at_ns 换算，自然日轴；match event +
     *                 response latency，未经交付量化；首笔 fill 为准，与 fill_ts 对称；
     *                 CSV key 为 visible_at——viewer 侧 legs.visible_ts 指「交付」即本
     *                 结构的 fill_ts，勿混）。
     *   - fill_ts   ：成交交付 tick 的 packed ts（on_order_update 的 PartiallyFilled/Filled
     *                 首次交付）= 成交对策略可见时刻；交付分辨率受 min(下一 market tick,
     *                 250ms clock grid) 量化。
     *
     * @note match_ts 记录「撮合发生」，model_visible_ts 记录「模型可见」，fill_ts 记录
     *       「成交交付」。同一单排序不变量：model_visible_ts ≤ fill_ts（同自然日轴数值
     *       序，可直接比，差 = 交付量化滞后）；match_ts(钟面) ≤ model_visible_ts 仅作
     *       钟面参考，且在模型可见跨午夜（撮合 23:59:59.x → 可见次日 00:00:00.x）时
     *       钟面比较失效，不作硬不变量。
     * @note 除 match_ts 外五腿均在自然日 packed 轴（单一单调时钟）；match_ts 的 date 位
     *       在 BT 路径是交易日，跨腿做差值时只比钟面不比 date。
     */
    struct LatencyInfo {
        std::string code;            ///< 合约代码（on_order_submit 设置）
        uint64_t symbol_id = 0;      ///< 合约 intern key（on_order_update 备选标识，code 为空时输出此值）
        bool is_buy = true;          ///< 买卖方向
        int64_t signal_ts = 0;       ///< 信号发出时的 packed ts（0 = 未到达）
        int64_t order_ts = 0;        ///< 下单时的 packed ts
        int64_t ack_ts = 0;          ///< 确认时的 packed ts
        int64_t fill_ts = 0;         ///< 成交交付时的 packed ts
        int64_t match_ts = 0;        ///< 撮合钟面 packed（on_trade 设置；见「六腿时间语义」）
        int64_t model_visible_ts = 0;  ///< 模型可见时刻 packed（on_trade 设置，自然日轴；首笔 fill 为准）
        XtOrderStatus last_status = XtOrderStatus::Submitted;  ///< 最新订单状态
        bool terminal_seen = false;  ///< 已收到终态回报，尚等同单 trade 补 match/visible 腿
        int64_t terminal_ts = 0;     ///< 终态回报的 packed ts（latency 行的事件时刻）
    };

    /** @brief 单行事件（event row）。 */
    struct RingEventRow {
        uint64_t seq = 0;           ///< 全局递增序列号（monotonic sequence number）
        int64_t ts = 0;             ///< 事件时刻的 packed timestamp
        std::string event_type;     ///< 事件类型标识（如 "TICK"、"FILL"）
        std::string payload;        ///< CSV 格式的事件负载
    };

    // ── 内部工具函数 ──────────────────────────────────────────────────────
    void set_config(const Config& cfg);
    /** @brief Full 模式下打开 events_full.csv 并写入列头。 */
    void open_files();
    /** @brief 构造 {ts, seq, event_type, payload} 行并写入 full 流。 */
    void append_ring_event(int64_t ts, const char* event_type, const char* payload, int payload_len);
    /** @brief full 模式下将统一事件流持续导出。 */
    void write_full_event(const RingEventRow& row);
    /** @brief 计算微观结构指标（spread/depth/ofi）。 */
    void compute_micro_metrics(const XtRawTick& t, double& spread, double& depth, double& ofi);
    /** @brief 当链路信息完整时写入延迟记录。 */
    /// @brief 回收终态已到、trade 始终未到的 latency 条目（输出后 erase）。
    /// @return 本次回收条数。
    size_t sweep_terminal_latency_();

    void write_latency_if_ready(uint64_t order_id, const LatencyInfo& info, int64_t ts);

    // ── 配置与输出控制 ──────────────────────────────────────────────────
    Config::TraceMode trace_mode_ = Config::TraceMode::Off;  ///< 追踪模式（Off / Full）
    bool export_tick_ = false;                  ///< 是否导出 tick 事件
    std::string report_dir_ = "reports";        ///< 报告输出子目录名
    std::string base_dir_;                      ///< 报告根目录
    size_t full_max_rows_ = 2000000;            ///< Full 模式最大行数
    uint64_t full_max_bytes_ = 512ULL * 1024ULL * 1024ULL;  ///< Full 模式最大字节数
    size_t max_latency_map_entries_ = 4096;     ///< 延迟追踪 map 最大条目数（防无界增长）
    uint64_t ring_event_seq_ = 0;               ///< 全局递增序列号（events_full.csv seq 列来源）
    size_t full_rows_written_ = 0;              ///< Full 模式已写行数
    uint64_t full_bytes_written_ = 0;           ///< Full 模式已写字节数
    bool full_row_cap_logged_      = false;     ///< Full 行数上限日志是否已输出
    bool full_bytes_cap_logged_    = false;     ///< Full 字节上限日志是否已输出
    bool latency_map_full_logged_  = false;     ///< 延迟 map 满载日志是否已输出
    size_t latency_terminal_no_trade_ = 0;      ///< 终态已到但 trade 始终未到、被回收扫出的条目数
    size_t sys_stats_sample_n_ticks_ = 100;     ///< 系统状态采样间隔（按 on_sys_stats 调用次数）
    size_t sys_stats_call_seq_ = 0;             ///< sys_stats 内部调用计数

    // ── 文件句柄 ──────────────────────────────────────────────────────────
    std::ofstream full_events_;     ///< Full 模式输出文件（events_full.csv）

    // ── 行构造 scratch（复用 string capacity，事件入口零节点分配）────────
    RingEventRow scratch_row_;

    // ── 延迟状态缓存 ──────────────────────────────────────────────────────
    std::unordered_map<uint64_t, LatencyInfo> latency_map_;  ///< order_id → 延迟链路状态（上界受 max_latency_map_entries_ 限制）

    // ── payload 格式化缓冲区（栈复用，避免 ostringstream 聚合）──────
    char fmt_buf_[1024];  ///< snprintf 格式化缓冲区（单线程，无需同步）
};
