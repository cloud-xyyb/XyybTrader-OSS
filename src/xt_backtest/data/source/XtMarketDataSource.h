#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "api/IMarketDataReader.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_core/market/XtL2MarketStructs.h"
#include "xt_backtest/data/source/XtMarketDataSourceConfig.h"
#include "xt_backtest/data/legacy/XtBarModeDriver.h"

enum class XtReplayMode : uint8_t {
    Bar,        ///< bar/simtick 驱动路径。
    Tick,       ///< tick-only 回放路径。
    EventStream ///< tick + Level-2 orderflow 统一事件流路径。
};

enum class XtReplayEventType : uint8_t {
    Tick = 0,
    // 以下三类 Level-2 逐笔事件仅用于 event_stream 回放路径；真实数据覆盖仍依赖输入源质量。
    OrderQueue = 1,
    OrderDetail = 2,
    Transaction = 3
};

/**
 * @brief 回测回放事件（backtest replay event）。
 *
 * 统一封装回放时序中的单个事件，由 `type` 指定本次事件的有效 payload 字段。
 * `key` 是统一 sort key，`seq` 是同 key 下的稳定序号，供多源事件合并排序使用。
 * 调用方必须先检查 `type`，再读取对应 payload 字段。
 */
struct XtReplayEvent {
    XtReplayEventType type = XtReplayEventType::Tick; ///< 指示本事件有效 payload。
    uint64_t key = 0; ///< 统一 sort key；tick 与 orderflow 使用同一时间轴。
    uint64_t seq = 0; ///< 相同 key 下的稳定 tie-breaker。
    XtRawTick tick; ///< type=Tick 时有效。
    XtOrderQueue order_queue; ///< type=OrderQueue 时有效。
    XtOrderDetail order_detail; ///< type=OrderDetail 时有效。
    XtTransaction transaction; ///< type=Transaction 时有效。
};

/// @brief 下一事件的时间头（non-consuming peek 用）。
///
/// sort key 只保证序（夜盘旋转编码，不可反解真实时刻）；event-axis 时间
/// 一律取自事件 payload 本头：`to_ns(action_date != 0 ? action_date : trading_date,
/// action_time)`。四类事件 payload 均携带三字段。
struct XtReplayEventHeader {
    uint64_t key = 0;           ///< 统一 sort key（只管序，不是时间源）。
    uint32_t action_date = 0;   ///< 自然日 YYYYMMDD；0 = payload 未携带（fallback trading_date）。
    uint32_t action_time = 0;   ///< HHMMSSmmm。
    uint32_t trading_date = 0;  ///< 交易日 YYYYMMDD。
};

/// @brief tick mode 选中的互斥输入分支。
///
/// `prepare_tick_mode()` 在启动期写入；reader 初始化与 memory 加载阶段只按该值
/// dispatch，不重新解释 YAML 组合。`None` 表示尚未选中来源，或 bin memory
/// load 失败后的 fail-closed 状态；该状态不会自动回退到 CSV。
///
/// 选择顺序：Fbs > EventsParquet > ShardStream > BinStream > Csv。
/// @note simtick-bin 属于 bar driver，不写入本枚举；stream reader 会在
///       BinStream 之后、Csv 之前查询 bar_driver_。
enum class TickSource : uint8_t {
    None = 0,       ///< 未解析 / bin 加载失败降级后的中性态。
    Csv,            ///< XtCsvTickReader / load_csvs_to_memory。
    BinStream,      ///< CSV->bin cache 命中，runtime 走 XtBinTickReader。
    ShardStream,    ///< RawShardV3 分片库命中，runtime 走 XtRawShardTickReader。
    Fbs,            ///< fbs event store（XtFbsTickReader，最高优先级）。
    EventsParquet,  ///< canonical events parquet（XtEventParquetReader）。
};

/// @brief 回测行情数据源（market data source for backtest）。
///
/// 按配置加载 tick/bar/orderflow 数据，并通过 `next_event()` 提供单消费者
/// 顺序读取接口。策略执行、账户更新和报告统计由 runtime/reporter 处理。
///
/// @note 内部维护读取游标和 1-event look-ahead 状态，不支持并发读取。
class XtMarketDataSource {
public:
    // ── 生命周期与主入口 ──────────────────────────────────────────────
    ~XtMarketDataSource();
    /// @brief 按配置准备数据源路径、缓存和运行模式，并重置到首条事件前。
    /// @return true 表示后续 next_event() 可以开始读取。
    bool load(const XtMarketDataSourceConfig& cfg);
    /// @brief 重置运行期游标和 reader；不重新解析配置。
    /// @note 幂等：当前已停在首条事件之前（load() 刚返回，或上次 reset() 之后尚未
    ///       **交付**过任何事件）时本次调用是 no-op——此时运行期状态与已物化数据就
    ///       等于「重建后」的结果，重跑只会逐字重读同一批输入。交付过事件之后调用
    ///       则完整重建（回卷语义不变）。
    /// @note 只 peek 不 next_event 仍算「停在首条之前」：peek 后的 reset() 是 no-op。
    ///       peek 把事件挪进 1-event cache 并把底层游标推进一格，但下一次 next_event
    ///       先交付 cache，消费者看到的序列与「没 peek 过」一字不差。
    void reset();
    /// @brief 读取下一条统一 replay event；tick/bar 模式会包装为 Tick event。
    bool next_event(XtReplayEvent& out);

    /// @brief 查看下一个事件的时间 key，不消费（non-consuming peek）。
    /// @param out_key 若返回 true，写入下一事件的统一 sort key。
    /// @return true 表示还有事件，false 表示数据源枯竭。
    /// @post 调用此方法不改变"已消费事件序列"，可多次调用得到相同结果；
    ///       下一次 next_event 将返回被 peek 的事件。
    /// @note 内部通过 cached_next_event_ 缓存实现 1-event look-ahead。
    bool peek_next_key(uint64_t& out_key);

    /// @brief 查看下一个事件的时间头，不消费（non-consuming peek）。
    /// @return 同 peek_next_key；true 时 out 填充 key + 三时间字段（按事件类型抽取）。
    bool peek_next_event_header(XtReplayEventHeader& out);

    XtReplayMode mode() const { return mode_; }
    bool streaming() const { return use_stream_; }
    /// @brief 配置的 `data_mode: memory` 被 tick 源强制降级为 stream 的次数。
    /// @note fbs / events-parquet reader 只支持按 trading_day lazy open 的流式读取，
    ///       配 memory 也照 stream 跑。纯观测不改行为；首次降级在 load() 期告警。
    uint64_t data_mode_ignored_count() const { return data_mode_ignored_count_; }
    /// @brief 回放期间底层 reader 是否报过 I/O 错误（sticky，reset() 清零）。
    /// @note runner 必须在 replay 结束后检查：错误时 fail run——否则中段
    ///       损坏的数据源会被当成正常 EOF 静默截断出假结果。
    bool has_io_error() const { return stream_io_error_; }

    // ── 消费清单观测（consumed manifest；runner 收尾读取落盘） ──────────
    /// @brief load() 解析后实际喂给加载/回放层的 tick 源文件列表。
    /// @note bin cache 命中时数据从合并 bin 读出，本列表仍为其源 CSV 集合
    ///       （cache 签名覆盖同一文件集）；fbs/parquet 分支该列表为空。
    const std::vector<std::string>& consumed_tick_files() const { return tick_.files; }
    /// @brief 当前 tick 输入分支名，与 TickSource 枚举一一对应。
    const char* tick_source_name() const {
        switch (tick_.source) {
            case TickSource::Csv:           return "csv";
            case TickSource::BinStream:     return "bin_stream";
            case TickSource::ShardStream:   return "shard_stream";
            case TickSource::Fbs:           return "fbs";
            case TickSource::EventsParquet: return "events_parquet";
            default:                        return "none";
        }
    }
    bool bin_cache_enabled() const { return tick_.use_bin_cache; }
    /// @brief 本次 load() 是否命中既有 bin cache（enabled 且未重建）。
    bool bin_cache_hit() const { return tick_.bin_cache_hit; }
    /// @brief bin cache 文件路径；未走 cache 为空串。
    std::string bin_cache_path() const {
        return tick_.bin_paths.empty() ? std::string() : tick_.bin_paths.front();
    }
    /// @brief 本次 load() 选中的 RawShardV3 分片路径（按交易日升序）；未走分片库为空。
    const std::vector<std::string>& shard_cache_paths() const { return tick_.shard_paths; }
    /// @brief 本次 load() 是否整窗命中分片池（一天都没重建）。
    bool shard_cache_hit() const { return tick_.shard_cache_hit; }

    /// @brief RawShardV3 建库的复用/重建分解（增量是否真的生效，看这组数）。
    struct ShardCacheStats {
        uint64_t days_required = 0;  ///< 窗内需要的交易日数。
        uint64_t days_reused = 0;    ///< 直接复用池内既有分片的天数。
        uint64_t days_built = 0;     ///< 本轮新建的天数。
        /// @brief 是否退回全量重建（读全部源、产出全部天）。
        ///
        /// 只在交易日全集失去依据时发生：某个文件名无日期的源尚未入册（首次见到，或
        /// 签名变了），它里面藏着哪些交易日既圈不出也查不到。入册之后清单顶上，同一批
        /// 源不再触发。是保守兜底，不是常态。
        bool full_rebuild = false;

        // ── 回落与异常（非零即意味着本轮没走成分片库，或 family 里有坏产物）──

        /// @brief 因源列表含重复路径而整批让位给默认 cache 路径的次数。
        ///
        /// 重复路径下两条路交付的条数不同（默认路径把同一文件读两遍，分片库按路径去重
        /// 只读一遍），分片给不出前者的口径，故检出即回落。
        uint64_t duplicate_sources = 0;
        /// @brief 计划已定却交付不出分片路径的次数（分片在计划落定后消失/损坏）。
        uint64_t publish_failed = 0;
        /// @brief CURRENT generation 里解析不了的 forensic 产物个数（清单 / 闭包索引）。
        ///
        /// 非零不改变交付的任何一个字节：分片走池内指纹直接命中，不读清单；闭包索引读不到
        /// 只是让本轮重扫一遍源、重新入册（指纹与产出不变，代价是多读一次）。它只说明这个
        /// family 目录里躺着半份产物或被外部改坏的文件。
        uint64_t manifest_malformed = 0;
    };
    const ShardCacheStats& shard_cache_stats() const { return tick_.shard_stats; }

    /// @brief 载入→交付分类丢弃计数（consumed manifest 分解用）。
    ///
    /// `rows_loaded` = 交付前总读入行数（-1 = 该来源不可归因）；`window/session/
    /// skip_last/symbol/bad_row` = 数据源层丢弃分类。CSV/bin 路可精确归因
    /// （`loaded_trackable=true`）；fbs/parquet reader 内部预筛不可见 → 不可归因。
    /// 满足（trackable 时）：`rows_loaded == 交付 tick 数 + window + session +
    /// skip_last + symbol + bad_row`。stream 路在 next_event 消费期间累积，
    /// 故需在 replay 全部消费后读取。
    struct DropStats {
        int64_t rows_loaded = -1;  ///< 交付前总读入；-1 = 不可归因。
        int64_t window   = 0;      ///< 交易日窗外丢弃。
        int64_t session  = 0;      ///< session 时段外丢弃。
        int64_t skip_last = 0;     ///< 每 symbol/day 去尾丢弃。
        int64_t symbol   = 0;      ///< 合约白名单不命中丢弃。
        int64_t bad_row  = 0;      ///< 坏行丢弃。
        bool loaded_trackable = false;  ///< rows_loaded 是否覆盖来源全部读入行。
    };
    const DropStats& drop_stats() const { return drop_stats_; }

private:
    // ── 运行期状态模型 ──────────────────────────────────────────────
    /// @brief 回放交易日窗口；0 表示对应边界未启用。
    struct DateRange {
        uint32_t start = 0; ///< YYYYMMDD inclusive；0 表示未配置下界。
        uint32_t end   = 0; ///< YYYYMMDD inclusive；0 表示未配置上界。
    };

    /// @brief tick mode 的配置快照、输入选择和 stream reader 状态。
    struct TickSourceState {
        double qty_base = 100.0; ///< simple CSV 或 simtick 缺省盘口数量。
        std::string volume_mode = "cumulative"; ///< CSV/parquet volume 解释方式。
        std::string root; ///< tick.root 目录；prepare 阶段可扫描出 files。
        std::vector<std::string> files; ///< 已解析且存在的 CSV tick 文件列表。
        std::vector<std::string> symbols; ///< tick symbols 白名单；空表示不过滤。
        std::unordered_set<std::string> symbol_set; ///< symbols 的 membership view。
        TickSource source = TickSource::None; ///< 解析得出的 tick source 种类；apply_common_config 归零，prepare_tick_mode 写入。
        std::string cache_dir = "data/ticks/cache"; ///< CSV->bin cache 目录。
        bool use_bin_cache = false; ///< true 表示允许 prepare 阶段构建/复用 bin cache。
        bool bin_cache_hit = false; ///< ensure_tick_bin_cache 命中既有 cache（非重建）；apply_common_config 归零。
        std::vector<std::string> bin_paths; ///< bin stream 输入列表；当前 cache 协议通常只有一个文件。
        std::string cache_format = "v2"; ///< tick cache 落盘格式；`v3` 走 RawShardV3 分片库。
        std::vector<std::string> shard_paths; ///< 命中窗口的 RawShardV3 分片，按交易日升序。
        bool shard_cache_hit = false; ///< 整窗命中分片池（一天都没重建）；apply_common_config 归零。
        ShardCacheStats shard_stats; ///< 分片库复用/重建分解；apply_common_config 归零。
        std::unique_ptr<IMarketDataReader> reader; ///< 当前 stream reader；reset_runtime_state() 释放。
        bool reader_is_bar_simtick = false; ///< reader 为 bar-driver simtick bin：走 legacy 直通，
                                            ///< 不套 tick-mode 准入（其 memory 孪生同样不过滤）。

        // fbs/parquet/bin/csv 四源互斥由 source 枚举编码（见 TickSource）。
        std::string fbs_root;            ///< 非空时走 XtFbsTickReader。
        std::string events_parquet_root; ///< 非空时走 XtEventParquetReader。
    };

    /// @brief memory data mode 的 tick 缓存与消费游标。
    struct MemoryState {
        size_t cursor = 0; ///< 下一条 memory tick 下标。
        std::vector<XtRawTick> ticks; ///< memory 模式或 event_stream 构建前的 tick 缓存。
    };

    /// @brief skip_last_tick_per_day 流式实现的单日缓冲；reset_runtime_state 归零。
    struct DayBufferState {
        std::vector<XtRawTick> ready; ///< 当日已去尾、待放行的 tick。
        size_t cursor = 0;            ///< ready 的消费游标。
        XtRawTick pending{};          ///< 读越日界时暂存的下一日首 tick。
        bool has_pending = false;
        bool exhausted = false;       ///< 底层 reader 已 EOF。
        bool seen_any = false;        ///< 此前 chunk 曾有 admitted tick（单 tick 数据集边界语义用，
                                      ///< 与 memory 的全局 size()>1 guard 对齐——被去尾清空的 chunk 也算见过）。
    };

    /// @brief event_stream 模式的 Level-2 orderflow 输入文件集合。
    struct OrderFlowState {
        std::vector<std::string> order_queue_files; ///< Level-2 order queue CSV 文件列表。
        std::vector<std::string> order_detail_files; ///< Level-2 order detail CSV 文件列表。
        std::vector<std::string> transaction_files; ///< Level-2 transaction CSV 文件列表。
    };

    // ── 配置加载阶段 ──────────────────────────────────────────────
    void apply_common_config(const XtMarketDataSourceConfig& cfg);
    bool collect_existing_tick_files(bool fail_on_missing);
    bool prepare_tick_mode(const XtMarketDataSourceConfig& cfg);
    void reset_runtime_state();
    void init_stream_reader();

    // ── 读取与转换阶段 ──────────────────────────────────────────────
    void load_memory_source();
    bool load_csvs_to_memory(const std::vector<std::string>& paths);
    bool build_tick_bin_from_csvs(const std::vector<std::string>& csv_paths, const std::string& bin_path);
    bool ensure_tick_bin_cache(const std::vector<std::string>& sources);
    /// @brief 准备 RawShardV3 分片库（`cache_format: v3` 时的 cache 路径）。
    ///
    /// 定位 family（身份不含窗口）→ 按闭包圈出窗内交易日 → 逐日查分片池，缺哪天建哪天
    /// → 写 generation 清单并提交 CURRENT。任一环节不可用（建库失败/脏日期行/命名约定
    /// 被打破/缺片）都返回 false，让调用方逐级回落 v2 → CSV，交付的 tick 序列与不开 v3
    /// 时逐条一致。
    /// @param all_sources 源**全集**（与窗口无关）：显式 tick.files 原样，root 扫描则是
    ///        全月扫描结果。传窗口子集会让每个窗口各自成家，增量随之失效。
    /// @param allow_undated_sources 文件名无日期的源是否可接受。root 有窗扫描时为 false：
    ///        那种情形下 CSV 路对无日期文件的取用随窗口的月份跨度变化，分片无法给出一份
    ///        与窗口无关的内容。
    /// @return true 表示 `tick_.shard_paths` 已就绪。
    bool ensure_tick_shard_cache(const std::vector<std::string>& all_sources,
                                 bool allow_undated_sources);
    bool load_tick_bin_into_memory();
    /// @brief 把选中的 RawShardV3 分片整体装入 mem_.ticks（data_mode: memory 路径）。
    /// @return false 表示无分片、分片损坏或窗内无 tick；调用方按 fail-closed 处理。
    bool load_tick_shards_into_memory();
    bool build_event_stream_cache();
    // tick-only 读取原语；唯一 caller 是非 event_stream 路的 read_next_event_uncached_()
    // （它把 tick 包装成 Tick event）。这条「唯一」是承重的：read_next_event_uncached_()
    // 因此是推进底层游标的穷尽入口，reset() 幂等闸的正确性建立在它之上——本原语若被
    // 直接对外开放，绕过漏斗的消费就会让闸门误判「还停在首条之前」。
    bool next_tick(XtRawTick& out);
    bool read_next_event_uncached_(XtReplayEvent& out);
    /// @brief stream 读端逐 tick 准入：range 外 / session 外 → false（丢弃）；
    ///        保留的 tick 按 session 语义改写 kind（Normal/Preopen）。
    ///        与 memory 加载共用 detail/XtTickAdmission.h 判定，保证两种
    ///        data_mode 对同一 config 产生同一 tick 序列。
    /// @note 非 const：drop 分支自增 drop_stats_.window/session（accepted 路零改）。
    bool admit_stream_tick_(XtRawTick& t);
    /// @brief skip_last_tick_per_day 的流式实现：按交易日缓冲一天（内存上界 =
    ///        单日 tick 量），日切/EOF 时全局去每 symbol 最后一条后保序放行，
    ///        语义与 memory 的整批 drop 逐字一致。
    bool next_tick_day_buffered_(XtRawTick& out);
    /// @brief reader 返 false 后查 has_io_error()，错误则闩入 stream_io_error_。
    void latch_reader_io_error_();
    /// @brief stream 路 reader EOF 后归因 drop_stats_.rows_loaded / symbol / bad_row。
    ///        bin 无内部丢弃 → rows_loaded=读入数；CSV 拉 reader read_stats（含
    ///        symbol/bad_row）；fbs/parquet 内部预筛不可见 → rows_loaded=-1。幂等。
    void finalize_stream_load_stats_();
    /// @brief 记录 `data_mode: memory` 被强制降级为 stream（首次告警，之后只计数）。
    /// @param source_tag 触发降级的 tick 源名（进日志，如 "fbs"）。
    /// @note 已是 stream 时为 no-op——只有真降级才计数，避免污染观测量。
    void note_stream_forced_(const char* source_tag);

    // ── 主状态字段 ──────────────────────────────────────────────
    XtReplayMode mode_      = XtReplayMode::Bar; ///< 当前 replay 模式，由 config mode/replay_mode 派生。
    bool         use_stream_ = true; ///< true 表示优先通过 reader 流式读取。
    bool         skip_last_tick_per_day_ = false; ///< 过滤每个 symbol/day 最后一条 tick（match_delay>=1 时 HFT config 自动启用，对齐 WT store_ticks）。
    xt_core::Config session_;  ///< session 过滤配置（load 阶段应用，默认 disabled = no-op）。
    DateRange   range_; ///< load() 应用的交易日范围。
    TickSourceState   tick_;  ///< tick mode 的配置快照与 runtime reader 状态。
    MemoryState mem_;   ///< memory 模式下的 tick 缓存和消费游标。
    DayBufferState day_buf_;  ///< stream × skip_last_tick_per_day 的单日缓冲。
    XtBarModeDriver bar_driver_;  ///< bar/simtick 输入路径；mode=bar 与 simtick-bin 使用。
    OrderFlowState  orderflow_; ///< event_stream 模式的 Level-2 orderflow 输入文件。
    std::vector<XtReplayEvent> events_; ///< event_stream 模式下已排序事件缓存。
    size_t      event_cursor_   = 0;    ///< 下一条 events_ 下标。

    std::optional<XtReplayEvent> cached_next_event_; ///< peek_next_* 预读但尚未消费的事件。
    bool stream_io_error_ = false; ///< reader I/O 错误 sticky 标记；reset_runtime_state() 清零。
    uint64_t data_mode_ignored_count_ = 0; ///< memory→stream 强制降级次数；纯观测，不随 reset 清零。
    DropStats drop_stats_;         ///< 载入→交付分类丢弃计数；reset_runtime_state() 清零。
    int64_t stream_records_read_ = 0; ///< stream 路 reader 已读入记录数（rows_loaded 归因）。
    bool stream_stats_finalized_ = false; ///< finalize_stream_load_stats_ 幂等闸。
    /// @brief 是否停在首条事件之前（reset() 幂等闸）。
    ///
    /// 置位：reset() 的四条物化分支全部**正常**走完之后（异常中途退出不置位，见其实现
    /// 里的成功域守卫）。清位：next_event() 真的交付一条事件时（cache 与 uncached 两条
    /// 交付路径）；以及 apply_common_config()——新一轮 load() 重新解析配置，上一轮物化
    /// 结果按新配置作废。**peek 不清位**：它只把事件挪进 cache，消费序列不变。
    /// 置位期间 reset() 早退，见其声明处契约。
    bool at_start_ = false;
};
