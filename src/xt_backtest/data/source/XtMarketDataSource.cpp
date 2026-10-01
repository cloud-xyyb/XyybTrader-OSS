#include "xt_backtest/data/source/XtMarketDataSource.h"
#include "XtLogger.h"
#include "TickCsvUtil.h"  // make_tick_sort_key
#include "xt_backtest/data/reader/XtCsvTickReader.h"  // stream-CSV read_stats 归因
#include "xt_backtest/data/source/detail/XtTickAdmission.h"
#include "xt_core/market/XtTick.h"  // TickKind

#include <exception>  // std::uncaught_exceptions（reset() 的成功域置位守卫）

// XtMarketDataSource 的主 TU 只负责编排生命周期、运行期 reset 和读端分发。
// source 选择在 *_prepare.cpp 完成，memory/session/event cache 构建在 *_memory.cpp 完成；
// 本文件维护单消费者游标、stream reader 和 1-event look-ahead cache 的一致性。

XtMarketDataSource::~XtMarketDataSource() = default;

// ---------------------------------------------------------------------------
// 生命周期
// ---------------------------------------------------------------------------

bool XtMarketDataSource::load(const XtMarketDataSourceConfig& cfg) {
    apply_common_config(cfg);
    if (mode_ == XtReplayMode::EventStream && cfg.mode != "tick") {
        XtLogger::error_m("MD", "event_stream requires datasource.mode=tick");
        return false;
    }
    const bool prepared = (mode_ == XtReplayMode::Tick || mode_ == XtReplayMode::EventStream)
        ? prepare_tick_mode(cfg)
        : bar_driver_.prepare(cfg);
    if (!prepared) return false;

    reset();
    if (mode_ == XtReplayMode::EventStream && events_.empty()) return false;
    return true;
}

void XtMarketDataSource::reset() {
    // 幂等闸：已停在首条事件之前时，运行期状态与已物化数据就等于「重建后」的结果，
    // 再跑一遍只是逐字重读同一批输入——memory 模式下即全量重装载（GB 级 bin / CSV
    // collect 直接翻倍）。load() 尾部与 XtReplayer::run() 开头各调一次 reset()，
    // 这条早退让一次回放只物化一次；交付过事件后 at_start_ 清位，回卷仍走完整重建。
    if (at_start_) return;
    reset_runtime_state();

    // 置位必须等物化真的完成：四条物化分支任一中途抛出（GB 级 bin 的 bad_alloc、
    // 解析异常）时闸门保持清位，下一次 reset() 仍会完整重建，不把半装载态锁成「已就绪」。
    struct ArmOnNormalExit {
        bool& flag;
        int entry_uncaught = std::uncaught_exceptions();
        ~ArmOnNormalExit() { if (std::uncaught_exceptions() == entry_uncaught) flag = true; }
    } arm{at_start_};

    if (mode_ == XtReplayMode::EventStream) {
        // EventStream 读端只消费 materialized mem_.ticks 与 events_ sorted cache；
        // BinStream 需临时走 memory load，避免 event merge 和 stream reader 双游标推进。
        const bool saved_stream = use_stream_;
        if (tick_.source == TickSource::BinStream) use_stream_ = false;
        load_memory_source();
        use_stream_ = saved_stream;
        if (!build_event_stream_cache())
            XtLogger::error_m("MD", "event_stream cache is empty, load will fail");
        return;
    }

    // 非 EventStream reset 保持三路互斥：BinStream memory、stream reader、memory source。
    // 这里不重新解释 YAML；TickSource 已由 prepare_tick_mode() 固化。
    if (tick_.source == TickSource::BinStream && !use_stream_) {
        if (load_tick_bin_into_memory()) return;
        // bin cache 命中后 CSV 只作为构建输入；runtime 加载失败必须 fail-closed，
        // 避免同一 load() 周期内悄悄换输入源导致回放样本漂移。
        XtLogger::error_m("MD", "failed to load tick bin {}",
                          tick_.bin_paths.empty() ? std::string{} : tick_.bin_paths.front());
        tick_.source = TickSource::None;
    }
    if (tick_.source == TickSource::ShardStream && !use_stream_) {
        if (load_tick_shards_into_memory()) return;
        // 同 bin：分片库选定之后 CSV 只是建库输入，runtime 装载失败不得改换输入源。
        XtLogger::error_m("MD", "failed to load tick shards ({} selected)",
                          tick_.shard_paths.size());
        tick_.source = TickSource::None;
    }
    if (use_stream_) { init_stream_reader(); return; }
    load_memory_source();
}

// ---------------------------------------------------------------------------
// 读取接口
// ---------------------------------------------------------------------------

bool XtMarketDataSource::next_tick(XtRawTick& out) {
    if (cached_next_event_) {
        // peek_next_key() 可能已预读下一条 event；若它不是 Tick，tick-only 入口不能越过它。
        if (cached_next_event_->type != XtReplayEventType::Tick) return false;
        out = cached_next_event_->tick;
        cached_next_event_.reset();
        return true;
    }

    // EventStream reset 已 materialize mem_.ticks 并构建 events_；该兼容入口只读
    // memory tick 游标，不推进底层 reader 或 event_cursor_。
    if (!use_stream_ || mode_ == XtReplayMode::EventStream) {
        if (mem_.cursor >= mem_.ticks.size()) return false;
        out = mem_.ticks[mem_.cursor++];
        return true;
    }
    if (tick_.reader) {
        // bar-driver simtick bin 走 legacy 直通：其 memory 孪生（XtBarModeDriver
        // load_simtick_bin）不做 range/session/skip_last，套 tick-mode 准入反而
        // 制造 bar 路径的 data_mode 分叉。
        if (tick_.reader_is_bar_simtick) {
            if (tick_.reader->next_tick(out)) return true;
            latch_reader_io_error_();
            return false;
        }
        // skip_last_tick_per_day（match_delay>=1 自动启用）需要知道每 symbol/day 的
        // 最后一条——流式下按交易日缓冲一天，日切/EOF 时全局去尾后保序放行，
        // 语义与 memory 的 drop_last_tick_per_symbol_day 逐字一致。
        if (skip_last_tick_per_day_) return next_tick_day_buffered_(out);
        // 逐条准入：range/session 过滤在读端完成，与 memory 加载（整批 compaction）
        // 同判定。fbs/parquet reader 内部已按日期开窗，此处复核为同规则 no-op。
        while (tick_.reader->next_tick(out)) {
            ++stream_records_read_;
            if (admit_stream_tick_(out)) return true;
        }
        latch_reader_io_error_();
        finalize_stream_load_stats_();
        return false;
    }
    if (bar_driver_.stream_active())
        return bar_driver_.next_tick(out);
    return false;
}

bool XtMarketDataSource::next_tick_day_buffered_(XtRawTick& out) {
    // 出队优先：上一批已去尾的当日 tick 顺序放行。
    while (true) {
        if (day_buf_.cursor < day_buf_.ready.size()) {
            out = day_buf_.ready[day_buf_.cursor++];
            return true;
        }
        if (day_buf_.exhausted && !day_buf_.has_pending) return false;

        // 装填下一交易日：从 pending（上一轮的日界首 tick）或 reader 续读，
        // 直到 grouping_day 变化或 EOF。
        day_buf_.ready.clear();
        day_buf_.cursor = 0;
        uint32_t day = 0;
        if (day_buf_.has_pending) {
            day = xt_bt_admission::grouping_day(day_buf_.pending);
            day_buf_.ready.push_back(day_buf_.pending);
            day_buf_.has_pending = false;
        }
        XtRawTick t;
        while (true) {
            if (!tick_.reader || !tick_.reader->next_tick(t)) {
                latch_reader_io_error_();
                finalize_stream_load_stats_();
                day_buf_.exhausted = true;
                break;
            }
            ++stream_records_read_;
            if (!admit_stream_tick_(t)) continue;
            const uint32_t d = xt_bt_admission::grouping_day(t);
            if (day == 0) day = d;
            if (d != day) {
                day_buf_.pending = t;
                day_buf_.has_pending = true;
                break;
            }
            day_buf_.ready.push_back(t);
        }
        // 与 memory 整批语义对齐：memory 对全量 vector 判 size>1 才 drop——
        // 「整个数据集只有 1 条 tick」时保留该条；其余情况每 symbol/day 去尾
        // （单条日的那条即其 key 的最后一条，同样被去掉）。判据用「此前 chunk
        // 曾有 admitted tick」（seen_any，含被去尾清空的 chunk）而非「曾放行」，
        // 否则「前面每日都塌缩清空 + 末日单 tick」会错误保留末条。
        const bool chunk_had_ticks = !day_buf_.ready.empty();
        const bool dataset_bigger_than_one =
            day_buf_.seen_any || day_buf_.has_pending || day_buf_.ready.size() > 1;
        if (day_buf_.ready.size() > 1) {
            const size_t before = day_buf_.ready.size();
            xt_bt_admission::drop_last_tick_per_symbol_day(day_buf_.ready);
            drop_stats_.skip_last += static_cast<int64_t>(before - day_buf_.ready.size());
        } else if (day_buf_.ready.size() == 1 && dataset_bigger_than_one) {
            day_buf_.ready.clear();
            ++drop_stats_.skip_last;
        }
        if (chunk_had_ticks) day_buf_.seen_any = true;
    }
}

void XtMarketDataSource::latch_reader_io_error_() {
    if (tick_.reader && tick_.reader->has_io_error()) stream_io_error_ = true;
}

void XtMarketDataSource::finalize_stream_load_stats_() {
    if (stream_stats_finalized_) return;
    stream_stats_finalized_ = true;
    switch (tick_.source) {
        case TickSource::Csv: {
            // CSV reader 内部丢弃 symbol/bad_row（不进 stream_records_read_）；
            // 拉 reader 精确计数，rows_loaded 取送 parse 的数据行总数。
            if (auto* csv = dynamic_cast<XtCsvTickReader*>(tick_.reader.get())) {
                const auto& rs = csv->read_stats();
                drop_stats_.rows_loaded = rs.lines_read;
                drop_stats_.symbol  += rs.symbol;
                drop_stats_.bad_row += rs.bad_row;
                drop_stats_.loaded_trackable = true;
            } else {
                drop_stats_.rows_loaded = stream_records_read_;
                drop_stats_.loaded_trackable = false;
            }
            break;
        }
        case TickSource::BinStream:
        case TickSource::ShardStream:
            // bin / 分片都无内部丢弃：读入记录数即交付前总量。
            drop_stats_.rows_loaded = stream_records_read_;
            drop_stats_.loaded_trackable = true;
            break;
        default:
            // fbs / parquet / bar-simtick：reader 内部预筛不可见 → 不可归因。
            drop_stats_.rows_loaded = -1;
            drop_stats_.loaded_trackable = false;
            break;
    }
}

bool XtMarketDataSource::admit_stream_tick_(XtRawTick& t) {
    using xt_bt_admission::TickAdmit;
    // drop 分支自增分类计数；accepted（return true）路零新增指令。
    if (!xt_bt_admission::in_day_range(t, range_.start, range_.end)) {
        ++drop_stats_.window;
        return false;
    }
    switch (xt_bt_admission::classify_session(t, session_)) {
        case TickAdmit::Drop:      ++drop_stats_.session; return false;
        case TickAdmit::Preopen:   t.kind = static_cast<uint32_t>(TickKind::Preopen); return true;
        case TickAdmit::Normal:    t.kind = static_cast<uint32_t>(TickKind::Normal);  return true;
        case TickAdmit::NoSession: return true;
    }
    return true;
}

bool XtMarketDataSource::next_event(XtReplayEvent& out) {
    // 幂等闸清位点 = 真的**交付**了一条事件（两条交付路径都在本函数内）。放在这里而非
    // 底层漏斗，是因为 peek 只把事件挪进 cache、不改变调用方看到的消费序列：peek 之后
    // 「cache 里一条 + 底层游标前进一格」与「什么都没读」对消费者等价（下次 next_event
    // 先交付 cache，序列一字不差），故 peek 后的 reset() 应保持 no-op，不为一次前瞻付
    // 一次全量重装载。空源/全过滤源上 peek 失败同理：EOF 已 latch，闸门保持置位，
    // reset() no-op 后再跑立刻 EOF，与重建后的结果一致。
    if (cached_next_event_) {
        // 1-event cache 是逻辑不消费的唯一状态：peek 后的 next_event 必须返回同一对象。
        out = *cached_next_event_;
        cached_next_event_.reset();
        at_start_ = false;
        return true;
    }
    if (!read_next_event_uncached_(out)) return false;
    at_start_ = false;
    return true;
}

bool XtMarketDataSource::peek_next_key(uint64_t& out_key) {
    if (!cached_next_event_) {
        // peek 会推进底层数据游标或 reader 一步并缓存结果；对调用方观察到的消费序列仍保持不变。
        XtReplayEvent evt;
        if (!read_next_event_uncached_(evt)) return false;
        cached_next_event_ = evt;
    }
    out_key = cached_next_event_->key;
    return true;
}

bool XtMarketDataSource::peek_next_event_header(XtReplayEventHeader& out) {
    uint64_t key = 0;
    if (!peek_next_key(key)) return false;
    const XtReplayEvent& ev = *cached_next_event_;
    out.key = key;
    switch (ev.type) {
        case XtReplayEventType::Tick:
            out.action_date  = ev.tick.action_date;
            out.action_time  = ev.tick.action_time;
            out.trading_date = ev.tick.trading_date;
            break;
        case XtReplayEventType::OrderQueue:
            out.action_date  = ev.order_queue.action_date;
            out.action_time  = ev.order_queue.action_time;
            out.trading_date = ev.order_queue.trading_date;
            break;
        case XtReplayEventType::OrderDetail:
            out.action_date  = ev.order_detail.action_date;
            out.action_time  = ev.order_detail.action_time;
            out.trading_date = ev.order_detail.trading_date;
            break;
        case XtReplayEventType::Transaction:
            out.action_date  = ev.transaction.action_date;
            out.action_time  = ev.transaction.action_time;
            out.trading_date = ev.transaction.trading_date;
            break;
    }
    return true;
}

void XtMarketDataSource::reset_runtime_state() {
    // 只清运行期消费状态：memory/event 游标、打开的 stream reader、peek cache 与
    // legacy bar driver cursor。prepare 阶段写入的 source/path/filter 配置保持不变。
    mem_.cursor   = 0;
    tick_.reader.reset();
    tick_.reader_is_bar_simtick = false;
    day_buf_ = {};
    stream_io_error_ = false;
    drop_stats_ = DropStats{};
    stream_records_read_ = 0;
    stream_stats_finalized_ = false;
    events_.clear();
    event_cursor_ = 0;
    cached_next_event_.reset();
    bar_driver_.reset();
}

bool XtMarketDataSource::read_next_event_uncached_(XtReplayEvent& out) {
    // 读端唯一漏斗（next_event 与 peek_next_key 都经此推进底层游标/reader）。
    // 注意：这里**不动** at_start_ —— 推进底层游标不等于交付，peek 走的正是这条路而
    // 它对消费者不消费任何东西；清位在 next_event() 的两条交付路径上（见其注释）。
    if (mode_ != XtReplayMode::EventStream) {
        // 非事件流模式只产生 Tick payload；其他 union-like 字段保持未定义语义，
        // 调用方必须先看 type 再读对应字段。
        out.seq  = 0;
        out.type = XtReplayEventType::Tick;
        if (!next_tick(out.tick)) return false;
        // 与 ReplayEventBuilder / XtTickCollector 共用统一 sort key，保留 exchange
        // trading day 的夜盘旋转语义；旧数据缺 trading_date 时回退 action_date。
        const uint32_t td = out.tick.trading_date != 0 ? out.tick.trading_date
                                                       : out.tick.action_date;
        out.key = make_tick_sort_key(td, out.tick.action_time);
        return true;
    }
    // EventStream 已在 reset() 期间完成全量归并排序，读端只推进单一 events_ 游标。
    if (event_cursor_ >= events_.size()) return false;
    out = events_[event_cursor_++];
    return true;
}
