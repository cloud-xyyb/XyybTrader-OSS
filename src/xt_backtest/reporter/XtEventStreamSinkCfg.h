#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

/// @brief backtest event stream sink 的输出控制配置。
///
/// 控制 full trace 是否开启、是否导出 tick 事件，以及行数/字节数和 latency map 的上界
/// 输出目录由 `XtReportCfg` 单点解析后传入 sink，不在本结构中保存。
struct XtEventStreamSinkCfg {
    enum class TraceMode { Off, Full };

    TraceMode     trace_mode                 = TraceMode::Off;  ///< Off 不记录；非法 YAML 值由配置 schema 拦截。
    bool          export_tick                = false;
    // 输出目录不在 cfg 里：yaml backtest.report_dir 由 XtReportCfg 单点解析，
    // runner resolve 后经 XtEventStreamSink 构造参数传入（单一 truth，免双写同步）。
    std::size_t   full_max_rows              = 2000000;
    std::uint64_t full_max_bytes             = 512ULL * 1024ULL * 1024ULL;
    std::size_t   sys_stats_sample_n_ticks   = 100;
    std::size_t   max_latency_map_entries    = 4096;

    void validate(std::vector<std::string>& errors) const;
    static const char* mode_name(TraceMode mode);
};

namespace YAML { class Node; }

/// @brief 从 YAML 根节点解析 event stream sink 配置。
XtEventStreamSinkCfg parse_event_stream_sink_config(const YAML::Node& node);
