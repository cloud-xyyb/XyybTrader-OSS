#pragma once

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "xt_core/market/XtRawTick.h"
#include "xt_core/market/XtTickBinCodec.h"

struct XtMarketDataSourceConfig;

/**
 * @brief Bar 模式驱动器（bar mode driver）。
 *
 * @warning [Legacy] 仅服务 bar/simtick/hot 的历史数据输入路径；HFT 回测优先使用 tick mode。
 *          保留可用，但新功能不应扩展 bar 合成语义。
 * @note 支持 `next_tick()` 流式读取，也支持 `load_to_memory()` 批量生成 tick；调用方单线程消费。
 */
class XtBarModeDriver {
public:
    // ── Bar 文件状态 ──────────────────────────────────────────────
    /// @brief bar CSV 输入、路径推导结果和当前读取游标。
    struct BarState {
        std::string path; ///< 单文件 bar 输入路径；paths 为空时作为 fallback。
        std::string root = "data/bars/csv"; ///< 按 symbol/month 推导 bar 路径的根目录。
        std::string symbol; ///< bar 文件 symbol；参与路径推导。
        std::string suffix = ".csv"; ///< bar 文件后缀；参与路径推导和 simtick cache 命名。
        std::string code = "BAR"; ///< 生成 simtick 时写入 XtRawTick::code。
        int interval_sec = 300; ///< 单根 bar 秒数，用于计算合成 tick 时间点。
        std::vector<std::string> paths; ///< prepare 阶段推导出的月份级 bar 文件列表。
        bool is_csv = false; ///< true 表示 runtime 从 bar CSV 生成 simtick。
        // ── runtime（reset() 清零） ─────────────────────────────────
        size_t path_idx = 0; ///< 下一次 open_next_bar_file() 尝试打开的 paths 下标。
        bool stream_is_bin = false; ///< 当前 in 是否指向 simtick bin stream。
        XtTickBinFormat stream_bin_fmt = XtTickBinFormat::DualV2; ///< 当前 bin stream 格式。
        bool stream_bin_fmt_inited = false; ///< true 表示当前 bin stream 已完成 header 探测。
        std::unique_ptr<std::ifstream> in; ///< 当前 bar CSV 或 simtick bin stream。
        std::vector<XtRawTick> ticks; ///< 当前 CSV bar 行生成但尚未完全消费的 simtick 缓冲。
        size_t tick_idx = 0; ///< 下一条 ticks 下标。
        std::string pending_line; ///< hot 月份切换时保留的首条下一月份 bar 行。
        double total_volume = 0.0; ///< bar->simtick 生成路径的累计成交量。
    };

    // ── SimTick 缓存状态 ──────────────────────────────────────────────
    /// @brief bar->simtick cache 的配置、路径和当前写入状态。
    struct SimtickState {
        bool from_bar = false; ///< true 表示从 bar CSV 合成 simtick。
        bool save = false; ///< true 表示合成 simtick 时同步写入 cache。
        std::string format = "bin"; ///< `bin` 或 `csv`。
        std::string dir = "data/simtick"; ///< simtick cache 输出目录。
        std::string path; ///< 单文件 simtick cache 路径。
        std::vector<std::string> paths; ///< 月份级 simtick bin cache 路径列表。
        bool is_bin = false; ///< true 表示 prepare 阶段确认可直接读 simtick bin。
        // ── runtime ────────────────────────────────────────────────
        std::string cur_bin_yymm; ///< 当前正在写入的月份级 bin cache YYMM。
        std::unique_ptr<std::ofstream> csv_out; ///< CSV cache 输出 stream。
        std::unique_ptr<std::ofstream> bin_out; ///< bin cache 输出 stream。
        XtTickBinFormat bin_out_fmt = XtTickBinFormat::DualV2; ///< bin cache 输出格式。
        bool bin_out_fmt_inited = false; ///< true 表示 bin_out 已写入/确认 header。
        std::string bin_out_path; ///< 当前 bin_out 对应的目标路径。
    };

    // ── 主力合约映射状态 ──────────────────────────────────────────────
    /// @brief hot symbol 月份映射和 per-month simtick cache 命中状态。
    struct HotState {
        bool use_hot = false; ///< true 表示 bar 模式走主力合约映射路径。
        std::string path; ///< hot CSV 输入路径。
        std::vector<std::string> months; ///< 日期范围展开出的 YYMM 月份序列。
        std::vector<bool> bin_exists; ///< 与 months/simtick.paths 对齐的 cache 命中标记。
        bool range_enabled = false; ///< true 表示 months 非空，可按月份遍历。
        std::string bin_path; ///< 单文件 hot simtick bin cache 路径。
        bool bin_available = false; ///< true 表示 bin_path 可直接读取。
        // ── runtime ────────────────────────────────────────────────
        size_t month_idx = 0; ///< 下一次 hot 月份遍历下标。
        std::string month_filter; ///< 当前 CSV 读取允许输出的 YYMM；空表示不过滤。
    };

    // ── 外部输入参数（apply_common_config 阶段写入） ──────────────────────────────────────────────
    /// @brief datasource 配置下沉到 bar driver 的读取参数。
    struct ReadConfig {
        double spread = 0.2; ///< bar->simtick 盘口 spread。
        double qty_base = 100.0; ///< bar->simtick 盘口基础数量。
        uint32_t range_start = 0; ///< YYYYMMDD inclusive；0 表示未配置。
        uint32_t range_end = 0; ///< YYYYMMDD inclusive；0 表示未配置。
    };

    // ── 公开配置字段（prepare 阶段直接写入） ──────────────────────────────────────────────
    BarState     bar;      ///< bar 文件配置、推导结果和 runtime 读取状态。
    SimtickState simtick;  ///< simtick cache 配置、路径和写入状态。
    HotState     hot;      ///< 主力合约映射配置、月份范围和 runtime 过滤状态。
    ReadConfig   read_cfg; ///< prepare/load 阶段共享的外部读取参数。

    // ── 路径推导（prepare 阶段调用，runtime 也会调用） ──────────────────────────────────────────────
    /// @brief 由 bar.root/symbol/suffix 与 read_cfg 日期范围推导月份路径列表。
    bool build_bar_paths_from_symbol();
    /// @brief 基于 bar.paths 填写 simtick.paths（per-month bin 路径）。
    void build_simtick_paths_from_bar_paths();
    /// @brief 构建无月份后缀的 simtick 输出路径。
    std::string build_simtick_path() const;
    /// @brief 构建月份级 simtick 输出路径。
    std::string build_simtick_path_for_month(const std::string& yymm) const;

    // ── 配置注入与 bar 输入源解析（每个 load() 调用一次） ──────────────────────────────────────────────
    /// @brief 把 datasource 配置 seed 到 read_cfg/bar/simtick/hot 子结构并归零分支状态。
    void apply_config(const XtMarketDataSourceConfig& cfg);
    /// @brief 解析 bar 输入源：hot per-month simtick-bin cache 命中 / explicit bar_path /
    ///        symbol 推导月份路径。日期范围取自 read_cfg。
    /// @pre 必须先调 apply_config(cfg)（每个 load 都调），否则 read_cfg 为上一轮 stale 值。
    /// @return true 表示后续 next_tick()/load_to_memory() 可读取。
    bool prepare(const XtMarketDataSourceConfig& cfg);

    // ── 运行期接口 ──────────────────────────────────────────────
    /// @brief 重置所有 runtime 状态字段，不影响 prepare 阶段写入的配置字段。
    void reset();
    /// @brief 流式读取下一条 tick（bar stream 模式）。
    bool next_tick(XtRawTick& out);
    /// @brief 将全部 bar/simtick 数据追加到 out（memory 模式）。
    bool load_to_memory(std::vector<XtRawTick>& out);

    // ── MDS 分发谓词（供 XtMarketDataSource 选择 bar runtime 路径） ──────────────────────────────────────────────
    /// @brief next_tick 的 bar CSV-stream fallback 是否可用。
    bool stream_active() const { return bar.is_csv && !bar.path.empty(); }
    /// @brief init_stream_reader 是否应打开 simtick-bin reader；命中时输出 reader 输入 paths + fallback。
    bool has_simtick_bin_stream(std::vector<std::string>& out_paths, std::string& out_fallback) const {
        if (simtick.is_bin && (!simtick.paths.empty() || !simtick.path.empty())) {
            out_paths = simtick.paths;
            out_fallback = simtick.path;
            return true;
        }
        return false;
    }
    /// @brief load_memory_source 是否有 bar/simtick 内存源。
    bool has_memory_source() const { return bar.is_csv || simtick.is_bin; }

private:
    bool open_next_bar_file();
    bool load_simtick_bin(const std::string& path, std::vector<XtRawTick>& out);
    bool load_bar_file(const std::string& path, std::vector<XtRawTick>& out);
};
