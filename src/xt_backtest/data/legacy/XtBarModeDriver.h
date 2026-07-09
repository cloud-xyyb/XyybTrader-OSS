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

    struct BarState {
        std::string path;
        std::string root = "data/bars/csv";
        std::string symbol;
        std::string suffix = ".csv";
        std::string code = "BAR";
        int interval_sec = 300;
        std::vector<std::string> paths;
        bool is_csv = false;

        size_t path_idx = 0;
        bool stream_is_bin = false;
        XtTickBinFormat stream_bin_fmt = XtTickBinFormat::DualV2;
        bool stream_bin_fmt_inited = false;
        std::unique_ptr<std::ifstream> in;
        std::vector<XtRawTick> ticks;
        size_t tick_idx = 0;
        std::string pending_line;
        double total_volume = 0.0;
    };

    struct SimtickState {
        bool from_bar = false;
        bool save = false;
        std::string format = "bin";
        std::string dir = "data/simtick";
        std::string path;
        std::vector<std::string> paths;
        bool is_bin = false;

        std::string cur_bin_yymm;
        std::unique_ptr<std::ofstream> csv_out;
        std::unique_ptr<std::ofstream> bin_out;
        XtTickBinFormat bin_out_fmt = XtTickBinFormat::DualV2;
        bool bin_out_fmt_inited = false;
        std::string bin_out_path;
    };

    struct HotState {
        bool use_hot = false;
        std::string path;
        std::vector<std::string> months;
        std::vector<bool> bin_exists;
        bool range_enabled = false;
        std::string bin_path;
        bool bin_available = false;

        size_t month_idx = 0;
        std::string month_filter;
    };

    struct ReadConfig {
        double spread = 0.2;
        double qty_base = 100.0;
        uint32_t range_start = 0;
        uint32_t range_end = 0;
    };

    BarState     bar;
    SimtickState simtick;
    HotState     hot;
    ReadConfig   read_cfg;

    bool build_bar_paths_from_symbol();

    void build_simtick_paths_from_bar_paths();

    std::string build_simtick_path() const;

    std::string build_simtick_path_for_month(const std::string& yymm) const;

    void apply_config(const XtMarketDataSourceConfig& cfg);

    bool prepare(const XtMarketDataSourceConfig& cfg);

    void reset();

    bool next_tick(XtRawTick& out);

    bool load_to_memory(std::vector<XtRawTick>& out);

    bool stream_active() const { return bar.is_csv && !bar.path.empty(); }

    bool has_simtick_bin_stream(std::vector<std::string>& out_paths, std::string& out_fallback) const {
        if (simtick.is_bin && (!simtick.paths.empty() || !simtick.path.empty())) {
            out_paths = simtick.paths;
            out_fallback = simtick.path;
            return true;
        }
        return false;
    }

    bool has_memory_source() const { return bar.is_csv || simtick.is_bin; }

private:
    bool open_next_bar_file();
    bool load_simtick_bin(const std::string& path, std::vector<XtRawTick>& out);
    bool load_bar_file(const std::string& path, std::vector<XtRawTick>& out);
};
