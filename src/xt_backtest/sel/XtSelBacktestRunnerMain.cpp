//
// SEL backtest runner CLI 入口（hello world）。
//
// 用法：XtSelBacktestRunner <config.yaml>
//

#include <cstdio>
#include <exception>
#include <string>

#include "XtSelRunnerConfig.h"
#include "XtSelBacktestRunner.h"
#include "xt_logger/XtLogger.h"
#include "xt_logger/XtLoggerCfg.h"
#include "xt_util/path/PathUtil.h"

int main(int argc, char** argv) {
    try {
        // logger init with console output（mirror cta runner pattern - 简化版）
        XtLogger::init_fast("logs/sel_runner.log",
                             /*level=*/LogLevel::Info,
                             /*max_size=*/50ULL * 1024 * 1024,
                             /*max_files=*/10,
                             /*queue_size=*/8192,
                             /*thread_count=*/1,
                             /*console=*/true);

        xt_bt_sel::XtSelRunnerConfig cfg;
        std::string err;
        if (!xt_bt_sel::load_sel_config(argc, argv, cfg, err)) {
            std::fprintf(stderr, "[XtSelBacktestRunner] config error: %s\n", err.c_str());
            return 64;  // EX_USAGE
        }
        // 相对 report_dir（yaml report.output_dir）按 config 所在目录解析
        // （仓库 convention；绝对路径 passthrough）。
        cfg.report_dir = PathUtil::resolve_relative(cfg.cfg_path, cfg.report_dir);
        return xt_bt_sel::XtSelBacktestRunner::run(cfg);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[XtSelBacktestRunner] exception: %s\n", e.what());
        return 70;  // EX_SOFTWARE
    } catch (...) {
        std::fprintf(stderr, "[XtSelBacktestRunner] unknown exception\n");
        return 71;
    }
}
