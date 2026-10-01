#include "DeclaredUniverse.h"

#include <algorithm>

#include "XtLiveConfig.h"
#include "xt_core/market/XtRollEngine.h"

namespace xt_live {
namespace {

// 线性去重：declared 规模封顶 kMaxDeclaredSymbols，且只在配置装配期跑一次；
// 保持插入顺序是对外契约（见 header），排序容器不适用。
void add_unique(std::vector<std::string>& out, const std::string& symbol) {
    if (symbol.empty()) return;
    if (std::find(out.begin(), out.end(), symbol) == out.end()) out.push_back(symbol);
}

}  // namespace

std::vector<std::string> collect_declared_universe(const XtLiveConfig& cfg,
                                                   const std::string& instrument_symbol) {
    std::vector<std::string> out;
    add_unique(out, instrument_symbol);
    for (const auto& entry : cfg.parsed.strategies) {
        add_unique(out, entry.init_cfg.get_string("symbol", ""));
    }
    // roll 派生合约：换月表列出的历史/未来主力都要订阅，否则切换日无行情。
    if (cfg.bootstrap.has_roll_cfg) {
        XtRollEngine roll(cfg.bootstrap.roll_cfg);
        for (const auto& code : roll.all_symbols()) add_unique(out, code);
    }
    return out;
}

}  // namespace xt_live
