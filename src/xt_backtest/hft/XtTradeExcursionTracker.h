#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <unordered_map>

#include "xt_core/order/XtTradeRecord.h"

struct XtTick;
namespace xt_account { class AccountStateStore; }

namespace xt_bt::hft {

/// @brief HFT 回测的逐笔 MAE/MFE（最大不利/有利游程）追踪器。
///
/// 按 symbol 跟踪净持仓从 flat 开仓到回到 flat 或反向穿零的一段，并把每段的
/// entry/exit、realized PnL、running MAE/MFE 写入 `trade_excursions.csv`。
class XtTradeExcursionTracker {
public:

    void init(const std::string& report_dir);

    void on_tick(const XtTick& tick, const xt_account::AccountStateStore& account);

    void on_fill(const XtTradeRecord& tr, int64_t before_qty_fp, int64_t after_qty_fp,
                 int64_t after_avg_fp, int64_t realized_fp);

    void flush();

private:
    /// @brief 单个 symbol 当前打开的 excursion 段。
    struct Seg {
        bool open = false;
        int64_t entry_avg_fp = 0;
        int64_t entry_qty_fp = 0;
        uint32_t open_date = 0;
        uint32_t open_time = 0;
        int64_t realized_acc_fp = 0;
        int64_t mae_fp = 0;
        int64_t mfe_fp = 0;
        int64_t mae_px_fp = 0;
        int64_t mfe_px_fp = 0;
        int64_t ticks_held = 0;
    };

    void start_segment_(Seg& s, int64_t qty_fp, int64_t avg_fp, uint32_t date, uint32_t time);
    void emit_(const std::string& code, const Seg& s, int64_t exit_px_fp,
               uint32_t close_date, uint32_t close_time);

    std::unordered_map<std::string, Seg> seg_;
    std::ofstream out_;
    int64_t dropped_open_at_eof_ = 0;
};

}
