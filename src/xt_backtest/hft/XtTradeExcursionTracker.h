#pragma once

#include <cstdint>
#include <deque>
#include <fstream>
#include <string>
#include <unordered_map>

#include "xt_core/order/XtTradeRecord.h"

struct XtTick;
class XtCommodityCatalog;
namespace xt_account { class AccountStateStore; }

namespace xt_bt::hft {

/// @brief HFT 回测的逐笔 MAE/MFE（最大不利/有利游程）追踪器。
///
/// 按 symbol 维护 FIFO 开仓批次（lot）队列：净仓每减一分就按开仓序与批次配对，
/// 每个「批次切片 × 平仓成交」配对写一行 `trade_excursions.csv`。出行条件是
/// **净仓减少**，不是净仓归零——持续持有库存的做市类策略（净仓长期不回 flat）
/// 因此同样逐笔出行。
///
/// MAE/MFE 按批次独立累计：running 值以**价差**（fp 价格单位、按批次方向定号）
/// 保存，落行时才乘配对量与 vol_scale 折算成金额，故批次被部分平掉后剩余部分的
/// 游程仍然自洽。单批次整进整出时与「净仓 unrealized 极值」口径等价。
///
/// @note realized_pnl 列是 **FIFO 成本基准**口径：`(exit_px − 批次 entry_px) × 配对量 ×
///       vol_scale`（空头反号），与同行的 entry_px/exit_px/qty 自洽，故下游按 realized
///       正负分胜负行的判定与该行自身的价格一致。account / fills.csv / equity 走的是
///       **移动均价**口径：账户对整个持仓边维护一个加权均价，一笔平仓的 realized 按
///       该均价而非被平批次的开仓价结转。两者在「某批次开仓价 ≠ 当时持仓均价」时逐行
///       有差；同一批多批次全部平完后差额相消，只要还有批次未平（含 EOF 未平），
///       两口径的累计总额就会留下与未平批次成本差等值的偏差。该偏差在 flush 时按
///       fp 总额记一条日志，不做修正。
class XtTradeExcursionTracker {
public:

    void init(const std::string& report_dir, const XtCommodityCatalog* catalog);

    void on_tick(const XtTick& tick, const xt_account::AccountStateStore& account);

    void on_fill(const XtTradeRecord& tr, int64_t before_qty_fp, int64_t after_qty_fp,
                 int64_t after_avg_fp, int64_t realized_fp);

    void flush();

private:
    /// @brief 单个开仓批次（FIFO 配对单元）。
    struct Lot {
        int64_t qty_fp = 0;
        bool is_long = true;
        int64_t entry_px_fp = 0;
        uint32_t open_date = 0;
        uint32_t open_time = 0;
        int64_t mae_diff_fp = 0;
        int64_t mfe_diff_fp = 0;
        int64_t mae_px_fp = 0;
        int64_t mfe_px_fp = 0;
        int64_t ticks_held = 0;
    };

    /// @brief per-code 批次队列（队内批次同方向，按开仓序）。
    struct CodeState {
        std::deque<Lot> lots;
        int64_t vol_scale_fp = 0;
    };

    int64_t vol_scale_fp_(CodeState& st, const char* code);

    void resync_lots_(CodeState& st, int64_t before_qty_fp, int64_t entry_px_fp,
                      uint32_t date, uint32_t time);
    void emit_(const std::string& code, const Lot& lot, int64_t slice_qty_fp,
               int64_t exit_px_fp, int64_t realized_fp, int64_t vol_scale_fp,
               uint32_t close_date, uint32_t close_time);

    std::unordered_map<std::string, CodeState> state_;
    const XtCommodityCatalog* catalog_ = nullptr;
    std::ofstream out_;
    int64_t dropped_open_at_eof_ = 0;
    int64_t resync_count_ = 0;
    int64_t realized_basis_gap_fp_ = 0;
};

}
