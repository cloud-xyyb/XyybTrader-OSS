#include "XtLiveReportAdapter.h"

#include <cstdlib>
#include <string>

#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_report/util/XtReportEventBuilder.h"
#include "xt_util/time/XtTime.h"

namespace xt_report {
namespace {

uint64_t stable_trade_id_u64(const std::string& raw) noexcept {
    if (raw.empty()) return 0;
    char* endp = nullptr;
    const unsigned long long parsed = std::strtoull(raw.c_str(), &endp, 10);
    if (endp && *endp == '\0') return static_cast<uint64_t>(parsed);
    // fills.csv 写 uint64；非数字 venue trade-id 统一走 xt_core::sync FNV-1a。
    // 该值只服务报表展示/排查，不作为 dedup、replay 或持久化主键。
    const uint64_t h = xt_core::sync::fnv1a_bytes(raw.data(), raw.size());
    return h == 0 ? 1 : h;
}

}  // namespace

int64_t trade_ts_ms_from_record(const XtTradeRecord& tr, int64_t fallback_wall_ms) noexcept {
    // fill_ts_us 是归一后的 epoch µs；broker date/time 只在缺少该时间戳时兜底。
    if (tr.fill_ts_us != 0) {
        return static_cast<int64_t>(tr.fill_ts_us) / 1000LL;
    }
    // range 校验与 builder 同源（report_wall_clock_in_range）：date 合法 + 时分秒在界
    // （time=0 合法午夜）才走 to_ns；半空/越界直接 fallback，杀 year-0 garbage 负 epoch。
    if (report_wall_clock_in_range(tr.trade_date, tr.trade_time)) {
        return xt_time::to_ns(tr.trade_date, tr.trade_time) / 1'000'000LL;
    }
    return fallback_wall_ms;
}

void build_live_fill_events(const std::vector<XtTradeRecord>& recent_trades,
                            int64_t fallback_wall_ms,
                            std::vector<XtLiveFillEvent>& out) {
    out.clear();
    out.reserve(recent_trades.size());
    for (const auto& tr : recent_trades) {
        if (tr.qty_fp <= 0) continue;
        XtLiveFillEvent fill;
        fill.strategy_id = tr.strategy_id;
        fill.symbol      = tr.code;
        fill.ts_ms       = trade_ts_ms_from_record(tr, fallback_wall_ms);
        fill.ts_event_ns = tr.fill_ts_us > 0
            ? static_cast<int64_t>(tr.fill_ts_us) * 1000LL
            : (report_wall_clock_in_range(tr.trade_date, tr.trade_time)
                ? xt_time::to_ns(tr.trade_date, tr.trade_time)
                : fill.ts_ms * 1'000'000LL);
        fill.ts_recv_ns  = tr.local_cb_us > 0
            ? static_cast<int64_t>(tr.local_cb_us) * 1000LL
            : fill.ts_event_ns;
        fill.order_id    = tr.order_id;
        fill.trade_id    = stable_trade_id_u64(tr.trade_id);
        fill.is_buy      = tr.is_buy;
        fill.qty         = order_exec_from_fp(tr.qty_fp);
        fill.price       = tr.exec_price > 0.0 ? tr.exec_price : tr.price;
        fill.fee         = tr.fee;
        fill.slippage    = tr.slippage;
        fill.is_close    = tr.is_close ? 1 : 0;
        fill.is_today    = tr.is_today ? 1 : 0;
        fill.trace_id    = tr.trace_id.empty() ? tr.user_tag : tr.trace_id;
        // trading_day 由 venue gateway 写入；0 表示下游按 ts_ms 自然日兜底。
        fill.trading_day = tr.trading_day;
        // realized_pnl_fp 是 engine on_trade 写回的 account_store 口径，reporter 不重算。
        fill.realized_pnl_fp = tr.realized_pnl_fp;
        out.push_back(fill);
    }
}

}  // namespace xt_report
