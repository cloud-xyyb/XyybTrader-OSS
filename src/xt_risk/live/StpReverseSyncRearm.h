#pragma once

#include <vector>

#include "api/AliveInflightOrder.h"
#include "live/XtSymbolPriceBoundSTP.h"

/// @brief reverse-sync STP re-arm 分叉（cold 清场重播 / warm no-op）。
///
/// **设计裁决（R4）**：STP 对「谁活着」不再自己裁——跟随 book 的 exchange-as-truth 裁决。
/// warm reverse-sync 回调点不再 union-seed（那会复活「本进程已终态、venue 快照仍含」的
/// 争议单）；re-arm 改由 book fillup 的实际接受点驱动（engine 对 book 接受/复活的每一单
/// 调 XtLiveOrderGateway::rearm_active_order → STP upsert + 清 tombstone，book 真值覆盖
/// STP 私有 tombstone）。
///
/// 三 venue 的 is_cold_start 语义一致（`is_first_recovery_` 首次为 cold，之后 warm）：
///   - **cold**（首次 recovery，is_cold_start=true）：clear_all_active() 清场后逐单
///     seed（belt）。首次 recovery 时 trading_ready 尚未放行 → readiness 门禁挡住并发
///     submit，clear 与逐单 seed 之间的空边界窗口无害；book fillup 的 rearm 为 suspenders
///     （upsert 幂等，两条路径重叠无害）。
///   - **warm**（后续 resync，is_cold_start=false）：**完全 no-op**——不 clear、不 seed。
///     warm resync 在 trading_ready 常驻 true 下发生（binance reconcile_resync /
///     CTP·XTP 周期对账），同进程活单要么已 stamp 在案、要么已终态 erase；照 cold 清场
///     会抹掉快照采集(t0)→回调(t2) 窗口内的并发 stamp。re-arm 唯一来源是 book fillup
///     的实际接受点。
///
/// warm 残留（披露）：book 复活的单在「reverse-sync 回调 → worker fillup 落地」间隙不受
/// STP 保护（复活案例=本地误判终态的争议单，罕见且窗口 ms 级）。而 book 快照缺失的本地
/// stale 条目（venue 已死但本进程终态丢失）由既有 lost-terminal 对账（reconcile_lost_terminals
/// / binance ghost sweep）合成终态 → notify_order_terminal(id) → 自然 erase 收敛。
///
/// @note 纯分叉逻辑（无 stp_enabled 门）；gateway 侧 stp_rearm_reverse_sync 负责 enabled
///       门与 stp_checker_ 注入。抽出为自由函数供单测直接对真 STP 做 cold/warm A/B。
inline void stp_rearm_on_reverse_sync(XtSymbolPriceBoundSTP& stp,
                                      const std::vector<AliveInflightOrder>& orders,
                                      bool is_cold_start) {
    // warm：完全 no-op（re-arm 由 book fillup 接受点驱动，见上设计裁决）。
    if (!is_cold_start) return;
    // cold：清场重播（readiness 门禁挡并发 submit，clear 窗无害）。
    stp.clear_all_active();
    for (const auto& o : orders)
        stp.seed_active_order(o.code, o.local_id, o.is_buy, o.price_fp);
}
