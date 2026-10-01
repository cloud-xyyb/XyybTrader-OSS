#pragma once

/// @file
/// @brief 收盘强平（closeout）的判定与执行入口。
///
/// 到了收盘窗口，runner 把剩余持仓强制平掉，避免带仓过夜。
/// decide_closeout_gate() 是可单测的纯判定（开没开、在不在窗口、通道可不可用、
/// 节流到没到）；try_runner_force_close() 是控制环每轮调用的执行入口，
/// 具体撤单/下单阶段在实现文件内。

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <cstdint>  // ForceCloseResult 底层类型

// kill_switch_source_latches_across_restart（bootstrap restore 单源谓词）已上移
// 跨引擎共享 TU；本头保留 include 让既有 caller 继续可见。
#include "xt_live/control/SafetyStateWire.h"
#include "xt_core/order/XtOrder.h"  // 强平请求出参元素类型

// XtHftRunnerControlContext 完整定义在 HftRunnerControl.h；本头仅 forward decl。
struct XtHftRunnerControlContext;
class XtCommodityCatalog;
namespace xt_account { class AccountStateStore; }

/// @brief 收盘强平入口的结果分类。
enum class ForceCloseResult : uint8_t {
    kNotNeeded,       ///< 功能未开启 / 不在强平窗口 / 无持仓 — 正常跳过
    kSessionGated,    ///< session gate 过滤，非交易时段 — 正常跳过
    kGatewayDown,     ///< TD 通道不可用（恢复中）— 需要关注
    kThrottled,       ///< 节流冷却中 — 短期正常，长期累积需告警
    kRejectedAmbiguousScope, ///< sid_scope=0 是歧义输入，拒绝静默 global bypass
    kCancelledFirst,  ///< 有在途单，本轮只撤不平 — 正常中间状态
    kExecuted,        ///< 已下发强平单
};

/// @brief closeout gate 纯函数输入。
///
/// 时间字段使用 HHMM 和 steady ms；调用方负责在同一控制帧内构造，避免本地时钟、
/// tick 时钟和节流时间戳跨帧混用。
struct CloseoutGateInput {
    bool cfg_enabled = false;                   ///< cfg->closeout_enabled
    bool has_windows = false;                   ///< cfg->closeout_windows 非空
    bool session_gate_enabled = false;          ///< cfg->session_gate_enabled
    const std::vector<std::pair<int, int>>* session_gate_sections = nullptr; ///< 允许时段列表
    const std::vector<int>* force_hhmm_list = nullptr;                       ///< 每个 closeout window 的 force_hhmm
    int  local_hhmm = -1;                       ///< 本地时钟 HHMM, -1=未知
    int  tick_hhmm  = -1;                       ///< tick 时钟 HHMM, -1=未知
    bool td_available = false;                  ///< load_td_for_runner_force_close() != nullptr
    int64_t tnow_ms = 0;                        ///< steady now
    int64_t closeout_last_try_ms = 0;           ///< 上次 try 时间
    int64_t closeout_retry_ms = 1000;           ///< 节流周期
};

/// @brief 执行一次 runner-wide 收盘强平尝试。
///
/// @param ctx                   控制上下文 (td/order_book/account_store/cfg)
/// @param closeout_last_try_ms  节流时间戳；gate 通过时更新
/// @return                      调用方据此累加 closeout streak 或记录执行结果
/// @param closeout_force_active_since_ms  强平首次见持仓的 steady ms；flat/出窗口归零，passive→aggressive 升级计时基准
ForceCloseResult try_runner_force_close(XtHftRunnerControlContext& ctx,
                                        int64_t& closeout_last_try_ms,
                                        int64_t& closeout_force_active_since_ms);

/// @brief 判断当前是否允许进入强平 execute 阶段。
/// @return nullopt = gate 通过；否则返回跳过或阻塞原因。
std::optional<ForceCloseResult> decide_closeout_gate(const CloseoutGateInput& in);

/// @brief 按候选合约生成本轮强平请求（读账户 → 定价 → 拆今昨腿）。
///
/// @param account         账户状态存储（持仓量、可平量、venue 行情价）
/// @param catalog         品种规格目录，供激进价的 tick 对齐；nullptr 时不对齐
/// @param candidate_codes 本轮候选合约（无在途单、未在强平中）
/// @param escalate        true = IOC @ 穿价激进价；false = 被动 GTC @ mark
/// @param buffer_bps      激进价穿价 buffer（bps）
/// @param out             生成的强平请求，调用时清空
/// @note **定价只认 venue 真实行情价**：估值口径（`PositionSnapshot::mark_price`）在
///       venue 缺失时回退持仓成本价，按成本价挂强平单会定出与盘口无关的价格。venue
///       缺失的合约本轮不产生任何请求（持仓留到下一轮或人工处置），不发 0 价单。
void collect_closeout_force_flat_orders(const xt_account::AccountStateStore& account,
                                        const XtCommodityCatalog* catalog,
                                        const std::vector<std::string>& candidate_codes,
                                        bool escalate,
                                        int buffer_bps,
                                        std::vector<xt_core::XtOrder>& out);

/// @brief closeout 是否从被动 GTC@mark 升级到激进 IOC@aggressive（纯决策）。
///
/// @param escalate_after_ms      被动维持时长 ms；<=0 立即激进
/// @param force_active_since_ms  强平首次见持仓 steady ms；0=本帧首次（仍被动）
/// @param now_ms                 当前 steady ms
/// @return true=升级激进；false=维持被动
[[nodiscard]] bool closeout_should_escalate(int64_t escalate_after_ms,
                                            int64_t force_active_since_ms,
                                            int64_t now_ms) noexcept;

/// @brief 收盘强平结果是否应升级为持久化 kill_switch（emergency latch）。
///
/// 连通性类结果 (kGatewayDown / kThrottled) 不升级：TD 下线时策略开仓已被 trading_ready
/// 挡住，持久化 latch 冗余且会跨日误锁。
/// 只有 kRejectedAmbiguousScope（通道在线却拒绝平仓）才需人工介入的 emergency latch。
[[nodiscard]] bool closeout_result_trips_kill_switch(ForceCloseResult r) noexcept;

/// @brief kill_switch_source 是否属冷启 flat 自动释放 allow-set（单源谓词）。
///        closeout_latch_auto_release_decision 与 SafetyState RETAINED 告警共用，
///        新增 auto-releasable source 只改这一处（防两处 allow-set 漂移）。
[[nodiscard]] bool kill_switch_source_auto_releasable(uint8_t source) noexcept;

/// @brief 冷启是否应自动释放自动 trip 的 kill_switch（纯决策）。
///
/// 仅当 latch 仍激活、来源在 allow-set（AutoCloseoutBlock / UnmatchedBudget）、
/// 账户已 hydrate 且确为 flat 时释放。manual / 旧文件 latch (None) 与 reconcile
/// 自清类 (ReconcileDivergence) 一律保守保留。
/// @param kill_switch_active  当前 kill_switch 是否激活
/// @param source              kill_switch_source（XtRunnerSignals::kKillSwitchSource*）
/// @param account_hydrated    venue 持仓已恢复（td_position_ready && has_account_snapshot）
/// @param account_flat        账户无持仓且无在途单
[[nodiscard]] bool closeout_latch_auto_release_decision(bool kill_switch_active,
                                                        uint8_t source,
                                                        bool account_hydrated,
                                                        bool account_flat) noexcept;

/// @brief 探测 closeout "边界陷阱": 某 force_flat_hhmm 不落在任何 session 半开区间内。
/// (典型: 落在区间右端点如 0230 ∉ [2100,0230)) → hhmm_reaches_mark_in_sections 对该
/// mark 恒 false → force_phase 永假 → closeout 该窗口永不触发。纯函数无时钟；
/// 配置正确时恒 false，适合做配置校验和回归守卫。
/// @param force_hhmm_list  每个 closeout window 的 force_hhmm
/// @param sections         session_gate_sections (空 → 走 hhmm>=mark fallback, 无陷阱)
/// @return true = 至少一个 force 窗口永不触发 (配置错误)
[[nodiscard]] bool closeout_boundary_trap(const std::vector<int>& force_hhmm_list,
                                          const std::vector<std::pair<int, int>>& sections);
