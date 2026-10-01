#pragma once

/// @file
/// @brief DeclaredUniverse —— live config 声明可交易 symbol 集合的唯一收集口。
///
/// 该集合是 `md_cfg.subs` 的内容，也是 account universe 冻结、STP/velocity
/// pre-warm、HFT market cache 预热的共同输入；收集规则集中在本文件，装配点只
/// 调用不复制逻辑。

#include <cstddef>
#include <string>
#include <vector>

struct XtLiveConfig;

namespace xt_live {

/// @brief DeclaredUniverse 规模上限（超过则启动期 fail-fast）。
///
/// mark registry 的 intern id 共 8192 个，其中 1024 个结构性预留给「持有仓位但
/// 未声明」的 foreign symbol（recovery 恢复仓 / 外来仓），使「slot 持有者 mark
/// 注册失败」不可达；declared 上限因此为 8192 − 1024 = 7168。
inline constexpr std::size_t kMaxDeclaredSymbols = 7168;

/// @brief 收集 live config 声明的可交易 symbol 集合（DeclaredUniverse）。
///
/// 构成 = `runtime.instrument.symbol` ∪ `runtime.strategies[].symbol` ∪ roll
/// 换月表全部合约；空串跳过，重复按首次出现去重。返回值保持**声明顺序**
/// （instrument → strategies 顺序 → roll 表顺序）而非字典序：下游按位置读该
/// 序列（MD 订阅顺序、account universe 冻结顺序、multibook 单/多 symbol 判定）。
///
/// @param cfg `parsed.strategies` 已装配、roll 段已解析的 config；
///            `bootstrap.has_roll_cfg == false` 时结果不含 roll 派生合约。
/// @param instrument_symbol `runtime.instrument.symbol` 原值。config 对象不留存
///            该字段（`strategies[0].init_cfg` 的 symbol 可被 per-entry 值覆盖），
///            由解析期调用方穿透。
std::vector<std::string> collect_declared_universe(const XtLiveConfig& cfg,
                                                   const std::string& instrument_symbol);

}  // namespace xt_live
