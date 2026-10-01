#pragma once

/// @file
/// @brief 读写 live strategy userdata 的跨重启持久化文件。
///
/// write_strategy_userdata() / read_strategy_userdata() 供 runner 在 graceful
/// shutdown 时落盘策略 KV userdata，并在下次启动装配期读回。
/// `XtStrategyContext` 保持 IO-free，只提供 snapshot/restore。
///
/// userdata 读失败按 fail-safe 处理：runner 以空 userdata 启动，不 abort；
/// crash 时不落盘，策略自负新鲜度校验。
/// 不负责 CTA reconcile position snapshot；仓位快照使用独立文件和 fail-closed 语义。

#include <cstdint>
#include <string>
#include <unordered_map>

namespace xt_live::common {

/// @brief 写策略 userdata 到 per-sid JSON 文件（tmp + rename 原子写）。
///
/// @param state_dir     目标目录；不存在时自动创建。
/// @param sid           策略唯一标识；作为文件名组件，必须过 path-component 校验。
/// @param trading_date  当前交易日（YYYYMMDD，0 表示未知）；写入快照供 stale 诊断。
/// @param kv            userdata 键值对（策略任意字符串，值经 JSON 正确转义）。
/// @param err           sid 非法 / IO / rename 失败时填充；调用方负责记录日志。
/// @return true 表示写入并 rename 成功；false 表示未落盘（err 已填）。
/// @pre 调用方保证在 worker 已停止的主线程调用（snapshot_userdata 契约）。
bool write_strategy_userdata(const std::string& state_dir,
                             const std::string& sid,
                             uint32_t trading_date,
                             const std::unordered_map<std::string, std::string>& kv,
                             std::string& err);

/// @brief 从 per-sid JSON 文件读回策略 userdata（fail-safe）。
///
/// @param state_dir  源目录。
/// @param sid        策略唯一标识；用于定位文件并校验文件内 strategy_id 匹配。
/// @param out_kv     成功时覆盖为解析到的 userdata；失败时不修改。
/// @param err        malformed / schema 未知 / strategy_id 不匹配时填充。
/// @param log_tag    成功时年龄 info 日志的模块标签（HFT="RT" / "CTA" / "SEL"）。
/// @return true 表示读到有效 userdata；false 表示无可用状态。
///
/// fail-safe 语义（调用方据此静默/告警空启动，**绝不 abort**）：
///   - 文件不存在        → 返回 false，err 空（clean start，调用方静默）；
///   - malformed JSON /
///     schema_version 未知 /
///     文件内 strategy_id 与 sid 不匹配 → 返回 false + err（调用方 warn + 空启动）。
/// 成功时 helper 内部 log 快照年龄（generated_at 距今 + trading_date）供 ops 观测
/// stale-but-valid 状态。
bool read_strategy_userdata(const std::string& state_dir,
                            const std::string& sid,
                            std::unordered_map<std::string, std::string>& out_kv,
                            std::string& err,
                            const char* log_tag = "RT");

/// @brief per-sid userdata 文件路径推导：<state_dir>/strategy_userdata_<sid>.json。
/// @note 未校验 sid 合法性；write/read 内部各自做 path-component 校验。
std::string strategy_userdata_path(const std::string& state_dir, const std::string& sid);

/// @brief 推导 userdata state 目录（不新增 yaml 配置键）。
///
/// 优先从 recording file_path 推导（live 布局 .../live_runs/<profile>/recordings/
/// live.jsonl → .../live_runs/<profile>/state）；recording 禁用（空）或路径过浅推不出
/// 真实 profile 目录（相对浅路径推出空、绝对浅路径推出纯根目录）时回退到 config
/// 文件同级的 var/strategy_state（镜像 CTA reconcile 的 var/cta_runner_state 默认）。
///
/// @param recording_file_path  runtime.recording.file_path（已相对 config 解析；可空）。
/// @param cfg_path             config yaml 路径，供 recording 空时推导回退基准。
/// @return userdata 目录路径（未创建；write 时按需 create_directories）。
std::string derive_userdata_state_dir(const std::string& recording_file_path,
                                      const std::string& cfg_path);

}  // namespace xt_live::common
