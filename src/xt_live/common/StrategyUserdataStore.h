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

bool write_strategy_userdata(const std::string& state_dir,
                             const std::string& sid,
                             uint32_t trading_date,
                             const std::unordered_map<std::string, std::string>& kv,
                             std::string& err);

bool read_strategy_userdata(const std::string& state_dir,
                            const std::string& sid,
                            std::unordered_map<std::string, std::string>& out_kv,
                            std::string& err,
                            const char* log_tag = "RT");

std::string strategy_userdata_path(const std::string& state_dir, const std::string& sid);

std::string derive_userdata_state_dir(const std::string& recording_file_path,
                                      const std::string& cfg_path);

}
