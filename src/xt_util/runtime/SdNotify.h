#pragma once

/// @file SdNotify.h
/// @brief runner 跟 systemd 打招呼的小工具
///
/// SdNotify.h 是 systemd Type=notify / watchdog 协议的轻量封装，只负责发状态通知

namespace xt_util {

/// @brief 发送 `READY=1` 给 systemd，表示启动完成。
/// @return true 表示已成功写入 socket；false 表示 no-op 或发送失败。
bool sd_notify_ready() noexcept;

/// @brief 发送 `WATCHDOG=1` 心跳；调用间隔应小于 unit 的 WatchdogSec。
bool sd_notify_watchdog() noexcept;

/// @brief 发送 `STOPPING=1`，告知 systemd 进程进入退出路径。
bool sd_notify_stopping() noexcept;

/// @brief 发送自定义 sd_notify state string。
/// @param state 形如 `"READY=1\n"` 或 `"STATUS=running\n"`；空指针/空串返回 false。
bool sd_notify(const char* state) noexcept;

}  // namespace xt_util
