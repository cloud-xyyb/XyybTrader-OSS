#pragma once

/// @file SdNotify.h
/// @brief runner 跟 systemd 打招呼的小工具
///
/// SdNotify.h 是 systemd Type=notify / watchdog 协议的轻量封装，只负责发状态通知

namespace xt_util {

bool sd_notify_ready() noexcept;

bool sd_notify_watchdog() noexcept;

bool sd_notify_stopping() noexcept;

bool sd_notify(const char* state) noexcept;

}
