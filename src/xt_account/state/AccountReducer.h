#pragma once

/// @file
/// @brief AccountState reducer 的单事件应用入口。
///
/// 本文件只暴露 reducer 写入口；同步、线程归属和 snapshot 发布由
/// AccountStateStore 在调用外层保证。

#include "state/AccountState.h"
#include "state/AccountEvent.h"

/// @brief 将一个 reducer-private 事件应用到 AccountState。
/// @warning 无同步原语；调用方必须保证同一 AccountState 只有一个 writer。
void apply_event(AccountState& account, const XtAccountEvent& ev);
