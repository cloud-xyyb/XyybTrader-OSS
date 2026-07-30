#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string_view>

#include "xt_core/order/XtInstrumentRegistry.h"

namespace xt_account {

/// @brief AccountStateStore 内部的 mark price side table。
///
/// 管理 code -> dense id 映射和最新有效 mark price；由 store worker 单线程写入。
/// freeze() 后 symbol universe 固定，未知 code 的行情/持仓/成交更新会被丢弃并诊断。
///
/// @note symbol universe 的唯一 owner 是 AccountStateStore；本表只维护 mark 语义，
///       并通过 on_* 的返回值告诉 store 哪些 code 需要并入 universe。
class MarkPriceTable {
public:

    void reset();

    void pre_warm_one(std::string_view code);

    void freeze() noexcept { frozen_.store(true, std::memory_order_release); }

    [[nodiscard]] bool is_frozen() const noexcept { return frozen_.load(std::memory_order_acquire); }

    bool on_market_tick(const char* code, int64_t mark_price);

    bool on_set_position(const char* code, int64_t mark_price_hint);

    bool on_trade(const char* code);

    [[nodiscard]] int64_t mark_price(const char* code) const;

private:

    void warn_unknown_frozen_code_(const char* origin, const char* code);

    alignas(64) std::array<int64_t, xt_core::kMaxInstruments + 1> marks_{};
    std::array<bool, xt_core::kMaxInstruments + 1> has_mark_{};

    xt_core::XtInstrumentRegistry registry_;

    bool warned_unknown_frozen_ = false;

    std::atomic<bool> frozen_{false};
};

}
