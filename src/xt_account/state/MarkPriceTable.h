#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "xt_core/order/XtInstrumentRegistry.h"

namespace xt_account {

/// @brief AccountStateStore 内部的 mark price side table。
///
/// 管理 code -> dense id 映射和最新有效 mark price；由 store worker 单线程写入。
/// freeze() 后 symbol universe 固定，未知 code 的行情/持仓/成交更新会被丢弃并诊断。
class MarkPriceTable {
public:

    void reset();

    void pre_warm(const std::vector<std::string>& known_codes);

    void pre_warm_one(std::string_view code);

    void freeze() noexcept { frozen_.store(true, std::memory_order_release); }

    [[nodiscard]] bool is_frozen() const noexcept { return frozen_.load(std::memory_order_acquire); }

    void on_market_tick(const char* code, int64_t mark_price);

    void on_set_position(const char* code, int64_t mark_price_hint);

    void on_trade(const char* code);

    [[nodiscard]] int64_t mark_price(const char* code) const;

    [[nodiscard]] const std::vector<std::string>& tracked_codes_sorted() const noexcept {
        return tracked_codes_;
    }

    [[nodiscard]] const std::unordered_set<std::string>& tracked_codes_compat() const noexcept {
        return tracked_codes_compat_;
    }

private:

    void add_tracked_code_(const char* code);

    alignas(64) std::array<int64_t, xt_core::kMaxInstruments + 1> marks_{};
    std::array<bool, xt_core::kMaxInstruments + 1> has_mark_{};

    xt_core::XtInstrumentRegistry registry_;

    std::vector<std::string> tracked_codes_;
    std::unordered_set<std::string> tracked_codes_compat_;

    std::unordered_set<std::string> warned_frozen_codes_;

    std::atomic<bool> frozen_{false};
};

}
