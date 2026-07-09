#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "xt_core/order/XtOffset.h"

namespace xt_strategy_engine {

enum class StopSlotState : uint8_t { Armed = 0, Fired = 1 };

enum class StopTerminalAction : uint8_t {
    kIgnored = 0,
    kConsumed,
    kRearmed,
    kExhausted,
};

struct StopSlot {
    uint64_t      stop_id = 0;
    StopSlotState state = StopSlotState::Armed;
    char          code[32] = {};
    bool          is_buy = false;
    bool          is_trailing = false;
    int64_t       trigger_price_fp = 0;
    int64_t       distance_fp = 0;
    int64_t       watermark_fp = 0;
    int64_t       qty_fp = 0;
    XtOffset      offset = XtOffset::Open;
    char          tag[32] = {};
    uint64_t      fired_order_id = 0;
    int64_t       fired_qty_fp = 0;
    uint32_t      fire_attempts = 0;
    bool          warned_local_reject = false;
};

/// @brief 策略 context 里的本地条件单列表
///
/// XtStopOrderBook 保存策略登记的 fixed stop 和 trailing stop。engine 每个 tick
/// 把最新价交给 check()；命中条件后，调用方根据返回的 stop_id 发出真实订单。
///
/// XtStopOrderBook 不直接提交订单，发单、可平量裁剪和订单终态处理由 XtStrategyContext 完成。
///
/// 触发语义（价源 = last）：
///   fixed：buy-stop `last >= trigger`；sell-stop `last <= trigger`。
/// check() 只报告已触发的 Armed stop，不消费 slot；状态由调用方在真实订单结果明确后推进。
class XtStopOrderBook {
public:
    static constexpr std::size_t kMaxStopSlotsPerCtx  = 64;
    static constexpr uint32_t    kMaxStopFireAttempts = 32;

    uint64_t arm_fixed(const char* code, bool is_buy, int64_t trigger_price_fp,
                       int64_t qty_fp, XtOffset offset, const char* tag);

    uint64_t arm_trailing(const char* code, bool is_buy, int64_t distance_fp,
                          int64_t qty_fp, XtOffset offset, const char* tag,
                          int64_t seed_price_fp);

    bool disarm(uint64_t stop_id, uint64_t& out_fired_order_id);

    void check(const char* code, int64_t last_fp,
               std::vector<uint64_t>& out_fired_ids);

    StopSlot* find(uint64_t stop_id);

    void on_fired(uint64_t stop_id, uint64_t order_id, int64_t fired_qty_fp);

    void consume(uint64_t stop_id);

    StopTerminalAction on_order_terminal(uint64_t order_id, int64_t remaining_fp);

    std::size_t clear_for_code(const char* code);

    bool empty() const { return slots_.empty(); }
    std::size_t size() const { return slots_.size(); }
    const std::vector<StopSlot>& slots() const { return slots_; }

private:
    static bool code_matches_(const StopSlot& s, const char* code);

    StopSlot* new_slot_(const char* code, bool is_buy, int64_t qty_fp,
                        XtOffset offset, const char* tag);
    void erase_at_(std::size_t idx);

    uint64_t next_stop_id_ = 1;
    std::vector<StopSlot> slots_;
};

}
