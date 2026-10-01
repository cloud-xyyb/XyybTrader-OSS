#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "xt_core/order/XtOffset.h"

namespace xt_strategy_engine {

/// slot 生命周期：Armed（等待触发）⇄ Fired（触发单在途，等终态回报）。
enum class StopSlotState : uint8_t { Armed = 0, Fired = 1 };

/// on_order_terminal 的处理结果，供 caller 派发 counter / 日志。
enum class StopTerminalAction : uint8_t {
    kIgnored = 0,  ///< unknown / duplicate order_id（幂等 no-op）
    kConsumed,     ///< 登记量已足额成交，slot 消费
    kRearmed,      ///< 零/部分成交，余量重新 Armed
    kExhausted,    ///< venue fire 计数达上限，slot 丢弃
};

struct StopSlot {
    uint64_t      stop_id = 0;
    StopSlotState state = StopSlotState::Armed;
    char          code[32] = {};
    bool          is_buy = false;       ///< 触发后下单方向
    bool          is_trailing = false;
    int64_t       trigger_price_fp = 0; ///< fixed 触发价（fp ×1e6）
    int64_t       distance_fp = 0;      ///< trailing 回撤距离（绝对价格 fp，非 tick 数）
    int64_t       watermark_fp = 0;     ///< trailing 水位；0 = 未播种（lazy-seed）
    int64_t       qty_fp = 0;           ///< 剩余登记量（partial re-arm 时递减）
    XtOffset      offset = XtOffset::Open;
    char          tag[32] = {};
    uint64_t      fired_order_id = 0;   ///< Fired 态绑定的真实单 id
    int64_t       fired_qty_fp = 0;     ///< 本次 fire 实发量（clamp 后）
    uint32_t      fire_attempts = 0;    ///< 真实 venue fire 次数（本地拒不计）
    bool          warned_local_reject = false; ///< caller 管理的 warn-once
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

    /// @return stop_id（>0）；入参非法或 book 满返 0。
    uint64_t arm_fixed(const char* code, bool is_buy, int64_t trigger_price_fp,
                       int64_t qty_fp, XtOffset offset, const char* tag);

    /// @param seed_price_fp 非 0 时以此预播种水位（如入场价）；0 = armed 后
    ///        该 code 首个 check tick 的 last 播种。
    uint64_t arm_trailing(const char* code, bool is_buy, int64_t distance_fp,
                          int64_t qty_fp, XtOffset offset, const char* tag,
                          int64_t seed_price_fp);

    /// @brief 撤销任意状态 slot。
    /// @param out_fired_order_id Fired 态带回在途单 id（Armed 态为 0），
    ///        caller 决定是否补撤真实单。
    bool disarm(uint64_t stop_id, uint64_t& out_fired_order_id);

    /// @brief 按 last 推进 trailing 水位并收集 Armed 命中的 stop_id（不消费）。
    void check(const char* code, int64_t last_fp,
               std::vector<uint64_t>& out_fired_ids);

    /// @return slot 指针；不存在返 nullptr。指针在下一次 mutate 调用前有效。
    StopSlot* find(uint64_t stop_id);

    /// @brief Armed→Fired（真实 venue 单已受理），fire_attempts++。
    void on_fired(uint64_t stop_id, uint64_t order_id, int64_t fired_qty_fp);

    /// @brief 消费 slot（CTA intent accepted / caller 决定终结）。
    void consume(uint64_t stop_id);

    /// @brief Fired 单终态处理。
    /// @param remaining_fp 终态时未成交量（caller 按 order_lifecycle::
    ///        effective_remaining_fp 口径计算，venue cancel 终态 filled 不可裸信）。
    /// @return kConsumed（足额）/ kRearmed（余量重挂，清 fired_order_id，
    ///         水位保留）/ kExhausted（attempts 达上限丢弃）/ kIgnored（幂等）。
    StopTerminalAction on_order_terminal(uint64_t order_id, int64_t remaining_fp);

    /// @brief roll 清仓：erase 该 code 全部 slot。@return 清掉的数量。
    std::size_t clear_for_code(const char* code);

    bool empty() const { return slots_.empty(); }
    std::size_t size() const { return slots_.size(); }
    const std::vector<StopSlot>& slots() const { return slots_; }

private:
    static bool code_matches_(const StopSlot& s, const char* code);
    /// 校验公共入参 + 容量后追加新 slot；非法/满返 nullptr。
    StopSlot* new_slot_(const char* code, bool is_buy, int64_t qty_fp,
                        XtOffset offset, const char* tag);
    void erase_at_(std::size_t idx); // swap-and-pop，caller 不得在遍历中调用

    uint64_t next_stop_id_ = 1;
    std::vector<StopSlot> slots_;
};

} // namespace xt_strategy_engine
