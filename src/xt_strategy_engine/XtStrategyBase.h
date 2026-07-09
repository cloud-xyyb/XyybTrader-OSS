#pragma once

/// @file
/// @brief 策略插件的 C++ SDK 基类与能力声明。
///
/// 写一个策略 = 继承 XtStrategyBase，override 需要的回调（on_tick/on_bar/on_fill 等），
/// 编译成 .so 由 runner 加载。capabilities() 自报策略需要哪些回调通道；
/// runner 启动期校验，部署形态不符合时直接拒绝，避免回调静默失效。

#include <cstdint>

// xt_core 类型前向声明（global scope）
struct XtTick;
struct XtBar;
struct XtFillEvent;
struct XtOrderExecUpdate;
struct XtAccountUpdate;
struct XtOrderQueue;
struct XtOrderDetail;
struct XtTransaction;

#include "XtStrategyInitConfig.h"

class IStrategyContext;

enum class StrategyCapability : uint64_t {
    None              = 0,

    RequiresOnFill    = 1ULL << 0,

    RequiresOnOrder   = 1ULL << 1,

    SubscribesTick    = 1ULL << 2,
};

inline constexpr uint64_t to_capability_bit(StrategyCapability c) noexcept {
    return static_cast<uint64_t>(c);
}

inline constexpr uint64_t kExecSinkRequiredCaps =
    to_capability_bit(StrategyCapability::RequiresOnFill) |
    to_capability_bit(StrategyCapability::RequiresOnOrder);

/// @brief 策略 plugin 的统一基类。
///
/// 子类通过 override market、trade、session 和 SEL callbacks 接入 runner。
/// `init()` 接收静态配置，`on_init()` 接收运行时 context；方向下单 helper
/// 只负责把 enter/exit 语义转成 context 的 `buy_fp` / `sell_fp` 调用。
///
/// @note 引擎在同一 event-loop 线程串行调用策略回调；默认策略实现不需要为回调并发加锁。
/// @warning 策略若自行启动 background thread，并在其中访问 context、策略内部状态或
///          回调输入保存的引用，必须自行同步并保证生命周期覆盖访问期。
class XtStrategyBase {
public:
    virtual ~XtStrategyBase() = default;

    virtual void init(const XtStrategyInitConfig&) {}

    virtual void on_init(IStrategyContext*) {}

    virtual void on_tick(IStrategyContext*, const XtTick&) {}

    virtual void on_bar(IStrategyContext* ctx,
                        const char*       code,
                        const char*       period,
                        const XtBar&      bar) {
        (void)ctx; (void)code; (void)period; (void)bar;
    }

    virtual void on_bar_open(IStrategyContext* ctx,
                             const char*       code,
                             const char*       period,
                             int64_t           open_px_fp) {
        (void)ctx; (void)code; (void)period; (void)open_px_fp;
    }

    virtual void on_order_queue(IStrategyContext*, const XtOrderQueue&) {}

    virtual void on_order_detail(IStrategyContext*, const XtOrderDetail&) {}

    virtual void on_transaction(IStrategyContext*, const XtTransaction&) {}

    virtual void on_order(IStrategyContext*, const XtOrderExecUpdate&) {}

    virtual void on_fill(IStrategyContext*, const XtFillEvent&) {}

    virtual void on_account(IStrategyContext*, const XtAccountUpdate&) {}

    virtual void on_session_begin(IStrategyContext*, uint32_t  ) {}

    virtual void on_session_end(IStrategyContext*, uint32_t  ) {}

    virtual void on_roll(IStrategyContext* ctx,
                         const char*       old_code,
                         const char*       new_code,
                         uint32_t          trading_date,
                         int64_t           old_last_price_fp) {
        (void)ctx;
        (void)old_code;
        (void)new_code;
        (void)trading_date;
        (void)old_last_price_fp;
    }

    virtual void on_params(IStrategyContext*) {}

    virtual uint64_t capabilities() const noexcept { return 0; }

    virtual void on_channel_ready(IStrategyContext*,
                                  const char*  ) {}

    virtual void on_channel_lost(IStrategyContext*,
                                 const char*  ,
                                 int  ) {}

    virtual void on_schedule(IStrategyContext*  ,
                             uint32_t  ,
                             uint32_t  ,
                             int64_t   ) {}

    virtual void on_session_finish(IStrategyContext*  ) {}

    static uint64_t enter_long(IStrategyContext* ctx,
                               const char* code,
                               int64_t price_fp,
                               int64_t qty_fp,
                               const char* tag = nullptr);

    static uint64_t exit_long(IStrategyContext* ctx,
                              const char* code,
                              int64_t price_fp,
                              int64_t qty_fp,
                              const char* tag = nullptr);

    static uint64_t enter_short(IStrategyContext* ctx,
                                const char* code,
                                int64_t price_fp,
                                int64_t qty_fp,
                                const char* tag = nullptr);

    static uint64_t exit_short(IStrategyContext* ctx,
                               const char* code,
                               int64_t price_fp,
                               int64_t qty_fp,
                               const char* tag = nullptr);
};
