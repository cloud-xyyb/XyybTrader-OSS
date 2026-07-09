#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "xt_core/market/XtTick.h"
#include "xt_core/market/XtBarStruct.h"
#include "xt_core/order/XtOrderTypes.h"
#include "api/XtStrategyRuntimeTypes.h"
#include "api/XtRejectCode.h"

/**
 * @brief 统一策略上下文接口（unified strategy context interface）。
 *
 * @details
 * 策略通过 context 发起下单/撤单，读取行情、订单、持仓、账户、时间、参数，
 * 并上报日志与指标。
 *
 * @warning 策略只能在 engine callback 或 owner event-loop thread 内调用 context。
 * context 不承诺跨线程安全；跨线程使用必须先投递回 owner thread。
 */
class IStrategyContext {
public:
    virtual ~IStrategyContext() = default;

    static constexpr XtTimeInForce TIF_GTC = XtTimeInForce::GTC;
    static constexpr XtTimeInForce TIF_IOC = XtTimeInForce::IOC;
    static constexpr XtTimeInForce TIF_FOK = XtTimeInForce::FOK;

    virtual uint64_t buy_fp(const char* code,
                            int64_t     price_fp,
                            int64_t     qty_fp,
                            XtTimeInForce tif,
                            const char* tag,
                            XtOffset    offset) = 0;

    virtual uint64_t sell_fp(const char* code,
                             int64_t     price_fp,
                             int64_t     qty_fp,
                             XtTimeInForce tif,
                             const char* tag,
                             XtOffset    offset) = 0;

    virtual void cancel(uint64_t order_id) = 0;

    [[nodiscard]] virtual const XtTick* get_last_tick(const char* code) = 0;

    [[nodiscard]] virtual int64_t get_last_price_fp(const char* code) = 0;

    [[nodiscard]] virtual size_t get_ticks(const char* code, XtTick* buf, size_t max) = 0;

    [[nodiscard]] virtual size_t get_bars(const char* code, const char* period,
                            XtBar* buf, size_t max) = 0;

    [[nodiscard]] virtual size_t get_open_orders(const char* code,
                                   XtOrderExecUpdate* buf, size_t max) = 0;

    [[nodiscard]] virtual int64_t get_position_fp(const char* code) = 0;

    [[nodiscard]] virtual int64_t projected_inflight_fp(const char* code) = 0;

    [[nodiscard]] virtual int64_t get_position_avg_price_fp(const char* code) = 0;

    [[nodiscard]] virtual int64_t get_position_unrealized_fp(const char* code) = 0;

    [[nodiscard]] virtual size_t get_position_detail(const char* code,
                                       xt_runtime::XtStraPositionDetail* buf,
                                       size_t max) = 0;

    [[nodiscard]] virtual int64_t get_cash_fp() = 0;

    [[nodiscard]] virtual int64_t get_equity_fp() = 0;

    [[nodiscard]] virtual const xt_runtime::XtStraContractInfo* get_contract_info(const char* code) = 0;

    virtual uint32_t get_date() = 0;
    virtual uint32_t get_trading_date() = 0;
    virtual uint32_t get_time_ms() = 0;

    virtual std::string get_param_str(const char* key, const char* def) = 0;

    virtual int64_t get_param_int(const char* key, int64_t def) = 0;

    virtual double get_param_double(const char* key, double def) = 0;

    virtual bool get_param_bool(const char* key, bool def) = 0;

    virtual void sub_tick(const char* code) = 0;

    virtual void sub_bar(const char* code, const char* period, size_t max) = 0;

    virtual void log_debug(const char* msg) = 0;

    virtual void log_info(const char* msg) = 0;

    virtual void log_warn(const char* msg) = 0;

    virtual void report_metric(const char* name, double val) = 0;

    virtual void set_next_order_reason(const char*  ) {}

    [[nodiscard]] virtual bool can_submit() const { return true; }

    virtual void save_userdata(const char* key, const char* val) = 0;

    virtual const char* load_userdata(const char* key, const char* def) = 0;

    virtual void declare_position_interest(const char*  ) {}

    virtual void set_target_weights(
        const std::unordered_map<std::string, double>&  ) {}

    [[nodiscard]] virtual int64_t current_fire_time_ms() const noexcept { return 0; }

    [[nodiscard]] virtual xt_runtime::XtRejectCode last_reject_code() const noexcept {
        return xt_runtime::XtRejectCode::NoError;
    }

    virtual uint64_t submit_stop_fp(const char*  , bool  ,
                                    int64_t  ,
                                    int64_t  , XtOffset  ,
                                    const char*  ) { return 0; }

    virtual uint64_t submit_trailing_stop_fp(const char*  , bool  ,
                                             int64_t  ,
                                             int64_t  , XtOffset  ,
                                             const char*  ,
                                             int64_t  ) { return 0; }

    virtual bool cancel_stop(uint64_t  ) { return false; }
};
