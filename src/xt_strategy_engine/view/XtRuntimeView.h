#pragma once

#include <cassert>
#include <cstdint>
#include <string>

#include "xt_runtime/market/XtRuntimeMarketCache.h"
#include "api/IAccountView.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/market/XtBarStruct.h"
#include "api/XtStrategyRuntimeTypes.h"

#include "api/StrategySid.h"

class XtCommodityCatalog;
namespace xt_runtime::order {
class XtOrderRuntimeBook;
}  // namespace xt_runtime::order

namespace xt_strategy_engine {

// 策略 SDK 暴露同名 DTO alias；真实定义仍由 xt_runtime 持有。
using XtStraContractInfo  = ::xt_runtime::XtStraContractInfo;
using XtStraPositionDetail = ::xt_runtime::XtStraPositionDetail;

/// @brief 策略查询行情、账户和在途单的统一只读 view
///
/// 策略回调里的所有查询（bars/持仓/资金/open orders）都通过 XtRuntimeView
/// HFT/CTA/回测共用，差异由构造期注入的 market/account/order 组件决定。
class XtRuntimeView final {
public:
    struct Components {
        ::xt_runtime::XtRuntimeMarketCache*            market = nullptr;      ///< 行情缓存；bars() 需要 non-const。
        const ::xt_runtime::IAccountView*              account = nullptr;     ///< 账户只读视图；必须非空。
        const ::xt_runtime::order::XtOrderRuntimeBook* order_book = nullptr;   ///< 委托运行时簿记（open-order 枚举 + undone_qty 查询源）
        const XtCommodityCatalog*                      catalog = nullptr;      ///< 合约信息目录；可为空。
    };

    explicit XtRuntimeView(const Components& comp)
        : market_(comp.market)
        , account_(comp.account)
        , order_book_(comp.order_book)
        , catalog_(comp.catalog) {
        assert(market_ && "XtRuntimeView requires non-null market");
        assert(account_ && "XtRuntimeView requires non-null account");
    }

    // ── 引擎设置接口（仅操作 View 本地状态）──────────────────────

    /// @brief 设置 HFT 时钟；每 tick 推进一次。
    void set_clock(uint32_t date, uint32_t hhmm,
                   uint32_t action_time_packed, uint32_t trading_date) {
        date_               = date;
        hhmm_               = hhmm;
        action_time_packed_ = action_time_packed;
        trading_date_       = trading_date;
    }

    /// @brief 设置 CTA 时钟；time_ms 使用 HHMMSSmmm packed 格式。
    void set_clock(uint32_t date, uint32_t time_ms, uint32_t trading_date) {
        date_               = date;
        hhmm_               = time_ms / 100000;
        action_time_packed_ = time_ms;
        trading_date_       = trading_date;
    }

    // ── Market 查询 ─────────────────────────────────────────────

    /** @brief 返回指定合约最新 tick 指针（无数据时返回 nullptr）。 */
    const XtTick* last_tick(const char* code) const;
    /** @brief 拷贝指定合约最近 max 条 tick 到 out，返回实际拷贝数。 */
    size_t ticks(const char* code, XtTick* out, size_t max) const;
    /** @brief 拷贝指定合约+周期最近 max 条 bar 到 out，返回实际拷贝数。 */
    size_t bars(const char* code, const char* period, XtBar* out, size_t max);
    /** @brief 返回指定合约最新价格（fixed-point，无数据时返回 0）。 */
    int64_t last_price_fp(const char* code) const;
    /** @brief 查询合约规格信息（contract info），不存在时返回 false。 */
    bool contract_info(const char* code, XtStraContractInfo& out) const;

    /// @brief CTA 兼容：设置 bar 历史容量（委托 market cache）。
    void set_bar_capacity(const char* code, const char* period, size_t max);
    /// @brief 返回底层 market cache 指针；供引擎写入路径与 runner 直读闩绑定使用。
    ::xt_runtime::XtRuntimeMarketCache* market_cache() { return market_; }
    /// @brief 返回账户视图指针；CTA 子引擎构造子 XtRuntimeView 时复用。
    const ::xt_runtime::IAccountView* account_view_ptr() const { return account_; }

    // ── Order 查询 ──────────────────────────────────────────────

    /** @brief 拷贝当前在途订单到 out，返回实际拷贝数。
     *  sid_filter 非 0 时在 book 层按 slot.sid_id 过滤（截断 sid-aware，见 book 契约）。 */
    size_t open_orders(XtOrderExecUpdate* out, size_t max, uint32_t sid_filter = 0) const;
    /** @brief 拷贝指定 code 的在途订单到 out（code/sid 过滤下沉到 order book，一趟过滤）。 */
    size_t open_orders(const char* code, XtOrderExecUpdate* out, size_t max,
                       uint32_t sid_filter = 0) const;
    /** @brief 查询指定合约全方向在途未成交量（fixed-point）。 */
    int64_t undone_qty_fp(const char* code) const;

    /** @brief 查询 per-(sid,code) 有符号在途量（projected = confirmed + signed_inflight 的 undone 半）。 */
    int64_t signed_inflight_fp(StrategySid sid, const char* code) const;

    // ── Account 查询（委托 IAccountView）──────────────────────────

    /** @brief 查询指定合约净持仓量（fixed-point）。 */
    int64_t pos_qty_fp(const char* code) const;
    /** @brief 查询指定合约持仓均价（fixed-point）。 */
    int64_t pos_avg_fp(const char* code) const;
    /** @brief 查询指定合约未实现盈亏（fixed-point）。 */
    int64_t pos_unrealized_fp(const char* code) const;
    /** @brief 查询指定合约持仓明细（多/空/今/昨拆分）。 */
    bool pos_detail(const char* code, XtStraPositionDetail& out) const;
    /** @brief 查询账户可用资金（fixed-point）。 */
    int64_t cash_fp() const;
    /** @brief 查询账户权益（fixed-point，= cash + unrealized_pnl）。 */
    int64_t equity_fp() const;

    // ── Clock 查询 ──────────────────────────────────────────────

    uint32_t date() const { return date_; }
    uint32_t trading_date() const { return trading_date_; }
    uint32_t hhmm() const { return hhmm_; }
    /// @brief 返回 HHMMSSmmm packed 时间。
    uint32_t time_ms() const { return action_time_packed_; }

private:
    // ── 组件指针（non-owning）─────────────────────────────────────
    ::xt_runtime::XtRuntimeMarketCache*            market_ = nullptr;
    const ::xt_runtime::IAccountView*              account_ = nullptr;
    const ::xt_runtime::order::XtOrderRuntimeBook* order_book_ = nullptr;
    const XtCommodityCatalog*                      catalog_ = nullptr;

    // ── 本地时钟状态（每 tick 由引擎 set_clock 更新）──────────────
    uint32_t date_               = 0;
    uint32_t hhmm_               = 0;
    uint32_t action_time_packed_ = 0;
    uint32_t trading_date_       = 0;
};

} // namespace xt_strategy_engine
