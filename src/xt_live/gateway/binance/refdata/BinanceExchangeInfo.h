#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace binance_u {

/// @file BinanceExchangeInfo.h
/// @brief Binance 每个 symbol 的价格/数量网格（tickSize/stepSize）缓存与对齐 helper
///
/// - TD 网关启动时从 `GET /fapi/v1/exchangeInfo` 拉一次数据，填进程级缓存；拉不到就不交易
/// - 下单/平仓前用文件末尾的 snap helper 把价格和数量对齐到网格，否则会被交易所拒单

/// @brief 单 symbol 的下单约束（来自 PRICE_FILTER / LOT_SIZE）。
struct SymbolFilter {
    double  tick = 0.0;     ///< PRICE_FILTER.tickSize（价格最小变动）
    double  step = 0.0;     ///< LOT_SIZE.stepSize（数量最小变动；0 = venue 未给）
    int64_t tick_fp = 0;    ///< tick × 1e6（snap_price_fp 用，免 double 重算）
    int64_t step_fp = 0;    ///< step × 1e6（snap_qty_fp 用）
};

/// @brief 解析 exchangeInfo JSON body → per-symbol filter map（symbol 原样大写）。
/// @param body  `GET /fapi/v1/exchangeInfo` 响应原文。
/// @param out   输出 map（清空后填充）。
/// @param err   失败原因（可 nullptr）。
/// @return true：JSON 合法且含 `symbols[]` 数组（即便 0 个有效 symbol 也算结构成功）；
///         false：JSON 非法或缺 `symbols[]`。
/// @note 宽松解析：单个 symbol 缺 PRICE_FILTER（无 tickSize）则跳过该 symbol，不致整体失败；
///       该 symbol 在缓存中缺席 → 下单路径靠 `BinanceSymbolFilters::has()` gate fail-closed。
/// @note 定义在 BinanceExchangeInfo.cpp（依赖 nlohmann）；不在本 header 暴露 json。
bool parse_exchange_info(const std::string& body,
                         std::unordered_map<std::string, SymbolFilter>& out,
                         std::string* err);

/// @brief 进程级 symbol filters 缓存；全程序共享一份（inline singleton）。
///
/// TD 网关 init 时 load() 一次，下单和平仓路径随时查；读写线程安全。
class BinanceSymbolFilters {
public:
    using Map = std::unordered_map<std::string, SymbolFilter>;

    /// @brief 进程级单例（inline Meyers → 程序内单实例）。
    static BinanceSymbolFilters& global() {
        static BinanceSymbolFilters inst;
        return inst;
    }

    /// @brief 原子发布新 filter 集合（替换旧 immutable snapshot）。
    void load(Map m) {
        auto next = std::make_shared<const Map>(std::move(m));
        std::lock_guard<std::mutex> lk(mu_);
        map_ = std::move(next);
    }

    /// @brief symbol 的 PRICE_FILTER.tickSize（double）；未知/未加载返回 0。
    double tick_size(const std::string& symbol) const {
        const auto snap = snapshot_();
        const auto it = snap->find(symbol);
        return it == snap->end() ? 0.0 : it->second.tick;
    }

    /// @brief symbol 的 LOT_SIZE.stepSize（double）；未知/未加载返回 0。
    double qty_step(const std::string& symbol) const {
        const auto snap = snapshot_();
        const auto it = snap->find(symbol);
        return it == snap->end() ? 0.0 : it->second.step;
    }

    /// @brief symbol 的 tickSize × 1e6（fp，snap_price_fp 直接用，免每次 double→llround）；未知返回 0。
    int64_t tick_size_fp(const std::string& symbol) const {
        const auto snap = snapshot_();
        const auto it = snap->find(symbol);
        return it == snap->end() ? 0 : it->second.tick_fp;
    }

    /// @brief symbol 的 stepSize × 1e6（fp，snap_qty_fp 直接用）；未知返回 0。
    int64_t qty_step_fp(const std::string& symbol) const {
        const auto snap = snapshot_();
        const auto it = snap->find(symbol);
        return it == snap->end() ? 0 : it->second.step_fp;
    }

    /// @brief 该 symbol 的 filter 是否已加载（fail-closed gate 用）。
    bool has(const std::string& symbol) const {
        const auto snap = snapshot_();
        return snap->find(symbol) != snap->end();
    }

    /// @brief 已加载的 symbol 数（init 后应 > 0）。
    std::size_t size() const { return snapshot_()->size(); }

private:
    BinanceSymbolFilters() : map_(std::make_shared<const Map>()) {}

    std::shared_ptr<const Map> snapshot_() const {
        std::lock_guard<std::mutex> lk(mu_);
        return map_;
    }

    mutable std::mutex         mu_;
    std::shared_ptr<const Map> map_;  ///< immutable snapshot；load 整体替换。
};

// ── tick/step snap helper ────────────────────────────────────────────────────
//
// Binance perp 各 symbol 的 PRICE_FILTER.tickSize / LOT_SIZE.stepSize 不同；LIMIT
// 单价格/数量必须落在对应网格上，否则 REST 会返回 `-4014` / `-1111`。缓存未加载该
// symbol 时 tick/step 返回 0，snap helper 原样返回输入值；下单路径必须先通过
// `binance_filters_loaded()` gate fail-closed，避免发送 off-tick/off-step 订单。

/// @brief 查 symbol 的 PRICE_FILTER.tickSize（exchangeInfo 缓存，double）；未加载/未知返回 0。
inline double symbol_tick_size(const std::string& symbol) noexcept {
    return BinanceSymbolFilters::global().tick_size(symbol);
}

/// @brief 查 symbol 的 LOT_SIZE.stepSize（exchangeInfo 缓存，double）；未加载/未知返回 0（不量化）。
inline double symbol_qty_step(const std::string& symbol) noexcept {
    return BinanceSymbolFilters::global().qty_step(symbol);
}

/// @brief 该 symbol 的 venue filters 是否已加载（下单 fail-closed gate 用）。
inline bool binance_filters_loaded(const std::string& symbol) noexcept {
    return BinanceSymbolFilters::global().has(symbol);
}

/// @brief 把价格 snap 到 tickSize 整数倍。
/// @param px        原始价格
/// @param tick      tickSize（必须 > 0；<= 0 时返回原值）
/// @param snap_up   true = ceil 取上整（BUY 平空偏高激进）；
///                  false = floor 取下整（SELL 平多偏低激进）
/// @return          snap 后的价格
inline double snap_price_to_tick(double px, double tick, bool snap_up) noexcept {
    if (tick <= 0.0) return px;
    const double n = px / tick;
    const double rounded = snap_up ? std::ceil(n) : std::floor(n);
    return rounded * tick;
}

/// @brief 把订单价格 fp（scale 1e6）round 到最近的 tickSize 整数倍。
/// @note round-to-nearest 同时消除 `to_fp_trunc=(int64)(v*1e6)` 的截断伪影，并保证
///       输出落在 venue tickSize 上，避免 Binance `-1111` / `-4014`。tick 未加载（0）
///       时原样返回，由下单路径 gate 兜住（不发 off-tick）。直接用缓存 fp，免 double 重算。
inline int64_t snap_price_fp(int64_t price_fp, const std::string& symbol) noexcept {
    if (price_fp <= 0) return price_fp;
    const int64_t tick_fp = BinanceSymbolFilters::global().tick_size_fp(symbol);
    if (tick_fp <= 0) return price_fp;
    return ((price_fp + tick_fp / 2) / tick_fp) * tick_fp;
}

/// @brief 把订单数量 fp（scale 1e6）round 到最近的 stepSize 整数倍（修 qty 侧 -1111）。
/// @note 未加载 step（=0）原样透传，不猜 step。返回 0 表示残量小于半个 step，
///       调用方应丢弃不发，避免 qty=0 被 venue 拒绝。
inline int64_t snap_qty_fp(int64_t qty_fp, const std::string& symbol) noexcept {
    if (qty_fp <= 0) return qty_fp;
    const int64_t step_fp = BinanceSymbolFilters::global().qty_step_fp(symbol);
    if (step_fp <= 0) return qty_fp;
    return ((qty_fp + step_fp / 2) / step_fp) * step_fp;
}

}  // namespace binance_u
