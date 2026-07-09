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
    double  tick = 0.0;
    double  step = 0.0;
    int64_t tick_fp = 0;
    int64_t step_fp = 0;
};

bool parse_exchange_info(const std::string& body,
                         std::unordered_map<std::string, SymbolFilter>& out,
                         std::string* err);

/// @brief 进程级 symbol filters 缓存；全程序共享一份（inline singleton）。
///
/// TD 网关 init 时 load() 一次，下单和平仓路径随时查；读写线程安全。
class BinanceSymbolFilters {
public:
    using Map = std::unordered_map<std::string, SymbolFilter>;

    static BinanceSymbolFilters& global() {
        static BinanceSymbolFilters inst;
        return inst;
    }

    void load(Map m) {
        auto next = std::make_shared<const Map>(std::move(m));
        std::lock_guard<std::mutex> lk(mu_);
        map_ = std::move(next);
    }

    double tick_size(const std::string& symbol) const {
        const auto snap = snapshot_();
        const auto it = snap->find(symbol);
        return it == snap->end() ? 0.0 : it->second.tick;
    }

    double qty_step(const std::string& symbol) const {
        const auto snap = snapshot_();
        const auto it = snap->find(symbol);
        return it == snap->end() ? 0.0 : it->second.step;
    }

    int64_t tick_size_fp(const std::string& symbol) const {
        const auto snap = snapshot_();
        const auto it = snap->find(symbol);
        return it == snap->end() ? 0 : it->second.tick_fp;
    }

    int64_t qty_step_fp(const std::string& symbol) const {
        const auto snap = snapshot_();
        const auto it = snap->find(symbol);
        return it == snap->end() ? 0 : it->second.step_fp;
    }

    bool has(const std::string& symbol) const {
        const auto snap = snapshot_();
        return snap->find(symbol) != snap->end();
    }

    std::size_t size() const { return snapshot_()->size(); }

private:
    BinanceSymbolFilters() : map_(std::make_shared<const Map>()) {}

    std::shared_ptr<const Map> snapshot_() const {
        std::lock_guard<std::mutex> lk(mu_);
        return map_;
    }

    mutable std::mutex         mu_;
    std::shared_ptr<const Map> map_;
};

inline double symbol_tick_size(const std::string& symbol) noexcept {
    return BinanceSymbolFilters::global().tick_size(symbol);
}

inline double symbol_qty_step(const std::string& symbol) noexcept {
    return BinanceSymbolFilters::global().qty_step(symbol);
}

inline bool binance_filters_loaded(const std::string& symbol) noexcept {
    return BinanceSymbolFilters::global().has(symbol);
}

inline double snap_price_to_tick(double px, double tick, bool snap_up) noexcept {
    if (tick <= 0.0) return px;
    const double n = px / tick;
    const double rounded = snap_up ? std::ceil(n) : std::floor(n);
    return rounded * tick;
}

inline int64_t snap_price_fp(int64_t price_fp, const std::string& symbol) noexcept {
    if (price_fp <= 0) return price_fp;
    const int64_t tick_fp = BinanceSymbolFilters::global().tick_size_fp(symbol);
    if (tick_fp <= 0) return price_fp;
    return ((price_fp + tick_fp / 2) / tick_fp) * tick_fp;
}

inline int64_t snap_qty_fp(int64_t qty_fp, const std::string& symbol) noexcept {
    if (qty_fp <= 0) return qty_fp;
    const int64_t step_fp = BinanceSymbolFilters::global().qty_step_fp(symbol);
    if (step_fp <= 0) return qty_fp;
    return ((qty_fp + step_fp / 2) / step_fp) * step_fp;
}

}
