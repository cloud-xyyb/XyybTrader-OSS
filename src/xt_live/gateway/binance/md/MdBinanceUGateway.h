#pragma once

#include <memory>

#include "api/IMdGateway.h"
#include "BinanceMdCfg.h"

namespace binance_u {

/// @brief Binance U 本位永续的行情网关，实现 IMdGateway
///
/// - MdBinanceUGateway 是 Binance 行情入口：连 Binance、维护本地盘口、合成 tick、推给策略引擎。
/// - 支持多 symbol（`subs.size() >= 1`）：一条 combined stream 订阅全部 symbol 的行情
class MdBinanceUGateway : public IMdGateway {
public:

    MdBinanceUGateway();

    ~MdBinanceUGateway() override;

    MdBinanceUGateway(const MdBinanceUGateway&)            = delete;
    MdBinanceUGateway& operator=(const MdBinanceUGateway&) = delete;

    void configure_binance(const BinanceMdCfg& cfg);

    bool init(const Config& cfg) override;

    bool start() override;

    void stop() override;

    Capabilities capabilities() const override {
        return Capabilities{.supports_l2_orderflow = false};
    }

    void bind_callbacks(Callbacks callbacks) override;

    bool check_health(uint32_t now_ms) override;

#ifdef XT_BUILD_TESTS

    void* _test_impl_void() noexcept;
#endif

private:
    class Impl;
    std::unique_ptr<Impl> impl_;

#ifdef XT_BUILD_TESTS
    friend void md_binance_test_inject_message(void*, const char*, unsigned long);
    friend void md_binance_test_make_book_ready(void*, const char*);
    friend void md_binance_test_disconnect(void*);
    friend int md_binance_test_backfill_inflight(void*);
#endif
};

#ifdef XT_BUILD_TESTS

void md_binance_test_inject_message(void* impl_void, const char* payload, unsigned long len);

void md_binance_test_make_book_ready(void* impl_void, const char* symbol);

void md_binance_test_disconnect(void* impl_void);

int md_binance_test_backfill_inflight(void* impl_void);
#endif

}
