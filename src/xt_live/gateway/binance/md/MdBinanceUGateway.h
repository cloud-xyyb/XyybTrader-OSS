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
    /// @brief 构造空网关；实际端点由 `configure_binance()` / `init()` 注入。
    MdBinanceUGateway();

    /// @brief 析构时停止内部 WS 与 snapshot 线程。
    ~MdBinanceUGateway() override;

    /// @note 持有线程与 socket 状态，禁止复制。
    MdBinanceUGateway(const MdBinanceUGateway&)            = delete;
    MdBinanceUGateway& operator=(const MdBinanceUGateway&) = delete;

    /// @brief 注入 Binance MD 专属配置；需在 `init()` 前调用。
    /// @note `base_ws_url` 与 `base_rest_url` 应指向同一行情数据面，确保 depth
    ///       diff 与 REST snapshot 的 update id 可拼接。
    void configure_binance(const BinanceMdCfg& cfg);

    // 生命周期。
    /// @brief 校验多 symbol 订阅（`subs.size() >= 1`）并构造 Binance combined stream
    ///        target（每 symbol 的 depth/aggTrade/bookTicker）；stream 数超 1024 上限拒绝。
    bool init(const Config& cfg) override;

    /// @brief 启动内部 WS 线程；连接建立后异步拉取 depth snapshot。
    bool start() override;

    /// @brief 停止 WS 连接、取消 snapshot 拉取并等待内部线程退出；可重复调用。
    void stop() override;

    // 能力与回调。
    /// @brief Binance 公共行情不提供 CTP 风格逐笔委托/成交。
    Capabilities capabilities() const override {
        return Capabilities{.supports_l2_orderflow = false};
    }

    /// @brief 绑定行情回调；`on_tick` / `on_ready` 由内部 WS 线程调用。
    void bind_callbacks(Callbacks callbacks) override;

    // 健康检查。
    /// @brief 检查本地 tick freshness；stale 时异步请求 WS reconnect。
    bool check_health(uint32_t now_ms) override;

#ifdef XT_BUILD_TESTS
    /// @brief 单测入口（XT_BUILD_TESTS only）：暴露 PImpl 指针给自由函数，不泄漏 Impl
    ///        类型/private 字段到 header。单测通过 md_binance_test_* 自由函数驱动 demux。
    void* _test_impl_void() noexcept;
#endif  // XT_BUILD_TESTS

private:
    class Impl;
    std::unique_ptr<Impl> impl_;  ///< 持有 Boost.Asio/OpenSSL、WS、snapshot 与 book 状态。

#ifdef XT_BUILD_TESTS
    friend void md_binance_test_inject_message(void*, const char*, unsigned long);
    friend void md_binance_test_make_book_ready(void*, const char*);
    friend void md_binance_test_disconnect(void*);
    friend int md_binance_test_backfill_inflight(void*);
    friend bool md_binance_test_agg_state(void*, const char*, long long*,
                                          unsigned long long*);
#endif  // XT_BUILD_TESTS
};

#ifdef XT_BUILD_TESTS
// ── 单测集成 seam（XT_BUILD_TESTS only）────────────────────────────────────
// PImpl 私有路径驱动入口：拿 _test_impl_void() 的 void* 注入。仅保留跨子系统闭环
// 无法从组件外驱动的入口；depth 状态机 / freshness 判定由 BinanceDepthSync /
// PerSymbolFreshnessWitness 公开 API 直测。
//
/// @brief 把一条 combined-stream 原始 JSON payload 喂进 handle_message（同 WS 回调路径）。
void md_binance_test_inject_message(void* impl_void, const char* payload, unsigned long len);

/// @brief 复刻 worker 成功 handoff：走真 on_snapshot_ready 路径（drain + witness +
///        mark seed + first-ready on_ready）让指定 symbol 的 book is_ready()，
///        以便后续 aggTrade/bookTicker 注入触发 push_tick（绕过真 REST snapshot）。
void md_binance_test_make_book_ready(void* impl_void, const char* symbol);

/// @brief 复刻 WS 整断：驱动 on_connection_disconnect（reset 所有 symbol + cancel 在途
///        backfill + 置 reconnect_pending_，供 gap→backfill launch 生产路径测试）。
void md_binance_test_disconnect(void* impl_void);

/// @brief 在跑 backfill 线程数（detach 协议 stop 专项测试的非 vacuous 断言入口）。
int md_binance_test_backfill_inflight(void* impl_void);

/// @brief agg 序列观测：symbol 高水位 last_agg_id + 非有限 p/q 丢弃累计。
/// @return symbol 未订阅返回 false，出参不写。
bool md_binance_test_agg_state(void* impl_void, const char* symbol,
                               long long* out_last_agg_id,
                               unsigned long long* out_dropped);
#endif  // XT_BUILD_TESTS

}  // namespace binance_u
