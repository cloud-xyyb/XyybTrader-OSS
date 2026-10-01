#pragma once

#include "ITradeTransport.h"

#include <cstdint>
#include <memory>

namespace binance_u {

class BinanceRestClient;

/// @brief REST cancel 对 venue 限频/封禁类失败的有界重试配置。
/// @note 仅作用于 cancel：submit 失败有 kRejected ack 收敛路径；cancel 失败
///       若不重试，委托会一直挂在交易所，堆到 per-symbol max-open-order 上限
///       后所有新单（含平仓）被 -2025 拒。
struct RestCancelRetryCfg {
    int      max_tries          = 4;      ///< 总尝试次数上限（含首发）。
    uint64_t base_backoff_ms    = 500;    ///< 指数退避基数（500ms → 1s → 2s ...）。
    uint64_t max_backoff_ms     = 5000;   ///< 单次指数退避上限（不约束 Retry-After 路径）。
    uint64_t max_retry_after_ms = 60000;  ///< Retry-After 采纳上限，防 418 长封禁拖垮 worker 节奏。
};

/// @brief cancel 失败是否属于可重试的 venue 限频/封禁类错误
///        （HTTP 429/418 或业务码 -1015/-1003）。
bool rest_cancel_error_retryable(long http_status, int err_code);

/// @brief 第 tries_done 次失败后的退避毫秒数。
/// @param tries_done       已完成的尝试次数（>= 1）。
/// @param retry_after_sec  响应 Retry-After 秒数（0 = 无 header）；非 0 时优先于
///                         指数退避，并 clamp 到 cfg.max_retry_after_ms。
uint64_t rest_cancel_backoff_ms(int tries_done, long retry_after_sec,
                                const RestCancelRetryCfg& cfg);

/// @brief REST submit 失败的分类：明确业务拒绝 vs 执行状态未知（BIN-D14）。
enum class RestSubmitFailureClass {
    kBusinessReject,    ///< 明确拒绝（4xx 业务码 / 限流 429/418 / -1003 / -1008）：订单未挂出，可安全终态化。
    kExecutionUnknown,  ///< 执行状态未知（3xx / 5xx / 网络层失败 / -1007 / -1006 / -1001）：订单可能已挂出或成交，不得终态化。
};

/// @brief 依据 HTTP 状态与 Binance 业务错误码判定 REST submit 失败类别。
/// @param http_status  HTTP 状态码；0 表示 curl transport 失败（无 HTTP 响应）。
/// @param err_code     响应体 `code` 字段值（缺失时为 0）。
/// @return kExecutionUnknown 时调用方须保留在途、交 reconcile 补查真实终态。
/// @note Binance 官方对 5xx 明示 "execution status unknown"（可能已成交）；执行状态未知的
///       业务码同理不能销账：-1007 TIMEOUT（后端响应超时）、-1006 UNEXPECTED_RESP（官方明示
///       execution status unknown）、-1001 DISCONNECTED（内部错误，是否被处理不可判定）；
///       curl transport 失败（http==0）与 3xx 重定向下请求可能已抵达撮合引擎。以上均不能据此
///       销账，否则本地销账后委托仍 live 在 venue → 漏成交 / 幽灵仓（BIN-D14）。反之 -1008
///       SERVER_BUSY 是过载拒绝（委托明确未挂出），保留为业务拒不纳入未知。
RestSubmitFailureClass classify_rest_submit_failure(long http_status,
                                                    int err_code) noexcept;

/// @brief Binance TD 的 REST 下单发送器，实现 ITradeTransport
///
/// - TraderBinanceUGateway 把报单/撤单交给RestTradeTransport
/// - 把任务先入本地队列，然后由一个 REST worker 线程串行调用 Binance signed REST 接口处理
class RestTradeTransport : public ITradeTransport {
public:
    /// @brief 绑定 REST client；仅保存引用，不拥有其生命周期。
    /// @param cancel_retry cancel 限频/封禁类失败的重试预算。
    explicit RestTradeTransport(BinanceRestClient& rest,
                                RestCancelRetryCfg cancel_retry = {});

    /// @brief 析构时停止 worker 线程。
    ~RestTradeTransport() override;

    /// @brief 启动 REST worker 线程；重复调用幂等。
    bool start() override;

    /// @brief 停止 worker；未到期的 cancel 重试不再继续发送。
    void stop() override;

    /// @brief 非阻塞入队 submit；结果通过 AckHandler 从 worker 线程异步返回。
    void submit(TransportSubmitReq&& req) override;

    /// @brief 非阻塞入队 cancel；限频/封禁类失败由本 transport 内部有界重试。
    void cancel(TransportCancelReq&& req) override;

    /// @brief 绑定 REST ack 回调；应在 `start()` 前完成。
    /// @warning 回调在 REST worker 线程执行，必须快速返回。
    void bind_ack_handler(AckHandler handler) override;

    /// @brief 返回 worker 观测到的 in-flight REST 调用与最近 ack 时间。
    Health health() const override;

    /// @brief 返回 running 标志：false 表示 stop() 已完成 running 置位（**不**保证
    ///        worker 线程已退出——stop() 可能仍在 join worker）。
    /// @note 测试用确定性 barrier：调用方轮询本方法直到返回 false，以确保
    ///       stop() 已完成置位，再触发被 stop() 阻塞等待的外部事件，消除
    ///       置位与并发释放之间的调度窗口竞态。
    bool is_running() const;

    /// @brief 进程内累计的 submit execution-unknown 事件数（BIN-D14：5xx / 网络层
    ///        失败下保留在途、交 reconcile 收敛的次数）。观测用，无 prom metric；
    ///        跨线程原子读。
    uint64_t submit_exec_unknown_count() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;  ///< 持有 REST worker、请求队列和 cancel retry 队列。
};

}  // namespace binance_u
