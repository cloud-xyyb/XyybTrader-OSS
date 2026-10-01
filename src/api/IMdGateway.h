#pragma once

#include <functional>
#include <string>
#include <vector>

struct XtRawTick;
struct XtOrderQueue;
struct XtOrderDetail;
struct XtTransaction;

/**
 * @brief 行情网关抽象接口（provider-neutral MD gateway）。
 *
 * @details
 * `init/start/stop` 定义 MD lifecycle；`capabilities()` 查询 provider 能力；
 * `bind_callbacks()` 绑定 tick/front/ready 与 L2（order_queue/detail/transaction）callback。
 *
 * @warning `bind_callbacks()` 绑定的 callback 可由 provider-owned thread 触发。
 * 调用方不得假设线程归属；callback 必须快速返回，需要串行化时由 consumer
 * bridge 到 owner thread。
 */
class IMdGateway {
public:
    // ── 生命周期 ──────────────────────────────────────────────
    virtual ~IMdGateway() = default;

    // ── 配置模型（provider-neutral common fields only）──────────────────
    //
    // Provider-specific 字段：CTP 走 `CTPMdCfg`（via `MdCTPGateway::configure_ctp`）；
    // Binance U-perp 走 `BinanceMdCfg`。通过 `gateway_factory::create_md_gateway`
    // 的 `MdProviderCfg` variant 参数注入。
    //
    struct Config {
        std::string front;                       ///< 前置地址（e.g. tcp://... 或 wss://...）
        std::string user;                        ///< 用户名 / API-key holder
        /// @warning 明文凭证字段：进程内存驻留期间不受 mlock/zeroize 保护。
        /// 用 std::string 是因为 gateway 实现层广泛读 cfg.password.c_str()
        /// （MdCTPGateway / MdBinanceUGateway 等）。
        std::string password;                    ///< 密码 / API secret
        std::vector<std::string> subs;           ///< 订阅合约列表
        uint32_t stale_reconnect_ms = 0;         ///< MD tick 超时自愈阈值 ms（0 = 禁用）
    };

    struct Capabilities {
        bool supports_l2_orderflow = false; ///< 支持 Level 2 逐笔行情（orderflow data）
    };

    // ── 回调类型 ──────────────────────────────────────────────
    struct Callbacks {
        /// tick 行情回调（market tick）。第二参 `capture_ns` 是 SHM connector publish 前
        /// stamp 的 CLOCK_MONOTONIC steady ns（跨进程可比，供 md_ingress 起点上溯）；
        /// 直连 venue provider（ParserCTP/MdBinanceU/ParserXTP）无 capture，一律传 0。
        /// 精确二参（非默认参）：std::function 数据成员无法表达默认参，且强制编译器扫清
        /// 全部调用面（binder 转发真值 / provider 传 0 / consumer 消费或忽略）。
        std::function<void(const XtRawTick&, uint64_t capture_ns)> on_tick;
        std::function<void(const XtOrderQueue&)> on_order_queue;     ///< 委托队列回调（L2 order queue）
        std::function<void(const XtOrderDetail&)> on_order_detail;   ///< 逐笔委托回调（L2 order detail）
        std::function<void(const XtTransaction&)> on_transaction;    ///< 逐笔成交回调（L2 transaction）
        std::function<void()> on_ready;                              ///< 通道就绪回调（gateway ready）
        std::function<void(bool connected, int reason)> on_front;    ///< 前置连接/断连回调（front connection state）
    };

    // ── 通道生命周期 ──────────────────────────────────────────────
    [[nodiscard]] virtual bool init(const Config& cfg) = 0;
    [[nodiscard]] virtual bool start() = 0;
    virtual void stop() = 0;

    // ── 能力与回调 ──────────────────────────────────────────────
    [[nodiscard]] virtual Capabilities capabilities() const = 0;
    virtual void bind_callbacks(Callbacks callbacks) = 0;

    // ── 健康检查（冷路径，Runner 控制环 ~1Hz 调用） ──────────────────
    /// @brief 周期性健康检查。检测到异常时实现应自行处理（reconnect/retry）并返回 false。
    /// @param now_ms steady_clock 毫秒时间戳
    /// @return true = 健康; false = 检测到异常已自行处理
    [[nodiscard]] virtual bool check_health(uint32_t now_ms) = 0;
};
