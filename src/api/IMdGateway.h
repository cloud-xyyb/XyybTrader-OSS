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

    virtual ~IMdGateway() = default;

    struct Config {
        std::string front;
        std::string user;

        std::string password;
        std::vector<std::string> subs;
        uint32_t stale_reconnect_ms = 0;
    };

    struct Capabilities {
        bool supports_l2_orderflow = false;
    };

    struct Callbacks {
        std::function<void(const XtRawTick&)> on_tick;
        std::function<void(const XtOrderQueue&)> on_order_queue;
        std::function<void(const XtOrderDetail&)> on_order_detail;
        std::function<void(const XtTransaction&)> on_transaction;
        std::function<void()> on_ready;
        std::function<void(bool connected, int reason)> on_front;
    };

    [[nodiscard]] virtual bool init(const Config& cfg) = 0;
    [[nodiscard]] virtual bool start() = 0;
    virtual void stop() = 0;

    [[nodiscard]] virtual Capabilities capabilities() const = 0;
    virtual void bind_callbacks(Callbacks callbacks) = 0;

    [[nodiscard]] virtual bool check_health(uint32_t now_ms) = 0;
};
