#pragma once

#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <utility>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>

namespace xt_live::gateway {

/// @brief io_context 的独占线程的生命周期封装
///
/// boost::asio 的每条网络连接线程都要重复写「起线程、work_guard 保活、
/// 停止时 stop + join」这套样板，IoContextThread 把这层统一处理；
/// 连接、重连、回调编排仍归调用方，各 WS/UDS 传输线程复用同一套管理。
///
/// stop() 返回后线程已 join，不会再有 handler 触发；start/stop 幂等，
/// 由外部控制线程串行调用。
class IoContextThread {
public:
    using WorkGuard =
        boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

    /// @param with_ssl 是否构造随身 ssl::context（tlsv12_client + verify_peer +
    ///        default_verify_paths）；MD/UDS/WS trade 均需，仅纯 timer 用途可省。
    explicit IoContextThread(bool with_ssl = true) {
        if (with_ssl) {
            ssl_ctx_ = std::make_unique<boost::asio::ssl::context>(
                boost::asio::ssl::context::tlsv12_client);
            ssl_ctx_->set_default_verify_paths();
            ssl_ctx_->set_verify_mode(boost::asio::ssl::verify_peer);
        }
    }

    ~IoContextThread() { stop([] {}); }

    IoContextThread(const IoContextThread&) = delete;
    IoContextThread& operator=(const IoContextThread&) = delete;

    boost::asio::io_context& ioc() noexcept { return ioc_; }
    boost::asio::ssl::context& ssl_ctx() noexcept { return *ssl_ctx_; }

    /// @return true 当且仅当调用方运行在本 io_context 线程上。
    bool on_io_thread() const noexcept {
        return std::this_thread::get_id() == thread_.get_id();
    }

    bool running() const noexcept { return thread_.joinable(); }

    /// @brief 起线程并跑 ioc.run()；on_thread_start 在 run() 前于 io 线程执行一次。
    /// @return false 表示已在运行（幂等 no-op）。
    template <typename OnThreadStart>
    bool start(OnThreadStart&& on_thread_start) {
        if (thread_.joinable()) return false;
        ioc_.restart();
        work_guard_ = std::make_unique<WorkGuard>(ioc_.get_executor());
        // 首个任务在**控制线程**上 post（起线程之前），保证它先于调用方 start() 返回后
        // 排入的任何后续 post——否则 spawn-thread 内 post 与控制线程 post 会竞争入队序。
        // task 先入队、run() 起来即以 FIFO 序消费（不变量测试锚：WsTradeTransport.HalfOpenConnection）。
        boost::asio::post(ioc_,
                          [cb = std::forward<OnThreadStart>(on_thread_start)]() mutable { cb(); });
        thread_ = std::thread([this] { ioc_.run(); });
        return true;
    }

    /// @brief 停 io 线程：先在 io 线程执行 on_stop_on_io（连接关闭/timer cancel 等
    ///        调用方 teardown），再 stop ioc + join + 释放 work_guard。
    ///
    /// 调用方自身在 io 线程时直接同步执行 teardown（不 post，避免自 join 死锁）。
    /// @param wait_for_teardown  true：控制线程阻塞到 io 线程跑完 teardown 才 `ioc.stop()`
    ///        （WS trade / UDS 语义）；false：post 后不等，直接 `ioc.stop()` + join
    ///        （MD 语义——teardown 与 stop 可竞争，端态一致）。
    template <typename OnStopOnIo>
    void stop(OnStopOnIo&& on_stop_on_io, bool wait_for_teardown = true) {
        if (!thread_.joinable()) return;
        if (on_io_thread()) {
            on_stop_on_io();
            ioc_.stop();
            return;  // 无法在自身线程 join；调用方处于 io 回调栈内，返回后 run() 自然退出。
        }
        if (wait_for_teardown) {
            auto done = std::make_shared<std::promise<void>>();
            auto fut = done->get_future();
            boost::asio::post(
                ioc_, [cb = std::forward<OnStopOnIo>(on_stop_on_io), done]() mutable {
                    cb();
                    done->set_value();
                });
            fut.wait();
        } else {
            boost::asio::post(
                ioc_, [cb = std::forward<OnStopOnIo>(on_stop_on_io)]() mutable { cb(); });
        }
        ioc_.stop();
        thread_.join();
        work_guard_.reset();
    }

private:
    boost::asio::io_context ioc_;
    std::unique_ptr<boost::asio::ssl::context> ssl_ctx_;
    std::unique_ptr<WorkGuard> work_guard_;
    std::thread thread_;
};

}  // namespace xt_live::gateway
