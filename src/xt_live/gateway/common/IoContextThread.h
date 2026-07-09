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

    bool on_io_thread() const noexcept {
        return std::this_thread::get_id() == thread_.get_id();
    }

    bool running() const noexcept { return thread_.joinable(); }

    template <typename OnThreadStart>
    bool start(OnThreadStart&& on_thread_start) {
        if (thread_.joinable()) return false;
        ioc_.restart();
        work_guard_ = std::make_unique<WorkGuard>(ioc_.get_executor());

        boost::asio::post(ioc_,
                          [cb = std::forward<OnThreadStart>(on_thread_start)]() mutable { cb(); });
        thread_ = std::thread([this] { ioc_.run(); });
        return true;
    }

    template <typename OnStopOnIo>
    void stop(OnStopOnIo&& on_stop_on_io, bool wait_for_teardown = true) {
        if (!thread_.joinable()) return;
        if (on_io_thread()) {
            on_stop_on_io();
            ioc_.stop();
            return;
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

}
