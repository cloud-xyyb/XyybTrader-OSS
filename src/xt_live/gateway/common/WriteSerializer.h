#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <boost/system/error_code.hpp>

namespace xt_live::gateway {

/// @brief WebSocket 写入侧的单写者排队器
///
/// - boost::beast 的 async_write 不允许对同一 stream 并发写；
///   WriteSerializer 把要发的 payload 排进 FIFO 队列，一次放行一个。
/// - 类自身不加锁。
class WriteSerializer {
public:
    using Callback = std::function<void()>;

    struct Pending {
        std::shared_ptr<std::string> data;
        Callback on_success;
    };

    void enqueue(std::shared_ptr<std::string> data, Callback on_success) {
        queue_.push_back(Pending{std::move(data), std::move(on_success)});
    }

    std::optional<Pending> begin_next() {
        if (in_flight_) return std::nullopt;
        if (queue_.empty()) return std::nullopt;
        in_flight_ = true;
        return queue_.front();
    }

    void on_done(const boost::system::error_code& ec) {
        if (!in_flight_) return;
        in_flight_ = false;
        if (queue_.empty()) return;
        Pending entry = std::move(queue_.front());
        queue_.pop_front();
        if (!ec && entry.on_success) {
            entry.on_success();
        }
    }

    void clear() {
        queue_.clear();
    }

    size_t queue_depth() const { return queue_.size(); }
    bool in_flight() const { return in_flight_; }

private:
    std::deque<Pending> queue_;
    bool in_flight_ = false;
};

}
