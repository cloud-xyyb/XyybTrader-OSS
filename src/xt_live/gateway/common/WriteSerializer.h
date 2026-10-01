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
        Callback on_success;  ///< 仅 async_write 无错误完成才 fire
    };

    /// @brief 入队待写 payload (caller 在 strand 内调)
    void enqueue(std::shared_ptr<std::string> data, Callback on_success) {
        queue_.push_back(Pending{std::move(data), std::move(on_success)});
    }

    /// @brief 取下一 pending 标记 in-flight
    /// @return Pending entry (data + cb) 喂入 stream.async_write,
    ///         若已 in-flight 或队空 return nullopt
    ///
    /// 调用方拿到 entry 后:
    ///   1. 用 entry->data 喂 stream.async_write
    ///   2. async_write completion 内调 on_done(ec) 推进 serializer
    std::optional<Pending> begin_next() {
        if (in_flight_) return std::nullopt;
        if (queue_.empty()) return std::nullopt;
        in_flight_ = true;
        return queue_.front();
    }

    /// @brief async_write completion 调用; 推进 serializer 到下一态
    ///
    /// ec ok: fire pending.on_success → pop queue
    /// ec err: 不 fire cb (caller 走 timeout 兜底), 仍 pop queue
    /// 重复调用防御: in_flight_=false 后再 on_done() no-op
    void on_done(const boost::system::error_code& ec) {
        if (!in_flight_) return;  // 防御重复 on_done 调用
        in_flight_ = false;
        if (queue_.empty()) return;
        Pending entry = std::move(queue_.front());
        queue_.pop_front();
        if (!ec && entry.on_success) {
            entry.on_success();
        }
    }

    /// @brief 断连时清队列 (pending cb 不调)
    ///
    /// in_flight_ 不重置: 现有 in-flight async_write 的 completion handler
    /// 仍会 fire (ec=eof/aborted) 自然走 on_done 清状态
    void clear() {
        queue_.clear();
    }

    size_t queue_depth() const { return queue_.size(); }
    bool in_flight() const { return in_flight_; }

private:
    std::deque<Pending> queue_;
    bool in_flight_ = false;
};

}  // namespace xt_live::gateway
