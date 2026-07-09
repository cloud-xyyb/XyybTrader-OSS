#pragma once

/// @file
/// @brief UDS 私有回报通道原语：按 strategy_id 将定长 report frame 点对点送回策略进程。
///
/// frame 只覆盖注册和最小路由载荷，不承载真实订单、成交、账户或风控协议。
/// server 侧自带 accept 线程；发送路径使用非阻塞 send，慢 peer 通过返回值表达背压。

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "xt_ipc/XtMdWireContract.h"

namespace xt_ipc {

/// @brief UDS report 固定帧，SEQPACKET 一帧承载一条。
///
/// 该结构是 IPC 通道路由用的最小 wire frame。字段含义由通道调用方约定，
/// 不能表达完整订单、成交、撤单或账户回报；扩展字段必须 bump `XT_MD_WIRE_VERSION`。
struct UdsReport {
    uint32_t strategy_id = 0;
    uint32_t kind = 0;
    int64_t  order_id = 0;
    double   price = 0.0;
    double   qty = 0.0;
};
static_assert(std::is_trivially_copyable_v<UdsReport> && std::is_standard_layout_v<UdsReport>);
static_assert(sizeof(UdsReport) == 32,
              "UdsReport wire size 钉死（4+4+8+8+8）；任何字段变更 bump XT_MD_WIRE_VERSION");

inline constexpr uint32_t kUdsKindRegister = 0;

namespace detail {

inline bool fill_un(sockaddr_un& a, const std::string& path) {
    if (path.size() + 1 > sizeof(a.sun_path)) return false;
    std::memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    std::memcpy(a.sun_path, path.c_str(), path.size());
    return true;
}
}

/// @brief UDS report server，负责接收 client 注册并按 strategy_id 路由固定帧。
///
/// `start()` 绑定并拥有 socket path，后台 accept 线程只接受第一帧为 register 且
/// version 匹配的连接。同一个 sid 重连会替换旧 fd。发送接口在内部持锁访问 sid 表，
/// 但使用 `MSG_DONTWAIT`，不会因单个慢 client 长时间阻塞全通道。
class UdsReportServer {
public:
    ~UdsReportServer() { stop(); }

    bool start(const std::string& path) {
        path_ = path;
        ::unlink(path_.c_str());
        listen_fd_ = ::socket(AF_UNIX, SOCK_SEQPACKET, 0);
        if (listen_fd_ < 0) return false;
        sockaddr_un addr{};
        if (!detail::fill_un(addr, path_)) return false;
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
        if (::listen(listen_fd_, 16) != 0) return false;
        running_.store(true, std::memory_order_release);
        accept_thr_ = std::thread([this] { accept_loop_(); });
        return true;
    }

    bool send_to(uint32_t sid, const UdsReport& r) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = sid_fd_.find(sid);
        if (it == sid_fd_.end()) return false;

        ssize_t n = ::send(it->second, &r, sizeof(r), MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n == static_cast<ssize_t>(sizeof(r))) return true;
        if (n < 0 && (errno == EPIPE || errno == ECONNRESET || errno == EBADF)) {
            ::close(it->second);
            sid_fd_.erase(it);
        }
        return false;
    }

    int broadcast(const UdsReport& r) {
        std::lock_guard<std::mutex> lk(mu_);
        int n = 0;
        std::vector<uint32_t> dead;
        for (auto& [sid, fd] : sid_fd_) {
            ssize_t s = ::send(fd, &r, sizeof(r), MSG_NOSIGNAL | MSG_DONTWAIT);
            if (s == static_cast<ssize_t>(sizeof(r))) ++n;
            else if (s < 0 && (errno == EPIPE || errno == ECONNRESET || errno == EBADF)) dead.push_back(sid);
        }
        for (uint32_t sid : dead) {
            auto it = sid_fd_.find(sid);
            if (it != sid_fd_.end()) { ::close(it->second); sid_fd_.erase(it); }
        }
        return n;
    }

    size_t client_count() {
        std::lock_guard<std::mutex> lk(mu_);
        return sid_fd_.size();
    }

    void stop() {
        if (!running_.exchange(false)) return;
        if (listen_fd_ >= 0) { ::shutdown(listen_fd_, SHUT_RDWR); ::close(listen_fd_); listen_fd_ = -1; }
        if (accept_thr_.joinable()) accept_thr_.join();
        std::lock_guard<std::mutex> lk(mu_);
        for (auto& [sid, fd] : sid_fd_) ::close(fd);
        sid_fd_.clear();
        if (!path_.empty()) ::unlink(path_.c_str());
    }

private:

    void accept_loop_() {
        while (running_.load(std::memory_order_acquire)) {
            int cfd = ::accept(listen_fd_, nullptr, nullptr);
            if (cfd < 0) break;

            timeval tv{2, 0};
            ::setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            UdsReport reg{};
            ssize_t n = ::recv(cfd, &reg, sizeof(reg), 0);

            if (n != static_cast<ssize_t>(sizeof(reg)) || reg.kind != kUdsKindRegister ||
                reg.order_id != static_cast<int64_t>(XT_MD_WIRE_VERSION)) {
                ::close(cfd);
                continue;
            }
            std::lock_guard<std::mutex> lk(mu_);
            auto old = sid_fd_.find(reg.strategy_id);
            if (old != sid_fd_.end()) ::close(old->second);
            sid_fd_[reg.strategy_id] = cfd;
        }
    }

    std::string path_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread accept_thr_;
    std::mutex mu_;
    std::unordered_map<uint32_t, int> sid_fd_;
};

/// @brief UDS report client，连接 server、注册 sid，并接收路由给本策略的 report frame。
class UdsReportClient {
public:
    ~UdsReportClient() { close(); }

    bool connect_register(const std::string& path, uint32_t sid) {
        sid_ = sid;
        fd_ = ::socket(AF_UNIX, SOCK_SEQPACKET, 0);
        if (fd_ < 0) return false;
        sockaddr_un addr{};
        if (!detail::fill_un(addr, path)) return false;
        if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
        UdsReport reg{};
        reg.strategy_id = sid;
        reg.kind = kUdsKindRegister;
        reg.order_id = static_cast<int64_t>(XT_MD_WIRE_VERSION);
        return ::send(fd_, &reg, sizeof(reg), MSG_NOSIGNAL) == static_cast<ssize_t>(sizeof(reg));
    }

    bool recv(UdsReport& out, int timeout_ms = -1) {
        if (fd_ < 0) return false;
        if (timeout_ms >= 0) {
            timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
            ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        }
        return ::recv(fd_, &out, sizeof(out), 0) == static_cast<ssize_t>(sizeof(out));
    }

    void close() {
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    }

private:
    int fd_ = -1;
    uint32_t sid_ = 0;
};

}
