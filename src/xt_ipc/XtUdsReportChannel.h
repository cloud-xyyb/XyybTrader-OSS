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

#include "xt_ipc/XtMdWireContract.h"  // XT_MD_WIRE_VERSION 复用同一 IPC version 握手。

namespace xt_ipc {

/// @brief UDS report 固定帧，SEQPACKET 一帧承载一条。
///
/// 该结构是 IPC 通道路由用的最小 wire frame。字段含义由通道调用方约定，
/// 不能表达完整订单、成交、撤单或账户回报；扩展字段必须 bump `XT_MD_WIRE_VERSION`。
struct UdsReport {
    uint32_t strategy_id = 0;  ///< 目标/来源策略 id；注册帧用该字段声明 sid。
    uint32_t kind = 0;         ///< `kUdsKindRegister` 表示注册；其他值由调用方自定义。
    int64_t  order_id = 0;     ///< 注册帧携带 `XT_MD_WIRE_VERSION`；业务帧可用作关联 id。
    double   price = 0.0;      ///< 调用方自定义数值载荷；不是完整价格 schema。
    double   qty = 0.0;        ///< 调用方自定义数值载荷；不是完整数量 schema。
};
static_assert(std::is_trivially_copyable_v<UdsReport> && std::is_standard_layout_v<UdsReport>);
static_assert(sizeof(UdsReport) == 32,
              "UdsReport wire size 钉死（4+4+8+8+8）；任何字段变更 bump XT_MD_WIRE_VERSION");

/// @brief client 建连后的第一帧类型，用于注册 strategy_id 并校验 wire version。
inline constexpr uint32_t kUdsKindRegister = 0;

namespace detail {
/// @brief 填充 AF_UNIX 地址；路径超过 sun_path 容量时返回 false。
inline bool fill_un(sockaddr_un& a, const std::string& path) {
    if (path.size() + 1 > sizeof(a.sun_path)) return false;
    std::memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    std::memcpy(a.sun_path, path.c_str(), path.size());
    return true;
}
}  // namespace detail

/// @brief UDS report server，负责接收 client 注册并按 strategy_id 路由固定帧。
///
/// `start()` 绑定并拥有 socket path，后台 accept 线程只接受第一帧为 register 且
/// version 匹配的连接。同一个 sid 重连会替换旧 fd。发送接口在内部持锁访问 sid 表，
/// 但使用 `MSG_DONTWAIT`，不会因单个慢 client 长时间阻塞全通道。
class UdsReportServer {
public:
    ~UdsReportServer() { stop(); }

    /// @brief bind + listen 并启动 accept 线程；系统调用失败时 errno 保留底层原因。
    bool start(const std::string& path) {
        path_ = path;
        ::unlink(path_.c_str());  // 清残留（owner）
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

    /// @brief 向已注册 sid 点对点发送一帧 report。
    /// @return true 表示完整写入；false 表示 sid 未注册、peer 背压或连接已失效。
    bool send_to(uint32_t sid, const UdsReport& r) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = sid_fd_.find(sid);
        if (it == sid_fd_.end()) return false;
        // MSG_DONTWAIT：绝不持锁阻塞（慢 peer 不该 HOL 拖垮全通道）。EAGAIN=背压保留 fd；
        // EPIPE/ECONNRESET/EBADF=对端死，清理 fd（防运行期泄漏）。
        ssize_t n = ::send(it->second, &r, sizeof(r), MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n == static_cast<ssize_t>(sizeof(r))) return true;
        if (n < 0 && (errno == EPIPE || errno == ECONNRESET || errno == EBADF)) {
            ::close(it->second);
            sid_fd_.erase(it);
        }
        return false;
    }

    /// @brief 将同一帧发送给所有已注册 client。
    /// @return 成功完整写入的 client 数；已断开的 fd 会在本次调用中清理。
    int broadcast(const UdsReport& r) {
        std::lock_guard<std::mutex> lk(mu_);
        int n = 0;
        std::vector<uint32_t> dead;
        for (auto& [sid, fd] : sid_fd_) {
            ssize_t s = ::send(fd, &r, sizeof(r), MSG_NOSIGNAL | MSG_DONTWAIT);
            if (s == static_cast<ssize_t>(sizeof(r))) ++n;
            else if (s < 0 && (errno == EPIPE || errno == ECONNRESET || errno == EBADF)) dead.push_back(sid);
        }
        for (uint32_t sid : dead) {  // 出循环再删，避免迭代器失效
            auto it = sid_fd_.find(sid);
            if (it != sid_fd_.end()) { ::close(it->second); sid_fd_.erase(it); }
        }
        return n;
    }

    /// @brief 返回当前已注册 client 数。
    size_t client_count() {
        std::lock_guard<std::mutex> lk(mu_);
        return sid_fd_.size();
    }

    /// @brief 若已启动，停止 accept 线程、关闭所有 client fd，并删除本 server 拥有的 socket path。
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
    /// @brief accept 注册连接；注册帧超时、类型错误或 version 不匹配时 fail-closed。
    void accept_loop_() {
        while (running_.load(std::memory_order_acquire)) {
            int cfd = ::accept(listen_fd_, nullptr, nullptr);
            if (cfd < 0) break;  // listen_fd 被 stop() 关 → 退出
            // 注册帧读超时：不发帧的 client 不能永久卡死 accept 线程（否则 stop() 的 join 挂死 +
            // head-of-line 阻塞后续 accept）。2s 后放弃该 cfd 继续。
            timeval tv{2, 0};
            ::setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            UdsReport reg{};
            ssize_t n = ::recv(cfd, &reg, sizeof(reg), 0);  // 第一帧 = 注册
            // fail-closed：大小/类型/version 任一不符即拒（reg.order_id 携带 version 握手，防 ABI 漂移）。
            if (n != static_cast<ssize_t>(sizeof(reg)) || reg.kind != kUdsKindRegister ||
                reg.order_id != static_cast<int64_t>(XT_MD_WIRE_VERSION)) {
                ::close(cfd);
                continue;
            }
            std::lock_guard<std::mutex> lk(mu_);
            auto old = sid_fd_.find(reg.strategy_id);
            if (old != sid_fd_.end()) ::close(old->second);  // 同 sid 重连，替换
            sid_fd_[reg.strategy_id] = cfd;
        }
    }

    std::string path_;                 ///< server 拥有的 UDS filesystem path。
    int listen_fd_ = -1;               ///< listening socket fd；-1 表示未启动或已停止。
    std::atomic<bool> running_{false}; ///< accept loop 停止标志。
    std::thread accept_thr_;           ///< accept 线程，由 stop()/析构 join。
    std::mutex mu_;                    ///< 保护 sid_fd_ 以及发送路径的死连接清理。
    std::unordered_map<uint32_t, int> sid_fd_;  ///< 已注册 strategy_id -> client fd。
};

/// @brief UDS report client，连接 server、注册 sid，并接收路由给本策略的 report frame。
class UdsReportClient {
public:
    ~UdsReportClient() { close(); }

    /// @brief connect 后发送 register frame；失败返回 false，调用方可丢弃对象或调用 close()。
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
        reg.order_id = static_cast<int64_t>(XT_MD_WIRE_VERSION);  // version 握手
        return ::send(fd_, &reg, sizeof(reg), MSG_NOSIGNAL) == static_cast<ssize_t>(sizeof(reg));
    }

    /// @brief 接收一条完整 report frame。
    /// @param timeout_ms 小于 0 表示阻塞等待；非负值设置本 socket 的 recv timeout。
    /// @return true 表示读到完整帧；false 表示超时、错误或对端关闭。
    bool recv(UdsReport& out, int timeout_ms = -1) {
        if (fd_ < 0) return false;
        if (timeout_ms >= 0) {
            timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
            ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        }
        return ::recv(fd_, &out, sizeof(out), 0) == static_cast<ssize_t>(sizeof(out));
    }

    /// @brief 关闭当前 socket；可重复调用。
    void close() {
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    }

private:
    int fd_ = -1;       ///< 已连接 socket fd；-1 表示未连接或已关闭。
    uint32_t sid_ = 0;  ///< connect_register() 记录的 sid，仅作为本地状态。
};

}  // namespace xt_ipc
