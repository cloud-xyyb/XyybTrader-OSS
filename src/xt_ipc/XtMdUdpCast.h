#pragma once

/// @file
/// @brief MD 行情的 UDP 组播变体（跨主机 fan-out）。
///
/// 同机 fan-out 走共享内存 ring；跨主机广播时 UDP 组播一发多收。
/// UDP 不可靠且可能乱序，每包携带单调 seq，receiver 只负责检测
/// gap/reorder，不负责补包。
///
/// 本头只提供轻量 framing + gap 统计；不提供 A/B feed、快照或 TCP repair。

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include "xt_core/market/XtRawTick.h"
#include "xt_ipc/XtMdWireContract.h"

namespace xt_ipc {

/// @brief UDP 组播 MD 包（定长 POD，一包一 tick + 单调 seq 供 gap 检测）。
struct UdpMdPacket {
    uint64_t magic;
    uint32_t wire_version;
    uint32_t _pad;
    uint64_t seq;
    XtRawTick tick;
};
static_assert(std::is_trivially_copyable_v<UdpMdPacket> && std::is_standard_layout_v<UdpMdPacket>);
static_assert(sizeof(UdpMdPacket) == 536,
              "UdpMdPacket wire size 钉死（8+4+4+8+512）；任何 framing/element 变更 bump XT_MD_WIRE_VERSION");

/// @brief UDP 组播发送端。
class UdpMdSender {
public:
    ~UdpMdSender() { if (fd_ >= 0) ::close(fd_); }

    bool open(const std::string& group_ip, uint16_t port, int ttl = 1, bool loop = true) {
        fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0) return false;
        unsigned char ttl_v = static_cast<unsigned char>(ttl);
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl_v, sizeof(ttl_v));
        unsigned char loop_v = loop ? 1 : 0;
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop_v, sizeof(loop_v));
        std::memset(&dst_, 0, sizeof(dst_));
        dst_.sin_family = AF_INET;
        dst_.sin_port = htons(port);
        if (::inet_pton(AF_INET, group_ip.c_str(), &dst_.sin_addr) != 1) return false;
        return true;
    }

    bool send(uint64_t seq, const XtRawTick& tick) {
        UdpMdPacket p{};
        p.magic = kMdRingMagic;
        p.wire_version = XT_MD_WIRE_VERSION;
        p.seq = seq;
        p.tick = tick;
        return ::sendto(fd_, &p, sizeof(p), 0, reinterpret_cast<sockaddr*>(&dst_), sizeof(dst_)) ==
               static_cast<ssize_t>(sizeof(p));
    }

private:
    int fd_ = -1;
    sockaddr_in dst_{};
};

/// @brief UDP 组播接收端 + seq gap/reorder 检测。
class UdpMdReceiver {
public:
    ~UdpMdReceiver() { if (fd_ >= 0) ::close(fd_); }

    bool open(const std::string& group_ip, uint16_t port) {
        fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0) return false;
        int reuse = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
        ip_mreq mreq{};
        if (::inet_pton(AF_INET, group_ip.c_str(), &mreq.imr_multiaddr) != 1) return false;
        mreq.imr_interface.s_addr = htonl(INADDR_ANY);
        if (::setsockopt(fd_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) != 0) return false;
        return true;
    }

    bool recv(XtRawTick& out, uint64_t& out_seq, int timeout_ms = -1) {
        if (fd_ < 0) return false;
        if (timeout_ms >= 0) {
            timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
            ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        }
        UdpMdPacket p{};
        ssize_t n = ::recv(fd_, &p, sizeof(p), 0);
        if (n != static_cast<ssize_t>(sizeof(p))) return false;
        if (p.magic != kMdRingMagic || p.wire_version != XT_MD_WIRE_VERSION) {
            ++bad_packet_count_;
            return false;
        }

        if (have_last_) {
            if (p.seq > last_seq_ + 1) gap_count_ += (p.seq - last_seq_ - 1);
            else if (p.seq <= last_seq_) ++reorder_count_;
        }
        last_seq_ = p.seq;
        have_last_ = true;
        out = p.tick;
        out_seq = p.seq;
        return true;
    }

    uint64_t gap_count() const { return gap_count_; }
    uint64_t reorder_count() const { return reorder_count_; }
    uint64_t bad_packet_count() const { return bad_packet_count_; }

private:
    int fd_ = -1;
    bool have_last_ = false;
    uint64_t last_seq_ = 0;
    uint64_t gap_count_ = 0;
    uint64_t reorder_count_ = 0;
    uint64_t bad_packet_count_ = 0;
};

}
