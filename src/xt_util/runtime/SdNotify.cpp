#include "xt_util/runtime/SdNotify.h"

#include <cstdlib>
#include <cstring>

#ifdef _WIN32
// Windows 无 systemd/unix socket，保留相同 API 的 no-op stub。
namespace xt_util {
bool sd_notify(const char*) noexcept { return false; }
bool sd_notify_ready() noexcept { return false; }
bool sd_notify_watchdog() noexcept { return false; }
bool sd_notify_stopping() noexcept { return false; }
}  // namespace xt_util
#else

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace xt_util {

bool sd_notify(const char* state) noexcept {
    if (!state || !state[0]) return false;
    const char* socket_path = std::getenv("NOTIFY_SOCKET");
    if (!socket_path || !socket_path[0]) return false;

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    const std::size_t path_len = std::strlen(socket_path);
    if (path_len >= sizeof(addr.sun_path)) return false;

    // systemd abstract namespace socket 用 '@' 表示首字节 NUL。
    if (socket_path[0] == '@') {
        addr.sun_path[0] = '\0';
        std::memcpy(addr.sun_path + 1, socket_path + 1, path_len - 1);
    } else {
        std::memcpy(addr.sun_path, socket_path, path_len);
    }

    int fd = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return false;

    const ssize_t n = ::sendto(fd, state, std::strlen(state), MSG_NOSIGNAL,
                                reinterpret_cast<sockaddr*>(&addr),
                                sizeof(addr.sun_family) + path_len);
    ::close(fd);
    return n >= 0;
}

bool sd_notify_ready() noexcept {
    return sd_notify("READY=1\n");
}

bool sd_notify_watchdog() noexcept {
    return sd_notify("WATCHDOG=1\n");
}

bool sd_notify_stopping() noexcept {
    return sd_notify("STOPPING=1\n");
}

}  // namespace xt_util

#endif  // _WIN32
