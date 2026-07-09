#include "xt_util/runtime/XtProcessUtil.h"

#include <csignal>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include "xt_util/runtime/ThreadUtil.h"

namespace {

std::atomic<bool>* g_shutdown_ptr = nullptr;
std::atomic<int>* g_shutdown_sig_ptr = nullptr;

void on_signal(int sig) {
    if (g_shutdown_sig_ptr) {
        g_shutdown_sig_ptr->store(sig, std::memory_order_relaxed);
    }
    if (g_shutdown_ptr) {
        g_shutdown_ptr->store(true, std::memory_order_release);
    }
}

}

void XtProcessUtil::install_shutdown_signal_handlers(std::atomic<bool>& shutdown_flag,
                                                     std::atomic<int>& shutdown_sig) {
    g_shutdown_ptr = &shutdown_flag;
    g_shutdown_sig_ptr = &shutdown_sig;
#if defined(__linux__) || defined(__APPLE__)
    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);

    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT,  &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGQUIT, &sa, nullptr);

    sigaction(SIGHUP,  &sa, nullptr);
#else
    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);
#  ifdef SIGQUIT
    std::signal(SIGQUIT, on_signal);
#  endif
#endif
}

XtProcessUtil::RuntimeTuningResult
XtProcessUtil::apply_runtime_tuning(const XtProcessUtil::RuntimeTuningConfig& cfg) {
    if (cfg.main_affinity >= 0) {
        ThreadUtil::set_current_thread_affinity(cfg.main_affinity);
        ThreadUtil::bind_current_thread_numa(cfg.main_affinity);
    }

    if (!cfg.main_priority.empty()) {
        ThreadUtil::set_current_thread_priority(cfg.main_priority);
    }

    RuntimeTuningResult result{};
    result.mlockall_requested = cfg.enable_mlockall;
    if (cfg.enable_mlockall) {
        result.mlockall_succeeded = ThreadUtil::mlock_process_memory();
    }
    return result;
}

XtProcessUtil::SingleInstanceLock::~SingleInstanceLock() {
    release();
}

bool XtProcessUtil::SingleInstanceLock::acquire(const std::string& cfg_path,
                                                std::string* holder_pid) {
    release();
    const std::string lock_path = cfg_path + ".lock";
    int fd = ::open(lock_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) return false;

    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        if (holder_pid) {
            char buf[64] = {0};
            ::lseek(fd, 0, SEEK_SET);
            const ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
            if (n > 0) {
                buf[n] = '\0';
                *holder_pid = std::string(buf);
            }
        }
        ::close(fd);
        return false;
    }

    const int truncate_rc = ::ftruncate(fd, 0);
    (void)truncate_rc;
    const std::string pid = std::to_string(::getpid());
    const ssize_t written = ::write(fd, pid.c_str(), pid.size());
    (void)written;
    fd_ = fd;
    return true;
}

void XtProcessUtil::SingleInstanceLock::release() {
    if (fd_ < 0) return;
    ::flock(fd_, LOCK_UN);
    ::close(fd_);
    fd_ = -1;
}
