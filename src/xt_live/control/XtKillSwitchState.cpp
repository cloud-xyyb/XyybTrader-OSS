// ============================================================================
// XtKillSwitchState — 因果链状态实现
//
// 设计见 XtKillSwitchState.h 文件头。
// ============================================================================

#include "XtKillSwitchState.h"

#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace xt_live::control {

void XtKillSwitchState::update(bool active, std::string actor, std::string reason,
                                int64_t ts_unix_ns) {
    std::lock_guard<std::mutex> lk(mu_);
    active_         = active;
    last_actor_     = std::move(actor);
    last_reason_    = std::move(reason);
    since_unix_ns_  = ts_unix_ns;

    KillSwitchTripEvent ev;
    ev.active     = active_;
    ev.actor      = last_actor_;
    ev.reason     = last_reason_;
    ev.ts_unix_ns = ts_unix_ns;
    recent_trips_.push_back(std::move(ev));
    while (recent_trips_.size() > kMaxTrips) {
        recent_trips_.pop_front();
    }
}

KillSwitchSnapshot XtKillSwitchState::snapshot() const {
    std::lock_guard<std::mutex> lk(mu_);
    KillSwitchSnapshot s;
    s.active         = active_;
    s.last_actor     = last_actor_;
    s.last_reason    = last_reason_;
    s.since_unix_ns  = since_unix_ns_;
    s.scope          = scope_;
    s.recent_trips.assign(recent_trips_.begin(), recent_trips_.end());
    return s;
}

bool XtKillSwitchState::is_active() const {
    std::lock_guard<std::mutex> lk(mu_);
    return active_;
}

namespace {

std::string json_escape(const std::string& s) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(s.size() + 8);
    for (unsigned char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b";  break;
            case '\f': out += "\\f";  break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if (c < 0x20) {
                    out += "\\u00";
                    out += kHex[(c >> 4) & 0x0f];
                    out += kHex[c & 0x0f];
                } else {
                    out.push_back(static_cast<char>(c));
                }
                break;
        }
    }
    return out;
}

std::string utc_yyyymmdd(int64_t ts_unix_ns) {
    const std::time_t t = static_cast<std::time_t>(ts_unix_ns / 1'000'000'000LL);
    std::tm tm{};
    gmtime_r(&t, &tm);

    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d%02d%02d",
                  tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    return std::string(buf);
}

}

XtKillSwitchAuditLog::XtKillSwitchAuditLog(std::string dir)
    : dir_(std::move(dir)) {}

void XtKillSwitchAuditLog::append(const KillSwitchTripEvent& ev) {
    if (dir_.empty()) return;

    std::lock_guard<std::mutex> lk(mu_);

    std::error_code ec;
    std::filesystem::create_directories(dir_, ec);
    if (ec) return;

    const std::string fname = "kill_switch_" + utc_yyyymmdd(ev.ts_unix_ns) + ".jsonl";
    const std::string path  = dir_ + "/" + fname;

    std::ofstream ofs(path, std::ios::binary | std::ios::app);
    if (!ofs) return;

    std::ostringstream os;
    os << "{\"active\":"  << (ev.active ? "true" : "false")
       << ",\"actor\":\""  << json_escape(ev.actor)
       << "\",\"reason\":\"" << json_escape(ev.reason)
       << "\",\"ts_unix_ns\":" << ev.ts_unix_ns
       << "}\n";
    ofs << os.str();

}

}
