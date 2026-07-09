#pragma once

/// @file BtRunnerUtil.h
/// @brief Backtest runner 共享的进程级 glue helper。
///
/// 提供进度 stdout 协议、logger RAII 守卫、BT run 专用 frozen sid registry 装配，
/// 以及 summary writer 使用的最小 JSON 字符串转义。
/// 本文件保持 header-only，避免 CTA/HFT/SEL runner 为 CLI glue 引入额外链接依赖；
/// `xt_risk` 是各 runner 既有依赖，sid registry helper 不扩展链接边界。

#include "XtLogger.h"
#include "xt_identity/StrategySidRegistry.h"

#include <cstdint>
#include <cstdio>
#include <exception>
#include <memory>
#include <string>
#include <vector>

namespace xt_bt {

inline std::unique_ptr<xt_identity::StrategySidRegistry>
make_frozen_sid_registry(const std::vector<std::string>& strategy_ids) {
    auto reg = std::make_unique<xt_identity::StrategySidRegistry>();
    xt_identity::StrategySidRegistry::set_global(reg.get());
    for (const auto& id : strategy_ids) {
        const uint32_t sid_id = reg->register_or_lookup(id);
        XtLogger::info_m("BT", "sid_registry: '{}' -> sid_id={}", id, sid_id);
    }
    reg->freeze();
    return reg;
}

inline void emit_progress(int pct, const std::string& stage, const std::string& detail) {
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    std::fprintf(stdout, "XT_PROGRESS|%d|%s|%s\n", pct, stage.c_str(), detail.c_str());
    std::fflush(stdout);
}

/// @brief Backtest runner 进程内全局 logger 的 RAII 守卫。
struct LoggerGuard {
    bool active = false;

    bool init_fast(const std::string& log_file, std::string& err) {
        try {
            XtLogger::init_fast(log_file.c_str(), LogLevel::Info);
            active = true;
            return true;
        } catch (const std::exception& ex) {
            err = std::string("init logger failed: ") + ex.what();
            return false;
        } catch (...) {
            err = "init logger failed";
            return false;
        }
    }

    bool init(const XtLogger::Config& cfg, std::string& err) {
        try {
            XtLogger::init(cfg);
            active = true;
            return true;
        } catch (const std::exception& ex) {
            err = std::string("init logger failed: ") + ex.what();
            return false;
        } catch (...) {
            err = "init logger failed";
            return false;
        }
    }

    void shutdown() {
        if (!active) return;
        XtLogger::shutdown();
        active = false;
    }

    ~LoggerGuard() { shutdown(); }
    LoggerGuard() = default;
    LoggerGuard(const LoggerGuard&) = delete;
    LoggerGuard& operator=(const LoggerGuard&) = delete;
};

inline std::string json_escape(const std::string& in) {
    std::string out;
    out.reserve(in.size() + 16);
    for (const char c : in) {
        switch (c) {
        case '\"': out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\b': out += "\\b"; break;
        case '\f': out += "\\f"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default: out.push_back(c); break;
        }
    }
    return out;
}

}
