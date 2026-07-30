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
#include <fstream>
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

/// @brief 载入→交付之间被丢弃的行数按原因分解（exclusion breakdown）。
///
/// 语义 = 「rows_loaded 计量位之后、rows_delivered 计量位之前」被丢弃的行数，
/// 按原因分桶。缺类别写 0；精确守恒做不到的引擎用 `other` 兜差。字段单位与
/// rows_loaded/rows_total 一致（同为「行/事件」计数）。
struct ConsumedExclusions {
    int64_t roll      = 0;
    int64_t session   = 0;
    int64_t warmup    = 0;
    int64_t window    = 0;
    int64_t symbol    = 0;
    int64_t bad_row   = 0;
    int64_t skip_last  = 0;
    int64_t other     = 0;

    int64_t sum() const {
        return roll + session + warmup + window + symbol + bad_row + skip_last + other;
    }
};

/// @brief engine 级（rows_total 计量位之后）丢弃计数；HFT-only 可选块。
///
/// HFT 的 roll/session/calendar 丢弃发生在 replayer 交付之后（engine 侧），
/// 故不并入 exclusions（会破 rows_loaded==rows_total+Σexclusions 守恒），另立本块。
struct PostDeliveryDrops {
    int64_t roll     = 0;
    int64_t session  = 0;
    int64_t calendar = 0;
};

/// @brief 一次 backtest run 实际消费输入的落盘载荷（consumed manifest）。
///
/// 与 config 引用全集区分：`files` 是 load() 解析后真实喂给加载层的文件集，
/// `rows_total` 是 replay 实际交付的事件数。per-file 行数在 bin cache 命中
/// 路径不可归因（合并 bin 无文件边界），故刻意不设该字段。
///
/// schema 2 新增载入→交付分解：`rows_loaded`（载入前总读入，-1=不可得）、
/// `delivery_stage`（rows_total 的语义面）、`exclusions`（分类扣减）、
/// `reconciled`（守恒式是否精确闭合）。守恒目标：
/// `rows_loaded == rows_total + exclusions.sum()`（reconciled=true 时）。
struct ConsumedManifest {
    std::vector<std::string> files;
    std::string tick_source;
    size_t rows_total = 0;
    bool cache_enabled = false;
    bool cache_hit = false;
    std::string cache_path;
    int64_t clock_resolution_ns = 0;

    int64_t rows_loaded = -1;
    std::string delivery_stage;
    ConsumedExclusions exclusions;
    bool reconciled = false;
    bool truncated_at_window_end = false;

    bool has_post_delivery_drops = false;
    PostDeliveryDrops post_delivery_drops;
};

inline bool write_consumed_manifest(const std::string& path, const ConsumedManifest& m) {
    std::ofstream ofs(path);
    if (!ofs) return false;
    ofs << "{\n"
        << "  \"schema\": 2,\n"
        << "  \"tick_source\": \"" << json_escape(m.tick_source) << "\",\n"
        << "  \"consumed_files\": [";
    for (size_t i = 0; i < m.files.size(); ++i) {
        ofs << (i ? ",\n    " : "\n    ")
            << "{\"path\": \"" << json_escape(m.files[i]) << "\"}";
    }
    ofs << (m.files.empty() ? "" : "\n  ") << "],\n"
        << "  \"consumed_rows_total\": " << m.rows_total << ",\n"
        << "  \"bin_cache\": {\"enabled\": " << (m.cache_enabled ? "true" : "false")
        << ", \"hit\": " << (m.cache_hit ? "true" : "false")
        << ", \"path\": \"" << json_escape(m.cache_path) << "\"},\n"
        << "  \"latency_delivery\": {\"clock_resolution_ns\": " << m.clock_resolution_ns << "},\n"
        << "  \"rows_loaded\": " << m.rows_loaded << ",\n"
        << "  \"delivery_stage\": \"" << json_escape(m.delivery_stage) << "\",\n"
        << "  \"reconciled\": " << (m.reconciled ? "true" : "false") << ",\n"
        << "  \"truncated_at_window_end\": " << (m.truncated_at_window_end ? "true" : "false")
        << ",\n"
        << "  \"exclusions\": {"
        << "\"roll\": "     << m.exclusions.roll     << ", "
        << "\"session\": "  << m.exclusions.session  << ", "
        << "\"warmup\": "   << m.exclusions.warmup   << ", "
        << "\"window\": "   << m.exclusions.window   << ", "
        << "\"symbol\": "   << m.exclusions.symbol   << ", "
        << "\"bad_row\": "  << m.exclusions.bad_row  << ", "
        << "\"skip_last\": " << m.exclusions.skip_last << ", "
        << "\"other\": "    << m.exclusions.other    << "}";
    if (m.has_post_delivery_drops) {
        ofs << ",\n"
            << "  \"post_delivery_drops\": {"
            << "\"roll\": "     << m.post_delivery_drops.roll     << ", "
            << "\"session\": "  << m.post_delivery_drops.session  << ", "
            << "\"calendar\": " << m.post_delivery_drops.calendar << "}";
    }
    ofs << "\n}\n";
    return ofs.good();
}

}
