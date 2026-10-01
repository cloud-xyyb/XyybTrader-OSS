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

/// @brief BT run 独占 sid registry 装配（set_global → 逐策略预注册 → freeze）。
///
/// boot 期一次性建立 sid_id 映射；freeze 后运行期新增 sid 返回 kNoSidId，
/// 未声明策略不能绕过 per-sid 配置。HFT/CTA runner 共用此唯一实现。
/// @return 拥有权交还 caller，生命周期须覆盖整个 backtest run。
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

/// @brief 输出 `XT_PROGRESS|pct|stage|detail` stdout 行协议；pct 会 clamp 到 [0,100]。
inline void emit_progress(int pct, const std::string& stage, const std::string& detail) {
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    std::fprintf(stdout, "XT_PROGRESS|%d|%s|%s\n", pct, stage.c_str(), detail.c_str());
    std::fflush(stdout);
}

/// @brief Backtest runner 进程内全局 logger 的 RAII 守卫。
struct LoggerGuard {
    bool active = false;

    /// @brief 用 log 文件路径初始化 fast logger。
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

    /// @brief 用完整配置初始化 logger。
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

    /// @brief 若本 guard 成功初始化过 logger，则关闭它。
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

/// @brief 最小 JSON 字符串转义，用于 summary 中的已知文本字段。
///
/// @note 这是 writer helper，不是通用 JSON serializer。
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
    int64_t roll      = 0;  ///< roll 主力过滤丢弃。
    int64_t session   = 0;  ///< session 时段外丢弃。
    int64_t warmup    = 0;  ///< warmup 喂指标不计入交付。
    int64_t window    = 0;  ///< 交易日窗（start 侧）外丢弃。
    int64_t symbol    = 0;  ///< 合约白名单不命中丢弃。
    int64_t bad_row   = 0;  ///< 坏行（解析失败 / 必需字段缺失）丢弃。
    int64_t skip_last  = 0; ///< 每 symbol/day 去尾（skip_last_tick_per_day）丢弃。
    int64_t other     = 0;  ///< 未精确归类的兜差（如窗口截断剩余量）。

    int64_t sum() const {
        return roll + session + warmup + window + symbol + bad_row + skip_last + other;
    }
};

/// @brief engine 级（rows_total 计量位之后）丢弃计数；HFT-only 可选块。
///
/// HFT 的 roll/session/calendar 丢弃发生在 replayer 交付之后（engine 侧），
/// 故不并入 exclusions（会破 rows_loaded==rows_total+Σexclusions 守恒），另立本块。
struct PostDeliveryDrops {
    int64_t roll     = 0;  ///< engine roll should_dispatch 拒绝。
    int64_t session  = 0;  ///< engine session gate 拒绝（含 pre-session 非 preopen）。
    int64_t calendar = 0;  ///< engine calendar 节假日过滤拒绝。
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
    std::vector<std::string> files;   ///< 实际消费的源数据文件（解析后路径）。
    std::string tick_source;          ///< 输入分支名（csv/bin_stream/fbs/events_parquet/none）。
    size_t rows_total = 0;            ///< replay 实际交付的事件总数。
    bool cache_enabled = false;       ///< 是否允许 bin cache。
    bool cache_hit = false;           ///< 本轮是否命中既有 bin cache（enabled 且未重建）。
    std::string cache_path;           ///< bin cache 文件路径；未走 cache 为空串。
    int64_t clock_resolution_ns = 0;  ///< fill 交付量化的 clock grid 有效分辨率（ns）。

    // ── schema 2 载入→交付分解 ──────────────────────────────────────────
    int64_t rows_loaded = -1;         ///< 载入前总读入行数；-1 = 该引擎/来源不可得。
    std::string delivery_stage;       ///< rows_total 语义面：pre_engine/post_engine/rebalance。
    ConsumedExclusions exclusions;    ///< 载入→交付分类扣减。
    bool reconciled = false;          ///< true = rows_loaded==rows_total+Σexclusions 精确闭合。
    bool truncated_at_window_end = false;  ///< CTA bar 窗口尾截断（剩余量并入 other）。

    bool has_post_delivery_drops = false;  ///< true 时写 post_delivery_drops 块（HFT-only）。
    PostDeliveryDrops post_delivery_drops; ///< engine 级丢弃（rows_total 之后）。
};

/// @brief 把 consumed manifest 写成 report_dir 下的 JSON 文件（schema 2）。
///
/// 既有字段（tick_source/consumed_files/consumed_rows_total/bin_cache/
/// latency_delivery）名称与名义序不动（viewer schema 1 消费者兼容）；schema 2
/// 新字段一律追加在后。schema 1 旧产物无新字段 → viewer 分解面不渲染，零痕迹。
/// @return 打开或写入失败返回 false；caller 决定告警级别（观测面不升级停机）。
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

} // namespace xt_bt
