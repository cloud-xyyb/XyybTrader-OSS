#include "XtDatalakeRoot.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

#include "XtLogger.h"

namespace xt_data {
namespace datalake {

namespace {

// 空环境变量按未设置处理，避免把 root 解析到当前工作目录。
std::string env_or(const char* name, const std::string& fallback) {
    const char* val = std::getenv(name);
    if (val && val[0]) {
        return std::string(val);
    }
    return fallback;
}

// ts_code 只清理 ingest 侧偶发空白；大小写和交易所后缀属于协议字段，不在 reader 改写。
std::string normalize_ts_code(const std::string& code) {
    std::string out;
    out.reserve(code.size());
    for (char c : code) {
        if (c != ' ' && c != '\t' && c != '\n') {
            out.push_back(c);
        }
    }
    return out;
}

// schema 不符上报：Strict → throw；否则 warn。调用方保留自己的 early-return /
// continue / all_ok 控制流，本 helper 只收敛「构造 message + throw-or-warn」。
void report_schema_violation(SchemaCompatPolicy policy, const std::string& msg) {
    if (policy == SchemaCompatPolicy::Strict) {
        throw std::runtime_error(msg);
    }
    XtLogger::warn_m("DataLake", "{}", msg);
}

}  // namespace

void XtDatalakeRoot::set_root(std::string root) {
    if (root.empty()) {
        // 空 root 是显式 fallback 信号：仅取 XT_DATALAKE_ROOT；仍为空则保持空，
        // 由 resolve_path/validate_schema 的 "root not set" fail-fast 报清晰错误。
        root = env_or("XT_DATALAKE_ROOT", std::string());
    }
    root_ = std::move(root);
    XtLogger::info_m("DataLake", "set_root: '{}'", root_);
}

bool XtDatalakeRoot::validate_schema(const std::vector<TableSchemaExpect>& expects) {
    if (root_.empty()) {
        report_schema_violation(policy_, "XtDatalakeRoot::validate_schema: root not set");
        return false;
    }

    namespace fs = std::filesystem;
    const fs::path meta_path = fs::path(root_) / "_meta" / "schema_version.json";
    if (!fs::exists(meta_path)) {
        report_schema_violation(policy_, "_meta/schema_version.json not found at " +
            meta_path.string() + " (cra ingest 未写入或 root path 错误)");
        return false;
    }

    std::ifstream ifs(meta_path);
    if (!ifs) {
        report_schema_violation(policy_, "Cannot open " + meta_path.string());
        return false;
    }

    std::stringstream ss;
    ss << ifs.rdbuf();
    schema_json_raw_ = ss.str();

    nlohmann::json doc;
    try {
        doc = nlohmann::json::parse(schema_json_raw_);
    } catch (const std::exception& e) {
        report_schema_violation(policy_,
            std::string("schema_version.json parse error: ") + e.what());
        return false;
    }

    bool all_ok = true;
    const auto& tables = doc.value("tables", nlohmann::json::object());
    for (const auto& expect : expects) {
        const auto it = tables.find(expect.table_name);
        if (it == tables.end()) {
            report_schema_violation(policy_, "Table '" + expect.table_name +
                "' not registered in schema_version.json");
            all_ok = false;
            continue;
        }
        const int actual_ver = it->value("schema_version", -1);
        if (actual_ver != expect.schema_version) {
            report_schema_violation(policy_, "Table '" + expect.table_name +
                "' schema_version mismatch: expected=" + std::to_string(expect.schema_version) +
                " actual=" + std::to_string(actual_ver));
            all_ok = false;
            continue;
        }
        // 只校验调用方必需字段；ingest 追加新列不应破坏既有 reader。
        const auto& cols = it->value("columns", nlohmann::json::array());
        for (const auto& req : expect.required_columns) {
            bool found = false;
            for (const auto& c : cols) {
                if (c.is_string() && c.get<std::string>() == req) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                report_schema_violation(policy_, "Table '" + expect.table_name +
                    "' missing required column '" + req + "'");
                all_ok = false;
            }
        }
    }
    if (all_ok) {
        XtLogger::info_m("DataLake", "validate_schema PASS for {} tables", expects.size());
    }
    return all_ok;
}

std::string XtDatalakeRoot::resolve_path(TableKind kind,
                                           const std::vector<std::string>& keys) const {
    namespace fs = std::filesystem;
    if (root_.empty()) {
        throw std::runtime_error("XtDatalakeRoot::resolve_path: root not set");
    }
    fs::path p = fs::path(root_) / "ashare";

    // keys 的解释由 TableKind 决定；不足时 fail-fast，避免返回可读但错误的路径。
    switch (kind) {
        case TableKind::Calendar:
            p /= "calendar.parquet";
            break;
        case TableKind::StockBasic:
            p /= "stock_basic.parquet";
            break;
        case TableKind::Industry:
            if (keys.empty()) throw std::runtime_error("Industry: keys[0] = classifier required");
            p /= "industry";
            p /= (keys[0] + ".parquet");
            break;
        case TableKind::DailyBasic:
            if (keys.size() < 2) throw std::runtime_error("DailyBasic: keys = [YYYY, YYYY-MM]");
            p /= "daily_basic";
            p /= keys[0];
            p /= (keys[1] + ".parquet");
            break;
        case TableKind::PriceBarHfq:
            if (keys.empty()) throw std::runtime_error("PriceBarHfq: keys[0] = ts_code required");
            p /= "pro_bar_hfq";
            p /= (normalize_ts_code(keys[0]) + ".parquet");
            break;
        case TableKind::FinancialPit:
            if (keys.empty()) throw std::runtime_error("FinancialPit: keys[0] = ts_code required");
            p /= "financial_pit";
            p /= (normalize_ts_code(keys[0]) + ".parquet");
            break;
        case TableKind::Universe:
            if (keys.size() < 2) throw std::runtime_error("Universe: keys = [YYYY, YYYY-MM]");
            p /= "universe";
            p /= keys[0];
            p /= (keys[1] + ".parquet");
            break;
    }
    return p.string();
}

}  // namespace datalake
}  // namespace xt_data
