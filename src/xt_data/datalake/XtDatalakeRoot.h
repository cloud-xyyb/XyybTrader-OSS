#pragma once

/// @file
/// @brief 共享数据湖 root 路径解析与 schema 版本校验。
///
/// A 股的日线、财务、日历等基础数据以 parquet 落在一个共享「数据湖」目录树里；各 reader
/// 都要先知道根路径、再按表类型拼出具体 parquet 路径，并确认磁盘上的 schema 版本与自己
/// 期望的一致。本文件把这三件事收成一处：root 解析、按表类型的路径拼装、以及对
/// `_meta/schema_version.json` 的版本/必需字段校验。root 传空时回退到 `XT_DATALAKE_ROOT`，
/// 仍为空则保持空；Strict 策略遇 mismatch fail-fast，AllowSkew 记 warning 并返回 false。

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace xt_data {
namespace datalake {

/// @brief 数据湖表路径分类，用于 resolve_path() 与各 reader。
enum class TableKind {
    Calendar,     ///< `ashare/calendar.parquet`。
    StockBasic,   ///< `ashare/stock_basic.parquet`。
    Industry,     ///< `ashare/industry/<classifier>.parquet`。
    DailyBasic,   ///< `ashare/daily_basic/<YYYY>/<YYYY-MM>.parquet`。
    PriceBarHfq,  ///< `ashare/pro_bar_hfq/<ts_code>.parquet`。
    FinancialPit, ///< `ashare/financial_pit/<ts_code>.parquet`。
    Universe,     ///< `ashare/universe/<YYYY>/<YYYY-MM>.parquet`。
};

/// @brief schema mismatch 处理策略。
enum class SchemaCompatPolicy {
    Strict,    ///< mismatch / missing metadata 直接 throw。
    AllowSkew, ///< mismatch / missing metadata 记录 warning，validate_schema() 返回 false。
};

/// @brief 单表 schema 期望；required_columns 是 reader 运行所需的最小字段集合。
struct TableSchemaExpect {
    std::string table_name;  ///< schema_version.json 中的完整表名。
    int schema_version;      ///< reader 可接受的精确 schema_version。
    std::vector<std::string> required_columns; ///< 必须存在；顺序无关，新增字段允许共存。
};

/// @brief 共享数据湖的 root 解析与 schema 校验入口。
///
/// `set_root()` 配置数据湖根路径，`validate_schema()` 校验 schema metadata，
/// `resolve_path()` 按表类型生成 parquet 文件路径。
///
/// 调用方应在 reader `load()` 前完成 root 与 schema 初始化。
/// `set_root()` / `set_compat_policy()` 不应与 reader `load()` 并发执行。
class XtDatalakeRoot {
public:
    XtDatalakeRoot() = default;

    /// @brief 设置 datalake 根路径；空字符串回退到 `XT_DATALAKE_ROOT`，仍为空则保持空。
    void set_root(std::string root);

    /// @brief 设置 schema 兼容策略；默认 Strict。
    void set_compat_policy(SchemaCompatPolicy policy) noexcept { policy_ = policy; }

    /// @brief 当前已解析 root；未调用 set_root() 时为空。
    [[nodiscard]] const std::string& root() const noexcept { return root_; }

    /// @brief 校验 `_meta/schema_version.json` 中的版本号和必需字段。
    /// @param expects 调用方依赖的表 schema 列表。
    /// @return true 表示全部通过；false 仅在 policy=AllowSkew 且存在 mismatch 时返回。
    /// @throws std::runtime_error policy=Strict 且 root/meta/schema 不满足合约。
    bool validate_schema(const std::vector<TableSchemaExpect>& expects);

    /// @brief 解析表 parquet 文件路径。
    /// @param kind 表类型。
    /// @param keys 按 TableKind 解释：Industry={"classifier"}；
    ///             DailyBasic/Universe={"YYYY","YYYY-MM"}；
    ///             PriceBarHfq/FinancialPit={"<ts_code>"}；Calendar/StockBasic 忽略。
    /// @return 完整 parquet 路径；root 为空或 keys 不足时抛出 std::runtime_error。
    [[nodiscard]] std::string resolve_path(TableKind kind,
                                           const std::vector<std::string>& keys) const;

    /// @brief 最近一次 validate_schema() 读取的原始 JSON；未读取时为空。
    [[nodiscard]] const std::string& schema_json_raw() const noexcept { return schema_json_raw_; }

private:
    std::string root_;
    SchemaCompatPolicy policy_ = SchemaCompatPolicy::Strict;
    std::string schema_json_raw_;
};

}  // namespace datalake
}  // namespace xt_data
