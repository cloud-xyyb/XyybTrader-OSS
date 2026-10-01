#pragma once

/// @file
/// @brief Apache Arrow / Parquet 读取通用 helper（shared parquet read plumbing）。
///
/// 供需要读 parquet 的 reader 共用：datalake 的 `XtCalendarReader` /
/// `XtPriceBarReader`、`xt_backtest/data` 的 `XtEventParquetReader`。
/// 下游 link `xt_arrow_parquet` INTERFACE target 即同时获得本 header 的
/// include path 与 Arrow / Parquet 链接。
///
/// @note 只放"读 parquet 的机械层"（打开文件 / 取列 / 按类型取单元）；列名映射、
///       schema 校验、领域结构组装由各 reader 自持，不上提。
/// @note 本文件故意放在 `xt_arrow/` 而不是 `xt_util/`：它直接暴露 Arrow / Parquet
///       headers 和版本兼容分支。需要 parquet 的 target 应显式 link
///       `xt_arrow_parquet`，避免把重型数据栈依赖传给底层通用工具库。

#include <cstdint>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/util/config.h>
#include <parquet/arrow/reader.h>

namespace xt_arrow {

/// @brief 读取整张 parquet table 并 CombineChunks，屏蔽 Arrow 版本 API 差异。
///
/// @param path  parquet 文件绝对路径
/// @param out   成功时写入合并后的 table（每列单 chunk，便于 `col_array` 取 chunk(0)）
/// @return false = 文件打不开 / 读失败 / combine 失败
inline bool read_parquet_table(const std::string& path,
                               std::shared_ptr<arrow::Table>& out) {
    auto maybe_input = arrow::io::ReadableFile::Open(path, arrow::default_memory_pool());
    if (!maybe_input.ok()) return false;
    std::shared_ptr<arrow::io::ReadableFile> input = *maybe_input;

    // Arrow >=19 提供 2-arg Result 形态；更早发行版只导出 3-arg Status 形态。
    // 只按 ARROW_VERSION_MAJOR 分派；ARROW_PACKAGE_KIND 在 wheel/deb 中均定义，
    // 不可作为 ABI 判据。
    std::unique_ptr<parquet::arrow::FileReader> reader;
#if ARROW_VERSION_MAJOR >= 19
    auto open_result = parquet::arrow::OpenFile(input, arrow::default_memory_pool());
    if (!open_result.ok()) return false;
    reader = std::move(open_result).MoveValueUnsafe();
    if (!reader) return false;
#else
    auto open_st = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader);
    if (!open_st.ok() || !reader) return false;
#endif

    // Arrow >=24 使用 Result 形态；更早发行版走 Table** 形态。
#if ARROW_VERSION_MAJOR >= 24
    auto table_result = reader->ReadTable();
    if (!table_result.ok()) return false;
    out = std::move(table_result).MoveValueUnsafe();
    if (!out) return false;
#else
    auto st = reader->ReadTable(&out);
    if (!st.ok() || !out) return false;
#endif

    auto combined = out->CombineChunks(arrow::default_memory_pool());
    if (!combined.ok()) return false;
    out = *combined;
    return true;
}

/// @brief 按列名取首个 chunk array（table 经 CombineChunks 后每列仅 1 chunk）。
/// @return 列不存在或为空时返回 nullptr。
inline std::shared_ptr<arrow::Array> col_array(const std::shared_ptr<arrow::Table>& table,
                                               const char* name) {
    auto chunked = table->GetColumnByName(name);
    if (!chunked || chunked->num_chunks() == 0) return {};
    return chunked->chunk(0);
}

/// @brief 读 double 列单元；null / 空 array 返回 fallback。
/// @note dynamic_pointer_cast 验物理类型：外部 vendor parquet 列类型漂移（float32/decimal/int）
///       时返 fallback 而非 static_cast 读错 buffer（垃圾值 / 越界 UB）。in-repo datalake 写
///       float64，cast 成功后值保持不变。
inline double double_at(const std::shared_ptr<arrow::Array>& arr, int64_t row,
                        double fallback = 0.0) {
    if (!arr || arr->IsNull(row)) return fallback;
    const auto typed = std::dynamic_pointer_cast<arrow::DoubleArray>(arr);
    return typed ? typed->Value(row) : fallback;
}

/// @brief 读 int64 列单元；null / 空 array / 物理类型不符 返回 fallback（默认 -1）。
inline int64_t int64_at(const std::shared_ptr<arrow::Array>& arr, int64_t row,
                        int64_t fallback = -1) {
    if (!arr || arr->IsNull(row)) return fallback;
    const auto typed = std::dynamic_pointer_cast<arrow::Int64Array>(arr);
    return typed ? typed->Value(row) : fallback;
}

/// @brief 读 string 列单元；null / 空 array / 物理类型不符 返回空串。
inline std::string string_at(const std::shared_ptr<arrow::Array>& arr, int64_t row) {
    if (!arr || arr->IsNull(row)) return {};
    const auto typed = std::dynamic_pointer_cast<arrow::StringArray>(arr);
    return typed ? typed->GetString(row) : std::string{};
}

/// @brief 读日期列单元（YYYYMMDD），兼容 Int32 / Int64 物理类型；null 返回 0。
inline uint32_t date_at(const std::shared_ptr<arrow::Array>& arr, int64_t row) {
    if (!arr || arr->IsNull(row)) return 0;
    const auto i32 = std::dynamic_pointer_cast<arrow::Int32Array>(arr);
    if (i32) return static_cast<uint32_t>(i32->Value(row));
    const auto i64 = std::dynamic_pointer_cast<arrow::Int64Array>(arr);
    if (i64) return static_cast<uint32_t>(i64->Value(row));
    return 0;
}

}  // namespace xt_arrow
