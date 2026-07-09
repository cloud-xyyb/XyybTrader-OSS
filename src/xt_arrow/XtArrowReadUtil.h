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

inline bool read_parquet_table(const std::string& path,
                               std::shared_ptr<arrow::Table>& out) {
    auto maybe_input = arrow::io::ReadableFile::Open(path, arrow::default_memory_pool());
    if (!maybe_input.ok()) return false;
    std::shared_ptr<arrow::io::ReadableFile> input = *maybe_input;

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

inline std::shared_ptr<arrow::Array> col_array(const std::shared_ptr<arrow::Table>& table,
                                               const char* name) {
    auto chunked = table->GetColumnByName(name);
    if (!chunked || chunked->num_chunks() == 0) return {};
    return chunked->chunk(0);
}

inline double double_at(const std::shared_ptr<arrow::Array>& arr, int64_t row,
                        double fallback = 0.0) {
    if (!arr || arr->IsNull(row)) return fallback;
    const auto typed = std::dynamic_pointer_cast<arrow::DoubleArray>(arr);
    return typed ? typed->Value(row) : fallback;
}

inline int64_t int64_at(const std::shared_ptr<arrow::Array>& arr, int64_t row,
                        int64_t fallback = -1) {
    if (!arr || arr->IsNull(row)) return fallback;
    const auto typed = std::dynamic_pointer_cast<arrow::Int64Array>(arr);
    return typed ? typed->Value(row) : fallback;
}

inline std::string string_at(const std::shared_ptr<arrow::Array>& arr, int64_t row) {
    if (!arr || arr->IsNull(row)) return {};
    const auto typed = std::dynamic_pointer_cast<arrow::StringArray>(arr);
    return typed ? typed->GetString(row) : std::string{};
}

inline uint32_t date_at(const std::shared_ptr<arrow::Array>& arr, int64_t row) {
    if (!arr || arr->IsNull(row)) return 0;
    const auto i32 = std::dynamic_pointer_cast<arrow::Int32Array>(arr);
    if (i32) return static_cast<uint32_t>(i32->Value(row));
    const auto i64 = std::dynamic_pointer_cast<arrow::Int64Array>(arr);
    if (i64) return static_cast<uint32_t>(i64->Value(row));
    return 0;
}

}
