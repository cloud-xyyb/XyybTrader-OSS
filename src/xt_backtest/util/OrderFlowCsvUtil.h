#pragma once

/// @file
/// @brief Level-2 orderflow CSV 表头识别与逐行解析 helper。
///
/// 本文件解析 order_queue、order_detail 和 transaction 三类 CSV 行，并生成统一
/// replay sort key；事件归并与 runtime 派发由 event stream builder 负责。

#include <cstdint>
#include <string>

#include "xt_core/market/XtL2MarketStructs.h"

/// @brief order_queue / order_detail / transaction CSV 表头映射。
///
/// `idx_* == -1` 表示对应列缺失。解析函数只接受由
/// build_orderflow_spec_from_header() 成功构建的 spec；code、trading_date、
/// action_time、price、side 以及各类型的必需字段缺失时直接拒绝该表头。
struct XtOrderFlowCsvSpec {
    bool inited = false;  ///< true 表示表头校验通过，可用于逐行解析。

    // 三类 Level-2 CSV 共用字段。
    int idx_exchg = -1;
    int idx_code = -1;
    int idx_trading_date = -1;
    int idx_action_date = -1;
    int idx_action_time = -1;

    int idx_price = -1;
    int idx_volume = -1;
    int idx_side = -1;
    int idx_index = -1;

    // OrderQueue 专用字段。
    int idx_order_items = -1;
    int idx_qsize = -1;

    // OrderDetail / Transaction 可选分类字段。
    int idx_order_type = -1;
    int idx_flags = -1;

    // OrderQueue 档位量数组起始列。
    int idx_vol_start = -1;
};

enum class XtOrderFlowCsvKind {
    OrderQueue,    ///< OrderQueue 行，额外要求 order_items/qsize。
    OrderDetail,   ///< OrderDetail 行，额外要求 volume/index。
    Transaction    ///< Transaction 行，额外要求 volume/index。
};

/// @brief 从 CSV 表头构建 Level-2 列索引规格。
///
/// `line` 必须是带字母列名的表头行；`spec` 应传入默认构造值。成功时填充
/// `spec` 并置 `spec.inited=true`；返回 false 表示表头无法识别或缺少当前 kind
/// 的必需列，调用方不能继续使用该 spec 解析数据行。
bool build_orderflow_spec_from_header(const std::string& line,
                                      XtOrderFlowCsvKind kind,
                                      XtOrderFlowCsvSpec& spec);

/// @brief 按 header spec 解析 OrderQueue 行。
/// @return true 表示 `out` 和 `key` 已更新；false 表示行为空或必需字段无效。
bool parse_order_queue_csv_line(const std::string& line,
                                const XtOrderFlowCsvSpec& spec,
                                XtOrderQueue& out,
                                uint64_t& key);

/// @brief 按 header spec 解析 OrderDetail 行。
/// @return true 表示 `out` 和 `key` 已更新；false 表示行为空或必需字段无效。
bool parse_order_detail_csv_line(const std::string& line,
                                 const XtOrderFlowCsvSpec& spec,
                                 XtOrderDetail& out,
                                 uint64_t& key);

/// @brief 按 header spec 解析 Transaction 行。
/// @return true 表示 `out` 和 `key` 已更新；false 表示行为空或必需字段无效。
bool parse_transaction_csv_line(const std::string& line,
                                const XtOrderFlowCsvSpec& spec,
                                XtTransaction& out,
                                uint64_t& key);
