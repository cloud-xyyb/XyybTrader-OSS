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
    bool inited = false;

    int idx_exchg = -1;
    int idx_code = -1;
    int idx_trading_date = -1;
    int idx_action_date = -1;
    int idx_action_time = -1;

    int idx_price = -1;
    int idx_volume = -1;
    int idx_side = -1;
    int idx_index = -1;

    int idx_order_items = -1;
    int idx_qsize = -1;

    int idx_order_type = -1;
    int idx_flags = -1;

    int idx_vol_start = -1;
};

enum class XtOrderFlowCsvKind {
    OrderQueue,
    OrderDetail,
    Transaction
};

bool build_orderflow_spec_from_header(const std::string& line,
                                      XtOrderFlowCsvKind kind,
                                      XtOrderFlowCsvSpec& spec);

bool parse_order_queue_csv_line(const std::string& line,
                                const XtOrderFlowCsvSpec& spec,
                                XtOrderQueue& out,
                                uint64_t& key);

bool parse_order_detail_csv_line(const std::string& line,
                                 const XtOrderFlowCsvSpec& spec,
                                 XtOrderDetail& out,
                                 uint64_t& key);

bool parse_transaction_csv_line(const std::string& line,
                                const XtOrderFlowCsvSpec& spec,
                                XtTransaction& out,
                                uint64_t& key);
