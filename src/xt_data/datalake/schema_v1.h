#pragma once

/// @file
/// @brief 共享数据湖 schema v1 的表名、版本号和字段名协议常量。
///
/// 本文件是 reader 与 ingest 端之间的 contract 。修改字段名、删除字段或 bump
/// schema_version 都是协议变更，必须同步 ingest 写入端、reader 校验逻辑和
/// `_meta/schema_version.json`

#include <string>
#include <vector>

namespace xt_data {
namespace datalake {
namespace schema_v1 {

constexpr int kSchemaVersion = 1;

// ── ashare.calendar ─────────────────────────────────────────────────────────
namespace calendar {
constexpr const char* kTableName = "ashare.calendar";
constexpr const char* kColDate          = "date";              // YYYYMMDD int32
constexpr const char* kColIsTradingDay  = "is_trading_day";    // bool / int8 均可读
constexpr const char* kColExchange      = "exchange";          // 缺失时 reader 不过滤交易所
}

// ── ashare.stock_basic ──────────────────────────────────────────────────────
namespace stock_basic {
constexpr const char* kTableName = "ashare.stock_basic";
constexpr const char* kColTsCode      = "ts_code";        // "600000.SH"
constexpr const char* kColSymbol      = "symbol";         // "600000"
constexpr const char* kColName        = "name";           // 上市公司名
constexpr const char* kColExchange    = "exchange";       // "SSE" / "SZSE"
constexpr const char* kColListStatus  = "list_status";    // "L" / "D" / "P"
constexpr const char* kColListDate    = "list_date";      // YYYYMMDD
constexpr const char* kColDelistDate  = "delist_date";    // YYYYMMDD or null
constexpr const char* kColIndustry    = "industry";       // 行业
}

// ── ashare.pro_bar_hfq ──────────────────────────────────────────────────────
namespace pro_bar_hfq {
constexpr const char* kTableName = "ashare.pro_bar_hfq";
constexpr const char* kColTsCode      = "ts_code";
constexpr const char* kColTradeDate   = "trade_date";     // YYYYMMDD int32
constexpr const char* kColOpen        = "open";           // double（hfq 后复权）
constexpr const char* kColHigh        = "high";
constexpr const char* kColLow         = "low";
constexpr const char* kColClose       = "close";
constexpr const char* kColVol         = "vol";            // 成交量 double
constexpr const char* kColAmount      = "amount";         // 成交额 double
}

// ── ashare.daily_basic ──────────────────────────────────────────────────────
namespace daily_basic {
constexpr const char* kTableName = "ashare.daily_basic";
constexpr const char* kColTsCode      = "ts_code";
constexpr const char* kColTradeDate   = "trade_date";
constexpr const char* kColPe          = "pe";             // PE
constexpr const char* kColPb          = "pb";             // PB
constexpr const char* kColPs          = "ps";             // PS
constexpr const char* kColTotalMv     = "total_mv";       // 总市值（万元）
constexpr const char* kColCircMv      = "circ_mv";        // 流通市值（万元）
constexpr const char* kColTurnover    = "turnover_rate";  // 换手率
}

// ── ashare.financial_pit ────────────────────────────────────────────────────
namespace financial_pit {
constexpr const char* kTableName = "ashare.financial_pit";
constexpr const char* kColTsCode      = "ts_code";
constexpr const char* kColAnnDate     = "ann_date";       // 公告日 YYYYMMDD
constexpr const char* kColEndDate     = "end_date";       // 报告期末日
constexpr const char* kColRevenue     = "revenue";        // 营业收入
constexpr const char* kColNetProfit   = "net_profit";
constexpr const char* kColTotalAssets = "total_assets";
constexpr const char* kColEquity      = "total_equity";
constexpr const char* kColEps         = "eps";            // 每股收益
}

// ── ashare.universe ─────────────────────────────────────────────────────────
namespace universe {
constexpr const char* kTableName = "ashare.universe";
constexpr const char* kColTradeDate   = "trade_date";
constexpr const char* kColTsCode      = "ts_code";
constexpr const char* kColInUniverse  = "in_universe";    // bool
}

// ── ashare.industry/sw2021_l1 ───────────────────────────────────────────────
namespace industry_sw2021 {
constexpr const char* kTableName = "ashare.industry.sw2021_l1";
constexpr const char* kColTsCode    = "ts_code";
constexpr const char* kColIndustry  = "industry";         // 行业名
constexpr const char* kColIndexCode = "index_code";       // 行业指数代码
}

}  // namespace schema_v1
}  // namespace datalake
}  // namespace xt_data
