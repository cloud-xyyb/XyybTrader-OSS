#include "OrderFlowCsvUtil.h"

#include "XtParseUtil.h"
#include "xt_util/string/XtStringUtil.h"
#include "xt_util/time/XtTimeParse.h"
#include "TickCsvUtil.h"  // make_tick_sort_key 统一 tick 与 Level-2 夜盘排序口径。

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

using XtParseUtil::split_csv_trim;

namespace {

enum class FieldId {
    Exchg,
    Code,
    TradingDate,
    ActionDate,
    ActionTime,
    Price,
    Volume,
    Side,
    Index,
    OrderItems,
    Qsize,
    OrderType,
    Flags,
    VolStart,
};

// Level-2 行排序键使用 trading_date + action_time，和 tick/event stream 共用夜盘旋转口径。
uint64_t make_key(uint32_t trading_date, uint32_t action_time) {
    return make_tick_sort_key(trading_date, action_time);
}

bool parse_dates(const std::vector<std::string>& cols,
                 int idx_action_date,
                 int idx_trading_date,
                 uint32_t& action_date,
                 uint32_t& trading_date) {
    action_date  = 0;
    trading_date = 0;

    auto pick = [&](int idx) -> const std::string* {
        if (idx < 0 || idx >= static_cast<int>(cols.size())) return nullptr;
        return &cols[idx];
    };

    if (const std::string* s = pick(idx_trading_date)) trading_date = XtTimeParse::parse_date_only(*s);
    if (trading_date == 0) return false;

    if (const std::string* s = pick(idx_action_date)) action_date = XtTimeParse::parse_date_only(*s);
    if (action_date == 0) action_date = trading_date;
    return true;
}

void copy_text(char* dst, size_t cap, const std::string& src) {
    if (!dst || cap == 0) return;
    std::memset(dst, 0, cap);
    if (!src.empty()) std::strncpy(dst, src.c_str(), cap - 1);
}

// 解析热路径只传 FieldId，不做字符串列名查找。
int get_idx_by_id(const XtOrderFlowCsvSpec& spec, XtOrderFlowCsvKind kind, FieldId id) {
    int idx = -1;
    switch (id) {
        case FieldId::Exchg:       idx = spec.idx_exchg;        break;
        case FieldId::Code:        idx = spec.idx_code;         break;
        case FieldId::TradingDate: idx = spec.idx_trading_date; break;
        case FieldId::ActionDate:  idx = spec.idx_action_date;  break;
        case FieldId::ActionTime:  idx = spec.idx_action_time;  break;
        case FieldId::Price:       idx = spec.idx_price;        break;
        case FieldId::Volume:      idx = spec.idx_volume;       break;
        case FieldId::Side:        idx = spec.idx_side;         break;
        case FieldId::Index:       idx = spec.idx_index;        break;
        case FieldId::OrderItems:  idx = spec.idx_order_items;  break;
        case FieldId::Qsize:       idx = spec.idx_qsize;        break;
        case FieldId::OrderType:   idx = spec.idx_order_type;   break;
        case FieldId::Flags:       idx = spec.idx_flags;        break;
        case FieldId::VolStart:    idx = spec.idx_vol_start;    break;
    }

    return idx;
}

const std::string& col_of(const std::vector<std::string>& cols, int idx) {
    static const std::string empty;
    if (idx < 0 || idx >= static_cast<int>(cols.size())) return empty;
    return cols[idx];
}

bool has_required_header_fields(const XtOrderFlowCsvSpec& spec, XtOrderFlowCsvKind kind) {
    if (spec.idx_code < 0 || spec.idx_action_time < 0) return false;
    if (spec.idx_trading_date < 0) return false;
    if (spec.idx_price < 0 || spec.idx_side < 0) return false;
    if (kind == XtOrderFlowCsvKind::OrderQueue) {
        return spec.idx_order_items >= 0 && spec.idx_qsize >= 0;
    }
    return spec.idx_volume >= 0 && spec.idx_index >= 0;
}

} // namespace

bool build_orderflow_spec_from_header(const std::string& line,
                                      XtOrderFlowCsvKind kind,
                                      XtOrderFlowCsvSpec& spec) {
    // 表头解析允许别名，但后续逐行解析严格依赖这里生成的 idx_*。
    thread_local std::vector<std::string> cols;
    split_csv_trim(line, cols);
    if (cols.empty()) return false;

    bool has_alpha = false;
    for (size_t i = 0; i < cols.size(); ++i) {
        const std::string norm = XtParseUtil::normalize_col(cols[i]);
        for (unsigned char c : norm) {
            if (std::isalpha(c)) { has_alpha = true; break; }
        }

        if      (norm == "exchg" || norm == "exchange" || norm == "exchangeid")        spec.idx_exchg        = static_cast<int>(i);
        else if (norm == "code"  || norm == "instrument" || norm == "instrumentid")    spec.idx_code         = static_cast<int>(i);
        else if (norm == "tradingdate" || norm == "tradingday")                        spec.idx_trading_date = static_cast<int>(i);
        else if (norm == "actiondate"  || norm == "date")                              spec.idx_action_date  = static_cast<int>(i);
        else if (norm == "actiontime"  || norm == "time" || norm == "updatetime")      spec.idx_action_time  = static_cast<int>(i);
        else if (norm == "price"       || norm == "lastprice")                         spec.idx_price        = static_cast<int>(i);
        else if (norm == "volume"      || norm == "qty" || norm == "size")             spec.idx_volume       = static_cast<int>(i);
        else if (norm == "side"        || norm == "bsflag")                            spec.idx_side         = static_cast<int>(i);
        else if (norm == "index"       || norm == "seq" || norm == "seqno")            spec.idx_index        = static_cast<int>(i);
        else if (norm == "orderitems"  || norm == "orders" || norm == "ordersize")     spec.idx_order_items  = static_cast<int>(i);
        else if (norm == "qsize"       || norm == "queuesize")                         spec.idx_qsize        = static_cast<int>(i);
        else if (norm == "ordertype")                                                  spec.idx_order_type   = static_cast<int>(i);
        else if (norm == "flags"       || norm == "tradeflag")                         spec.idx_flags        = static_cast<int>(i);
        else if (kind == XtOrderFlowCsvKind::OrderQueue &&
                 norm.size() >= 3 && norm.substr(0, 3) == "vol" &&
                 spec.idx_vol_start < 0) {
            spec.idx_vol_start = static_cast<int>(i);
        }
    }

    if (!has_alpha) return false;
    if (!has_required_header_fields(spec, kind)) return false;
    spec.inited = true;
    return true;
}

bool parse_order_queue_csv_line(const std::string& line,
                                const XtOrderFlowCsvSpec& spec,
                                XtOrderQueue& out,
                                uint64_t& key) {
    // 复用分割 buffer；本函数不保留字段指针或引用。
    thread_local std::vector<std::string> cols;
    split_csv_trim(line, cols);
    if (cols.empty()) return false;

    constexpr XtOrderFlowCsvKind K = XtOrderFlowCsvKind::OrderQueue;

    XtOrderQueue q{};
    copy_text(q.exchg, sizeof(q.exchg), col_of(cols, get_idx_by_id(spec, K, FieldId::Exchg)));
    copy_text(q.code,  sizeof(q.code),  col_of(cols, get_idx_by_id(spec, K, FieldId::Code)));
    if (q.code[0] == '\0') return false;

    if (!parse_dates(cols,
                     get_idx_by_id(spec, K, FieldId::ActionDate),
                     get_idx_by_id(spec, K, FieldId::TradingDate),
                     q.action_date, q.trading_date)) {
        return false;
    }

    uint32_t action_time = 0;
    XtParseUtil::parse_time_with_ms(col_of(cols, get_idx_by_id(spec, K, FieldId::ActionTime)), action_time);
    q.action_time = action_time;

    XtParseUtil::parse_u8     (col_of(cols, get_idx_by_id(spec, K, FieldId::Side)),        q.side);
    XtStringUtil::parse_double(col_of(cols, get_idx_by_id(spec, K, FieldId::Price)),       q.price);
    XtParseUtil::parse_u32    (col_of(cols, get_idx_by_id(spec, K, FieldId::OrderItems)), q.order_items);
    XtParseUtil::parse_u32    (col_of(cols, get_idx_by_id(spec, K, FieldId::Qsize)),       q.qsize);

    int vol_start = get_idx_by_id(spec, K, FieldId::VolStart);
    if (vol_start < 0) vol_start = 7;
    const int remain = static_cast<int>(cols.size()) - vol_start;
    const int n      = std::min(50, remain > 0 ? remain : 0);
    for (int i = 0; i < n; ++i) {
        XtStringUtil::parse_double(cols[vol_start + i], q.volumes[i]);
    }

    out = q;
    key = make_key(q.trading_date, q.action_time);
    return true;
}

bool parse_order_detail_csv_line(const std::string& line,
                                 const XtOrderFlowCsvSpec& spec,
                                 XtOrderDetail& out,
                                 uint64_t& key) {
    // 复用分割 buffer；本函数不保留字段指针或引用。
    thread_local std::vector<std::string> cols;
    split_csv_trim(line, cols);
    if (cols.empty()) return false;

    constexpr XtOrderFlowCsvKind K = XtOrderFlowCsvKind::OrderDetail;

    XtOrderDetail d{};
    copy_text(d.exchg, sizeof(d.exchg), col_of(cols, get_idx_by_id(spec, K, FieldId::Exchg)));
    copy_text(d.code,  sizeof(d.code),  col_of(cols, get_idx_by_id(spec, K, FieldId::Code)));
    if (d.code[0] == '\0') return false;

    if (!parse_dates(cols,
                     get_idx_by_id(spec, K, FieldId::ActionDate),
                     get_idx_by_id(spec, K, FieldId::TradingDate),
                     d.action_date, d.trading_date)) {
        return false;
    }

    uint32_t action_time = 0;
    XtParseUtil::parse_time_with_ms(col_of(cols, get_idx_by_id(spec, K, FieldId::ActionTime)), action_time);
    d.action_time = action_time;

    XtParseUtil::parse_u64    (col_of(cols, get_idx_by_id(spec, K, FieldId::Index)),     d.index);
    XtStringUtil::parse_double(col_of(cols, get_idx_by_id(spec, K, FieldId::Price)),     d.price);
    XtStringUtil::parse_double(col_of(cols, get_idx_by_id(spec, K, FieldId::Volume)),    d.volume);
    XtParseUtil::parse_u8     (col_of(cols, get_idx_by_id(spec, K, FieldId::Side)),      d.side);
    XtParseUtil::parse_u8     (col_of(cols, get_idx_by_id(spec, K, FieldId::OrderType)), d.order_type);

    out = d;
    key = make_key(d.trading_date, d.action_time);
    return true;
}

bool parse_transaction_csv_line(const std::string& line,
                                const XtOrderFlowCsvSpec& spec,
                                XtTransaction& out,
                                uint64_t& key) {
    // 复用分割 buffer；本函数不保留字段指针或引用。
    thread_local std::vector<std::string> cols;
    split_csv_trim(line, cols);
    if (cols.empty()) return false;

    constexpr XtOrderFlowCsvKind K = XtOrderFlowCsvKind::Transaction;

    XtTransaction t{};
    copy_text(t.exchg, sizeof(t.exchg), col_of(cols, get_idx_by_id(spec, K, FieldId::Exchg)));
    copy_text(t.code,  sizeof(t.code),  col_of(cols, get_idx_by_id(spec, K, FieldId::Code)));
    if (t.code[0] == '\0') return false;

    if (!parse_dates(cols,
                     get_idx_by_id(spec, K, FieldId::ActionDate),
                     get_idx_by_id(spec, K, FieldId::TradingDate),
                     t.action_date, t.trading_date)) {
        return false;
    }

    uint32_t action_time = 0;
    XtParseUtil::parse_time_with_ms(col_of(cols, get_idx_by_id(spec, K, FieldId::ActionTime)), action_time);
    t.action_time = action_time;

    XtParseUtil::parse_u64    (col_of(cols, get_idx_by_id(spec, K, FieldId::Index)),  t.index);
    XtStringUtil::parse_double(col_of(cols, get_idx_by_id(spec, K, FieldId::Price)),  t.price);
    XtStringUtil::parse_double(col_of(cols, get_idx_by_id(spec, K, FieldId::Volume)), t.volume);
    XtParseUtil::parse_u8     (col_of(cols, get_idx_by_id(spec, K, FieldId::Side)),   t.side);
    XtParseUtil::parse_u8     (col_of(cols, get_idx_by_id(spec, K, FieldId::Flags)),  t.flags);

    out = t;
    key = make_key(t.trading_date, t.action_time);
    return true;
}
