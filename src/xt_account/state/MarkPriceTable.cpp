#include "xt_account/state/MarkPriceTable.h"

#include <cstdio>
#include <string>

namespace xt_account {

// MarkPriceTable 分 pre-freeze / frozen 两个阶段：pre-freeze 可扩展 registry，
// frozen 后只更新已注册 slot，避免 live hot path 分配并保持 FullSnapshot symbol
// 下标稳定。

// freeze 后 reset 对 mark table 为 no-op；账户持仓恢复不应清 venue-truth mark。
// recovery 的 SetPosition 可能没有成本价；若这里清掉 marks_，读侧会按 mark=0
// 计算 unrealized_pnl，直到下一笔有效 MarketTick 到来。
void MarkPriceTable::reset() {
    if (frozen_.load(std::memory_order_acquire)) {
        return;
    }
    for (auto& m : marks_) m.store(0, std::memory_order_relaxed);
    for (auto& h : cost_hint_) h.store(0, std::memory_order_relaxed);
    registry_.reset();
}

void MarkPriceTable::clear_cost_hints() noexcept {
    for (auto& h : cost_hint_) h.store(0, std::memory_order_release);
}

bool MarkPriceTable::pre_warm_one(std::string_view code) {
    if (code.empty()) return true;
    // XtInstrumentRegistry 接口需要 null-terminated code；这里保留一次 string 拷贝，
    // 避免为单 symbol 预热构造临时 vector。
    const std::string buf(code);
    return registry_.register_or_get(buf.c_str()) != xt_core::kInvalidInstrumentId;
}

bool MarkPriceTable::intern_for_slot(std::string_view code) {
    if (code.empty()) return true;
    const std::string buf(code);
    return registry_.register_or_get(buf.c_str()) != xt_core::kInvalidInstrumentId;
}

MarkPriceTable::TrackResult MarkPriceTable::resolve_id_(const char* origin, const char* code,
                                                        xt_core::XtInstrumentId& out_id) {
    out_id = registry_.id_of(code);
    if (out_id != xt_core::kInvalidInstrumentId) {
        return TrackResult::kTracked;
    }
    // freeze 后 symbol set 应已由启动期注册完成；未知 code 属于漏注册或上游异常，
    // 这里只丢弃并告警，逐 code 记名由 store 侧 quarantine 承担。
    if (frozen_.load(std::memory_order_acquire)) {
        warn_unknown_frozen_code_(origin, code);
        return TrackResult::kIgnored;
    }
    out_id = registry_.register_or_get(code);
    if (out_id == xt_core::kInvalidInstrumentId) {
        // 非空 code 在 pre-freeze 注册失败只有一个原因：intern 表容量耗尽。
        // 静默丢弃会让该 code 永久缺席聚合（估值静默算错），交给调用方 fail-fast。
        return TrackResult::kInternFull;
    }
    return TrackResult::kTracked;
}

MarkPriceTable::TrackResult MarkPriceTable::on_market_tick(const char* code, int64_t mark_price) {
    if (!code || code[0] == '\0') {
        return TrackResult::kIgnored;
    }
    // 非正价格不是可用 mark：部分行情源在首个成交前会推 0 占位。
    // 保留上一笔有效 mark，避免持仓估值回退到 mark=0。
    if (mark_price <= 0) {
        return TrackResult::kIgnored;
    }
    xt_core::XtInstrumentId id = xt_core::kInvalidInstrumentId;
    const TrackResult r = resolve_id_("on_market_tick", code, id);
    if (r != TrackResult::kTracked) {
        return r;
    }
    marks_[id].store(mark_price, std::memory_order_release);
    return TrackResult::kTracked;
}

MarkPriceTable::TrackResult MarkPriceTable::on_set_position(const char* code) {
    if (!code || code[0] == '\0') {
        return TrackResult::kIgnored;
    }
    xt_core::XtInstrumentId id = xt_core::kInvalidInstrumentId;
    return resolve_id_("on_set_position", code, id);
}

MarkPriceTable::TrackResult MarkPriceTable::on_trade(const char* code) {
    if (!code || code[0] == '\0') {
        return TrackResult::kIgnored;
    }
    xt_core::XtInstrumentId id = xt_core::kInvalidInstrumentId;
    return resolve_id_("on_trade", code, id);
}

void MarkPriceTable::update_cost_hint(const char* code, int64_t cost_hint) noexcept {
    if (!code || code[0] == '\0') {
        return;
    }
    const auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {
        return;
    }
    cost_hint_[id].store(cost_hint, std::memory_order_release);
}

int64_t MarkPriceTable::venue_mark_price(const char* code) const {
    if (!code || code[0] == '\0') {
        return 0;
    }
    const auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {
        return 0;
    }
    return marks_[id].load(std::memory_order_acquire);
}

int64_t MarkPriceTable::effective_mark(const char* code) const {
    if (!code || code[0] == '\0') {
        return 0;
    }
    const auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {
        return 0;
    }
    const int64_t venue = marks_[id].load(std::memory_order_acquire);
    return venue != 0 ? venue : cost_hint_[id].load(std::memory_order_acquire);
}

int64_t MarkPriceTable::cost_hint(const char* code) const {
    if (!code || code[0] == '\0') {
        return 0;
    }
    const auto id = registry_.id_of(code);
    if (id == xt_core::kInvalidInstrumentId) {
        return 0;
    }
    return cost_hint_[id].load(std::memory_order_acquire);
}

void MarkPriceTable::warn_unknown_frozen_code_(const char* origin, const char* code) {
    if (warned_unknown_frozen_) {
        return;
    }
    warned_unknown_frozen_ = true;
    std::fprintf(stderr,
                 "[xt_account] WARN: MarkPriceTable::%s unknown code='%s' "
                 "after freeze(); skipping insert (further unknown-code warnings suppressed; "
                 "per-code naming lives in AccountStateStore quarantine)\n",
                 origin, code);
}

}  // namespace xt_account
