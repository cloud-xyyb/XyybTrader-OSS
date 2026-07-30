#include "xt_backtest/data/reader/XtFbsTickReader.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string_view>
#include <utility>

#include "xt_data/IEventStore.h"
#include "xt_data/LocalFsEventStore.h"
#include "xt_data/EventDepthContract.h"
#include "event_generated.h"

namespace {

inline double fp_to_d(int64_t fp) { return static_cast<double>(fp) * 1e-6; }

bool validate_md_tick(const ct::data::schema::MdTick& t, std::string& err) {
    using ct::data::schema::SourceKind;

    if (t.source_kind() == SourceKind::UNKNOWN) {
        err = "MdTick.source_kind is UNKNOWN";
        return false;
    }

    const int depth = t.depth_levels_real();
    if (depth < 1 || depth > ct::data::kEventStoreMdDepth) {
        err = "MdTick.depth_levels_real must be in [1, " +
              std::to_string(ct::data::kEventStoreMdDepth) + "], got " +
              std::to_string(depth);
        return false;
    }

    const auto check_vec = [&](const auto* v, std::string_view name) {
        if (!v) {
            err = std::string(name) + " is missing";
            return false;
        }
        if (static_cast<int>(v->size()) != depth) {
            err = std::string(name) + " size must equal depth_levels_real " +
                  std::to_string(depth);
            return false;
        }
        return true;
    };

    return check_vec(t.bid_price_fp(), "bid_price_fp")
        && check_vec(t.ask_price_fp(), "ask_price_fp")
        && check_vec(t.bid_qty_fp(), "bid_qty_fp")
        && check_vec(t.ask_qty_fp(), "ask_qty_fp");
}

bool decode_total_volume_fp(const ct::data::schema::MdTick& t,
                            int64_t& out_fp,
                            std::string& err) {
    using ct::data::schema::SourceKind;
    using ct::data::schema::VolumeEncoding;
    const int64_t v = t.total_volume_fp();
    if (t.volume_encoding() == VolumeEncoding::FP_1E6) {
        out_fp = v;
        return true;
    }
    switch (t.source_kind()) {
        case SourceKind::LIVE_BINANCE:
        case SourceKind::MIGRATED_V2:
            out_fp = v;
            return true;
        case SourceKind::LIVE_CTP:
        case SourceKind::LIVE_XTP:
            if (v < 0 || v > std::numeric_limits<int64_t>::max() / 1'000'000) {
                err = "legacy raw total_volume out of range for fp conversion";
                return false;
            }
            out_fp = v * 1'000'000;
            return true;
        default:
            err = "MdTick.total_volume_fp 单位不可判定（无 volume_encoding 且 "
                  "source_kind 不在查表内，如 REPLAY）；请用修复后的 "
                  "migrate_live_jsonl_to_fbs.py 重新生成";
            return false;
    }
}

void hydrate(const ct::data::schema::MdTick& t, XtRawTick& out) {
    out = XtRawTick{};

    if (auto* code = t.code())        std::strncpy(out.code, code->c_str(), sizeof(out.code) - 1);
    if (auto* exch = t.exchange_id()) std::strncpy(out.exchg, exch->c_str(), sizeof(out.exchg) - 1);

    out.trading_date = t.trading_day();
    out.action_date  = t.action_day();
    out.action_time  = t.action_time();

    out.price          = fp_to_d(t.price_fp());
    out.open           = fp_to_d(t.open_fp());
    out.high           = fp_to_d(t.high_fp());
    out.low            = fp_to_d(t.low_fp());
    out.settle         = fp_to_d(t.settle_fp());
    out.upper_limit    = fp_to_d(t.upper_limit_fp());
    out.lower_limit    = fp_to_d(t.lower_limit_fp());

    out.total_turnover = fp_to_d(t.total_turnover_fp());
    out.open_interest  = fp_to_d(t.open_interest_fp());

    out.pre_close    = fp_to_d(t.pre_close_fp());
    out.pre_settle   = fp_to_d(t.pre_settle_fp());
    out.pre_interest = fp_to_d(t.pre_interest_fp());

    const auto* bp = t.bid_price_fp();
    const auto* ap = t.ask_price_fp();
    const auto* bq = t.bid_qty_fp();
    const auto* aq = t.ask_qty_fp();
    int n = t.depth_levels_real();
    if (n > XtRawTick::DEPTH) n = XtRawTick::DEPTH;
    for (int i = 0; i < n; ++i) {
        if (bp && i < static_cast<int>(bp->size())) out.bid_prices[i] = fp_to_d(bp->Get(i));
        if (ap && i < static_cast<int>(ap->size())) out.ask_prices[i] = fp_to_d(ap->Get(i));
        if (bq && i < static_cast<int>(bq->size())) out.bid_qty[i]    = fp_to_d(bq->Get(i));
        if (aq && i < static_cast<int>(aq->size())) out.ask_qty[i]    = fp_to_d(aq->Get(i));
    }
}

}

XtFbsTickReader::XtFbsTickReader(Options opts) : opts_(std::move(opts)) {}
XtFbsTickReader::~XtFbsTickReader() = default;

bool XtFbsTickReader::init() {
    if (opts_.fbs_root.empty() || opts_.start_trading_day == 0 || opts_.end_trading_day == 0) {
        io_error_ = true;
        return false;
    }
    if (!std::filesystem::exists(opts_.fbs_root)) {
        io_error_ = true;
        return false;
    }

    store_ = std::make_unique<ct::data::LocalFsEventStore>(opts_.fbs_root);

    ct::data::IEventStore::QueryRange r;
    r.start_trading_day = opts_.start_trading_day;
    r.end_trading_day   = opts_.end_trading_day;
    r.symbols           = opts_.symbols;
    iter_ = store_->query(r);
    initialized_ = (iter_ != nullptr);
    return initialized_;
}

void XtFbsTickReader::reset() {
    iter_.reset();
    prev_total_volume_.clear();
    if (!store_) return;

    ct::data::IEventStore::QueryRange r;
    r.start_trading_day = opts_.start_trading_day;
    r.end_trading_day   = opts_.end_trading_day;
    r.symbols           = opts_.symbols;
    iter_ = store_->query(r);
    io_error_ = false;
}

bool XtFbsTickReader::next_tick(XtRawTick& out) {
    if (!initialized_ || !iter_) return false;

    ct::data::MdTickView view;
    if (!iter_->next(view)) {
        if (iter_->has_error()) io_error_ = true;
        return false;
    }
    std::string validation_err;
    if (!view.tick || !validate_md_tick(*view.tick, validation_err)) {

        std::fprintf(stderr, "[XtFbsTickReader] invalid MdTick record rejected: %s\n",
                     validation_err.c_str());
        io_error_ = true;
        return false;
    }
    int64_t total_volume_fp = 0;
    if (!decode_total_volume_fp(*view.tick, total_volume_fp, validation_err)) {
        const auto* code = view.tick->code();
        std::fprintf(stderr, "[XtFbsTickReader] MdTick rejected (code=%s): %s\n",
                     code ? code->c_str() : "?", validation_err.c_str());
        io_error_ = true;
        return false;
    }
    hydrate(*view.tick, out);
    out.total_volume = fp_to_d(total_volume_fp);

    auto it = prev_total_volume_.find(out.code);
    if (it != prev_total_volume_.end() && out.total_volume >= it->second) {
        out.volume = out.total_volume - it->second;
    }
    prev_total_volume_[out.code] = out.total_volume;
    return true;
}
