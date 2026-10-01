#include "HftLifecycleTelemetry.h"

#include <chrono>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "xt_util/path/PathUtil.h"

namespace xt_engine::hft {

void HftLifecycleTelemetry::init(Init init) {
    if (initialized_) {
        throw std::logic_error("HftLifecycleTelemetry::init called more than once");
    }
    state_path_ = std::move(init.state_path);
    emit_cb_ = std::move(init.lifecycle_sink);
    metric_cb_ = std::move(init.metric_sink);
    initialized_ = true;
    load_();
}

void HftLifecycleTelemetry::on_fill(const XtTradeRecord& tr, const std::string& effective_sid,
                                    int64_t qty_before_fp, int64_t avg_before_fp,
                                    int64_t qty_after_fp,  int64_t avg_after_fp) {
    if (emit_cb_) {
        emit_lifecycle_(tr, qty_before_fp, avg_before_fp,
                        qty_after_fp, avg_after_fp);
    }
    if (metric_cb_) {
        metric_cb_(effective_sid, tr.code.c_str(), qty_after_fp);
    }
}

void HftLifecycleTelemetry::emit_lifecycle_(const XtTradeRecord& tr,
                                            int64_t qty_before_fp, int64_t avg_before_fp,
                                            int64_t qty_after_fp,  int64_t avg_after_fp) {
    // 事件同时携带 steady clock 和 exchange/replay wall clock。
    const uint64_t ts_local_ns = tr.local_strategy_us * 1000ULL;  // steady, set at on_trade entry
    const uint64_t ts_exch_ns  = tr.fill_ts_us * 1000ULL;          // wall, exchange μs → ns
    // hold duration 使用成交 wall time；同一秒内开平允许为 0。
    const uint64_t wall_ms     = tr.fill_ts_us / 1000ULL;          // exchange/replay ms (open_ts_map_ key)

    std::string key;
    key.reserve(tr.strategy_id.size() + 1 + tr.code.size());
    key.append(tr.strategy_id);
    key.push_back('|');
    key.append(tr.code);

    // close 边界 realized 直取 engine 已算的权威 per-fill realized（tr.realized_pnl_fp，
    // read-delta：含 vol_scale + reducer int128 安全），始终是触发本 lifecycle 事件那笔
    // fill 的权威 realized。单边持仓下 close 事件由"全平 qty_before 的 fill"触发，该值即
    // 本次平仓 realized；锁仓(双边)书 net-based 边界分类与单笔 realized 非严格一一对应
    // （一腿全平可能不跨 net-零），故此处仅作 hold-duration analytics 的近似，**权威
    // per-fill PnL 单源是 aggregator（set_fill_realized_callback，每笔无条件）非本字段**。
    auto fire = [&](const char* event_type, int64_t qty_fp, int64_t avg_fp,
                    int64_t realized_fp, uint64_t hold_ms) {
        PositionLifecycleEvent ev;
        ev.ts_local_ns      = ts_local_ns;
        ev.ts_exch_ns       = ts_exch_ns;
        ev.strategy_id      = tr.strategy_id;
        ev.code             = tr.code;
        ev.event_type       = event_type;
        ev.qty_fp           = qty_fp;
        ev.avg_price_fp     = avg_fp;
        ev.realized_pnl_fp  = realized_fp;
        ev.hold_duration_ms = hold_ms;
        ev.cause_trade_id   = tr.local_cb_us ? tr.local_cb_us : tr.fill_ts_us;
        emit_cb_(ev);
    };

    const bool was_zero  = (qty_before_fp == 0);
    const bool is_zero   = (qty_after_fp  == 0);
    const bool sign_flip = (!was_zero && !is_zero) &&
                           ((qty_before_fp > 0) != (qty_after_fp > 0));

    bool map_mutated = false;
    if (was_zero && !is_zero) {
        open_ts_map_[key] = wall_ms;
        map_mutated = true;
        fire("open", qty_after_fp, avg_after_fp, /*realized_fp=*/0, /*hold_ms=*/0);
    } else if (!was_zero && is_zero) {
        uint64_t hold_ms = 0;
        auto it = open_ts_map_.find(key);
        if (it != open_ts_map_.end()) {
            hold_ms = (wall_ms >= it->second) ? (wall_ms - it->second) : 0;
            open_ts_map_.erase(it);
            map_mutated = true;
        }
        const int64_t realized_fp = tr.realized_pnl_fp;  // 权威 per-fill realized（read-delta）
        fire("close", qty_before_fp, avg_before_fp, realized_fp, hold_ms);
    } else if (sign_flip) {
        // 反手穿零按 close + open 两个 lifecycle 边界输出。
        uint64_t hold_ms = 0;
        auto it = open_ts_map_.find(key);
        if (it != open_ts_map_.end()) {
            hold_ms = (wall_ms >= it->second) ? (wall_ms - it->second) : 0;
            open_ts_map_.erase(it);
        }
        const int64_t realized_fp = tr.realized_pnl_fp;  // 权威 per-fill realized（read-delta）
        fire("close", qty_before_fp, avg_before_fp, realized_fp, hold_ms);
        open_ts_map_[key] = wall_ms;
        map_mutated = true;
        fire("open", qty_after_fp, avg_after_fp, /*realized_fp=*/0, /*hold_ms=*/0);
    }
    // 同向 add/reduce 不跨零，不发 lifecycle 事件。
    if (map_mutated) {
        save_();
    }
}

void HftLifecycleTelemetry::on_resume(const std::vector<ResumeEntry>& entries) {
    if (!emit_cb_) return;
    // PositionSnapshot 无 exchange ts；RESUME 使用 snapshot 到达时刻。
    const uint64_t now_ns_local = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
    const uint64_t now_ns_wall = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    const uint64_t now_ms_wall = now_ns_wall / 1'000'000ULL;

    for (const auto& e : entries) {
        std::string key;
        key.reserve(e.sid.size() + 1 + e.code.size());
        key.append(e.sid).push_back('|');
        key.append(e.code);

        // 缺失持久化 open_ts 时，从 snapshot 到达时刻重新计时。
        uint64_t prior_open_ms = now_ms_wall;
        uint64_t hold_ms = 0;
        auto it = open_ts_map_.find(key);
        if (it != open_ts_map_.end()) {
            prior_open_ms = it->second;
            hold_ms = (now_ms_wall >= prior_open_ms) ? (now_ms_wall - prior_open_ms) : 0;
        }

        PositionLifecycleEvent ev;
        ev.ts_local_ns      = now_ns_local;
        ev.ts_exch_ns       = now_ns_wall;
        ev.strategy_id      = e.sid;
        ev.code             = e.code;
        ev.event_type       = "resume";
        ev.qty_fp           = e.qty_fp;
        ev.avg_price_fp     = e.avg_price_fp;
        ev.realized_pnl_fp  = 0;
        ev.hold_duration_ms = hold_ms;
        ev.cause_trade_id   = 0;
        emit_cb_(ev);

        if (it == open_ts_map_.end()) {
            open_ts_map_[key] = now_ms_wall;
        }
        if (metric_cb_) {
            metric_cb_(e.sid, e.code.c_str(), e.qty_fp);
        }
    }

    // 为缺失 open_ts 的继承持仓补 entry 后落盘。
    save_();
}

void HftLifecycleTelemetry::save_() const {
    if (state_path_.empty()) return;
    // 持久化格式固定为 {"<sid>|<code>":open_ms,...}；key 不含 quote/backslash。
    std::ostringstream os;
    os << "{";
    bool first = true;
    for (const auto& [key, ts_ms] : open_ts_map_) {
        if (!first) os << ",";
        first = false;
        os << "\"" << key << "\":" << ts_ms;
    }
    os << "}\n";
    (void)PathUtil::write_atomic(state_path_, os.str());
}

void HftLifecycleTelemetry::load_() {
    if (state_path_.empty()) return;
    std::ifstream ifs(state_path_);
    if (!ifs.is_open()) return;
    std::stringstream buf;
    buf << ifs.rdbuf();
    const std::string content = buf.str();
    // 仅接受 save_() 写出的扁平对象；解析失败按空状态处理。
    std::size_t i = content.find('{');
    if (i == std::string::npos) return;
    ++i;
    while (i < content.size()) {
        while (i < content.size() && (content[i] == ' ' || content[i] == '\t' ||
                                       content[i] == '\n' || content[i] == ',')) ++i;
        if (i >= content.size() || content[i] == '}') break;
        if (content[i] != '"') return;
        ++i;
        const std::size_t key_end = content.find('"', i);
        if (key_end == std::string::npos) return;
        std::string key = content.substr(i, key_end - i);
        i = key_end + 1;
        while (i < content.size() && (content[i] == ':' || content[i] == ' ')) ++i;
        const std::size_t val_start = i;
        while (i < content.size() && content[i] >= '0' && content[i] <= '9') ++i;
        if (i == val_start) return;
        try {
            const uint64_t val = std::stoull(content.substr(val_start, i - val_start));
            open_ts_map_[std::move(key)] = val;
        } catch (...) {
            return;
        }
    }
}

}  // namespace xt_engine::hft
