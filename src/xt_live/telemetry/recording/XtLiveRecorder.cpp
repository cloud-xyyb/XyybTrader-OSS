#include "XtLiveRecorder.h"

#include <array>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

// fbs event-store sink
#include "flatbuffers/flatbuffers.h"
#include "event_generated.h"
#include "event_ext_generated.h"   // OrderSubmit / TradeRtn
#include "xt_data/EventDepthContract.h"
#include "xt_data/IEventStore.h"
#include "xt_logger/XtCallbackGuard.h"

// drain-thread Prometheus metric inc
// (XT_HAVE_METRICS defined by cmake when xt_metrics target available)
#ifdef XT_HAVE_METRICS
#include "xt_metrics/XtMetricRegistry.h"
#endif
namespace {
static_assert(ct::data::kEventStoreMdDepth <= XtLiveRecorder::kMdDepth, "FBS MdTick depth must fit JSONL recorder depth");
/// @brief wallclock epoch 秒（滚动后缀 + 时间窗复位用）。
inline int64_t wall_now_sec() noexcept {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

/// @brief 为 base 生成不冲突的 <base>.<epoch> 滚动目标；从 start_sec 起遇占用 ++ 后缀。
/// @note 后缀保持纯数字 epoch 秒（etl `date -d @<sfx>` 解析日期 + `^[0-9]+$` 校验）。
///       start-time 与运行时两条滚动路径共用，防同秒重启/滚动 rename 覆盖。
inline std::filesystem::path unique_rotation_target(const std::filesystem::path& base,
                                                    int64_t start_sec) {
    int64_t sfx = start_sec;
    std::filesystem::path target;
    do {
        target = base;
        target += "." + std::to_string(sfx);
        ++sfx;
    } while (std::filesystem::exists(target));
    return target;
}
/// @brief 把 SourceKind enum cast 转 venue id label（跟 venue_routes.yaml 对齐）
inline const char* venue_kind_to_id(int8_t k) {
    switch (k) {
        case 0:  return "unknown";    // UNKNOWN
        case 1:  return "ctp";        // LIVE_CTP
        case 4:  return "binance_u";  // LIVE_BINANCE
        case 5:  return "xtp";        // LIVE_XTP (A 股现货)
        case 3:  return "replay";     // REPLAY
        default: return "";
    }
}

/// @brief route_status 归一：已知 status 透传，未知→"unknown"；空 status 按
///   有无 strategy 推 "routed"/"unmatched"。供 TRADE_RTN 录制 + route triage 计数。
inline const char* canonical_route_status(const char* route_status,
                                          const char* strategy) {
    auto is_known = [](std::string_view s) {
        return s == "routed" || s == "unmatched" || s == "operator"
            || s == "fallback_recovery" || s == "unknown";
    };
    if (route_status && route_status[0] != '\0') {
        return is_known(route_status) ? route_status : "unknown";
    }
    return (strategy && strategy[0] != '\0') ? "routed" : "unmatched";
}

/// @brief SplitMix64 finalizer，用 trace_id 派生稳定 sampling uniform 值。
inline uint64_t mix_trace_id(uint64_t x) noexcept {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}
}  // namespace

namespace {

/// @brief 将 fixed-point (×1e6) 格式化追加到字符串末尾（"123.456789"，固定 6 位小数）。
/// @note 后台 drain 线程使用，零 heap alloc（仅栈上 snprintf）。
inline void format_fp_to(std::string& out, int64_t v) {
    const bool neg = v < 0;
    uint64_t u = static_cast<uint64_t>(neg ? -v : v);
    const uint64_t integ = u / 1000000ULL;
    const uint64_t frac  = u % 1000000ULL;
    char buf[48];
    const int n = std::snprintf(buf, sizeof(buf), "%s%llu.%06llu",
                                neg ? "-" : "",
                                static_cast<unsigned long long>(integ),
                                static_cast<unsigned long long>(frac));
    if (n > 0) out.append(buf, static_cast<size_t>(n));
}

/// @brief uint64 直接 append 到 string（avoid ostringstream alloc）。
inline void append_u64(std::string& out, uint64_t v) {
    char buf[32];
    const int n = std::snprintf(buf, sizeof(buf), "%llu",
                                static_cast<unsigned long long>(v));
    if (n > 0) out.append(buf, static_cast<size_t>(n));
}

/// @brief int64 (signed) 直接 append。
inline void append_i64(std::string& out, int64_t v) {
    char buf[32];
    const int n = std::snprintf(buf, sizeof(buf), "%lld",
                                static_cast<long long>(v));
    if (n > 0) out.append(buf, static_cast<size_t>(n));
}

/// @brief uint32 直接 append（trading_date / action_time 等）。
inline void append_u32(std::string& out, uint32_t v) {
    char buf[16];
    const int n = std::snprintf(buf, sizeof(buf), "%u", v);
    if (n > 0) out.append(buf, static_cast<size_t>(n));
}

/// @brief JSON string escape；合法 UTF-8 原样透传，非法高位字节逐字节 `\u00XX`。
/// @note 调用方仍应在源头做 GBK→UTF-8；这里是防止 tap 点污染 JSONL 的最后防线。
void json_escape_to(std::string& out, const char* s) {
    if (!s) return;
    auto emit_escaped = [&out](unsigned char b) {
        char esc[8];
        const int n = std::snprintf(esc, sizeof(esc), "\\u%04x", b);
        if (n > 0) out.append(esc, static_cast<size_t>(n));
    };
    const unsigned char* p = reinterpret_cast<const unsigned char*>(s);
    while (*p) {
        unsigned char c = *p;
        switch (c) {
            case '"':  out += "\\\""; ++p; continue;
            case '\\': out += "\\\\"; ++p; continue;
            case '\n': out += "\\n";  ++p; continue;
            case '\r': out += "\\r";  ++p; continue;
            case '\t': out += "\\t";  ++p; continue;
        }
        if (c < 0x20) {
            emit_escaped(c);
            ++p;
            continue;
        }
        if (c < 0x80) {
            out += static_cast<char>(c);
            ++p;
            continue;
        }
        // 非 ASCII: 验证是否合法 UTF-8 多字节序列
        size_t need = 0;
        if      ((c & 0xE0) == 0xC0) need = 1;       // 110xxxxx → 2-byte
        else if ((c & 0xF0) == 0xE0) need = 2;       // 1110xxxx → 3-byte
        else if ((c & 0xF8) == 0xF0) need = 3;       // 11110xxx → 4-byte
        else { emit_escaped(c); ++p; continue; }     // 10xxxxxx 或 11111xxx = 非法 lead
        bool valid = true;
        for (size_t i = 1; i <= need; ++i) {
            if ((p[i] & 0xC0) != 0x80) { valid = false; break; }
        }
        if (valid) {
            for (size_t i = 0; i <= need; ++i) out += static_cast<char>(p[i]);
            p += need + 1;
        } else {
            emit_escaped(c);
            ++p;
        }
    }
}

/// @brief 进程级全局 recorder 指针（process-global recorder pointer）。
/// @note tap point 侧通过 `XtLiveRecorder::global()` 读取；
///       bootstrap 在 create 完成后 `set_global(&recorder)`；
///       析构前必须 `set_global(nullptr)` 避免悬挂。
std::atomic<XtLiveRecorder*> g_recorder{nullptr};

// ── Order-id keyed side-channel registries ─────────────────────────────────
// bounded sharded map: note 在 gateway/control 线程，consume 在 event-loop 的
// ORDER_RTN tap。正常路径 entry 在 ms 级被 consume；容量上限抵御异常 (无回报)
// 导致的泄漏，超过上限丢弃新 entry 不影响正确性。
void copy_fixed_outln(char* dst, size_t dst_cap, const char* src) noexcept {
    XtCStringUtil::copy(dst, dst_cap, src);
}

struct BoundedOrderStrRegistry {
    static constexpr size_t kShards   = 8;
    static constexpr size_t kPerShard = 128;
    static_assert((kShards & (kShards - 1)) == 0,
                  "kShards must be power of two for `& (kShards-1)` mask");

    std::array<std::mutex, kShards>                                mus;
    std::array<std::unordered_map<uint64_t, std::string>, kShards> maps;

    /// @brief 按 order_id 派发分片索引（low-bit mask, power-of-two shards）。
    static size_t shard(uint64_t order_id) noexcept {
        return static_cast<size_t>(order_id) & (kShards - 1);
    }

    void note(uint64_t order_id, const char* value) noexcept {
        const size_t s = shard(order_id);
        try {
            std::lock_guard<std::mutex> g(mus[s]);
            auto& bucket = maps[s];
            if (bucket.size() >= kPerShard && bucket.find(order_id) == bucket.end()) {
                return;  // 分片上限保护: 丢弃新 entry, 不影响已注册 order
            }
            bucket[order_id] = value;
        } catch (...) {
            // 静默: registry 异常不能影响回报热路径
        }
    }

    /// @brief 消费并清除 order_id 对应的 value；未命中写入 fallback。
    void consume(uint64_t order_id, char* out, size_t cap, const char* fallback) noexcept {
        if (!out || cap == 0) return;
        const char* str = fallback;
        std::string tmp;
        const size_t s = shard(order_id);
        try {
            std::lock_guard<std::mutex> g(mus[s]);
            auto& bucket = maps[s];
            auto it = bucket.find(order_id);
            if (it != bucket.end()) {
                tmp = std::move(it->second);
                bucket.erase(it);
                str = tmp.c_str();
            }
        } catch (...) {
            // 静默
        }
        copy_fixed_outln(out, cap, str);
    }
};

BoundedOrderStrRegistry g_cancel_initiators;  ///< cancel 发起方；consume 未命中 → "auto"
BoundedOrderStrRegistry g_reject_errs;        ///< venue 拒单原因；consume 未命中 → 空串

} // namespace

void XtLiveRecorder::set_global(XtLiveRecorder* rec) noexcept {
    g_recorder.store(rec, std::memory_order_release);
}

XtLiveRecorder* XtLiveRecorder::global() noexcept {
    return g_recorder.load(std::memory_order_acquire);
}

bool XtLiveRecorder::sample_intent_trace(uint64_t trace_id, double sample_rate) noexcept {
    if (!std::isfinite(sample_rate) || sample_rate <= 0.0) return false;
    if (sample_rate >= 1.0) return true;
    // 使用高 53 bit 映射到 [0,1)，避免 uint64→double 低位舍入影响边界稳定性。
    const uint64_t mixed = mix_trace_id(trace_id);
    const double unit = static_cast<double>(mixed >> 11) * (1.0 / 9007199254740992.0);
    return unit < sample_rate;
}

// ── Order-id 侧信道 registry（cancel initiator / venue reject err） ──────────

void XtLiveRecorder::note_cancel_initiated(uint64_t order_id, const char* initiator) noexcept {
    if (order_id == 0 || !initiator || initiator[0] == '\0') return;
    g_cancel_initiators.note(order_id, initiator);
}

void XtLiveRecorder::consume_cancel_initiator(uint64_t order_id, char* out, size_t cap) noexcept {
    g_cancel_initiators.consume(order_id, out, cap, "auto");
}

void XtLiveRecorder::note_order_reject(uint64_t order_id, int err_code, const char* err_msg) noexcept {
    const bool has_msg = err_msg && err_msg[0] != '\0';
    if (order_id == 0 || (err_code == 0 && !has_msg)) return;
    try {
        // err_code 非 0 时格式化 "<code>: <msg>"，analyzer 可按数值前缀聚合。
        std::string v;
        if (err_code != 0) {
            v = std::to_string(err_code);
            if (has_msg) {
                v += ": ";
                v += err_msg;
            }
        } else {
            v = err_msg;
        }
        g_reject_errs.note(order_id, v.c_str());
    } catch (...) {
        // 静默: telemetry 不影响拒单回报路径
    }
}

void XtLiveRecorder::consume_order_reject_err(uint64_t order_id, char* out, size_t cap) noexcept {
    g_reject_errs.consume(order_id, out, cap, "");
}

XtLiveRecorder::XtLiveRecorder(const Config& cfg) : cfg_(cfg) {
    if (cfg_.queue_capacity == 0) cfg_.queue_capacity = 8192;
    // MPSC：多个 producer 线程（CTP 回调 / 策略 / EventLoop）+ 单 drain consumer。
    queue_ = std::make_unique<XtLockFreeQueue<Event>>(
        QueueMode::MPSC, cfg_.queue_capacity, /*use_huge_pages=*/false);

    // drain 线程独占的 FlatBufferBuilder，序列化 MD_TICK fbs 时 Clear() 复用
    fbb_handle_ = new ::flatbuffers::FlatBufferBuilder(512);
}

XtLiveRecorder::~XtLiveRecorder() {
    stop();
    if (fbb_handle_) {
        delete static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_handle_);
        fbb_handle_ = nullptr;
    }
}

void XtLiveRecorder::set_event_store(ct::data::IEventStore* store) noexcept {
    // caller (XtLiveBootstrap) 在 start() 后调用。store release 与 drain
    // 后续 acquire 形成 happens-before，store 内部状态对 drain 可见。
    event_store_.store(store, std::memory_order_release);
}

void XtLiveRecorder::set_venue_kind(int8_t kind) noexcept {
    venue_kind_.store(kind, std::memory_order_release);
}

namespace {
/// @brief 从 exchange_id 推导 fbs SourceKind。未知交易所返回 UNKNOWN。
/// @note drain 线程冷路径调用，~5ns/check (strncmp + branch)。
inline ::ct::data::schema::SourceKind venue_kind_from_exchange(const char* exchange_id) noexcept {
    using SK = ::ct::data::schema::SourceKind;
    if (exchange_id && std::strncmp(exchange_id, "BINANCE", 8) == 0) return SK::LIVE_BINANCE;
    if (!exchange_id) return SK::UNKNOWN;
    if (std::strcmp(exchange_id, "SHFE") == 0 ||
        std::strcmp(exchange_id, "DCE") == 0 ||
        std::strcmp(exchange_id, "CFFEX") == 0 ||
        std::strcmp(exchange_id, "CZCE") == 0 ||
        std::strcmp(exchange_id, "GFEX") == 0 ||
        std::strcmp(exchange_id, "INE") == 0) {
        return SK::LIVE_CTP;
    }
    if (std::strcmp(exchange_id, "SSE") == 0 || std::strcmp(exchange_id, "SZSE") == 0) {
        return SK::LIVE_XTP;
    }
    return SK::UNKNOWN;
}
}  // namespace

void XtLiveRecorder::start() {
    if (!cfg_.enabled) return;
    if (running_.load(std::memory_order_acquire)) return;

    // 兜底：自动创建父目录，避免 "enabled 但因目录缺失而静默禁用" 的运维陷阱。
    // 如果 file_path 无父目录（纯文件名），path.parent_path() 返回空，create_directories(空) 为 no-op。
    try {
        const std::filesystem::path p(cfg_.file_path);
        if (p.has_parent_path()) {
            std::filesystem::create_directories(p.parent_path());
        }
    } catch (const std::exception&) {
        // 目录创建失败走下面 open 的失败分支（禁用 recorder）
    }

    // 启动时 rotate 已存在的 live.jsonl → live.jsonl.<wallclock>，新 session 使用干净文件。
    // 动机: fork_analysis 按 logical_id 聚合事件; 若同文件跨 session append, 上个 session 的
    // lid=N 和新 session 的 lid=N (id 池从小重用) 被误判为同一订单 → 假阳 over_fill.
    // 运行时按 size/time 滚动（maybe_rotate）是同一 session 内切分，lid 单调不重用，不涉此假阳。
    try {
        const std::filesystem::path jsonl_path(cfg_.file_path);
        if (std::filesystem::exists(jsonl_path) && std::filesystem::file_size(jsonl_path) > 0) {
            // 同秒重启（crashloop）也不覆盖上个 session 的滚动文件：遇占用 ++ 后缀。
            const std::filesystem::path rotated = unique_rotation_target(jsonl_path, wall_now_sec());
            std::filesystem::rename(jsonl_path, rotated);
        }
    } catch (const std::exception&) {
        // rotate 失败不致命：继续以 append 模式写入同一文件，telemetry 降级但交易不受影响。
    }
    auto* ofs = new std::ofstream(cfg_.file_path, std::ios::out | std::ios::trunc);
    if (!ofs->is_open()) {
        // 打开失败：禁用 recorder，避免后续 emit 累积无效数据。
        delete ofs;
        cfg_.enabled = false;
        return;
    }
    ofs_handle_ = ofs;
    current_file_bytes_ = 0;
    file_opened_wall_sec_ = wall_now_sec();

    running_.store(true, std::memory_order_release);
    // drain 线程边界守卫：序列化/IO 异常不得逃出线程 (否则 std::terminate)；
    // telemetry 失败不影响交易，swallow (不升级停机)。
    worker_ = std::thread([this] {
        xt_log::run_guarded("REC", "drain_loop", [this] { drain_loop(); });
    });
}

void XtLiveRecorder::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;

    if (worker_.joinable()) worker_.join();

    if (ofs_handle_) {
        auto* ofs = static_cast<std::ofstream*>(ofs_handle_);
        ofs->flush();
        if (!ofs->good()) {
            write_failed_count_.fetch_add(1, std::memory_order_relaxed);
            ofs->clear();
        }
        ofs->close();
        delete ofs;
        ofs_handle_ = nullptr;
    }
}

void XtLiveRecorder::drain_loop() {
    // 批量 drain：减少 pop 调用次数，提升吞吐。
    constexpr size_t kBatch = 256;
    Event batch[kBatch];

    while (running_.load(std::memory_order_acquire)) {
        const size_t n = queue_->pop_bulk(batch, kBatch);
        if (n == 0) {
            // 空闲：按时间滚动当前文件（低量 session 也定期产出 .epoch）。
            maybe_rotate();
            // 队列空，短暂休眠避免 busy-spin（此线程非热路径，latency 不敏感）。
            std::this_thread::sleep_for(std::chrono::microseconds(500));
            continue;
        }
        for (size_t i = 0; i < n; ++i) {
            serialize_and_write(batch[i]);
        }
        // 批处理边界：按大小/时间滚动，避免单文件无界增长。
        maybe_rotate();
    }

    // Stop 后 flush 剩余：running_ 已置 false，producer 仍可能已入队的事件需要落盘。
    while (true) {
        const size_t n = queue_->pop_bulk(batch, kBatch);
        if (n == 0) break;
        for (size_t i = 0; i < n; ++i) {
            serialize_and_write(batch[i]);
        }
    }
}

void XtLiveRecorder::maybe_rotate() {
    if (!ofs_handle_ || current_file_bytes_ == 0) return;
    const bool by_size =
        cfg_.rotate_bytes > 0 && current_file_bytes_ >= cfg_.rotate_bytes;
    const int64_t now = wall_now_sec();
    const bool by_time =
        cfg_.rotate_interval_sec > 0 &&
        (now - file_opened_wall_sec_) >= static_cast<int64_t>(cfg_.rotate_interval_sec);
    if (!by_size && !by_time) return;
    rotate_active_file(by_size ? "size" : "time", now);
}

void XtLiveRecorder::rotate_active_file(const char* reason, int64_t now_sec) {
    // 1. 关闭当前文件（drain 线程独占，无并发写）。
    if (ofs_handle_) {
        auto* ofs = static_cast<std::ofstream*>(ofs_handle_);
        ofs->flush();
        if (!ofs->good()) {
            write_failed_count_.fetch_add(1, std::memory_order_relaxed);
            ofs->clear();
        }
        ofs->close();
        delete ofs;
        ofs_handle_ = nullptr;
    }
    // 2. rename live.jsonl → live.jsonl.<epoch>（纯数字后缀，etl 按 epoch 秒解析日期）。
    //    同秒冲突时 ++ 后缀；rename 失败不致命：数据仍在原文件，降级到 append。
    std::string rotated_name;
    try {
        const std::filesystem::path jsonl(cfg_.file_path);
        if (std::filesystem::exists(jsonl)) {
            const std::filesystem::path rotated = unique_rotation_target(jsonl, now_sec);
            std::filesystem::rename(jsonl, rotated);
            rotated_name = rotated.filename().string();
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[recorder] rotate rename failed: %s (continuing)\n", e.what());
    }
    // 3. 联动滚动并联 fbs event store，保持 events.<epoch> 命名可被 prune glob 识别。
    if (auto* es = event_store_.load(std::memory_order_acquire)) {
        try {
            const std::string ev = es->rotate();
            if (!ev.empty()) {
                std::fprintf(stderr, "[recorder] fbs events rotated -> %s\n", ev.c_str());
            }
        } catch (const std::exception& e) {
            std::fprintf(stderr, "[recorder] fbs events rotate failed: %s (continuing)\n", e.what());
        }
    }
    // 4. 重开新 live.jsonl。失败（罕见：空文件创建一般不因 ENOSPC 失败，多为权限/路径问题）→
    //    JSONL + fbs 两路都暂停（serialize_and_write 在 ofs 句柄空时 early-return），trading 不受影响，
    //    由磁盘水位告警兜底；已 rename 的旧文件数据不丢；下次进程重启自愈。
    const uint64_t rolled = current_file_bytes_;
    auto* nofs = new std::ofstream(cfg_.file_path, std::ios::out | std::ios::trunc);
    if (!nofs->is_open()) {
        delete nofs;
        current_file_bytes_ = 0;
        file_opened_wall_sec_ = now_sec;
        write_failed_count_.fetch_add(1, std::memory_order_relaxed);
        std::fprintf(stderr, "[recorder] rotate reopen FAILED file=%s; recording paused\n",
                     cfg_.file_path.c_str());
        return;
    }
    ofs_handle_ = nofs;
    current_file_bytes_ = 0;
    file_opened_wall_sec_ = wall_now_sec();
    const uint64_t total = rotation_count_.fetch_add(1, std::memory_order_relaxed) + 1;
    std::fprintf(stderr,
                 "[recorder] rotated live.jsonl -> %s reason=%s bytes=%llu rotations=%llu\n",
                 rotated_name.empty() ? "(rename-skipped)" : rotated_name.c_str(),
                 reason,
                 static_cast<unsigned long long>(rolled),
                 static_cast<unsigned long long>(total));
}

void XtLiveRecorder::serialize_and_write(const Event& ev) {
    auto note_write_failed = [this, &ev] {
        write_failed_count_.fetch_add(1, std::memory_order_relaxed);
        if (ev.type == kEvtIntentTrace) {
            intent_trace_write_failed_count_.fetch_add(1, std::memory_order_relaxed);
        }
    };
    if (!ofs_handle_) {
        note_write_failed();
        return;
    }
    auto& ofs = *static_cast<std::ofstream*>(ofs_handle_);

    // drain 线程开始处理事件前 acquire-load fbs sink 句柄。set_event_store
    // 的 release 写与本处 acquire 配对，store 内部状态对 drain 可见。
    auto* const es = event_store_.load(std::memory_order_acquire);

    // drain 线程独占复用 JSON buffer；clear() 保留 capacity，稳态避免重复分配。
    thread_local std::string g_serialize_buf;
    if (g_serialize_buf.capacity() < 2048) {
        g_serialize_buf.reserve(2048);
    }
    g_serialize_buf.clear();
    auto& os = g_serialize_buf;

    os.append("{\"ts_local_ns\":");
    append_u64(os, ev.ts_local_ns);
    if (ev.ts_exch_ns != 0) {
        os.append(",\"ts_exch_ns\":");
        append_u64(os, ev.ts_exch_ns);
    }

    switch (ev.type) {
        case kEvtMdTick:           serialize_md_tick(ev, os, es, fbb_handle_);            break;
        case kEvtStrategyDecision: serialize_strategy_decision(ev, os, es, fbb_handle_);  break;
        case kEvtOrderSubmit:      serialize_order_submit(ev, os, es, fbb_handle_);       break;
        case kEvtOrderRtn:         serialize_order_rtn(ev, os, es, fbb_handle_);          break;
        case kEvtTradeRtn:         serialize_trade_rtn(ev, os, es, fbb_handle_);          break;
        case kEvtConnState:        serialize_conn_state(ev, os, es, fbb_handle_);         break;
        case kEvtStrategyMetric:   serialize_strategy_metric(ev, os, es, fbb_handle_);    break;
        // 新事件类型 dispatch
        case kEvtRiskReject:       serialize_risk_reject(ev, os, es, fbb_handle_);        break;
        case kEvtParamUpdate:      serialize_param_update(ev, os, es, fbb_handle_);       break;
        case kEvtKillSwitch:       serialize_kill_switch(ev, os, es, fbb_handle_);        break;
        case kEvtPositionLifecycle: serialize_position_lifecycle(ev, os, es, fbb_handle_); break;
        case kEvtIntentTrace:      serialize_intent_trace(ev, os, es, fbb_handle_);       break;
        default:                   os.append(",\"type\":\"UNKNOWN\"");                    break;
    }
    os.append("}\n");

    ofs.write(os.data(), static_cast<std::streamsize>(os.size()));
    if (ofs.good()) {
        current_file_bytes_ += os.size();  // drain 独占；跨过 rotate_bytes 则下一批边界滚动。
        written_count_.fetch_add(1, std::memory_order_relaxed);
        if (ev.type == kEvtIntentTrace) {
            intent_trace_written_count_.fetch_add(1, std::memory_order_relaxed);
        }
    } else {
        // 写失败（如磁盘满）：清 failbit 让下条事件重试（磁盘释放后自愈），不计字节
        // → 避免空文件 rotation 风暴；不计 written_count 保持口径诚实。telemetry 失败不影响交易。
        note_write_failed();
        ofs.clear();
    }
}

void XtLiveRecorder::serialize_md_tick(const Event& ev, std::string& os,
                                    ct::data::IEventStore* es, void* fbb_ref) {
    (void)es; (void)fbb_ref;  // helper internals reference these via casts
            os.append(",\"type\":\"MD_TICK\",\"trading_date\":");
            append_u32(os, ev.md_trading_date);
            os.append(",\"action_date\":");
            append_u32(os, ev.md_action_date);
            os.append(",\"action_time\":");
            append_u32(os, ev.md_action_time);
            os.append(",\"exchange_id\":\"");
            json_escape_to(os, ev.md_exchange_id);
            os.append("\",\"code\":\"");
            json_escape_to(os, ev.md_code);
            os.append("\",\"last\":");
            format_fp_to(os, ev.md_last_fp);
            os.append(",\"volume\":");
            append_i64(os, ev.md_volume);
            os.append(",\"bid\":[");
            for (int i = 0; i < kMdDepth; ++i) {
                if (i > 0) os += ',';
                format_fp_to(os, ev.md_bid_prices_fp[i]);
            }
            os.append("],\"ask\":[");
            for (int i = 0; i < kMdDepth; ++i) {
                if (i > 0) os += ',';
                format_fp_to(os, ev.md_ask_prices_fp[i]);
            }
            os.append("],\"bid_vol\":[");
            for (int i = 0; i < kMdDepth; ++i) {
                if (i > 0) os += ',';
                append_i64(os, ev.md_bid_qty[i]);
            }
            os.append("],\"ask_vol\":[");
            for (int i = 0; i < kMdDepth; ++i) {
                if (i > 0) os += ',';
                append_i64(os, ev.md_ask_qty[i]);
            }
            os += ']';

            // 并列写 fbs EventStore；只在 drain 线程执行，producer 热路径不受影响。
            if (es && ev.md_trading_date != 0 &&
                ev.md_exchange_id[0] != '\0' && ev.md_code[0] != '\0') {
                auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
                fbb->Clear();

                using namespace ct::data::schema;
                const auto code_off = fbb->CreateString(ev.md_code);
                const auto exch_off = fbb->CreateString(ev.md_exchange_id);
                // depth 契约 v2：按 venue 真实档数写变长 depth。clamp 防御
                // 未填/越界的 md_depth_levels；下界取 1 是 lossy 取向（漏传
                // caller 只录 L1），emit_md_tick 参数无默认值由编译期强制。
                const int depth =
                    ev.md_depth_levels < 1 ? 1
                    : (ev.md_depth_levels > ct::data::kEventStoreMdDepth
                           ? ct::data::kEventStoreMdDepth
                           : ev.md_depth_levels);
                const auto bid_px_off  = fbb->CreateVector(ev.md_bid_prices_fp, depth);
                const auto ask_px_off  = fbb->CreateVector(ev.md_ask_prices_fp, depth);
                const auto bid_qty_off = fbb->CreateVector(ev.md_bid_qty, depth);
                const auto ask_qty_off = fbb->CreateVector(ev.md_ask_qty, depth);

                // 优先用 exchange_id 派生；未知 exchange 写 UNKNOWN，避免冒充 CTP。
                const auto kind = venue_kind_from_exchange(ev.md_exchange_id);
                // fbs wire 契约 total_volume_fp 统一为 qty ×1e6：CTP/XTP tap 传 raw
                // 手数、binance 传 fp（venue-native），仅在此单点归一；JSONL 侧保持
                // venue-native 直通（v1 wire 兼容，消费方按 exchange 判单位）。
                int64_t total_volume_fp = ev.md_volume;
                bool volume_unit_ok = true;
                if (kind == SourceKind::LIVE_CTP || kind == SourceKind::LIVE_XTP) {
                    // 与 reader 同款守卫：越界/负值不乘也不盖章，该记录由 reader fail-fast。
                    if (total_volume_fp < 0 ||
                        total_volume_fp >
                            std::numeric_limits<int64_t>::max() / 1'000'000) {
                        volume_unit_ok = false;
                    } else {
                        total_volume_fp *= 1'000'000;
                    }
                }

                MdTickBuilder b(*fbb);
                b.add_code(code_off);
                b.add_exchange_id(exch_off);
                b.add_trading_day(ev.md_trading_date);
                b.add_action_day(ev.md_action_date);
                b.add_action_time(ev.md_action_time);
                b.add_price_fp(ev.md_last_fp);
                b.add_total_volume_fp(total_volume_fp);
                b.add_bid_price_fp(bid_px_off);
                b.add_ask_price_fp(ask_px_off);
                b.add_bid_qty_fp(bid_qty_off);
                b.add_ask_qty_fp(ask_qty_off);
                b.add_ts_local_ns(ev.ts_local_ns);
                b.add_source_kind(kind);
                b.add_depth_levels_real(static_cast<uint8_t>(depth));
                // Event 当前不携带 settle/open/high/low/close，fbs 聚合字段保持 absent。
                b.add_has_settlement(false);
                // kind 未知或归一失败时不盖 FP_1E6 章（单位未归一）；该记录会被 reader 拒。
                if (volume_unit_ok && kind != SourceKind::UNKNOWN) {
                    b.add_volume_encoding(VolumeEncoding::FP_1E6);
                }
                fbb->Finish(b.Finish(), "XTEV");

                if (!es->append(ev.md_trading_date,
                                ev.md_exchange_id,
                                ev.md_code,
                                fbb->GetBufferPointer(),
                                fbb->GetSize())) {
                    // fbs sink 落盘失败（磁盘满/权限/非法路由 key）：JSONL 仍在，但 fbs
                    // 流断档。rate-limited 告警（首次 + 每 1000 次）避免每 tick 刷屏；
                    // store 内部不持 logger，可观测性由本 caller 承担。
                    if (++fbs_append_fail_count_ == 1 ||
                        fbs_append_fail_count_ % 1000 == 0) {
                        std::fprintf(stderr,
                            "[recorder] fbs append failed (count=%llu day=%u code=%s)\n",
                            static_cast<unsigned long long>(fbs_append_fail_count_),
                            ev.md_trading_date, ev.md_code);
                    }
                }
            }
            // 记最近 trading_day 给非 MD 事件用（drain 独占）
            if (ev.md_trading_date != 0) current_trading_day_ = ev.md_trading_date;
}
void XtLiveRecorder::serialize_strategy_decision(const Event& ev, std::string& os,
                                    ct::data::IEventStore* es, void* fbb_ref) {
    (void)es; (void)fbb_ref;  // helper internals reference these via casts
            os.append(",\"type\":\"STRATEGY_DECISION\",\"strat_id\":\"");
            json_escape_to(os, ev.sd_strat_id);
            os.append("\",\"code\":\"");
            json_escape_to(os, ev.sd_code);
            os.append("\",\"signal\":\"");
            json_escape_to(os, ev.sd_signal);
            os.append("\",\"target_pos\":");
            append_i64(os, ev.sd_target_pos);
            os.append(",\"trigger_tick_ts\":");
            append_u64(os, ev.sd_trigger_tick_ts);
            // 新字段仅非空时输出（向后兼容）
            if (ev.sd_book_id[0] != '\0') {
                os.append(",\"book_id\":\"");
                json_escape_to(os, ev.sd_book_id);
                os += '"';
            }
            if (ev.sd_account_id[0] != '\0') {
                os.append(",\"account_id\":\"");
                json_escape_to(os, ev.sd_account_id);
                os += '"';
            }

            // build EventExt(StrategyDecision) → append_event_ext
            if (es && current_trading_day_ != 0) {
                auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
                fbb->Clear();
                using namespace ct::data::schema;
                const auto strat_off   = fbb->CreateString(ev.sd_strat_id);
                const auto code_off    = fbb->CreateString(ev.sd_code);
                const auto signal_off  = fbb->CreateString(ev.sd_signal);
                const auto book_off    = fbb->CreateString(ev.sd_book_id);
                const auto account_off = fbb->CreateString(ev.sd_account_id);
                StrategyDecisionBuilder sb(*fbb);
                sb.add_ts_local_ns(ev.ts_local_ns);
                sb.add_trading_day(current_trading_day_);
                sb.add_strat_id(strat_off);
                sb.add_code(code_off);
                sb.add_signal(signal_off);
                sb.add_target_pos(ev.sd_target_pos);
                sb.add_trigger_tick_ts(ev.sd_trigger_tick_ts);
                sb.add_source_kind(static_cast<SourceKind>(venue_kind_.load(std::memory_order_relaxed)));  // venue 兜底（非 MD 事件）
                sb.add_book_id(book_off);
                sb.add_account_id(account_off);
                const auto sd_off = sb.Finish();
                EventExtBuilder eb(*fbb);
                eb.add_payload_type(EventExtPayload::StrategyDecision);
                eb.add_payload(sd_off.Union());
                fbb->Finish(eb.Finish(), "XTEX");
                es->append_event_ext(current_trading_day_,
                                                fbb->GetBufferPointer(),
                                                fbb->GetSize());
            }
}
void XtLiveRecorder::serialize_order_submit(const Event& ev, std::string& os,
                                    ct::data::IEventStore* es, void* fbb_ref) {
    (void)es; (void)fbb_ref;  // helper internals reference these via casts
            os.append(",\"type\":\"ORDER_SUBMIT\",\"logical_id\":");
            append_u64(os, ev.os_logical_id);
            os.append(",\"code\":\"");
            json_escape_to(os, ev.os_code);
            os.append("\",\"dir\":\"");
            json_escape_to(os, ev.os_dir);
            os.append("\",\"offset\":\"");
            json_escape_to(os, ev.os_offset);
            os.append("\",\"price\":");
            format_fp_to(os, ev.os_price_fp);
            os.append(",\"qty\":");
            append_i64(os, ev.os_qty);
            os.append(",\"parent_strat\":\"");
            json_escape_to(os, ev.os_parent_strat);
            os += '"';
            if (ev.os_decision_ts != 0) {
                os.append(",\"decision_ts\":");
                append_u64(os, ev.os_decision_ts);
            }
            if (ev.os_trigger_tick_ts != 0) {
                os.append(",\"trigger_tick_ts\":");
                append_u64(os, ev.os_trigger_tick_ts);
            }
            // event clock 投影（BestEffort）：valid 才写两字段，旧 analyzer
            // dict.get() 容忍缺失（append-only 契约，同 decision_ts 先例）。
            if (ev.os_event_clock_valid) {
                os.append(",\"event_clock_valid\":true");
                os.append(",\"event_ts_local_ns\":");
                append_i64(os, ev.os_event_ts_local_ns);
            }
            if (ev.os_reason[0] != '\0') {
                os.append(",\"reason\":\"");
                json_escape_to(os, ev.os_reason);
                os += '"';
            }
            // multi-strategy routing metadata
            if (ev.os_strat_id[0] != '\0') {
                os.append(",\"strat_id\":\"");
                json_escape_to(os, ev.os_strat_id);
                os += '"';
            }
            if (ev.os_book_id[0] != '\0') {
                os.append(",\"book_id\":\"");
                json_escape_to(os, ev.os_book_id);
                os += '"';
            }
            if (ev.os_account_id[0] != '\0') {
                os.append(",\"account_id\":\"");
                json_escape_to(os, ev.os_account_id);
                os += '"';
            }
            if (ev.os_user_tag[0] != '\0') {
                os.append(",\"user_tag\":\"");
                json_escape_to(os, ev.os_user_tag);
                os += '"';
            }
            if (ev.os_decision_mono_ns != 0) {
                os.append(",\"decision_mono_ns\":");
                append_u64(os, ev.os_decision_mono_ns);
            }

            // build EventExt(OrderSubmit) → append_event_ext
            if (es && current_trading_day_ != 0) {
                auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
                fbb->Clear();
                using namespace ct::data::schema;
                const auto code_off    = fbb->CreateString(ev.os_code);
                const auto dir_off     = fbb->CreateString(ev.os_dir);
                const auto offset_off  = fbb->CreateString(ev.os_offset);
                const auto strat_off   = fbb->CreateString(ev.os_parent_strat);
                const auto reason_off  = fbb->CreateString(ev.os_reason);
                const auto sid_off     = fbb->CreateString(ev.os_strat_id);
                const auto book_off    = fbb->CreateString(ev.os_book_id);
                const auto account_off = fbb->CreateString(ev.os_account_id);
                const auto user_tag_off = fbb->CreateString(ev.os_user_tag);
                OrderSubmitBuilder ob(*fbb);
                ob.add_ts_local_ns(ev.ts_local_ns);
                ob.add_trading_day(current_trading_day_);
                ob.add_code(code_off);
                ob.add_logical_id(ev.os_logical_id);
                ob.add_dir(dir_off);
                ob.add_offset(offset_off);
                ob.add_price_fp(ev.os_price_fp);
                ob.add_qty(ev.os_qty);
                ob.add_parent_strat(strat_off);
                ob.add_trigger_tick_ts(ev.os_trigger_tick_ts);
                ob.add_reason(reason_off);
                ob.add_source_kind(static_cast<SourceKind>(venue_kind_.load(std::memory_order_relaxed)));  // venue 兜底（非 MD 事件）
                ob.add_strat_id(sid_off);
                ob.add_book_id(book_off);
                ob.add_account_id(account_off);
                ob.add_user_tag(user_tag_off);
                ob.add_decision_mono_ns(ev.os_decision_mono_ns);
                const auto os_off = ob.Finish();

                EventExtBuilder eb(*fbb);
                eb.add_payload_type(EventExtPayload::OrderSubmit);
                eb.add_payload(os_off.Union());
                fbb->Finish(eb.Finish(), "XTEX");
                es->append_event_ext(current_trading_day_,
                                                fbb->GetBufferPointer(),
                                                fbb->GetSize());
            }
#ifdef XT_HAVE_METRICS
            // drain-thread Prometheus metric inc
            ct::metrics::XtMetricRegistry::instance().on_order_submit(
                ev.os_strat_id, ev.os_book_id,
                venue_kind_to_id(venue_kind_.load(std::memory_order_relaxed)),
                ev.os_code, ev.os_dir);
#endif
}
void XtLiveRecorder::serialize_order_rtn(const Event& ev, std::string& os,
                                    ct::data::IEventStore* es, void* fbb_ref) {
    (void)es; (void)fbb_ref;  // helper internals reference these via casts
            os.append(",\"type\":\"ORDER_RTN\",\"logical_id\":");
            append_u64(os, ev.or_logical_id);
            os.append(",\"phys_id\":");
            append_u64(os, ev.or_phys_id);
            os.append(",\"status\":\"");
            json_escape_to(os, ev.or_status);
            os.append("\",\"filled_qty\":");
            append_i64(os, ev.or_filled_qty);
            if (ev.or_err_msg[0] != '\0') {
                os.append(",\"err_msg\":\"");
                json_escape_to(os, ev.or_err_msg);
                os += '"';
            }
            if (ev.or_cancel_initiator[0] != '\0') {
                os.append(",\"cancel_initiator\":\"");
                json_escape_to(os, ev.or_cancel_initiator);
                os += '"';
            }
            // multi-strategy demux 主键
            if (ev.or_strat_id[0] != '\0') {
                os.append(",\"strat_id\":\"");
                json_escape_to(os, ev.or_strat_id);
                os += '"';
            }
            if (ev.or_book_id[0] != '\0') {
                os.append(",\"book_id\":\"");
                json_escape_to(os, ev.or_book_id);
                os += '"';
            }
            if (ev.or_account_id[0] != '\0') {
                os.append(",\"account_id\":\"");
                json_escape_to(os, ev.or_account_id);
                os += '"';
            }

            // build EventExt(OrderRtn) → append_event_ext
            if (es && current_trading_day_ != 0) {
                auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
                fbb->Clear();
                using namespace ct::data::schema;
                const auto status_off  = fbb->CreateString(ev.or_status);
                const auto err_off     = fbb->CreateString(ev.or_err_msg);
                const auto init_off    = fbb->CreateString(ev.or_cancel_initiator);
                const auto sid_off     = fbb->CreateString(ev.or_strat_id);
                const auto book_off    = fbb->CreateString(ev.or_book_id);
                const auto account_off = fbb->CreateString(ev.or_account_id);
                OrderRtnBuilder rb(*fbb);
                rb.add_ts_local_ns(ev.ts_local_ns);
                rb.add_trading_day(current_trading_day_);
                rb.add_logical_id(ev.or_logical_id);
                rb.add_phys_id(ev.or_phys_id);
                rb.add_status(status_off);
                rb.add_filled_qty(ev.or_filled_qty);
                rb.add_err_msg(err_off);
                rb.add_cancel_initiator(init_off);
                rb.add_source_kind(static_cast<SourceKind>(venue_kind_.load(std::memory_order_relaxed)));  // venue 兜底（非 MD 事件）
                rb.add_strat_id(sid_off);
                rb.add_book_id(book_off);
                rb.add_account_id(account_off);
                const auto or_off = rb.Finish();
                EventExtBuilder eb(*fbb);
                eb.add_payload_type(EventExtPayload::OrderRtn);
                eb.add_payload(or_off.Union());
                fbb->Finish(eb.Finish(), "XTEX");
                es->append_event_ext(current_trading_day_,
                                                fbb->GetBufferPointer(),
                                                fbb->GetSize());
            }
#ifdef XT_HAVE_METRICS
            // drain-thread Prometheus metric inc
            // OrderRtn 不带 code (反查 OrderSubmit) — symbol label 暂留空
            ct::metrics::XtMetricRegistry::instance().on_order_rtn(
                ev.or_strat_id, ev.or_book_id,
                venue_kind_to_id(venue_kind_.load(std::memory_order_relaxed)),
                /*symbol=*/"", ev.or_status);
#endif
}
void XtLiveRecorder::serialize_trade_rtn(const Event& ev, std::string& os,
                                    ct::data::IEventStore* es, void* fbb_ref) {
    (void)es; (void)fbb_ref;  // helper internals reference these via casts
            const uint32_t trade_day = ev.tr_trading_day != 0 ? ev.tr_trading_day
                                                              : current_trading_day_;
            const char* route_status =
                canonical_route_status(ev.tr_route_status, ev.tr_strat_id);
            // strategy_accounted 由 event 字段直接判定（route + 非空 strategy/symbol
            // + qty/price>0），不再经 ledger。today_* gauge 由权威 frame-loop emit
            // (set_strategy_ledger_today，来自 XtStrategyPnlAggregator − baseline)。
            const bool strategy_accounted =
                std::string_view(route_status) == "routed"
                && ev.tr_strat_id[0] != '\0' && ev.tr_code[0] != '\0'
                && ev.tr_qty > 0 && ev.tr_price_fp > 0;
            const int64_t realized_pnl_fp = ev.tr_realized_pnl_fp;  // 总是 engine-stamped
            // is_close 由各引擎 event loop 直接 stamp（XtTradeRecord::is_close）。
            const bool is_close = ev.tr_is_close;
            os.append(",\"type\":\"TRADE_RTN\",\"trade_id\":");
            append_u64(os, ev.tr_trade_id);
            os.append(",\"logical_id\":");
            append_u64(os, ev.tr_logical_id);
            if (trade_day != 0) {
                os.append(",\"trading_day\":");
                append_u32(os, trade_day);
            }
            if (ev.tr_code[0] != '\0') {
                os.append(",\"code\":\"");
                json_escape_to(os, ev.tr_code);
                os += '"';
            }
            if (ev.tr_side[0] != '\0') {
                os.append(",\"side\":\"");
                json_escape_to(os, ev.tr_side);
                os += '"';
            }
            os.append(",\"route_status\":\"");
            json_escape_to(os, route_status);
            os += '"';
            os.append(",\"price\":");
            format_fp_to(os, ev.tr_price_fp);
            os.append(",\"qty\":");
            append_i64(os, ev.tr_qty);
            os.append(",\"is_maker\":");
            os.append(ev.tr_is_maker ? "true" : "false");
            if (ev.tr_trade_action_date != 0) {
                os.append(",\"trade_action_date\":");
                append_u32(os, ev.tr_trade_action_date);
            }
            if (ev.tr_trade_action_time != 0) {
                os.append(",\"trade_action_time\":");
                append_u32(os, ev.tr_trade_action_time);
            }
            // multi-strategy routing metadata
            if (ev.tr_strat_id[0] != '\0') {
                os.append(",\"strat_id\":\"");
                json_escape_to(os, ev.tr_strat_id);
                os += '"';
            }
            if (ev.tr_book_id[0] != '\0') {
                os.append(",\"book_id\":\"");
                json_escape_to(os, ev.tr_book_id);
                os += '"';
            }
            if (ev.tr_account_id[0] != '\0') {
                os.append(",\"account_id\":\"");
                json_escape_to(os, ev.tr_account_id);
                os += '"';
            }
            if (ev.tr_fee_fp != 0) {
                os.append(",\"fee\":");
                format_fp_to(os, ev.tr_fee_fp);
            }
            if (realized_pnl_fp != 0) {
                os.append(",\"realized_pnl\":");
                format_fp_to(os, realized_pnl_fp);
            }
            if (is_close) {
                os.append(",\"is_close\":true");
            }

            // build EventExt(TradeRtn) → append_event_ext
            if (es && trade_day != 0) {
                auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
                fbb->Clear();
                using namespace ct::data::schema;
                const auto code_off    = fbb->CreateString(ev.tr_code);
                const auto exch_off    = fbb->CreateString(ev.tr_exchange_id);
                const auto sid_off     = fbb->CreateString(ev.tr_strat_id);
                const auto book_off    = fbb->CreateString(ev.tr_book_id);
                const auto account_off = fbb->CreateString(ev.tr_account_id);
                const auto side_off    = fbb->CreateString(ev.tr_side);
                const auto route_off   = fbb->CreateString(route_status);
                TradeRtnBuilder tb(*fbb);
                tb.add_ts_local_ns(ev.ts_local_ns);
                tb.add_ts_exch_ns(ev.ts_exch_ns);
                tb.add_trade_action_date(ev.tr_trade_action_date);
                tb.add_trade_action_time(ev.tr_trade_action_time);
                tb.add_trading_day(trade_day);
                tb.add_trade_id(ev.tr_trade_id);
                tb.add_logical_id(ev.tr_logical_id);
                tb.add_code(code_off);
                tb.add_exchange_id(exch_off);
                tb.add_price_fp(ev.tr_price_fp);
                tb.add_qty(ev.tr_qty);
                tb.add_source_kind(static_cast<SourceKind>(venue_kind_.load(std::memory_order_relaxed)));  // venue 兜底（非 MD 事件）
                tb.add_strat_id(sid_off);
                tb.add_book_id(book_off);
                tb.add_account_id(account_off);
                tb.add_is_maker(ev.tr_is_maker);
                tb.add_fee_fp(ev.tr_fee_fp);
                tb.add_realized_pnl_fp(realized_pnl_fp);
                tb.add_is_close(is_close);
                tb.add_side(side_off);
                tb.add_route_status(route_off);
                const auto tr_off = tb.Finish();

                EventExtBuilder eb(*fbb);
                eb.add_payload_type(EventExtPayload::TradeRtn);
                eb.add_payload(tr_off.Union());
                fbb->Finish(eb.Finish(), "XTEX");
                es->append_event_ext(trade_day,
                                                fbb->GetBufferPointer(),
                                                fbb->GetSize());
            }
#ifdef XT_HAVE_METRICS
            ct::metrics::XtMetricRegistry::instance().on_normalized_trade(
                ev.tr_strat_id, ev.tr_book_id,
                venue_kind_to_id(venue_kind_.load(std::memory_order_relaxed)),
                ev.tr_code, ev.tr_side, route_status,
                strategy_accounted);
            // drain-thread Prometheus metric inc
            if (strategy_accounted) {
                ct::metrics::XtMetricRegistry::instance().on_trade(
                    ev.tr_strat_id, ev.tr_book_id,
                    venue_kind_to_id(venue_kind_.load(std::memory_order_relaxed)),
                    ev.tr_code, ev.tr_side,
                    ev.tr_qty, ev.tr_price_fp,
                    ev.tr_fee_fp, realized_pnl_fp, ev.tr_is_maker);
            }
#endif
}
void XtLiveRecorder::serialize_conn_state(const Event& ev, std::string& os,
                                    ct::data::IEventStore* es, void* fbb_ref) {
    (void)es; (void)fbb_ref;  // helper internals reference these via casts
            os.append(",\"type\":\"CONN_STATE\",\"gateway\":\"");
            json_escape_to(os, ev.cs_gateway);
            os.append("\",\"state\":\"");
            json_escape_to(os, ev.cs_state);
            os += '"';
            if (ev.cs_err_msg[0] != '\0') {
                os.append(",\"err_msg\":\"");
                json_escape_to(os, ev.cs_err_msg);
                os += '"';
            }
            // venue routing
            if (ev.cs_venue_id[0] != '\0') {
                os.append(",\"venue_id\":\"");
                json_escape_to(os, ev.cs_venue_id);
                os += '"';
            }

            // build EventExt(ConnState) → append_event_ext
            // 早期 connect (current_trading_day_=0) 时 skip — JSONL 已记录
            if (es && current_trading_day_ != 0) {
                auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
                fbb->Clear();
                using namespace ct::data::schema;
                const auto gw_off    = fbb->CreateString(ev.cs_gateway);
                const auto state_off = fbb->CreateString(ev.cs_state);
                const auto err_off   = fbb->CreateString(ev.cs_err_msg);
                const auto venue_off = fbb->CreateString(ev.cs_venue_id);
                ConnStateBuilder cb(*fbb);
                cb.add_ts_local_ns(ev.ts_local_ns);
                cb.add_trading_day(current_trading_day_);
                cb.add_gateway(gw_off);
                cb.add_state(state_off);
                cb.add_err_msg(err_off);
                cb.add_source_kind(static_cast<SourceKind>(venue_kind_.load(std::memory_order_relaxed)));  // venue 兜底（非 MD 事件）
                cb.add_venue_id(venue_off);
                const auto cs_off = cb.Finish();
                EventExtBuilder eb(*fbb);
                eb.add_payload_type(EventExtPayload::ConnState);
                eb.add_payload(cs_off.Union());
                fbb->Finish(eb.Finish(), "XTEX");
                es->append_event_ext(current_trading_day_,
                                                fbb->GetBufferPointer(),
                                                fbb->GetSize());
            }
}
void XtLiveRecorder::serialize_strategy_metric(const Event& ev, std::string& os,
                                                ct::data::IEventStore* es, void* fbb_ref) {
    (void)es; (void)fbb_ref;
            // double -> 定长字符串, 兼容 JSON 数字格式 (避免 -nan/inf 直出破坏 parser)
            char nbuf[48];
            int nlen;
            if (std::isfinite(ev.sm_value)) {
                nlen = std::snprintf(nbuf, sizeof(nbuf), "%.9g", ev.sm_value);
            } else {
                nlen = std::snprintf(nbuf, sizeof(nbuf), "null");
            }
            os.append(",\"type\":\"STRATEGY_METRIC\",\"name\":\"");
            json_escape_to(os, ev.sm_name);
            os.append("\",\"value\":");
            if (nlen > 0) os.append(nbuf, static_cast<size_t>(nlen));
            // book/account/strat routing
            if (ev.sm_strat_id[0] != '\0') {
                os.append(",\"strat_id\":\"");
                json_escape_to(os, ev.sm_strat_id);
                os += '"';
            }
            if (ev.sm_book_id[0] != '\0') {
                os.append(",\"book_id\":\"");
                json_escape_to(os, ev.sm_book_id);
                os += '"';
            }
            if (ev.sm_account_id[0] != '\0') {
                os.append(",\"account_id\":\"");
                json_escape_to(os, ev.sm_account_id);
                os += '"';
            }

            // build EventExt(StrategyMetric) → append_event_ext
            // 跳过 NaN/Inf（JSONL 输出 null，fbs schema 是 double 不能直存 NaN）
            if (es && current_trading_day_ != 0 && std::isfinite(ev.sm_value)) {
                auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
                fbb->Clear();
                using namespace ct::data::schema;
                const auto name_off    = fbb->CreateString(ev.sm_name);
                const auto strat_off   = fbb->CreateString(ev.sm_strat_id);
                const auto book_off    = fbb->CreateString(ev.sm_book_id);
                const auto account_off = fbb->CreateString(ev.sm_account_id);
                StrategyMetricBuilder mb(*fbb);
                mb.add_ts_local_ns(ev.ts_local_ns);
                mb.add_trading_day(current_trading_day_);
                mb.add_strat_id(strat_off);
                mb.add_name(name_off);
                mb.add_value(ev.sm_value);
                mb.add_source_kind(static_cast<SourceKind>(venue_kind_.load(std::memory_order_relaxed)));  // venue 兜底（非 MD 事件）
                mb.add_book_id(book_off);
                mb.add_account_id(account_off);
                const auto sm_off = mb.Finish();
                EventExtBuilder eb(*fbb);
                eb.add_payload_type(EventExtPayload::StrategyMetric);
                eb.add_payload(sm_off.Union());
                fbb->Finish(eb.Finish(), "XTEX");
                es->append_event_ext(current_trading_day_,
                                                fbb->GetBufferPointer(),
                                                fbb->GetSize());
            }
}

// ─── 新事件类型 serialize ─────────────────────────────────────────────────────

void XtLiveRecorder::serialize_risk_reject(const Event& ev, std::string& os,
                                           ct::data::IEventStore* es, void* fbb_ref) {
    os.append(",\"type\":\"RISK_REJECT\",\"strat_id\":\"");
    json_escape_to(os, ev.rj_strat_id);
    os.append("\",\"book_id\":\"");
    json_escape_to(os, ev.rj_book_id);
    os.append("\",\"account_id\":\"");
    json_escape_to(os, ev.rj_account_id);
    os.append("\",\"code\":\"");
    json_escape_to(os, ev.rj_code);
    os.append("\",\"dir\":\"");
    json_escape_to(os, ev.rj_dir);
    os.append("\",\"price\":");
    format_fp_to(os, ev.rj_price_fp);
    os.append(",\"qty\":");
    append_i64(os, ev.rj_qty);
    os.append(",\"reason\":\"");
    json_escape_to(os, ev.rj_reason);
    os += '"';
    if (ev.rj_required_margin_fp != 0) {
        os.append(",\"required_margin\":");
        format_fp_to(os, ev.rj_required_margin_fp);
    }
    if (ev.rj_available_margin_fp != 0) {
        os.append(",\"available_margin\":");
        format_fp_to(os, ev.rj_available_margin_fp);
    }

    if (es && current_trading_day_ != 0) {
        auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
        fbb->Clear();
        using namespace ct::data::schema;
        const auto sid_off     = fbb->CreateString(ev.rj_strat_id);
        const auto book_off    = fbb->CreateString(ev.rj_book_id);
        const auto account_off = fbb->CreateString(ev.rj_account_id);
        const auto code_off    = fbb->CreateString(ev.rj_code);
        const auto dir_off     = fbb->CreateString(ev.rj_dir);
        const auto reason_off  = fbb->CreateString(ev.rj_reason);
        RiskRejectBuilder rb(*fbb);
        rb.add_ts_local_ns(ev.ts_local_ns);
        rb.add_trading_day(current_trading_day_);
        rb.add_strat_id(sid_off);
        rb.add_book_id(book_off);
        rb.add_account_id(account_off);
        rb.add_code(code_off);
        rb.add_dir(dir_off);
        rb.add_price_fp(ev.rj_price_fp);
        rb.add_qty(ev.rj_qty);
        rb.add_reason(reason_off);
        rb.add_required_margin_fp(ev.rj_required_margin_fp);
        rb.add_available_margin_fp(ev.rj_available_margin_fp);
        rb.add_source_kind(static_cast<SourceKind>(venue_kind_.load(std::memory_order_relaxed)));
        const auto rj_off = rb.Finish();
        EventExtBuilder eb(*fbb);
        eb.add_payload_type(EventExtPayload::RiskReject);
        eb.add_payload(rj_off.Union());
        fbb->Finish(eb.Finish(), "XTEX");
        es->append_event_ext(current_trading_day_, fbb->GetBufferPointer(), fbb->GetSize());
    }
#ifdef XT_HAVE_METRICS
    // drain-thread Prometheus metric inc
    ct::metrics::XtMetricRegistry::instance().on_risk_reject(
        ev.rj_strat_id, ev.rj_book_id,
        venue_kind_to_id(venue_kind_.load(std::memory_order_relaxed)),
        ev.rj_code, ev.rj_reason);
#endif
}

void XtLiveRecorder::serialize_param_update(const Event& ev, std::string& os,
                                            ct::data::IEventStore* es, void* fbb_ref) {
    os.append(",\"type\":\"PARAM_UPDATE\",\"actor\":\"");
    json_escape_to(os, ev.pu_actor);
    os.append("\",\"strat_id\":\"");
    json_escape_to(os, ev.pu_strat_id);
    os.append("\",\"key\":\"");
    json_escape_to(os, ev.pu_key);
    os.append("\",\"value\":\"");
    json_escape_to(os, ev.pu_value_str);
    os += '"';
    if (ev.pu_apply_mono_ns != 0) {
        os.append(",\"apply_mono_ns\":");
        append_u64(os, ev.pu_apply_mono_ns);
    }

    if (es && current_trading_day_ != 0) {
        auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
        fbb->Clear();
        using namespace ct::data::schema;
        const auto actor_off = fbb->CreateString(ev.pu_actor);
        const auto sid_off   = fbb->CreateString(ev.pu_strat_id);
        const auto key_off   = fbb->CreateString(ev.pu_key);
        const auto value_off = fbb->CreateString(ev.pu_value_str);
        ParamUpdateBuilder pb(*fbb);
        pb.add_ts_local_ns(ev.ts_local_ns);
        pb.add_trading_day(current_trading_day_);
        pb.add_actor(actor_off);
        pb.add_strat_id(sid_off);
        pb.add_key(key_off);
        pb.add_value_str(value_off);
        pb.add_apply_mono_ns(ev.pu_apply_mono_ns);
        pb.add_source_kind(static_cast<SourceKind>(venue_kind_.load(std::memory_order_relaxed)));
        const auto pu_off = pb.Finish();
        EventExtBuilder eb(*fbb);
        eb.add_payload_type(EventExtPayload::ParamUpdate);
        eb.add_payload(pu_off.Union());
        fbb->Finish(eb.Finish(), "XTEX");
        es->append_event_ext(current_trading_day_, fbb->GetBufferPointer(), fbb->GetSize());
    }
#ifdef XT_HAVE_METRICS
    // drain-thread Prometheus metric inc
    ct::metrics::XtMetricRegistry::instance().on_param_update(
        ev.pu_strat_id, ev.pu_actor, ev.pu_key);
#endif
}

void XtLiveRecorder::serialize_kill_switch(const Event& ev, std::string& os,
                                           ct::data::IEventStore* es, void* fbb_ref) {
    os.append(",\"type\":\"KILL_SWITCH\",\"actor\":\"");
    json_escape_to(os, ev.ks_actor);
    os.append("\",\"state\":\"");
    json_escape_to(os, ev.ks_state);
    os.append("\",\"reason\":\"");
    json_escape_to(os, ev.ks_reason);
    os.append("\",\"venue_id\":\"");
    json_escape_to(os, ev.ks_venue_id);
    os += '"';

    if (es && current_trading_day_ != 0) {
        auto* fbb = static_cast<::flatbuffers::FlatBufferBuilder*>(fbb_ref);
        fbb->Clear();
        using namespace ct::data::schema;
        const auto actor_off  = fbb->CreateString(ev.ks_actor);
        const auto state_off  = fbb->CreateString(ev.ks_state);
        const auto reason_off = fbb->CreateString(ev.ks_reason);
        const auto venue_off  = fbb->CreateString(ev.ks_venue_id);
        KillSwitchBuilder kb(*fbb);
        kb.add_ts_local_ns(ev.ts_local_ns);
        kb.add_trading_day(current_trading_day_);
        kb.add_actor(actor_off);
        kb.add_state(state_off);
        kb.add_reason(reason_off);
        kb.add_venue_id(venue_off);
        kb.add_source_kind(static_cast<SourceKind>(venue_kind_.load(std::memory_order_relaxed)));
        const auto ks_off = kb.Finish();
        EventExtBuilder eb(*fbb);
        eb.add_payload_type(EventExtPayload::KillSwitch);
        eb.add_payload(ks_off.Union());
        fbb->Finish(eb.Finish(), "XTEX");
        es->append_event_ext(current_trading_day_, fbb->GetBufferPointer(), fbb->GetSize());
    }
#ifdef XT_HAVE_METRICS
    // drain-thread Prometheus metric inc
    ct::metrics::XtMetricRegistry::instance().on_kill_switch(
        ev.ks_venue_id, ev.ks_actor, ev.ks_state, ev.ks_reason);
    // 同步更新 xt_kill_switch_active gauge (state="tripped" → 1, "resumed" → 0)
    const bool active = (std::strcmp(ev.ks_state, "tripped") == 0);
    ct::metrics::XtMetricRegistry::instance().set_kill_switch_active(ev.ks_venue_id, active);
#endif
}

void XtLiveRecorder::serialize_position_lifecycle(const Event& ev, std::string& os,
                                                  ct::data::IEventStore* es, void* fbb_ref) {
    // 热路径序列化保持零分配风格：复用 std::string buffer，
    // 用 json_escape_to / format_fp_to / append_i64 直接 append。
    (void)es; (void)fbb_ref;  // JSONL only; FBS lifecycle encoding is not wired here.
    os.append(",\"type\":\"POSITION_LIFECYCLE\",\"event\":\"");
    json_escape_to(os, ev.lc_event_type);
    os.append("\",\"strat_id\":\"");
    json_escape_to(os, ev.lc_strat_id);
    os.append("\",\"code\":\"");
    json_escape_to(os, ev.lc_code);
    os.append("\",\"qty\":");
    append_i64(os, ev.lc_qty_fp);
    os.append(",\"avg_price\":");
    format_fp_to(os, ev.lc_avg_price_fp);
    os.append(",\"cause_trade_id\":");
    append_u64(os, ev.lc_cause_trade_id);
    if (ev.lc_book_id[0] != '\0') {
        os.append(",\"book_id\":\"");
        json_escape_to(os, ev.lc_book_id);
        os += '"';
    }
    if (ev.lc_account_id[0] != '\0') {
        os.append(",\"account_id\":\"");
        json_escape_to(os, ev.lc_account_id);
        os += '"';
    }
    if (ev.lc_realized_pnl_fp != 0) {
        os.append(",\"realized_pnl\":");
        format_fp_to(os, ev.lc_realized_pnl_fp);
    }
    if (ev.lc_hold_duration_ms != 0) {
        os.append(",\"hold_ms\":");
        append_u64(os, ev.lc_hold_duration_ms);
    }
}

void XtLiveRecorder::serialize_intent_trace(const Event& ev, std::string& os,
                                            ct::data::IEventStore* es, void* fbb_ref) {
    (void)es; (void)fbb_ref;  // INTENT_TRACE 是 JSONL-only，不写 EventExt/FBS。
    const bool is_raw_intent =
        std::strcmp(ev.it_stage, "RAW_INTENT") == 0;
    const bool is_partial_failure =
        std::strcmp(ev.it_stage, "GATEWAY_PARTIAL_FAILURE") == 0;
    os.append(",\"type\":\"INTENT_TRACE\",\"trace_id\":");
    append_u64(os, ev.it_trace_id);
    os.append(",\"stage\":\"");
    json_escape_to(os, ev.it_stage);
    os += '"';
    if (ev.it_venue[0] != '\0') {
        os.append(",\"venue\":\"");
        json_escape_to(os, ev.it_venue);
        os += '"';
    }
    if (ev.it_strategy_id[0] != '\0') {
        os.append(",\"strategy_id\":\"");
        json_escape_to(os, ev.it_strategy_id);
        os += '"';
    }
    if (ev.it_book_id[0] != '\0') {
        os.append(",\"book_id\":\"");
        json_escape_to(os, ev.it_book_id);
        os += '"';
    }
    if (ev.it_account_id[0] != '\0') {
        os.append(",\"account_id\":\"");
        json_escape_to(os, ev.it_account_id);
        os += '"';
    }
    if (ev.it_code_raw[0] != '\0') {
        os.append(",\"code_raw\":\"");
        json_escape_to(os, ev.it_code_raw);
        os += '"';
    }
    if (ev.it_code_effective[0] != '\0') {
        os.append(",\"code_effective\":\"");
        json_escape_to(os, ev.it_code_effective);
        os += '"';
    }
    if (ev.it_dir[0] != '\0') {
        os.append(",\"dir\":\"");
        json_escape_to(os, ev.it_dir);
        os += '"';
    }
    os.append(",\"price_raw_fp\":");
    append_i64(os, ev.it_price_raw_fp);
    os.append(",\"price_effective_fp\":");
    append_i64(os, ev.it_price_effective_fp);
    os.append(",\"price_wire_fp\":");
    append_i64(os, ev.it_price_wire_fp);
    os.append(",\"qty_raw_fp\":");
    append_i64(os, ev.it_qty_raw_fp);
    os.append(",\"qty_effective_fp\":");
    append_i64(os, ev.it_qty_effective_fp);
    os.append(",\"qty_wire_fp\":");
    append_i64(os, ev.it_qty_wire_fp);
    if (ev.it_tif_raw[0] != '\0') {
        os.append(",\"tif_raw\":\"");
        json_escape_to(os, ev.it_tif_raw);
        os += '"';
    }
    if (ev.it_tif_effective[0] != '\0') {
        os.append(",\"tif_effective\":\"");
        json_escape_to(os, ev.it_tif_effective);
        os += '"';
    }
    if (ev.it_tif_wire[0] != '\0') {
        os.append(",\"tif_wire\":\"");
        json_escape_to(os, ev.it_tif_wire);
        os += '"';
    }
    if (ev.it_offset_raw[0] != '\0') {
        os.append(",\"offset_raw\":\"");
        json_escape_to(os, ev.it_offset_raw);
        os += '"';
    }
    if (ev.it_offset_effective[0] != '\0') {
        os.append(",\"offset_effective\":\"");
        json_escape_to(os, ev.it_offset_effective);
        os += '"';
    }
    os.append(",\"reduce_only\":");
    os.append(ev.it_reduce_only ? "true" : "false");
    if (ev.it_exec_flags[0] != '\0') {
        os.append(",\"exec_flags\":\"");
        json_escape_to(os, ev.it_exec_flags);
        os += '"';
    }
    if (ev.it_local_id != 0) {
        os.append(",\"local_id\":");
        append_u64(os, ev.it_local_id);
    }
    if (ev.it_logical_id != 0) {
        os.append(",\"logical_id\":");
        append_u64(os, ev.it_logical_id);
    }
    if (ev.it_phys_id != 0) {
        os.append(",\"phys_id\":");
        append_u64(os, ev.it_phys_id);
    }
    if (ev.it_slice_count != 0) {
        os.append(",\"slice_index\":");
        append_u32(os, ev.it_slice_index);
        os.append(",\"slice_count\":");
        append_u32(os, ev.it_slice_count);
    }
    if (ev.it_partial || is_partial_failure) {
        os.append(",\"partial\":");
        os.append(ev.it_partial ? "true" : "false");
    }
    if (is_partial_failure || ev.it_failure_slice_index != 0 ||
        ev.it_accepted_slice_count != 0 || ev.it_expected_slice_count != 0) {
        os.append(",\"failure_slice_index\":");
        append_u32(os, ev.it_failure_slice_index);
        os.append(",\"accepted_slice_count\":");
        append_u32(os, ev.it_accepted_slice_count);
        os.append(",\"expected_slice_count\":");
        append_u32(os, ev.it_expected_slice_count);
    }
    if (is_raw_intent || ev.it_trigger_tick_ts != 0) {
        os.append(",\"trigger_tick_ts\":");
        append_u64(os, ev.it_trigger_tick_ts);
    }
    if (is_raw_intent || ev.it_decision_mono_ns != 0) {
        os.append(",\"decision_mono_ns\":");
        append_u64(os, ev.it_decision_mono_ns);
    }
    os.append(",\"event_mono_ns\":");
    append_u64(os, ev.ts_local_ns);
    if (is_raw_intent || ev.it_best_bid_fp != 0) {
        os.append(",\"best_bid_fp\":");
        append_i64(os, ev.it_best_bid_fp);
    }
    if (is_raw_intent || ev.it_best_ask_fp != 0) {
        os.append(",\"best_ask_fp\":");
        append_i64(os, ev.it_best_ask_fp);
    }
    if (ev.it_reject_scope[0] != '\0') {
        os.append(",\"reject_scope\":\"");
        json_escape_to(os, ev.it_reject_scope);
        os += '"';
    }
    if (ev.it_reject_code[0] != '\0') {
        os.append(",\"reject_code\":\"");
        json_escape_to(os, ev.it_reject_code);
        os += '"';
    }
    if (ev.it_reject_reason[0] != '\0') {
        os.append(",\"reject_reason\":\"");
        json_escape_to(os, ev.it_reject_reason);
        os += '"';
    }
    os.append(",\"user_tag_hash\":\"");
    append_u64(os, ev.it_user_tag_hash);
    os.append("\",\"wire_client_order_id_hash\":\"");
    append_u64(os, ev.it_wire_client_order_id_hash);
    os += '"';
}

void XtLiveRecorder::push_or_drop(Event&& ev) noexcept {
    if (!cfg_.enabled) return;
    const bool is_intent_trace = ev.type == kEvtIntentTrace;
    if (!running_.load(std::memory_order_acquire)) {
        const uint64_t prev = drop_count_.fetch_add(1, std::memory_order_relaxed);
        if (is_intent_trace) {
            intent_trace_drop_count_.fetch_add(1, std::memory_order_relaxed);
        }
        // drop-counter convention: first-occurrence warn
        // (running_=false 路径,通常是 shutdown race 后期。一次性 warn,后续静默)。
        // noexcept — 不抛,失败计数即可。
        if (prev == 0) {
            // 不在 hot path log,直接 stderr 1 行 (XtLogger 在 push_or_drop 不安全:
            // recorder 可能正在 stop, logger 也可能正在 stop)。
            std::fputs("[recorder] drop FIRST: running=false (shutdown race)\n", stderr);
        }
        return;
    }
    if (queue_->push(std::move(ev))) {
        emit_count_.fetch_add(1, std::memory_order_relaxed);
        if (is_intent_trace) {
            intent_trace_emit_count_.fetch_add(1, std::memory_order_relaxed);
        }
    } else {
        const uint64_t prev = drop_count_.fetch_add(1, std::memory_order_relaxed);
        if (is_intent_trace) {
            intent_trace_drop_count_.fetch_add(1, std::memory_order_relaxed);
        }
        // drop-counter convention: first-occurrence warn
        // (queue 满路径,常见于 burst recording。一次性 warn,后续累计 1Hz heartbeat
        // 上报。不在 hot path log。)
        if (prev == 0) {
            std::fputs("[recorder] drop FIRST: queue full (burst recording overflow)\n", stderr);
        }
    }
}

// ── Hot-path emit API 实现 ──────────────────────────────────────────────────

// fbs sink + JSONL 双写。caller 显式传 trading_date/action_date/time + exchange_id；
// binance gateway 在派生 action_date/time 后即丢弃 ts_exch_ms（fbs schema 不存）。
void XtLiveRecorder::emit_md_tick(uint64_t ts_local_ns,
                                  uint32_t trading_date,
                                  uint32_t action_date,
                                  uint32_t action_time,
                                  const char* code,
                                  const char* exchange_id,
                                  int64_t last_fp,
                                  int64_t volume,
                                  const int64_t bid_prices_fp[kMdDepth],
                                  const int64_t ask_prices_fp[kMdDepth],
                                  const int64_t bid_qty[kMdDepth],
                                  const int64_t ask_qty[kMdDepth],
                                  int depth_levels) {
    // cross-venue tick counter 在 cfg_.enabled gate 之外 emit，保持 Prometheus 心跳
    // 与 JSONL 录制开关解耦。
#ifdef XT_HAVE_METRICS
    ct::metrics::XtMetricRegistry::instance().on_md_tick(
        venue_kind_to_id(venue_kind_.load(std::memory_order_relaxed)),
        code ? code : "");
#endif
    if (!cfg_.enabled) return;
    Event ev;
    ev.type = kEvtMdTick;
    ev.ts_local_ns = ts_local_ns;
    copy_fixed(ev.md_code, sizeof(ev.md_code), code);
    copy_fixed(ev.md_exchange_id, sizeof(ev.md_exchange_id), exchange_id);
    ev.md_trading_date = trading_date;
    ev.md_action_date = action_date;
    ev.md_action_time = action_time;
    ev.md_last_fp = last_fp;
    ev.md_volume  = volume;
    ev.md_bid_fp  = bid_prices_fp[0];
    ev.md_ask_fp  = ask_prices_fp[0];
    for (int i = 0; i < kMdDepth; ++i) {
        ev.md_bid_prices_fp[i] = bid_prices_fp[i];
        ev.md_ask_prices_fp[i] = ask_prices_fp[i];
        ev.md_bid_qty[i]       = bid_qty[i];
        ev.md_ask_qty[i]       = ask_qty[i];
    }
    ev.md_depth_levels = depth_levels;
    push_or_drop(std::move(ev));
}


void XtLiveRecorder::emit_strategy_decision(uint64_t ts_local_ns,
                                            const char* strat_id,
                                            const char* code,
                                            const char* signal,
                                            int64_t target_pos,
                                            uint64_t trigger_tick_ts,
                                            const char* book_id,
                                            const char* account_id) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type = kEvtStrategyDecision;
    ev.ts_local_ns = ts_local_ns;
    copy_fixed(ev.sd_strat_id, sizeof(ev.sd_strat_id), strat_id);
    copy_fixed(ev.sd_code,     sizeof(ev.sd_code),     code);
    copy_fixed(ev.sd_signal,   sizeof(ev.sd_signal),   signal);
    ev.sd_target_pos      = target_pos;
    ev.sd_trigger_tick_ts = trigger_tick_ts;
    copy_fixed(ev.sd_book_id,    sizeof(ev.sd_book_id),    book_id);
    copy_fixed(ev.sd_account_id, sizeof(ev.sd_account_id), account_id);
    push_or_drop(std::move(ev));
}

void XtLiveRecorder::emit_order_submit(const OrderSubmitParams& p) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type        = kEvtOrderSubmit;
    ev.ts_local_ns = p.ts_local_ns;
    ev.os_logical_id = p.logical_id;
    copy_fixed(ev.os_code,         sizeof(ev.os_code),         p.code);
    copy_fixed(ev.os_dir,          sizeof(ev.os_dir),          p.dir);
    copy_fixed(ev.os_offset,       sizeof(ev.os_offset),       p.offset);
    copy_fixed(ev.os_parent_strat, sizeof(ev.os_parent_strat), p.parent_strat_id);
    copy_fixed(ev.os_reason,       sizeof(ev.os_reason),       p.reason);
    ev.os_price_fp         = p.price_fp;
    ev.os_qty              = p.qty;
    ev.os_decision_ts      = p.decision_ts;
    ev.os_trigger_tick_ts  = p.trigger_tick_ts;
    copy_fixed(ev.os_strat_id,   sizeof(ev.os_strat_id),   p.strat_id);
    copy_fixed(ev.os_book_id,    sizeof(ev.os_book_id),    p.book_id);
    copy_fixed(ev.os_account_id, sizeof(ev.os_account_id), p.account_id);
    copy_fixed(ev.os_user_tag,   sizeof(ev.os_user_tag),   p.user_tag);
    ev.os_decision_mono_ns = p.decision_mono_ns;
    ev.os_event_clock_valid = p.event_clock_valid;
    ev.os_event_ts_local_ns = p.event_ts_local_ns;
    push_or_drop(std::move(ev));
}

void XtLiveRecorder::emit_order_rtn(uint64_t ts_local_ns,
                                    uint64_t logical_id,
                                    uint64_t phys_id,
                                    const char* status,
                                    int64_t filled_qty,
                                    const char* err_msg,
                                    const char* cancel_initiator,
                                    const char* strat_id,
                                    const char* book_id,
                                    const char* account_id) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type = kEvtOrderRtn;
    ev.ts_local_ns   = ts_local_ns;
    ev.or_logical_id = logical_id;
    ev.or_phys_id    = phys_id;
    copy_fixed(ev.or_status,           sizeof(ev.or_status),           status);
    copy_fixed(ev.or_err_msg,          sizeof(ev.or_err_msg),          err_msg);
    copy_fixed(ev.or_cancel_initiator, sizeof(ev.or_cancel_initiator), cancel_initiator);
    ev.or_filled_qty = filled_qty;
    copy_fixed(ev.or_strat_id,   sizeof(ev.or_strat_id),   strat_id);
    copy_fixed(ev.or_book_id,    sizeof(ev.or_book_id),    book_id);
    copy_fixed(ev.or_account_id, sizeof(ev.or_account_id), account_id);
    push_or_drop(std::move(ev));
}

void XtLiveRecorder::emit_strategy_metric(uint64_t ts_local_ns,
                                          const char* name,
                                          double value,
                                          const char* strat_id,
                                          const char* book_id,
                                          const char* account_id) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type = kEvtStrategyMetric;
    ev.ts_local_ns = ts_local_ns;
    copy_fixed(ev.sm_name, sizeof(ev.sm_name), name);
    ev.sm_value = value;
    copy_fixed(ev.sm_strat_id,   sizeof(ev.sm_strat_id),   strat_id);
    copy_fixed(ev.sm_book_id,    sizeof(ev.sm_book_id),    book_id);
    copy_fixed(ev.sm_account_id, sizeof(ev.sm_account_id), account_id);
    push_or_drop(std::move(ev));
}

void XtLiveRecorder::emit_trade_rtn(const TradeRtnParams& p) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type        = kEvtTradeRtn;
    ev.ts_local_ns = p.ts_local_ns;
    ev.ts_exch_ns  = p.ts_exch_ns;
    ev.tr_trade_id          = p.trade_id;
    ev.tr_logical_id        = p.logical_id;
    ev.tr_trading_day       = p.trading_day;
    ev.tr_trade_action_date = p.trade_action_date;
    ev.tr_trade_action_time = p.trade_action_time;
    copy_fixed(ev.tr_code,        sizeof(ev.tr_code),        p.code);
    copy_fixed(ev.tr_exchange_id, sizeof(ev.tr_exchange_id), p.exchange_id);
    copy_fixed(ev.tr_side,        sizeof(ev.tr_side),        p.side);
    ev.tr_price_fp = p.price_fp;
    ev.tr_qty      = p.qty;
    ev.tr_is_maker = p.is_maker;
    copy_fixed(ev.tr_strat_id,   sizeof(ev.tr_strat_id),   p.strat_id);
    copy_fixed(ev.tr_book_id,    sizeof(ev.tr_book_id),    p.book_id);
    copy_fixed(ev.tr_account_id, sizeof(ev.tr_account_id), p.account_id);
    ev.tr_fee_fp          = p.fee_fp;
    ev.tr_realized_pnl_fp = p.realized_pnl_fp;
    ev.tr_has_realized = p.has_realized;
    ev.tr_is_close        = p.is_close;
    copy_fixed(ev.tr_route_status, sizeof(ev.tr_route_status), p.route_status);
    push_or_drop(std::move(ev));
}

void XtLiveRecorder::emit_conn_state(uint64_t ts_local_ns,
                                     const char* gateway,
                                     const char* state,
                                     const char* err_msg,
                                     const char* venue_id) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type = kEvtConnState;
    ev.ts_local_ns = ts_local_ns;
    copy_fixed(ev.cs_gateway, sizeof(ev.cs_gateway), gateway);
    copy_fixed(ev.cs_state,   sizeof(ev.cs_state),   state);
    copy_fixed(ev.cs_err_msg, sizeof(ev.cs_err_msg), err_msg);
    copy_fixed(ev.cs_venue_id, sizeof(ev.cs_venue_id), venue_id);
    push_or_drop(std::move(ev));
}

// ─── 新事件类型 emit ──────────────────────────────────────────────────────────

void XtLiveRecorder::emit_risk_reject(uint64_t ts_local_ns,
                                      const char* strat_id,
                                      const char* book_id,
                                      const char* account_id,
                                      const char* code,
                                      const char* dir,
                                      int64_t price_fp,
                                      int64_t qty,
                                      const char* reason,
                                      int64_t required_margin_fp,
                                      int64_t available_margin_fp) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type = kEvtRiskReject;
    ev.ts_local_ns = ts_local_ns;
    copy_fixed(ev.rj_strat_id,   sizeof(ev.rj_strat_id),   strat_id);
    copy_fixed(ev.rj_book_id,    sizeof(ev.rj_book_id),    book_id);
    copy_fixed(ev.rj_account_id, sizeof(ev.rj_account_id), account_id);
    copy_fixed(ev.rj_code,       sizeof(ev.rj_code),       code);
    copy_fixed(ev.rj_dir,        sizeof(ev.rj_dir),        dir);
    copy_fixed(ev.rj_reason,     sizeof(ev.rj_reason),     reason);
    ev.rj_price_fp             = price_fp;
    ev.rj_qty                  = qty;
    ev.rj_required_margin_fp   = required_margin_fp;
    ev.rj_available_margin_fp  = available_margin_fp;
    push_or_drop(std::move(ev));
}

void XtLiveRecorder::emit_param_update(uint64_t ts_local_ns,
                                       const char* actor,
                                       const char* strat_id,
                                       const char* key,
                                       const char* value_str,
                                       uint64_t apply_mono_ns) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type = kEvtParamUpdate;
    ev.ts_local_ns = ts_local_ns;
    copy_fixed(ev.pu_actor,     sizeof(ev.pu_actor),     actor);
    copy_fixed(ev.pu_strat_id,  sizeof(ev.pu_strat_id),  strat_id);
    copy_fixed(ev.pu_key,       sizeof(ev.pu_key),       key);
    copy_fixed(ev.pu_value_str, sizeof(ev.pu_value_str), value_str);
    ev.pu_apply_mono_ns = apply_mono_ns;
    push_or_drop(std::move(ev));
}

void XtLiveRecorder::emit_kill_switch(uint64_t ts_local_ns,
                                      const char* actor,
                                      const char* state,
                                      const char* reason,
                                      const char* venue_id) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type = kEvtKillSwitch;
    ev.ts_local_ns = ts_local_ns;
    copy_fixed(ev.ks_actor,    sizeof(ev.ks_actor),    actor);
    copy_fixed(ev.ks_state,    sizeof(ev.ks_state),    state);
    copy_fixed(ev.ks_reason,   sizeof(ev.ks_reason),   reason);
    copy_fixed(ev.ks_venue_id, sizeof(ev.ks_venue_id), venue_id);
    push_or_drop(std::move(ev));
}

void XtLiveRecorder::emit_position_lifecycle(const PositionLifecycleParams& p) {
    if (!cfg_.enabled) return;
    Event ev;
    ev.type        = kEvtPositionLifecycle;
    ev.ts_local_ns = p.ts_local_ns;
    ev.ts_exch_ns  = p.ts_exch_ns;
    copy_fixed(ev.lc_strat_id,   sizeof(ev.lc_strat_id),   p.strat_id);
    copy_fixed(ev.lc_book_id,    sizeof(ev.lc_book_id),    p.book_id);
    copy_fixed(ev.lc_account_id, sizeof(ev.lc_account_id), p.account_id);
    copy_fixed(ev.lc_code,       sizeof(ev.lc_code),       p.code);
    copy_fixed(ev.lc_event_type, sizeof(ev.lc_event_type), p.event_type);
    ev.lc_qty_fp           = p.qty_fp;
    ev.lc_avg_price_fp     = p.avg_price_fp;
    ev.lc_realized_pnl_fp  = p.realized_pnl_fp;
    ev.lc_hold_duration_ms = p.hold_duration_ms;
    ev.lc_cause_trade_id   = p.cause_trade_id;
    push_or_drop(std::move(ev));
}

void XtLiveRecorder::emit_intent_trace(const IntentTraceParams& p) {
    if (!cfg_.enabled || !cfg_.intent_trace.enabled) return;
    if (!sample_intent_trace(p.trace_id, cfg_.intent_trace.sample_rate)) return;

    Event ev;
    ev.type = kEvtIntentTrace;
    ev.ts_local_ns = p.event_mono_ns;
    ev.it_trace_id = p.trace_id;
    copy_fixed(ev.it_stage,          sizeof(ev.it_stage),          p.stage);
    copy_fixed(ev.it_venue,          sizeof(ev.it_venue),          p.venue);
    copy_fixed(ev.it_strategy_id,    sizeof(ev.it_strategy_id),    p.strategy_id);
    copy_fixed(ev.it_book_id,        sizeof(ev.it_book_id),        p.book_id);
    copy_fixed(ev.it_account_id,     sizeof(ev.it_account_id),     p.account_id);
    copy_fixed(ev.it_code_raw,       sizeof(ev.it_code_raw),       p.code_raw);
    copy_fixed(ev.it_code_effective, sizeof(ev.it_code_effective), p.code_effective);
    copy_fixed(ev.it_dir,            sizeof(ev.it_dir),            p.dir);
    ev.it_price_raw_fp       = p.price_raw_fp;
    ev.it_price_effective_fp = p.price_effective_fp;
    ev.it_price_wire_fp      = p.price_wire_fp;
    ev.it_qty_raw_fp         = p.qty_raw_fp;
    ev.it_qty_effective_fp   = p.qty_effective_fp;
    ev.it_qty_wire_fp        = p.qty_wire_fp;
    copy_fixed(ev.it_tif_raw,       sizeof(ev.it_tif_raw),       p.tif_raw);
    copy_fixed(ev.it_tif_effective, sizeof(ev.it_tif_effective), p.tif_effective);
    copy_fixed(ev.it_tif_wire,      sizeof(ev.it_tif_wire),      p.tif_wire);
    copy_fixed(ev.it_offset_raw,    sizeof(ev.it_offset_raw),    p.offset_raw);
    copy_fixed(ev.it_offset_effective, sizeof(ev.it_offset_effective), p.offset_effective);
    ev.it_reduce_only = p.reduce_only;
    copy_fixed(ev.it_exec_flags, sizeof(ev.it_exec_flags), p.exec_flags);
    ev.it_local_id = p.local_id;
    ev.it_logical_id = p.logical_id;
    ev.it_phys_id = p.phys_id;
    ev.it_slice_index = p.slice_index;
    ev.it_slice_count = p.slice_count;
    ev.it_partial = p.partial;
    ev.it_failure_slice_index = p.failure_slice_index;
    ev.it_accepted_slice_count = p.accepted_slice_count;
    ev.it_expected_slice_count = p.expected_slice_count;
    ev.it_trigger_tick_ts = p.trigger_tick_ts;
    ev.it_decision_mono_ns = p.decision_mono_ns;
    ev.it_best_bid_fp = p.best_bid_fp;
    ev.it_best_ask_fp = p.best_ask_fp;
    copy_fixed(ev.it_reject_scope,  sizeof(ev.it_reject_scope),  p.reject_scope);
    copy_fixed(ev.it_reject_code,   sizeof(ev.it_reject_code),   p.reject_code);
    copy_fixed(ev.it_reject_reason, sizeof(ev.it_reject_reason), p.reject_reason);
    ev.it_user_tag_hash = p.user_tag_hash;
    ev.it_wire_client_order_id_hash = p.wire_client_order_id_hash;
    push_or_drop(std::move(ev));
}
