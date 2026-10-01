#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "xt_core/order/XtOrderTypes.h"

namespace xt_live::gateway {

/// @brief fill 记账条目的来源标注；仅作 provenance 诊断，不参与去重判定。
enum class FillSource : uint8_t {
    kLivePush = 0,     ///< venue 实时推送
    kQueryReplay = 1,  ///< 查询应答重放行
    kGapFill = 2,      ///< 对账 gap-fill 拉取行
};

/// @brief 终态记录的证据来源（provenance）。
enum class TerminalProvenance : uint8_t {
    kVenuePush = 0,          ///< venue 推送/应答的真实终态回报
    kVenueQuery,             ///< venue 定向查询确认的终态
    kSynthCancelNoTarget,    ///< 撤单撞「目标不存在/已终结」错误码合成
    kSynthCumFilled,         ///< 累计成交 >= 委托量合成 Filled
    kSynthReconcile,         ///< 对账路径合成/确认
    kSynthSuppressedResend,  ///< 去重器吞终态后的补投合成
};

/// @brief 撤单尝试的结局证据（弱 → 强）。
enum class CancelOutcome : uint8_t {
    kSentToVenue = 0,  ///< 请求已发往 venue，结局未知
    kLocalGateFalse,   ///< 本地闸门拒绝（限流/未就绪/无路由），未达 venue
    kTransportFail,    ///< 传输层失败，是否达 venue 未知
    kVenueNoTarget,    ///< venue 否证：目标单不存在或已终结
    kVenueAcked,       ///< venue 确认撤单受理
};

/// @brief per-order 交付投影（delivery projection），venue-neutral。
///
/// 记录三类交付事实：fill 记账（exactly-once 判据：`fill_key ∈ fills` ⇔ 已记账）、
/// 终态 payload + provenance、撤单尝试结局证据。venue 键以 opaque string 存储，
/// 路由语义留在各 venue 适配层。
///
/// 刻意不做：不存钱、不算 PnL、不持久化（in-memory，崩溃后由 venue 重建）；
/// 不复制 book/account 的账本职责——本类是投影不是第二账本。
///
/// retention：未终态记录不淘汰（上界 = venue 侧最大挂单量）；终态记录与
/// orderless fill key 归 FIFO 淘汰域（cap 见 ctor），retention 由 cap 兜底。
/// `clear_session()` 为同进程交易日轮转的可选整体清理入口，不是 retention 前提。
/// 前提：local_id 进程内单调不复用；同 id 重复注册按整体替换处理。
///
/// 线程契约：所有公开方法线程安全（内部单 mutex，不在锁内回调）；计数为
/// 进程内 atomic；查询一律按值出参/返回副本，不暴露内部引用。
class XtExecDeliveryLedger {
public:
    /// @brief 单笔 fill 的记账条目；per-order 有界（单笔订单 fill 数天然小）。
    struct ExecFillEntry {
        std::string fill_key;   ///< canonical composite key（venue 适配层拼装）
        int64_t qty_fp = 0;     ///< 本次 fill 量（cum 累计输入）
        FillSource source = FillSource::kLivePush;
        bool absorbed = false;  ///< true = 已记账但未下发（快照吸收的历史行）
        uint32_t query_gen = 0; ///< 交付该行的 replay 所属查询代；live push = 当前代
    };

    /// @brief 终态 payload；按值出参供合成重发使用。
    struct TerminalRecord {
        XtOrderStatus status = XtOrderStatus::Canceled;
        int64_t filled_fp = 0;  ///< 终态时刻的累计成交量
        TerminalProvenance prov = TerminalProvenance::kVenuePush;
        uint64_t seen_us = 0;   ///< gateway 侧观测到该终态的时刻（steady µs）
    };

    /// @brief 单次撤单尝试的结局证据。
    struct CancelAttempt {
        uint64_t ts_ms = 0;
        CancelOutcome outcome = CancelOutcome::kSentToVenue;
    };

    static constexpr std::size_t kCancelAttemptRing = 8;    ///< 撤单证据环形容量（保尾）
    static constexpr std::size_t kTerminalRecordCap = 200000;  ///< 终态域默认 cap

    /// @brief per-order 投影记录。fills 是 exactly-once 判据集合；
    ///        cum_filled_fp = Σ fills.qty_fp（含 absorbed 行）。
    struct ExecOrderRecord {
        // 身份（venue key 为 opaque string；user_tag 等 venue 细节留适配层）
        uint64_t local_id = 0;
        uint32_t sid_id = 0;
        std::string code;
        std::string venue_key;  ///< 空 = venue 侧键未回填；非空时进反向索引
        int64_t qty_fp = 0;
        bool is_buy = true;
        XtOffset close_offset = XtOffset::Open;
        // 交付事实 1：fill 记账
        std::vector<ExecFillEntry> fills;
        int64_t cum_filled_fp = 0;
        int64_t absorbed_watermark_fp = 0;  ///< 注册时 venue 已成交量封印（reverse-sync 用）
        // 交付事实 2：终态 payload + provenance
        bool has_terminal = false;
        TerminalRecord terminal{};
        // venue-truth 证据：撤单尝试环形（保最近 kCancelAttemptRing 条）+ 总计数
        std::array<CancelAttempt, kCancelAttemptRing> cancel_attempts{};
        uint32_t cancel_attempt_total = 0;
        uint32_t session_gen = 0;  ///< 记录创建时的 venue session 代
    };

    /// @brief 注册参数包（submit 预注册 / reverse-sync rebuild 共用）。
    struct OrderIdentity {
        uint32_t sid_id = 0;
        std::string code;
        std::string venue_key;  ///< 可空；后续 `set_venue_key` 回填
        int64_t qty_fp = 0;
        bool is_buy = true;
        XtOffset close_offset = XtOffset::Open;
        int64_t absorbed_watermark_fp = 0;
        uint32_t session_gen = 0;
    };

    /// @brief fill 记账参数包。
    struct FillArgs {
        std::string fill_key;
        int64_t qty_fp = 0;
        FillSource source = FillSource::kLivePush;
        bool absorbed = false;
        uint32_t query_gen = 0;
    };

    /// @brief 观测快照（进程内计数；不接 metrics 面）。
    struct Stats {
        // 规模（锁内快照）
        std::size_t open_records = 0;         ///< 未终态记录数
        std::size_t terminal_records = 0;     ///< 终态域记录数
        std::size_t accounted_fill_keys = 0;  ///< 已记账 fill key 总数（含 orderless）
        std::size_t orderless_fill_keys = 0;  ///< orderless 域 key 数
        // 事件计数（进程内累计）
        uint64_t duplicate_fill_suppressed = 0;  ///< 重 key fill 被抑制次数
        uint64_t duplicate_terminal_ignored = 0; ///< 重复终态（payload 未覆盖）次数
        uint64_t fill_record_miss = 0;           ///< 无归属 fill（入 orderless 域）次数
        uint64_t terminal_record_miss = 0;       ///< 无记录终态（骨架记录创建）次数
        uint64_t cancel_attempt_miss = 0;        ///< 无记录撤单证据被丢弃次数
        uint64_t venue_key_miss = 0;             ///< 无记录 venue 键回填被丢弃次数
        uint64_t venue_key_collision = 0;        ///< venue 键撞键（后写覆盖）次数
        uint64_t evicted_terminal_records = 0;   ///< 终态域 FIFO 淘汰记录数
        uint64_t evicted_orderless_keys = 0;     ///< orderless 域 FIFO 淘汰 key 数
        uint64_t session_clear_total = 0;        ///< clear_session 调用次数
        uint64_t session_cleared_entries = 0;    ///< clear_session 清掉的记录+key 总数
        uint64_t invalid_input_dropped = 0;      ///< 非法输入（空 key / id=0）丢弃次数
    };

    /// @param terminal_cap 终态域与 orderless 域各自的 FIFO 上限（测试注入用；0 视为 1）。
    explicit XtExecDeliveryLedger(std::size_t terminal_cap = kTerminalRecordCap);

    // ── 登记 ────────────────────────────────────────────────────────
    /// @brief 登记订单投影记录；同 id 已存在时整体替换（旧 fill key / venue 键出账）。
    /// @note local_id == 0 丢弃并计数。
    void register_order(uint64_t local_id, OrderIdentity identity);

    /// @brief 预注册失败回滚：删除记录并出账其索引条目；记录不存在时 no-op。
    void rollback_register(uint64_t local_id);

    // ── fill 记账（exactly-once 判据）──────────────────────────────
    /// @brief 查 + 记一体：fill_key 未记账则登记并累计 cum，返回 true；
    ///        已记账（任何来源、任何宿主）返回 false 且无副作用。
    /// @note local_id == 0 或记录不存在时 key 记入 orderless 域（保全局
    ///       exactly-once 判据）并计 fill_record_miss；终态后的迟到 fill
    ///       仍可记账（元数据保留，归因不丢）。空 fill_key 丢弃。
    bool record_fill(uint64_t local_id, FillArgs fill);

    /// @brief fill_key 是否已记账（per-order 与 orderless 域统一判据）。
    bool fill_accounted(const std::string& fill_key) const;

    /// @brief 已记账累计成交量（含 absorbed 行）；无记录返回 0。
    int64_t cum_filled_fp(uint64_t local_id) const;

    // ── 终态 payload ────────────────────────────────────────────────
    /// @brief 记录终态 payload；首个终态生效并把记录移入终态淘汰域，返回 true。
    ///        已有终态时不覆盖 payload，返回 false（升级仲裁归读侧调用方）。
    /// @note 记录不存在时创建骨架记录（身份未知）承载 payload 并计数。
    bool record_terminal(uint64_t local_id, const TerminalRecord& terminal);

    /// @brief 终态 payload 按值出参；未终态或无记录返回 false。
    bool terminal_record(uint64_t local_id, TerminalRecord& out) const;

    // ── 撤单证据 ────────────────────────────────────────────────────
    /// @brief 追加一次撤单尝试结局（环形保尾 + 总计数）；无记录丢弃并计数。
    void record_cancel_attempt(uint64_t local_id, uint64_t ts_ms, CancelOutcome outcome);

    // ── venue 键反向索引 ────────────────────────────────────────────
    /// @brief 回填/变更 venue 键；空 key no-op（不入索引也不清既有键），
    ///        键变更先删旧索引再插新；撞键后写覆盖并计数。
    void set_venue_key(uint64_t local_id, const std::string& venue_key);

    /// @brief venue 键 → local_id 反向查询。
    std::optional<uint64_t> local_for_venue_key(const std::string& venue_key) const;

    // ── 遍历 / 观测 ────────────────────────────────────────────────
    /// @brief 未终态记录 id 快照（锁内拷贝，调用方在锁外消费）。
    std::vector<uint64_t> open_ids() const;

    /// @brief 记录整体按值出参（观测/测试）；无记录返回 false。
    bool get_record(uint64_t local_id, ExecOrderRecord& out) const;

    /// @brief session 域清理：清空终态域记录与 orderless key；未终态记录
    ///        及其 fill 记账保留。同进程交易日轮转时可调用释放终态域内存。
    void clear_session();

    /// @brief 观测快照。
    Stats stats() const;

private:
    using RecordMap = std::unordered_map<uint64_t, ExecOrderRecord>;

    /// @pre 调用方已持有 mu_。venue 键入反向索引；撞键后写覆盖并计数。
    void bind_venue_key_unlocked_(uint64_t local_id, const std::string& venue_key);
    /// @pre 调用方已持有 mu_。记录连同 fill key / venue 键索引一起出账。
    void erase_record_unlocked_(RecordMap::iterator it);
    /// @pre 调用方已持有 mu_。终态域 FIFO 超 cap 淘汰（跳过已重注册的残留条目）。
    void enforce_terminal_cap_unlocked_();
    /// @pre 调用方已持有 mu_。orderless 域 FIFO 超 cap 淘汰。
    void enforce_orderless_cap_unlocked_();

    const std::size_t terminal_cap_;  ///< 终态域 / orderless 域各自 FIFO 上限

    mutable std::mutex mu_;  ///< 唯一锁；保护下列全部容器
    RecordMap records_;                                        ///< local_id → 投影记录
    std::unordered_map<std::string, uint64_t> venue_key_to_local_;  ///< venue 键反向索引
    std::unordered_set<std::string> fill_key_index_;  ///< 全部已记账 fill key（per-order + orderless）
    std::deque<uint64_t> terminal_fifo_;              ///< 终态域淘汰顺序
    std::deque<std::string> orderless_fifo_;          ///< orderless 域淘汰顺序
    std::size_t terminal_records_ = 0;                ///< 终态记录实数（FIFO 含残留条目故单独计）

    std::atomic<uint64_t> duplicate_fill_suppressed_{0};
    std::atomic<uint64_t> duplicate_terminal_ignored_{0};
    std::atomic<uint64_t> fill_record_miss_{0};
    std::atomic<uint64_t> terminal_record_miss_{0};
    std::atomic<uint64_t> cancel_attempt_miss_{0};
    std::atomic<uint64_t> venue_key_miss_{0};
    std::atomic<uint64_t> venue_key_collision_{0};
    std::atomic<uint64_t> evicted_terminal_records_{0};
    std::atomic<uint64_t> evicted_orderless_keys_{0};
    std::atomic<uint64_t> session_clear_total_{0};
    std::atomic<uint64_t> session_cleared_entries_{0};
    std::atomic<uint64_t> invalid_input_dropped_{0};
};

/// @brief 终态域 FIFO 淘汰的进程级累计计数（跨全部 ledger 实例聚合）。
///
/// 与 per-instance Stats::evicted_terminal_records 同点递增，供 1Hz frame-tail
/// 侧道 delta 告警免实例引用读取（对齐 xt_runtime_book_drops 进程静态口径）。
/// CTP runner 单 ledger 实例 → 进程级即该实例口径；binance runner 不用本 ledger
/// （走独立 BinanceOrderLedger），恒 0。
std::atomic<uint64_t>& exec_delivery_terminal_evicted_total();

}  // namespace xt_live::gateway
