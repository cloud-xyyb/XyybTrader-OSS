#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "xt_core/order/XtOrderTypes.h"

namespace binance_u {

struct TransportAck;

/// @brief Binance TD 的本地订单账本
///
/// - BinanceOrderLedger 是 Binance 交易网关里的订单身份和状态表，
///   给 ACK、UDS、撤单、对账提供统一的对账记录
/// - 所有方法线程安全。
class BinanceOrderLedger {
public:
    /// @brief 单笔订单的合并记录：身份路由、submit 原始字段和 in-flight 状态。
    ///
    /// 三部分的写入/清理时机互相独立，见各字段注释。
    struct OrderRecord {
        std::string client_order_id;  ///< 本进程 clientOrderId；终态销账（ACK/UDS/reconcile）后清空（撤单路由失效）。
        std::string symbol;           ///< Binance symbol；撤单/fallback 路由，终态后保留。
        uint64_t    trace_id = 0;     ///< intent_trace_id sidecar；0 = 未采样/已清理。
        std::string sys_id;           ///< exchange orderId；ACK 前为空字符串。
        XtOrderStatus status = XtOrderStatus::Submitted;  ///< in-flight facet 的最近状态。
        bool     is_buy = true;       ///< 原始委托方向。
        int64_t  price_fp = 0;        ///< 原始委托价格，scale 1e6。
        int64_t  qty_fp = 0;          ///< 原始委托数量，scale 1e6。
        XtOffset close_offset = XtOffset::Open;  ///< 原始开平语义。
        uint64_t submit_ts_ms = 0;    ///< steady ms（submit 路径）或 venue 报单时间（reverse-sync）。
        bool has_pending = false;     ///< pending 短窗尚未被消费。
        bool has_inflight = false;    ///< 本地仍认为在途。
        /// 已记账 fill 的 venue trade_id 集（live UDS 与 gap-fill 重放共用的
        /// exactly-once 判据）。venue trade_id 按 symbol 唯一且一单只有一个
        /// symbol，`symbol|trade_id` 复合 key 在 order 域内退化为裸 trade_id。
        /// retention 随 record（records_ 终身存活，无 FIFO 误逐出）；容量由
        /// kMaxFillKeysPerOrder 防御，触顶后新 key 改走全局 (symbol,tid) 窗。
        std::unordered_set<int64_t> fill_tids;
    };

    /// @brief in-flight 记录的只读快照行（ghost sweep / gap-fill 归因用）。
    struct InflightBrief {
        uint64_t    local_id = 0;
        std::string sys_id;
        std::string symbol;
        uint64_t    submit_ts_ms = 0;
        XtOffset    close_offset = XtOffset::Open;
    };

    /// @brief 终态判定结果；账本变更已在锁内完成，caller 按 action 在锁外 emit。
    struct TerminalDecision {
        enum class Action {
            kEmit,          ///< update 有效；caller 应在锁外走 emit 链。
            kSkip,          ///< 条件不满足或已被并发路径销账；caller 不 emit。
            kKeepInflight,  ///< 订单保持在途（cancel 非确认 ack）；未构造 update。
        };
        Action action = Action::kSkip;
        XtOrderExecUpdate update{};         ///< on_ack 路径的补齐产物；其余路径不使用。
        bool terminal_seen_before = false;  ///< 本次判定前该单已见终态（迟到 Accepted 抑制用）。
    };

    /// @brief reconcile 合成/确认终态的类别（on_reconcile_terminal 输入）。
    enum class ReconcileTerminal {
        kNotExistCanceled,  ///< -2013 venue 权威否认：补齐 + 记终态；in-flight 保留由 ghost 主循环销账。
        kBlankSysIdReject,  ///< 超龄仍无 sys_id：补齐 + 记终态 + 销账（含 trace 清理）。
        kVenueTerminal,     ///< venue 确认终态（字段已来自 REST）：记终态 + 销账。
    };

    static constexpr std::size_t kTerminalOrderCacheCap = 8192;  ///< 终态去重窗口容量（FIFO，无 TTL）。
    static constexpr std::size_t kMaxFillKeysPerOrder = 1024;    ///< per-order fill key 软上界（新 key 接受路径受限，触顶改走全局窗；跨域抑制回写与自成交腿耐久接受不受此限）。
    static constexpr std::size_t kOrderlessFillWindowCap = 8192; ///< 全局 (symbol,tid) fill 去重窗容量（FIFO）。

    /// @brief fill 去重侧道计数快照（进程内观测，非 prom）。
    struct FillDedupStats {
        uint64_t per_order_dups = 0;      ///< per-order fills 集（fill_tids）命中抑制数（仅 fill_tids 命中）。
        uint64_t cross_domain_dups = 0;   ///< per-order 分支跨域互查命中全局窗（owner∈{0,本单}）的抑制数。
        uint64_t self_trade_leg_passthrough = 0;  ///< 跨域命中但 owner 为其它订单（venue 自成交另一腿）的放行数。
        uint64_t per_order_overflow = 0;  ///< per-order 容器触顶、改走全局窗的判定数。
        uint64_t orderless_routed = 0;    ///< 走全局窗的判定数（order_id=0 / 无记录 / 溢出）。
        uint64_t orderless_dups = 0;      ///< 全局窗命中抑制数。
        uint64_t route_erased_ack = 0;        ///< ACK 终态清撤单路由数。
        uint64_t route_erased_reconcile = 0;  ///< reconcile 终态清撤单路由数。
    };

    // ── 登记 / 状态推进 ────────────────────────────────────────────
    /// @brief submit 热路径登记：单锁单 insert 写入身份 + pending + in-flight 三 facet。
    /// @note rec.client_order_id 同时写入反向索引；调用方保证 local_id 进程内唯一。
    void register_submit(uint64_t local_id, OrderRecord rec);

    /// @brief reverse-sync 回灌：emplace 语义，不覆盖已有身份/在途字段。
    /// @note record 不存在时整体插入（无 pending 短窗）；已存在时仅回填空缺的
    ///       client/symbol 路由，并在已销账时用 venue 字段复活 in-flight facet。
    ///       与 submit 不同，不写 client 反向索引（cross-restart 反解走
    ///       clientOrderId 格式解析兜底）。
    void upsert_recovered(uint64_t local_id, OrderRecord venue_rec);

    /// @brief blank-sys-id 订单经 openOrders 反查命中后回填 sys_id 并置 Accepted。
    /// @note 仅 in-flight 且 sys_id 仍为空时生效。
    void bind_sys_id_if_blank(uint64_t local_id, const std::string& sys_id);

    // ── 终态判定 / 销账（唯一权威入口；emit 由 caller 锁外执行）────────
    /// @brief transport ACK 的终态判定 + 字段补齐（pending 短窗优先，回退
    ///        in-flight 记录）+ Accepted 的 exchange orderId 绑定。
    ///
    /// cancel 请求的 ack 只认 kCanceled 为订单终态——req_kind==kCancel 且
    /// kind!=kCanceled 在函数最前早返回 kKeepInflight，不补齐 update、不做任何
    /// 终态登记。终态分支在销账同时清撤单路由（与 UDS/reconcile 终态对称，
    /// 防已终态订单继续经 cancel 路由打 venue 吃 -2011）。
    /// @param local_cb_us  caller 预取的回调时间戳（transport t1 或当前 steady µs）。
    /// @return kEmit 时 update 已补齐、终态已登记销账；日志/metric/fallback
    ///         善后由 caller 按 ack 的 kind 在锁外处理。
    TerminalDecision on_ack(const TransportAck& ack, uint64_t local_cb_us);

    /// @brief UDS order update 的终态销账：单锁内记终态 + 清 in-flight + 清撤单路由。
    /// @note UDS 终态可能早于 transport ACK；先记终态可防迟到 ACK 复活已结束
    ///       订单。非终态不改账本。当前恒返回 kEmit（UDS 信号放行，去重在下游
    ///       dedup）；update 字段不修改，caller 直接 emit 原 upd。
    TerminalDecision on_uds_terminal(const XtOrderExecUpdate& upd);

    /// @brief reconcile 合成/确认终态：按类别原子完成补齐 + 记终态 + 销账。
    /// @param upd  in-out：kNotExistCanceled / kBlankSysIdReject 在锁内用在途
    ///             记录补齐方向/开平/价格/数量；kVenueTerminal 不修改。
    /// @return kSkip = 已被并发路径销账或条件不满足（如 sys_id 已回填），
    ///         caller 不应 emit。
    TerminalDecision on_reconcile_terminal(uint64_t local_id, ReconcileTerminal kind,
                                           XtOrderExecUpdate& upd);

    /// @note 本窗（terminal_orders_ + terminal_order_fifo_）是 pre-dedup 位点、无 TTL
    ///       的 FIFO 窗，抑制迟到 Accepted 复活已终态订单；与 EffectiveSignalDedup 的
    ///       at-emit TTL 窗（5min）语义不同，不可合并。合并论证见
    ///       .claude/plans/binance_gw_terminal.md 附录 B。
    //
    // ── 终态去重窗口（FIFO cap 淘汰；查询位点 pre-dedup）──────────────
    /// @brief 是否已见终态；用于迟到 ACK/UDS/reconcile 过滤。
    bool terminal_seen(uint64_t local_id) const;

    /// @brief 仅记录已见终态并按 FIFO 淘汰旧 local_id（不动 in-flight facet）；
    ///        reconcile fetch 路径在销账前提前登记用。
    void remember_terminal(uint64_t local_id);

    // ── fill 去重（live UDS 与 REST gap-fill 唯一共用判定入口）─────────
    /// @brief fill 去重记账判定：首次见到的 (归属, symbol, trade_id) 返回 true。
    ///
    /// key 域：order_id>0 且有记录 → per-order fills 集（record 终身存活，
    /// 无淘汰误逐出）；order_id=0（gap-fill 归因失败行）/ 无记录 / per-order
    /// 触顶 → 全局 `symbol|trade_id` 复合窗（FIFO cap，跨 symbol 撞 tid 不误杀）。
    /// @param trade_id  venue trade id；<=0 无法去重，恒返回 true（按新成交处理）。
    /// @return true = 首次记账（caller 应 emit）；false = 重复（caller 抑制）。
    bool note_fill_seen(uint64_t order_id, std::string_view symbol, int64_t trade_id);

    /// @brief fill 去重 / 撤单路由清理侧道计数快照（relaxed 读，测试与 1Hz 侧道用）。
    FillDedupStats fill_dedup_stats() const;

    // ── venue 终态查询（ITradeGateway::is_order_terminal 转发目标）─────
    /// @brief local_id 是否已终态（engine lost-terminal 对账探测）。
    ///
    /// 判定序：in-flight 记录优先返回 false（venue-alive 证据优先，防止对
    /// reverse-sync 复活单合成误杀；-2013 保留 in-flight 的短窗内同样返回
    /// false，等 ghost 主循环销账后才转 true）→ 终态窗命中或「记录存在且
    /// in-flight 已销」返回 true（records_ 终身存活，兜住终态窗 FIFO cap
    /// 淘汰后的失忆）→ 无记录返回 false（未知 id 不得据此合成终态）。
    bool is_order_terminal(uint64_t local_id) const;

    /// @brief 终态 payload 查询（is_order_terminal 的 payload 版，只读）。
    ///
    /// 仅当按 is_order_terminal 判定序为终态、且记录自身 status 是终态值时给
    /// payload——ACK 路径的 Accepted-with-final-raw-status 会把终态回写记录
    /// status；kCanceled/UDS 终态销账不回写 status → 返回 false，caller 退回
    /// 布尔探测的合成兜底。filled：venue 不落 per-order 累计成交，Filled 按
    /// 全量成交语义写 qty_fp，其余写 -1（不可得）。
    /// @return true = out_status 为终态值且可信；false = payload 不可得。
    bool lookup_terminal(uint64_t local_id, XtOrderStatus& out_status,
                         int64_t& out_filled_fp) const;

    // ── 查询面（cancel 路由 / reconcile / ghost sweep / 测试观测共用）──
    /// @brief in-flight facet 是否存活。
    bool is_inflight(uint64_t local_id) const;
    /// @brief 在途单的 exchange orderId；无在途记录返回空字符串。
    std::string sys_id_for(uint64_t local_id) const;
    /// @brief 撤单路由 clientOrderId；无记录或路由已清返回空字符串。
    std::string client_order_id_for(uint64_t local_id) const;
    /// @brief clientOrderId 反向索引（仅 submit 登记；UDS 终态后清除）。
    std::optional<uint64_t> local_for_client(std::string_view client_order_id) const;
    /// @brief intent_trace sidecar；0 = 无。
    uint64_t trace_id_for(uint64_t local_id) const;
    /// @brief 撤单/fallback 路由 symbol；无记录返回空字符串。
    std::string symbol_for(uint64_t local_id) const;
    /// @brief in-flight 全量快照；调用方在锁外做 REST IO。
    std::vector<InflightBrief> inflight_snapshot() const;
    /// @brief 按 exchange orderId 反查在途单（gap-fill 归因）。
    std::optional<InflightBrief> find_inflight_by_sys_id(std::string_view sys_id) const;

private:
    /// @brief 补齐 upd 的方向/开平/价格/数量字段（filled/leaves 只在缺省时归一）。
    static void apply_record_fields_(const OrderRecord& rec, XtOrderExecUpdate& upd);

    /// @pre 调用方已持有 ledger_mu_。
    void remember_terminal_unlocked_(uint64_t local_id);

    /// @pre 调用方已持有 ledger_mu_。清 in-flight facet 与 trace sidecar。
    static void erase_inflight_unlocked_(OrderRecord& rec);

    /// @pre 调用方已持有 ledger_mu_。清 client 撤单双向路由（终态后 cancel 不再路由）。
    /// @return true = 本次真清了路由（此前非空）；用于分站点侧道计数。
    bool erase_client_route_unlocked_(OrderRecord& rec);

    /// @pre 调用方已持有 ledger_mu_。ACK 终态销账：清 in-flight + 撤单路由 + 计数。
    void settle_ack_terminal_unlocked_(OrderRecord& rec);

    /// @pre 调用方已持有 ledger_mu_。全局 `symbol|trade_id` 复合窗判定 + FIFO 淘汰。
    bool note_orderless_fill_unlocked_(std::string_view symbol, int64_t trade_id);

    /// @brief 拼装全局窗复合 key `symbol|trade_id`（纯函数，不触成员）。
    static std::string make_orderless_fill_key_(std::string_view symbol, int64_t trade_id);
    /// @pre 调用方已持有 ledger_mu_。全局窗成员判定（只查不改）。
    bool orderless_fill_contains_unlocked_(const std::string& key) const;
    /// @pre 调用方已持有 ledger_mu_。全局窗登记 + FIFO 淘汰；key 已在窗内时幂等
    ///      返回（不覆盖 owner、不重复入 FIFO → 首写者保留）。owner=0 表示
    ///      orderless 起源，>0 为 per-order 接受后同记的宿主 local_id。不计
    ///      routed/dups 侧道（per-order 命中后的双域同记专用，避免污染 orderless
    ///      计数语义）。
    void insert_orderless_fill_unlocked_(std::string key, uint64_t owner);

    mutable std::mutex ledger_mu_;  ///< 唯一锁；保护下列全部容器。
    std::unordered_map<uint64_t, OrderRecord> records_;          ///< local_id → 合并记录。
    std::unordered_map<std::string, uint64_t> client_to_local_;  ///< clientOrderId 反向索引。
    std::unordered_set<uint64_t> terminal_orders_;   ///< 已见终态 local_id 集。
    std::deque<uint64_t> terminal_order_fifo_;       ///< 终态窗口 FIFO 淘汰顺序。
    /// 全局 fill 去重窗：key=`symbol|tid` → owner local_id（0=orderless 起源，
    /// >0=per-order 接受后同记的宿主 local_id）。owner 让跨域互查区分自成交两腿。
    std::unordered_map<std::string, uint64_t> orderless_fill_keys_;
    std::deque<std::string> orderless_fill_fifo_;          ///< 全局 fill 窗 FIFO 淘汰顺序。

    // fill 去重 / 路由清理侧道计数（inc 均在 ledger_mu_ 内，atomic 使读侧免锁）。
    std::atomic<uint64_t> per_order_fill_dups_{0};
    std::atomic<uint64_t> cross_domain_fill_dups_{0};
    std::atomic<uint64_t> self_trade_leg_passthrough_{0};
    std::atomic<uint64_t> per_order_fill_overflow_{0};
    std::atomic<uint64_t> orderless_fill_routed_{0};
    std::atomic<uint64_t> orderless_fill_dups_{0};
    std::atomic<uint64_t> route_erased_ack_{0};
    std::atomic<uint64_t> route_erased_reconcile_{0};
};

}  // namespace binance_u
