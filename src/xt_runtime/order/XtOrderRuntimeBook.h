#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "api/StrategySid.h"
#include "xt_core/order/XtInstrumentRegistry.h"
#include "order/XtOrderSnapshot.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtOffset.h"
#include "xt_core/order/XtOrder.h"
#include "xt_account/api/AccountCloseableView.h"
#include "xt_util/sync/XtHazardDomain.h"
#include "xt_util/sync/XtSnapshotPool.h"

struct AliveInflightOrder;
struct XtTradeRecord;

namespace xt_account {
struct CloseableView;
}  // namespace xt_account

namespace xt_runtime {
namespace order {

// 引入 xt_core 的 instrument id 类型（XtInstrumentId / XtInstrumentRegistry）。
using xt_core::XtInstrumentId;
using xt_core::XtInstrumentRegistry;
using xt_core::kInvalidInstrumentId;
using xt_core::kMaxInstruments;

/// @brief reverse-sync fillup 实际接受/复活（建成或更新 slot）的一单——供上层 STP re-arm。
///
/// book 「接受」= 已过 terminal-tombstone skip（book 拒绝复活已终态 id）且成功建/更新
/// slot，即 book exchange-as-truth 裁定「该单活着」。engine 对该清单逐单调
/// XtLiveOrderGateway::rearm_active_order，让 STP 跟随 book 裁决（book 说活即活）。
struct AcceptedRearmOrder {
    std::string code;         ///< 合约代码
    uint64_t    id = 0;       ///< 订单唯一 id（venue local_id）
    bool        is_buy = true;///< 买卖方向
    int64_t     price_fp = 0; ///< 限价（fixed-point；STP 上膛价）
};

/// @brief 进程内在途订单的权威账本。
///
/// - 每笔订单从提交到终态的本地状态记录
/// - submit、回报和 recovery 以 XtOrderRuntimeBook 为准更新和查询。
/// - engine worker 单线程写；策略在同一 worker 线程可直读重建 open-order 列表，
/// - 其他线程只能读 Hazard Pointer 保护的 immutable OrderBookSnapshot，不碰内部 slot。
class XtOrderRuntimeBook {
public:
    /// @brief 构造期配置；运行期不可改（无 setter，避免中途换语义的观测窗口）。
    struct Config {
        /// pre-registration 暂存（update + trade 两表共用）TTL，单位 ms。
        /// <=0 表示下次 sweep 即过期——测试用确定性驱动，避免 sleep。
        int64_t pre_registration_ttl_ms = 5000;
    };

    XtOrderRuntimeBook();
    explicit XtOrderRuntimeBook(const Config& cfg);

    /// @brief 支持 bootstrap 阶段按值持有/返回的 move-only book。
    ///
    /// 被移动对象必须处于 marshal 未绑定状态；marshal 绑定捕获 `this`，
    /// 不随对象迁移。常规 bootstrap 路径在对象落定后再绑定 worker marshal，
    /// 未绑定直读线程由 first-touch 重建。
    XtOrderRuntimeBook(XtOrderRuntimeBook&& other) noexcept;
    XtOrderRuntimeBook& operator=(XtOrderRuntimeBook&& other) noexcept;
    XtOrderRuntimeBook(const XtOrderRuntimeBook&) = delete;
    XtOrderRuntimeBook& operator=(const XtOrderRuntimeBook&) = delete;

    // ── Submit 侧 worker marshal（跨线程注册栅栏）──────────────────────────
    /// @brief off-worker 注册闭包的入队函数；返回 false 表示队列拒绝（满/已停）。
    using WorkerPostFn = std::function<bool(std::function<void()>)>;

    /// @brief 绑定 submit 侧注册的 worker 线程与入队通道。
    ///
    /// runner 在 event loop start 后调用一次；未绑定视为单线程模式，所有注册
    /// 内联执行。post 必须把闭包投递到与 td order rtn 同一条事件队列：同队列
    /// FIFO 保证 off-worker 提交的注册先于该单首条回报被 worker 处理。
    /// @pre 绑定后对象地址不再变化（marshal 闭包按 this 捕获；move 不转移绑定）。
    void bind_worker_marshal(std::thread::id worker_tid, WorkerPostFn post);

    /// @brief 解除 marshal 绑定并重置直读 first-touch 绑定（test fixture seam）。
    ///
    /// 供 test fixture 在 event loop stop/join 后调用，让后续主线程直读
    /// （collect_open_orders 等）按 first-touch 重新绑定。生产路径不调用。
    /// @pre 调用时不得有并发的 submit / post_to_worker 在途。
    void unbind_worker_marshal();

    /// @brief 当前线程是否可安全直写 book。
    ///
    /// 未绑定（单线程 / backtest）或已 enter_quiescent（worker 已 join，无并发写者）
    /// 恒 true；否则要求当前线程 == 绑定的 worker 线程。
    [[nodiscard]] bool is_bound_worker_thread() const noexcept;

    /// @brief 把 off-worker 注册闭包必达投递到 worker 执行（不丢注册）。
    ///
    /// 投递优先级：(1) marshal 入队到 worker（与 td rtn 同队列 FIFO）；(2) 入队失败
    /// （stop 窗口 / 物理满）暂存有界溢出侧道，worker 每轮 / quiescent 收尾 drain；
    /// (3) quiescent（worker 已 join）内联执行。返回 true 表示已投递；仅未绑定
    /// （防御，caller 已按 is_bound_worker_thread 分流）或溢出侧道打满（理论极限，
    /// loud drop）返回 false。off-worker caller 一律不得在返回 false 时回退跨线程直写。
    [[nodiscard]] bool post_to_worker(std::function<void()> fn) const;

    /// @brief 标记 worker 已 join，此后 post_to_worker 内联执行（shutdown 收尾用）。
    /// @pre 调用后不得再有 worker 线程并发写 book（runner 在 event loop stop/join 后调用）。
    void enter_quiescent() noexcept;

    /// @brief 执行溢出侧道暂存的注册闭包。
    ///
    /// 调用点：HFT event loop 每轮 hook + book 自身 on_trade/on_order_update 入口
    /// 自清（覆盖 CTA/SEL 等无 loop hook 的宿主）+ runner quiescent shutdown 收尾。
    /// @pre 仅 worker 写线程或 enter_quiescent 后调用；侧道空时 O(1) 早退。
    void drain_marshal_side_channel();

    /// @brief runner shutdown 收尾：置 quiescent 闩 + 最终 drain 溢出侧道，一次调用完成。
    ///
    /// 两步必须按此序且不可漏做后半：先置闩使后续 post 不再入侧道（改内联直写），
    /// 再无条件进临界区 drain 清尾（stop 窗口 post 失败入侧道的注册不丢）。
    /// 最终 drain 与 push 后 quiescent 复查经 marshal_side_channel_mu_ 定序：任一
    /// pusher 临界区晚于本调用的，必能看到先行置位的闩并自清——关死「post 读旧闩 +
    /// push 晚于最终 drain」的驻留窗。worker 每轮 drain 仍走带早退 hint 的
    /// drain_marshal_side_channel()。
    /// @pre event loop 已 stop/join（无并发写者）；调用方若另持 AccountStateStore，
    ///      须在本调用**之前**同步置其 quiescent（drain 出的 gateway 注册闭包会在非
    ///      worker 线程 apply(order_accepted)）。
    void shutdown_quiesce();

    // Submit 与 execution 回报
    /// @brief submit 注册的结果分派（终态 tombstone 消费收进注册入口内部）。
    struct SubmitRegisterResult {
        enum class Outcome : uint8_t {
            kRegistered,       ///< slot 建立，pre-registration replay 已完成
            kSkippedTerminal,  ///< 终态 tombstone 被消费，未建 slot
            kBlockedDuplicate, ///< 同 id 命中存活 slot，fail-closed 拒绝注册
            kDeferred,         ///< off-worker 已投递 marshal，结果异步（不得同步消费进度）
            kDropped,          ///< marshal 投递失败（溢出侧道打满），注册未落地
        };
        Outcome outcome = Outcome::kRegistered;
        /// kRegistered 时 = replay 后累计成交（max(filled_upd, filled_trd)）；其余 outcome 恒 0。
        int64_t filled_fp = 0;
        /// kRegistered 时 = replay 后剩余量（同 slot_leaves_from_filled 口径）；其余 outcome 恒 0。
        int64_t leaves_fp = 0;
    };

    /// @brief 记录本进程已提交订单，并初始化平仓冻结量。
    ///
    /// 注册入口自带终态 tombstone 消费：注册前消费一次（终态先到 → kSkippedTerminal），
    /// pre-registration replay 后再消费一次（replay 把该单推到终态 → kSkippedTerminal），
    /// caller 不需要（也无法）在外面单独调用 skip 判定。
    ///
    /// @param sid per-strategy ID；no-sid/single-strategy 使用 0。
    ///            slot.sid_id 是 XtOrder.sid_id 的 hot-path mirror，成交回报用
    ///            order_id 反查策略归属。
    /// @return 见 SubmitRegisterResult：caller 按 outcome 分派 account 冻结/释放。
    ///         kRegistered 且 `filled_fp > 0` 表示 replay 已推进成交，caller 应据此
    ///         合成一次非终态 account update 让 frozen 按增量释放（ORBR-1）。
    /// @note off-worker 调用（closeout/panel 控制线程）自动经 post_to_worker 必达
    ///       投递到 worker 执行（入队 / 溢出侧道 / quiescent 内联），绝不跨线程直写；
    ///       此时返回 kDeferred（投递成功）或 kDropped（侧道打满），progress 字段无意义。
    /// @note fail-closed：`id` 命中仍存活的 slot 时保留旧 slot 全部进度（成交量 /
    ///       close 冻结 / sid 归属）并返回 kBlockedDuplicate，递增
    ///       `xt_runtime_book_drops::duplicate_submit_blocked()`。order id 由
    ///       per-instance 单调发号器产出、recovery 反向灌走独立入口，故重复注册
    ///       一律是上游发号/重投缺陷，覆盖会静默抹掉在途单的账本状态。
    SubmitRegisterResult on_order_submitted(uint64_t id,
                                            const char* code,
                                            bool is_buy,
                                            int64_t qty_fp,
                                            int64_t price_fp,
                                            XtOffset close_offset,
                                            const xt_account::CloseableView& closeable_view,
                                            StrategySid sid);

    /// @brief no-sid overload，供单策略/backtest caller 使用。
    SubmitRegisterResult on_order_submitted(uint64_t id,
                                            const char* code,
                                            bool is_buy,
                                            int64_t qty_fp,
                                            int64_t price_fp,
                                            XtOffset close_offset,
                                            const xt_account::CloseableView& closeable_view);

    /// @brief 处理成交回报并推进 leaves / close-outstanding 状态。
    ///
    /// `tr.order_id == 0` 或成交量非正时忽略；未知 order id 的成交不创建 slot。
    void on_trade(const XtTradeRecord& tr);

    /// @brief 按 order id 直读 slot 的 sid_id（worker 线程同线程直读）。
    ///
    /// stamp-miss（回报 sid_id==0，sidecar 遗忘/cancel-fill race）时 stop book
    /// 路由的反查源 —— 必须在终态 on_order_update（erase slot）**之前**调用。
    /// @return sid_id；未知 id / off-worker（fail-closed，同 collect_* 直读契约）返 0。
    [[nodiscard]] uint32_t order_sid_id(uint64_t id) const;

    /// @brief 处理订单状态回报。
    ///
    /// 非终态 update 未命中既有 slot 时先进 pre-registration 暂存表（注册闭包可能仍在
    /// marshal 队列途中，venue 同步 ack 先到）；注册落地时按到达序重放。TTL 内无注册
    /// （真孤儿：reverse sync 漏 / foreign order）或表满才计入 orphan update drop。
    /// 终态回报会记录 tombstone，避免 submit ack 乱序晚到时重新建 slot，并清除同 id
    /// 暂存项（stale）。
    void on_order_update(const XtOrderExecUpdate& upd,
                         bool final_status,
                         int64_t effective_remaining_fp);

    // ── Pre-registration 暂存表（先到的非终态 update）─────────────────────
    /// @brief 当前暂存的 pre-registration update 条数（worker 同线程直读，测试观测用）。
    [[nodiscard]] size_t pre_registration_pending_count() const noexcept {
        return pre_reg_stash_.size();
    }

    /// @brief 当前暂存的 pre-registration Trade 条数（worker 同线程直读，测试观测用）。
    [[nodiscard]] size_t pre_registration_pending_trade_count() const noexcept {
        return pre_reg_trade_stash_.size();
    }

    /// @brief 周期清理暂存表中 TTL 过期的 stale 早到 update（挂在 reap 节拍上调用）。
    ///
    /// 内部 sweep 此前只在 stash 插入 / 注册重放时懒触发；无后续 off-worker 注册或
    /// 回报的静默期，超时未注册的暂存项会滞留不被清理/计数。挂在 orphan reaper 节拍上
    /// 做周期 sweep：TTL 过期项按既有 orphan 口径计数（`orphan_update_dropped` +
    /// metric label "orphan_update"）+ 限频日志后移除。
    /// @pre worker 单写线程调用（同 on_order_update / on_order_submitted 契约）。
    void sweep_stale_pre_registration() { sweep_expired_pre_registration_(); }

    // Recovery 与 tracked symbols
    /// @brief 启动期把 code 预注册进 instrument registry 并发布 snapshot；空 code 忽略。
    ///
    /// **只暖 registry，不进 tracked set** —— 与 `track_code` 的唯一区别。tracked set
    /// 是承重语义：驱动 `sync_position_snapshot` 的 stale-code 判定（→ account 清零）、
    /// closeout 的 `collect_tracked_codes` 收集、以及 snapshot 的 tracked_codes 数组。
    /// 本入口只消除首次 code→XtInstrumentId 的冷查找，不声明该合约「在管」。
    void pre_warm_instrument(const char* code);

    /// @brief 将 code 加入 tracked set 并发布 snapshot；空 code 被忽略。
    /// @note 同时完成 instrument 注册；只需暖 registry 而不想进 tracked set 时用
    ///       `pre_warm_instrument`。
    void track_code(const char* code);

    /// @brief 按 recovery 类型重置或合并 runtime book 状态。
    ///
    /// @param position_codes venue recovery 后仍有持仓/在途语义的合约列表。
    /// @param is_cold_start true 表示进程冷启，全量清空 slot_pool_ 与聚合状态；
    ///                      false 表示 warm reconnect，仅合并 tracked_codes_，
    ///                      保留本进程已知 alive in-flight 订单。
    void reset_for_recovery(const std::vector<std::string>& position_codes,
                            bool is_cold_start);

    /// @brief 读取当前 tracked code 列表。
    ///
    /// `out` 调用前会被清空；结果来自 immutable snapshot，允许跨线程 reader 调用。
    void collect_tracked_codes(std::vector<std::string>& out) const;

    /// @brief Recovery 阶段按 venue alive in-flight 列表重建本地在途订单视图。
    ///
    /// 以 `local_id` 为 key 执行 upsert；`local_id == 0` 或空 code 的记录会被忽略。
    /// 对平仓单，调用方通过 `view_for(code, is_buy_to_close)` 提供当前可平量投影；
    /// `view_for` 为空时按无可平量兜底。
    /// @note 本函数一次 batch 发布一个新 snapshot；slot pool 满时对应订单被丢弃并递增
    ///       `xt_runtime_book_drops::slot_pool_full_drops()`。
    /// @note venue 回传的 total/filled 定点量先过 sanity 上限（负或 > 1e15 fp 视为反解/传输
    ///       损坏）：越界即 clamp 到 [0,cap]、递增 `venue_fillup_qty_sanitized()` 并每批限频
    ///       告警，保守续跑恢复而非弃整批。
    /// @param accepted_out 非空时，收集本 batch 实际接受/复活（建成或更新 slot）的每一单
    ///       （见 AcceptedRearmOrder）——供 engine 逐单驱动 STP re-arm（跟随 book 裁决）。
    ///       book 拒绝复活的 terminal-tombstone id 不入此清单，故 STP 不会为 book 认死的
    ///       id 上膛。worker 单写者线程调用；下游 rearm 只碰 STP（不回写 book）。
    void fillup_alive_inflight_orders_batch(
        const std::vector<AliveInflightOrder>& orders,
        std::function<xt_account::CloseableView(const std::string& code,
                                                 bool is_buy_to_close)> view_for,
        std::vector<AcceptedRearmOrder>* accepted_out = nullptr);

    /// @brief 同步持仓快照并返回需要清零的 stale code 列表。
    ///
    /// recovery 模式下，stale code 表示此前 tracked 但本轮 venue 快照缺席的合约；
    /// 非 recovery 模式下，stale code 表示本轮持仓快照未覆盖的已跟踪合约。
    [[nodiscard]] std::vector<std::string> sync_position_snapshot(
        const std::vector<std::string>& position_codes,
        bool is_recovery,
        bool is_cold_start);

    // Closeout / cancel helpers
    /// @brief 读取指定 code/side 的平仓在途冻结量。
    ///
    /// 输出参数调用前会被清零。
    void accumulate_close_outstanding_fp(const char* code,
                                         bool is_buy,
                                         int64_t& out_today_fp,
                                         int64_t& out_yesterday_fp) const;

    /// @brief 查询指定 code/side 的开仓未成交数量。
    ///
    /// 只统计非平仓单（`is_close == false`）且 `leaves_qty_fp > 0` 的 snapshot 条目。
    [[nodiscard]] int64_t open_no_trade_qty_fp(const char* code, bool is_buy) const;

    /// @brief 查询未完成订单总 leaves 数量。
    ///
    /// `code == nullptr` 或空字符串表示汇总所有 code；返回值已 clamp 到非负。
    [[nodiscard]] int64_t undone_qty_fp(const char* code) const;

    /// @brief 查询指定 sid/code 的 signed in-flight 数量。
    ///
    /// 计算 Σ(is_buy ? +leaves : -leaves)。sid.has_value() 时按 sid_id 过滤；
    /// no-sid(0) 表示单策略路径，按 code 汇总所有 sid。
    [[nodiscard]] int64_t signed_inflight_fp(StrategySid sid, const char* code) const;

    /// @brief 为 closeout 流程收集当前 open-order 待撤订单 ID。
    ///
    /// 只收集 `leaves_qty_fp > 0` 的订单；`cancel_ids` 调用前会被清空。
    void collect_closeout_requests(std::vector<uint64_t>& cancel_ids) const;

    /// @brief 记录由 panel/closeout 发起的 genuine force-flat 订单。
    ///
    /// `oid` 必须非 0，`code` 必须非空；无效输入会 abort。`sid` 决定该强平单的归属，
    /// 后续窄口径 force-flat 查询只认本入口登记的订单；runner-wide emergency 强平显式
    /// 传 `StrategySid::legacy()`（sid 无默认值，全局作用域必须写出来）。
    void record_force_flat_order(uint64_t oid,
                                 const std::string& code,
                                 bool is_buy,
                                 int64_t qty_fp,
                                 XtOffset close_offset,
                                 StrategySid sid);

    // Snapshot-backed 查询
    /// @brief 收集指定 code 当前 active order 的 bid/ask 价格。
    ///
    /// 两个输出容器调用前都会清空；价格保持 order fixed-point scale。
    /// @note STP 已改 id-keyed active set，不再是其 terminal 重建数据源；本方法仅
    ///       余单测覆盖（XtOrderRuntimeBookFillupTest），保留供白盒断言/诊断用。
    void collect_active_order_prices(const char* code,
                                     std::vector<int64_t>& bid_prices,
                                     std::vector<int64_t>& ask_prices) const;

    /// @brief 该 id 是否已进入终态 tombstone（trade/update 任一终态路径记录）。
    /// @note engine.on_trade 用于判定 trade-first 全成后再 notify STP 释放边界。
    ///       worker 单写者查询（同 slot_pool_ 直读契约），O(1) 集合查找。
    [[nodiscard]] bool terminal_order_seen(uint64_t id) const noexcept {
        return terminal_order_seen_(id);
    }

    /// @brief 收集指定 code 的所有 working order id。
    ///
    /// `ids` 调用前会被清空；仅返回 `leaves_qty_fp > 0` 的订单。
    void collect_working_ids_for_code(const char* code,
                                      std::vector<uint64_t>& ids) const;

    /// @brief 收集指定 code 且归属指定 sid 的 working order id。
    ///
    /// `sid` 无值时返回空结果；`ids` 调用前会被清空。
    void collect_working_ids_for_code_and_sid(const char* code,
                                              StrategySid sid,
                                              std::vector<uint64_t>& ids) const;

    /// @brief code 是否有任意平仓挂单（is_close && leaves>0）在途——宽口径。
    /// closeout 收集（HftCloseoutGate）用此避免对已有 working close 的 code 再发
    /// force-flat。含策略普通平仓单；panel 重复强平守卫不用此（用下方窄口径）。
    [[nodiscard]] bool is_force_flat_pending(const char* code) const;

    /// @brief 查询 code 当前 force-flat pending 条目的发起方 sid_id。0 = global/no-sid scope。
    /// @note 宽口径（条目含策略普通平仓单的发起 sid）；供诊断和测试断言
    ///       snapshot 条目的发起方 sid 归属，不参与 panel 重复强平守卫。
    [[nodiscard]] uint32_t force_flat_pending_sid_for(const char* code) const;

    /// @brief code 是否有「真·force-flat」单（panel/closeout 经 record_force_flat_order 发起）在途——窄口径。
    /// 区别于 is_force_flat_pending：不含策略普通平仓挂单，故 panel runner-wide 紧急平仓不会被
    /// 策略 passive 平仓单误阻断（落到 cancel-first 接管，与 runner closeout 语义对称）。
    [[nodiscard]] bool is_genuine_force_flat_pending(const char* code) const;

    /// @brief code 是否有指定 sid 发起的 genuine force-flat 单在途。
    [[nodiscard]] bool is_genuine_force_flat_pending_for_sid(const char* code, StrategySid sid) const;

    /// @brief code 是否有 global/no-sid scope 的 genuine force-flat 单在途。
    [[nodiscard]] bool has_genuine_global_force_flat_pending(const char* code) const;

    /// @brief 当前 snapshot 中 `leaves_qty_fp > 0` 的订单数量。
    [[nodiscard]] size_t open_order_count() const;

    /// @brief 当前 snapshot 中 tracked code 的数量。
    [[nodiscard]] size_t tracked_code_count() const;

    /// @brief 从 immutable snapshot 按 order id 查询订单快照；未命中返回空 `OrderSnapshot`。
    [[nodiscard]] OrderSnapshot snapshot_order_by_id(uint64_t id) const noexcept;

    /// @brief 取整本 immutable snapshot 的 HazardGuard（白盒断言用，prod 走领域查询）。
    /// @note test-observation 契约：生产路径一律走领域查询（snapshot_order_by_id /
    ///       accumulate_close_outstanding_fp / tracked_code_count / open_order_count）。
    ///       本入口只服务无派生查询可表达的整体量断言 —— snapshot `generation`
    ///       （batch 单次 publish 语义）与 `force_flat_pending_count`（条目数）。
    /// @warning guard 生命周期内禁止阻塞/长持：guard 钉住 8-deep SnapshotPool 的
    ///          一个槽位，长持跨多次写发布会耗尽 pool → publish 被跳过、所有
    ///          跨线程 reader 静默读 stale（snapshot_publish_skipped 计数）。
    [[nodiscard]] xt_util::sync::HazardGuard<const OrderBookSnapshot>
    snapshot_all_orders() const noexcept;

    /// @brief Worker 线程直读 open orders 并重建策略 DTO（同线程 only，不取 hazard guard）。
    ///
    /// 遍历 mutable slot_pool_，跳过 leaves_qty_fp<=0 的 slot（open-set 定义：
    /// effective_remaining<=0 不算 open + 匹配 rollover 自身 filter）。重建的
    /// XtOrderExecUpdate：真值字段取 slot（id/is_buy/close_offset/price_fp/leaves_fp/status/sid_id）；
    /// qty_fp/filled_fp 由 slot 派生（非承重）；symbol_id/last_fill_fp/时间戳留 0
    /// （open-order 枚举不带 exec 时间戳，要时取 on_order 回调）。
    ///
    /// @param code_filter nullptr/空=全部；非空=仅返回该 code 的 open 单（消灭 get_open_orders 二次 lookup）。
    /// @param sid_filter 0=全部；非 0=仅返回 slot.sid_id 匹配的单。多策略 route_by_sid
    ///   必须在 book 层过滤：max 截断沿 unordered_map 迭代序，book 外过滤会让他策略
    ///   单挤占缓冲（symbol-overlap + 高单量时本策略单可能被整批截掉，换月漏迁）。
    /// @return 写入 out 的条数（<= max）。
    /// @warning 仅 book 写线程可调（bind_worker_marshal 绑定的 worker；未绑定时
    ///          first-touch 线程）。其它线程调用直读 mutable slot_pool_ 是 data race，
    ///          release 下 fail-closed 返回 0 并递增 cross_thread_direct_read_blocked()。
    [[nodiscard]] size_t collect_open_orders(XtOrderExecUpdate* out, size_t max,
                                             const char* code_filter = nullptr,
                                             uint32_t sid_filter = 0) const;

    /// @brief Worker 线程：收集 recovered-unowned 孤儿单待撤 id，并清理 venue 已不存在的 orphan slot。
    ///
    /// 首次命中且实际交付 caller 撤单（写入 out[]）的孤儿 slot 才标记 orphan_reap_attempted
    /// 并返回其 id（供 caller 发撤单）；超过 max 的孤儿本轮不写 out[] 也不标记，下一轮重进
    /// 首轮分支再获撤单机会（防未发撤单先 erase venue 活单）。
    /// 再次命中（说明上一轮撤单 venue 返 -2011 Unknown order、gateway kCancelNoop 保活
    /// 未清 slot）判定 venue 已无此单，直接 erase slot —— 根除 reap→-2011→保活→reap 的
    /// 死循环（孤儿单非本进程 submit，无 cancel 跑赢 venue 注册的竞态窗口）。
    /// @return 写入 out 的待撤 id 条数（<= max）。被 erase 的 orphan slot 计入 orphan_reap_dropped()。
    /// @param reaped_ids 非空时，收集本轮**硬删**（次轮命中直接 erase_slot）的 id——该路径不发
    ///        terminal notify，engine 需据此逐 id 调 `notify_order_terminal(id)` 清理 STP 私有
    ///        条目（否则 STP bound 残留 → 同价反向单永久过度拒绝，跨重启不愈；R4/L2）。
    /// @warning 线程契约同 collect_open_orders：非 book 写线程调用 fail-closed 返回 0。
    [[nodiscard]] size_t reap_recovered_unowned(uint64_t* out, size_t max,
                                                std::vector<uint64_t>* reaped_ids = nullptr);

private:
    struct CloseOutstandingKey {
        XtInstrumentId instrument_id = kInvalidInstrumentId;
        bool is_buy = true;

        bool operator==(const CloseOutstandingKey& rhs) const noexcept {
            return instrument_id == rhs.instrument_id && is_buy == rhs.is_buy;
        }
    };

    struct CloseOutstandingKeyHash {
        size_t operator()(const CloseOutstandingKey& key) const noexcept {
            const uint64_t packed =
                (static_cast<uint64_t>(key.instrument_id) << 1) |
                static_cast<uint64_t>(key.is_buy ? 1 : 0);
            return std::hash<uint64_t>{}(packed);
        }
    };

    struct CloseOutstandingAgg {
        int64_t today_fp = 0;
        int64_t yesterday_fp = 0;
    };

    struct OrderLifecycleSlot {
        uint64_t id = 0;
        XtInstrumentId instrument_id = kInvalidInstrumentId;
        bool is_buy = true;
        /// close_offset 的派生量，不变量 `is_close == (close_offset != XtOffset::Open)`。
        /// 所有写路径（submit / update / fillup / force-flat）必须同步两字段。
        bool is_close = false;
        /// true = 本进程 panel/closeout 经 record_force_flat_order 发起的「真·force-flat」单；
        /// 普通策略平仓单（on_order_submitted, is_close=true）为 false。panel 重复强平守卫用
        /// 窄口径（is_genuine_force_flat_pending*）区分，避免策略 passive 平仓挂单误判为
        /// force-flat 在途而阻断 operator runner-wide 接管。closeout 收集仍用宽口径 is_close。
        /// @note recovery 反向灌入路径（fillup）不重建本标记——venue truth 无 force-flat 语义，
        ///       故 cold-start 重启后仍 venue-alive 的真·force-flat 降级为宽口径平仓单（该窗口内
        ///       panel dedup 退化为宽口径，cancel-first 接管仍正确）；warm reconnect 复用 slot 保留本标记。
        bool is_force_flat = false;
        XtOffset close_offset = XtOffset::Close;
        int64_t price_fp = 0;
        int64_t leaves_qty_fp = 0;
        /// 订单原始委托量；0 = 创建路径未知（兜底走相对扣减，理论不可达——
        /// 5 条创建路径均填）。venue update 携带 qty 时以 venue 为准覆盖。
        int64_t qty_fp = 0;
        /// 成交进度双估计器：同一笔成交 venue 既发 order_update（绝对累计 z）又发
        /// trade（增量 l），任一单独正确、对 leaves 叠加会双扣。两路各自单调推进，
        /// leaves 恒从 qty_fp - max(filled_upd_fp, filled_trd_fp) 重算，任意
        /// 到达时序幂等。
        int64_t filled_upd_fp = 0;   ///< update 路径绝对累计（max-merge，stale update 不回退进度）
        int64_t filled_trd_fp = 0;   ///< trade 路径增量累计（上游按 trade_id/recovery 去重）
        int64_t locked_today_fp = 0;
        int64_t locked_yesterday_fp = 0;
        int64_t filled_today_fp = 0;
        int64_t filled_yesterday_fp = 0;
        uint64_t seq = 0;
        /// sid_id 是 XtOrder.sid_id 的 slot 内镜像。
        /// engine.on_trade(fill) 用 fill.order_id → find_slot(id)->sid_id 反查 sid_id,
        /// 给 tracker.apply_fill(sid, ...) 用 (不靠 broker is_buy)。
        /// invariant: slot.sid_id == XtOrder.sid_id；0 表示 no-sid/single-strategy。
        uint32_t sid_id = 0;
        /// 订单状态镜像，仅供 collect_open_orders 重建策略 DTO 用。
        /// on_order_submitted / record_force_flat_order=Submitted；on_order_update 同步 upd.status；
        /// on_trade 部分成交置 PartiallyFilled；fillup_alive_inflight_orders_batch 置 Recovered。终态
        /// slot 已 erase，故此字段永远非终态。不进 OrderSnapshot（跨线程 reader 不需要，保持 snapshot 精简）。
        XtOrderStatus status = XtOrderStatus::Submitted;
        /// cross-restart 孤儿标记：reverse-sync insert 路径且 sid 解析为 0（本进程
        /// 无任何策略归属）时置 true。venue 回报会覆盖 status，但不清本标记（孤儿
        /// 归属不因状态推送改变）；后续 batch 解析出 sid≠0 或 slot 复用（submit /
        /// force-flat）时显式置 false。orphan reaper 据此回收。
        bool recovered_unowned = false;
        /// orphan reaper 已把本孤儿 slot 实际交付 caller 撤单（写入 reap 输出）过一次。
        /// 仅在真正交付撤单时置位——超过单轮 max 未交付的孤儿不置，留待下轮首轮分支重试。
        /// 下一轮仍为孤儿（venue 返 -2011 Unknown order、gateway kCancelNoop 保活不清 slot）
        /// 即判 venue 已无此单，直接 erase 防 reap 死循环。slot erase/复用时随
        /// recovered_unowned 重置。
        bool orphan_reap_attempted = false;
    };

    static constexpr uint16_t kMaxInflightOrders =
        static_cast<uint16_t>(kOrderSnapshotMaxOrders);
    static constexpr uint16_t kInvalidSlotIdx = 0xFFFF;
    static constexpr std::size_t kTerminalOrderCacheCap = 8192;

    // Slot pool 生命周期
    /// 双估计器合流：成交进度 = max(update 绝对累计, trade 增量累计)，
    /// leaves = qty - 进度。
    static int64_t slot_leaves_from_filled(const OrderLifecycleSlot& s);
    void init_slot_pool();
    uint16_t alloc_slot();
    void free_slot_by_idx(uint16_t idx);
    OrderLifecycleSlot* find_slot(uint64_t id);
    const OrderLifecycleSlot* find_slot(uint64_t id) const;
    OrderLifecycleSlot* upsert_slot(uint64_t id, bool& inserted);
    void erase_slot(uint64_t id);
    /// @brief 消费式终态 tombstone 判定；true = 该 id 终态已先到，不得建 slot。
    /// @pre `id != 0`。仅注册入口（on_order_submitted / record_force_flat_order）调用。
    [[nodiscard]] bool should_skip_submit(uint64_t id);
    [[nodiscard]] bool terminal_order_seen_(uint64_t id) const noexcept;
    void remember_terminal_order_(uint64_t id);

    // 数量投影 helper
    static int64_t unfilled_today_fp(const OrderLifecycleSlot& slot);
    static int64_t unfilled_yesterday_fp(const OrderLifecycleSlot& slot);
    static bool has_close_contribution(const OrderLifecycleSlot& slot);

    // CloseOutstanding 聚合维护
    void add_close_outstanding_delta(XtInstrumentId id,
                                     bool is_buy,
                                     int64_t delta_today_fp,
                                     int64_t delta_yesterday_fp);
    void add_slot_close_contribution(const OrderLifecycleSlot& slot);
    void remove_slot_close_contribution(const OrderLifecycleSlot& slot);
    /// @brief T1.153 tripwire：从 slot 池重算 close_outstanding_ 并比对；漂移则 loud +
    ///        counter + 采纳重算值自愈。挂 reap 节拍（worker 写线程）。返回 1=有漂移。0=无。
    size_t reconcile_close_outstanding_from_slots_();

    // 写路径 helper
    void track_code_unlocked(const char* code);
    void init_close_locks_on_submit(XtInstrumentId instrument_id,
                                    bool is_buy,
                                    int64_t qty_fp,
                                    XtOffset close_offset,
                                    const xt_account::CloseableView& closeable_view,
                                    OrderLifecycleSlot& slot) const;
    void accumulate_close_fills_on_trade(OrderLifecycleSlot& slot,
                                         int64_t fill_qty_fp,
                                         bool fill_today);
    [[nodiscard]] XtInstrumentId resolve_instrument_id_on_update(const XtOrderExecUpdate& upd) const;
    void upsert_order_atom_on_update(const XtOrderExecUpdate& upd,
                                     XtInstrumentId upd_instrument_id,
                                     int64_t effective_remaining_fp);
    /// @brief 用 submit 参数整槽初始化 slot；`is_close` 由 `close_offset` 派生。
    /// @note `close_offset == XtOffset::Auto` 违反 XtOffset.h 契约（sentinel 必须由提交
    ///       入口解析后才进 runtime book）：debug assert + 进程内 warn-once，行为不变。
    void populate_submit_slot_(OrderLifecycleSlot& atom,
                               uint64_t id,
                               XtInstrumentId iid,
                               bool is_buy,
                               int64_t qty_fp,
                               int64_t price_fp,
                               XtOffset close_offset,
                               StrategySid sid);
    // fillup_alive_inflight_orders_batch 的 per-order 步骤
    void fillup_populate_slot_(OrderLifecycleSlot& atom,
                               const AliveInflightOrder& ord,
                               XtInstrumentId iid,
                               bool is_close,
                               bool inserted_real,
                               int64_t leaves_fp);
    void fillup_resolve_sid_(OrderLifecycleSlot& atom,
                             const AliveInflightOrder& ord,
                             bool inserted_real);
    void fillup_alloc_close_locks_(OrderLifecycleSlot& atom,
                                   XtOffset close_offset,
                                   int64_t leaves_fp,
                                   xt_account::CloseableView& rem);
    static void fillup_alloc_filled_(OrderLifecycleSlot& atom, int64_t filled_fp);

    // Snapshot 发布
    [[nodiscard]] OrderSnapshot build_order_snapshot_(const OrderLifecycleSlot& slot) const noexcept;
    void publish_snapshot_() noexcept;

    /// @brief 直读线程契约检查（release 也生效）。
    ///
    /// bound marshal 时以 marshal_worker_tid_ 为权威；未绑定时 first-touch CAS
    /// 绑定当前线程。返回 false = 跨线程直读，caller 必须 fail-closed。
    [[nodiscard]] bool is_collect_thread_allowed_() const noexcept;

    /// @brief 直读契约失败的统一出口：计数 + metric + log-once；caller 随后返回 0。
    void on_cross_thread_direct_read_blocked_() const noexcept;

    /// @brief 把注册闭包暂存到溢出侧道；打满返回 false（loud drop）。
    [[nodiscard]] bool push_marshal_side_channel_(std::function<void()> fn) const;

    /// @brief 无条件进临界区的侧道 drain（不走 depth 早退 hint）；shutdown_quiesce 的后半步。
    void drain_marshal_side_channel_final_();

    // ── Pre-registration 暂存（worker-only，BT 单线程下注册严格先于 update → 不可达）──
    /// @brief 注册前先到的非终态 update 暂存项；到达序 = vector 序。
    struct PreRegStashEntry {
        uint64_t id = 0;                    ///< 订单 id（upd.id 镜像，查表 key）
        XtOrderExecUpdate upd;              ///< 原样回放的 update 载荷
        int64_t effective_remaining_fp = 0; ///< 暂存时刻的 effective remaining（回放透传）
        int64_t stash_ms = 0;               ///< 入表 steady ms（TTL 判定）
    };
    static constexpr std::size_t kPreRegStashCap = 64;

    /// @brief 注册前先到的独立 Trade 暂存项；到达序 = vector 序。
    ///
    /// 最小投影 POD：on_trade 的 slot 写路径只消费 order_id / qty_fp / is_today
    /// 三个字段（apply_trade_fill_ + accumulate_close_fills_on_trade），不存整个
    /// XtTradeRecord（10 个 std::string 成员）。
    /// @note 若 on_trade 的成交消费面扩大（读取更多 tr 字段），须同步扩本投影。
    struct PreRegTradeStashEntry {
        uint64_t id = 0;        ///< 订单 id（tr.order_id 镜像，查表 key）
        int64_t qty_fp = 0;     ///< 成交量增量（定点）
        bool is_today = false;  ///< 平今标志（close-lock 今昨分配）
        int64_t stash_ms = 0;   ///< 入表 steady ms（TTL 判定，共用 pre_reg_ttl_ms_）
    };
    static constexpr std::size_t kPreRegTradeStashCap = 64;

    /// @brief 暂存未命中 slot 的非终态 update；表满按真 orphan 直达丢弃（计数 + stderr）。
    void stash_pre_registration_update_(const XtOrderExecUpdate& upd,
                                        int64_t effective_remaining_fp);
    /// @brief 暂存未命中 slot 的独立 Trade；表满按真 orphan_trade 直达丢弃（计数 + stderr）。
    void stash_pre_registration_trade_(uint64_t id, int64_t qty_fp, bool is_today);
    /// @brief 懒清理：TTL 过期项按 orphan 计数（超时未注册）后移除（同扫 update + trade 两表）。
    void sweep_expired_pre_registration_();
    /// @brief 注册落地后按到达序重放同 id 暂存 update，并从表中移除。
    void replay_pre_registration_updates_(uint64_t id);
    /// @brief 注册落地后按到达序重放同 id 暂存 Trade（每笔重放前 find_slot miss-guard），并从表中移除。
    void replay_pre_registration_trades_(uint64_t id);
    /// @brief 消费一笔成交到既有 slot（双估计器推进 + 终态 erase / 部成 status）。
    ///
    /// caller 已定位 slot（on_trade 主路径 / replay 逐笔 find_slot miss-guard 后），
    /// 本 helper 不重取 slot——on_trade slot-found 热路径 0 额外查表。
    void apply_trade_fill_(OrderLifecycleSlot& atom, uint64_t id, int64_t qty_fp,
                           bool is_today);
    /// @brief 终态到达时静默清除同 id 暂存 update（stale，不计 orphan）。
    void clear_pre_registration_for_id_(uint64_t id);
    /// @brief 终态到达时静默清除同 id 暂存 Trade（stale，不计 orphan_trade）。
    void clear_pre_registration_trades_for_id_(uint64_t id);

    std::array<OrderLifecycleSlot, kMaxInflightOrders> slot_pool_;
    std::array<uint16_t, kMaxInflightOrders> free_stack_;
    uint16_t free_top_ = 0;
    std::unordered_map<uint64_t, uint16_t> id_to_slot_idx_;
    std::unordered_map<CloseOutstandingKey, CloseOutstandingAgg, CloseOutstandingKeyHash> close_outstanding_;
    uint64_t open_order_seq_ = 0;
    std::unordered_set<uint64_t> order_final_before_submit_;
    std::unordered_set<uint64_t> order_terminal_seen_;
    std::deque<uint64_t> order_terminal_fifo_;
    std::unordered_set<XtInstrumentId> tracked_codes_;
    /// publish_snapshot_ 的 (iid, sid)→out 索引去重 scratch；writer-only，复用容量避免每次发布
    /// 堆分配。map（非 set）以支持同 key 多 slot 时 OR 合并 is_genuine（不依赖迭代序）。
    std::unordered_map<uint64_t, std::size_t> publish_force_flat_keys_;

    XtInstrumentRegistry instrument_registry_;

    xt_util::sync::XtSnapshotPool<OrderBookSnapshot, 8> snapshot_pool_;
    xt_util::sync::HazardDomain hp_domain_;
    std::atomic<const OrderBookSnapshot*> current_{nullptr};
    int64_t next_generation_ = 1;
    /// marshal 未绑定时，collect_* 直读以 first-touch CAS 绑定允许线程。
    mutable std::atomic<std::thread::id> collect_first_touch_tid_{std::thread::id{}};
    // submit 侧 worker marshal 只在对象落定后绑定；marshal_bound_ 是
    // marshal_post_ / marshal_worker_tid_ 的 release/acquire 发布闸。
    // move 不转移这些字段，因为 post 闭包按对象地址捕获。
    WorkerPostFn marshal_post_;
    std::thread::id marshal_worker_tid_{};
    std::atomic<bool> marshal_bound_{false};
    /// worker join 后由 runner 置位：post_to_worker 改内联、is_bound_worker_thread /
    /// 直读闩恒放行（无并发写者）。BT/单测不置位。
    std::atomic<bool> marshal_quiescent_{false};

    // ── Marshal 溢出侧道（有界 mutex 保护）────────────────────────────
    // marshal 入队失败（stop 窗口 / 物理满）时暂存注册闭包，worker 每轮 drain（现实
    // 失败模式 = 停机窗口的低频控制面注册，远小于 cap；侧道打满是理论极限，loud drop）。
    static constexpr std::size_t kMarshalSideChannelCap = 1024;
    mutable std::mutex marshal_side_channel_mu_;
    mutable std::vector<std::function<void()>> marshal_side_channel_;  ///< 受 marshal_side_channel_mu_ 保护
    mutable std::atomic<std::size_t> marshal_side_channel_depth_{0};   ///< drain 早退用；push/drain 在锁内维护

    // ── Pre-registration 暂存表（worker 单写路径 only，无锁）────────────
    std::vector<PreRegStashEntry> pre_reg_stash_;  ///< 到达序暂存；≤ kPreRegStashCap
    std::vector<PreRegTradeStashEntry> pre_reg_trade_stash_;  ///< 到达序暂存；≤ kPreRegTradeStashCap
    int64_t pre_reg_ttl_ms_ = Config{}.pre_registration_ttl_ms;  ///< 构造期注入；两表共用
};

}  // namespace order
}  // namespace xt_runtime

namespace xt_runtime_book_drops {
std::atomic<uint64_t>& slot_pool_full_drops();
/// @brief 非终态 orphan update drop counter。
///
/// on_order_update 未命中 slot 的非终态 update 先进 pre-registration 暂存表
/// （注册在 marshal 途中的乱序被吸收，不计数）；TTL 内无注册（超时过期）或表满
/// 直达丢弃时递增。counter > 0 表示 reverse sync 漏单、foreign order 或
/// userTag 反解失败，需要生产告警。
std::atomic<uint64_t>& orphan_update_dropped();

/// @brief 注册前先到的独立 Trade drop counter（label "orphan_trade"）。
///
/// on_trade 未命中 slot 的独立 Trade 先进 pre-registration trade 暂存表（注册在
/// marshal 途中的乱序被吸收，不计数）；TTL 内无注册（超时过期）或表满直达丢弃时递增。
/// counter > 0 表示 reverse sync 漏单、foreign order 或成交回报早于注册且超时未落地，
/// 需要生产告警。label 独立于 orphan_update（同 xt_runtime_book_drop_total family）。
std::atomic<uint64_t>& orphan_trade_dropped();

/// @brief orphan reaper 判定 venue 已不存在并直接 erase 的 recovered-unowned slot 数。
///
/// reap_recovered_unowned 对同一孤儿单第二轮仍命中（上轮撤单 venue 返 -2011、gateway
/// kCancelNoop 保活未清 slot）时递增。counter 持续增长表示 cross-restart 后有 venue 已
/// 不存在的孤儿挂单被本地账本残留、靠 reaper 兜底清理（正常 < 个位数；持续高增查 reverse-sync）。
std::atomic<uint64_t>& orphan_reap_dropped();

/// @brief T1.153 LEAK-2 gate：stale venue 快照试图复活已见终态单被拦下的条数。
///
/// fillup_alive_inflight_orders_batch 对 terminal tombstone 命中的 local_id 直接跳过：
/// 快照拍在 fill/终态落地前的竞态窗口会把已 erase 的单复活成 Recovered 幽灵 slot
///（重上 close 账 + 污染 open 枚举）。正常偶发（重连窗口）；持续高增查 reverse-sync 时序。
std::atomic<uint64_t>& fillup_terminal_resurrect_blocked();

/// @brief T1.153 tripwire：close_outstanding_ 与 slot 池重算值漂移被自愈的次数。
///
/// 正常恒 0；非零 = 某写路径漏了 remove/add 括号（平仓 avail 会被残留 hold 卡死），
/// 已自愈但必须查根因。
std::atomic<uint64_t>& close_outstanding_drift_healed();

/// @brief snapshot pool 耗尽导致 publish 被跳过的次数。
///
/// publish 跳过期间跨线程 reader 持续读到 stale snapshot（订单状态静默滞后），
/// 等级不低于 slot_pool_full，需要生产告警。常见诱因：reader 长持 HazardGuard
/// 跨越多次写发布，8-deep pool 无法回收。
std::atomic<uint64_t>& snapshot_publish_skipped();

/// @brief 非 book 写线程调 collect_open_orders 等直读被 fail-closed 拦截的次数。
///
/// 正常 prod 调用图（策略 ctx / engine reap 均在 worker）应恒为 0；> 0 表示
/// 有代码从 panel/metrics 等线程误用直读接口（被拦截前是 data race）。
std::atomic<uint64_t>& cross_thread_direct_read_blocked();

/// @brief publish 时 tracked_codes_ 超过 snapshot 数组容量被截断的次数。
///
/// tracked code 数 > kOrderSnapshotMaxCodes(512) 时，snapshot 只装前 512 个，
/// 跨线程 reader（closeout 收集 / collect_tracked_codes）对被截断合约静默失明。
/// 生产 symbol 数 ≤ 个位数，应恒为 0；> 0 需要生产告警。
std::atomic<uint64_t>& tracked_codes_truncated();

/// @brief marshal 入队失败改走溢出侧道暂存的注册闭包数。
///
/// post_to_worker 入队 worker 失败（stop 窗口 / 物理满）时递增。现实主来源 = 停机窗口
/// 的 off-worker 注册；持续增长（非停机期）表示 td 事件队列长期满，需查 worker 卡顿。
std::atomic<uint64_t>& marshal_side_channel_used();

/// @brief 溢出侧道打满被丢弃的注册闭包数（理论极限）。
///
/// 低频控制面 producer + worker 每轮 drain 下 cap(1024) 不可达；> 0 表示 worker 长期
/// 不 drain 侧道（卡死）或控制面异常高频，需要生产告警。
std::atomic<uint64_t>& marshal_side_channel_overflow_dropped();

/// @brief on_order_submitted 命中仍存活 slot 被 fail-closed 拦下的注册数。
///
/// order id 出自 per-instance 单调发号器、recovery 反向灌走 fillup 独立入口，因此
/// 同 id 二次注册没有合法时序来源。counter > 0 = 上游发号重复 / 注册重投 / marshal
/// 闭包重复执行，本地已保留旧 slot 进度（不覆盖），需要生产告警查上游。
std::atomic<uint64_t>& duplicate_submit_blocked();

/// @brief recovery fillup 中 venue 回传定点量越界被 clamp 的订单数（按 order 计）。
///
/// fillup_alive_inflight_orders_batch 对 venue 反向灌的 total/filled 定点量做 sanity 上限
/// （负或 > 1e15 fp = 1e9 手视为 clientOrderId/OrderRef 反解或传输损坏），越界即 clamp 到
/// [0,cap] 并递增。正常应恒为 0；> 0 表示 venue 回传损坏，本地已保守 clamp 续跑恢复，需要
/// 生产告警核查 reverse-sync 反解链。
std::atomic<uint64_t>& venue_fillup_qty_sanitized();

#ifdef XT_BUILD_TESTS
/// @brief test-only 计数器重置 helper。
///
/// counter 是 process-global；测试 SetUp/TearDown 显式重置，避免跨 case 串扰。
/// 生产构建不暴露此入口。
inline void reset_for_test() {
    slot_pool_full_drops().store(0, std::memory_order_relaxed);
    orphan_update_dropped().store(0, std::memory_order_relaxed);
    orphan_trade_dropped().store(0, std::memory_order_relaxed);
    orphan_reap_dropped().store(0, std::memory_order_relaxed);
    fillup_terminal_resurrect_blocked().store(0, std::memory_order_relaxed);
    close_outstanding_drift_healed().store(0, std::memory_order_relaxed);
    snapshot_publish_skipped().store(0, std::memory_order_relaxed);
    cross_thread_direct_read_blocked().store(0, std::memory_order_relaxed);
    tracked_codes_truncated().store(0, std::memory_order_relaxed);
    marshal_side_channel_used().store(0, std::memory_order_relaxed);
    marshal_side_channel_overflow_dropped().store(0, std::memory_order_relaxed);
    duplicate_submit_blocked().store(0, std::memory_order_relaxed);
    venue_fillup_qty_sanitized().store(0, std::memory_order_relaxed);
}
#endif
}  // namespace xt_runtime_book_drops
