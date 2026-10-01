#pragma once
#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "ThostFtdcTraderApi.h"

/// @brief CTP 登录后恢复流程的状态机。
///
/// 进程重启或重连后本地是一张白纸，必须先把账户状态从柜台查回来才能交易。
/// CtpRecovery 按 持仓 → 资金 → 委托 → 成交 的顺序推进查询，全部完成后发布 ready
/// 查询的实际发起和结果落账都经 HostPorts 交回 TraderCTP。
/// login 和结算确认不归 CtpRecovery 管，属于外层 session 流程。
class CtpRecovery {
public:
    // ── 持仓快照 ──────────────────────────────────────────────
    /// @brief CTP recovery 阶段聚合后的持仓快照。
    /// @note CTP 通过 TodayPosition / YdPosition / PositionDate 推导今仓口径，long_today/
    ///       short_today 即结果；今昨可信度由 account store 账户级 closeable_known 标志承载。
    struct PositionSnapshot {
        std::string code;
        double long_qty          = 0.0;
        double long_today        = 0.0;
        double long_avg          = 0.0;   ///< 多头每手持仓成本（PositionCost / Position，含乘数）
        double short_qty         = 0.0;
        double short_today       = 0.0;
        double short_avg         = 0.0;   ///< 空头每手持仓成本（PositionCost / Position，含乘数）
        double long_frozen       = 0.0;   ///< broker-reported LongFrozen，总冻结量，不分今/昨
        double short_frozen      = 0.0;   ///< broker-reported ShortFrozen，总冻结量，不分今/昨
    };

    /// @brief 持仓恢复阶段完成后的快照发布口；参数引用仅在调用期间有效。
    using PositionsPublisher = std::function<void(const std::vector<PositionSnapshot>&)>;

    /// @brief recovery 完成发布口；同一 generation 只发射一次。
    using ReadyPublisher = std::function<void()>;

    /// @brief 启动一个 CTP 异步查询请求的宿主入口。
    ///
    /// `QueryStarter` 本身不携带阶段信息；阶段由 `HostPorts` 字段名表达。
    using QueryStarter = std::function<void()>;

    /// @brief Recovery 状态机依赖的宿主端口包。
    /// @note `start_*_query` 由 recovery 推进阶段时调用，最终回到宿主发起
    ///       CTP 查询；`publish_*` 在阶段完成后把聚合结果 / ready 交回宿主。
    /// @note 所有端口都在 recovery 锁外同步执行，允许查询入口反入
    ///       `TraderCTP::query_once(...)`。
    struct HostPorts {
        PositionsPublisher publish_positions;      ///< 持仓聚合完成后的落账出口
        QueryStarter       start_position_query;   ///< 发起持仓查询
        QueryStarter       start_account_query;    ///< 发起资金查询
        QueryStarter       start_order_query;      ///< 发起委托查询
        QueryStarter       start_trade_query;      ///< 发起成交查询
        ReadyPublisher     publish_ready;          ///< recovery 完成后的 ready / reverse-sync 出口
    };

    // ── 依赖绑定 ──────────────────────────────────────────────
    /// @brief 构造期绑定宿主端口，运行期不可重绑。
    explicit CtpRecovery(HostPorts ports);

    // ── 恢复控制 ──────────────────────────────────────────────
    /// @brief 设定是否启用登录后恢复查询流程。
    /// @note bootstrap-only：由 TraderCTP 装配期设定一次，不在运行期切换。
    void set_recovery_enabled(bool enabled);
    /// @brief 恢复流程是否启用（enabled 单一存储的读口；冷路径，持锁读）。
    bool recovery_enabled() const;
    /// @brief 当前是否仍处于恢复中（无锁 atomic 读）。
    ///
    /// risk InputGuard 路径会频繁调用；实现不得加锁。
    /// 内部状态机在持锁期间同步写入 recovering_，以 acquire/release 保证可见性。
    bool is_recovering() const;

    // ── 恢复流程推进 ──────────────────────────────────────────────
    /// @brief 重置 recovery 状态，用于 front reconnect / session reset。
    void reset();
    /// @brief 进入恢复入口：恢复模式查持仓，非恢复模式查资金或直接 ready。
    void on_login();

    /// @brief 开始新一轮持仓查询：绑定 request_id 归属并清空聚合缓存。
    ///
    /// 由查询发送端（recovery pipeline 与 Ready 稳态 refresh 共用同一发送入口
    /// request_query）在 ReqQryInvestorPosition 之前调用，令本轮查询独占聚合缓存。
    /// 配合 on_position 的 request_id 归属判定，杜绝上一轮超时重发后新旧两轮 row
    /// 叠加造成的持仓翻倍/混轮。
    /// @param request_id 本轮发往柜台的 nRequestID，回包时按其归属过滤。
    void begin_position_query(int request_id);

    /// @brief 推进持仓阶段：bIsLast 时锁外 emit positions，再查询资金。
    /// @param request_id 回包携带的 nRequestID（与 begin_position_query 绑定值比对）。
    /// @return 归属当前轮返回 true；过期轮回包返回 false，调用方据此跳过 bIsLast
    ///         收尾，避免旧轮 bIsLast 提前收口 / 清当前在途。
    /// @note fail-open：仅双方 id 均有效（owner!=0 且 request_id>0）才比对；
    ///       owner==0（无绑定/单测直调）全收，request_id<=0（柜台不回显 nRequestID）
    ///       接受 + unfenceable 计数——栅栏不得成为可用性单点。
    bool on_position(const CThostFtdcInvestorPositionField* pPosition,
                     bool bIsLast, int request_id = 0);

    /// @brief 判定某 nRequestID 是否归属当前持仓查询轮（错误回包路径的轻量前置检查）。
    /// @note owner==0（无绑定/单测直调）与 request_id<=0（柜台不回显，无法比对）
    ///       均视为归属（fail-open）；仅双方 id 有效且不匹配才判过期轮。
    bool position_query_owns(int request_id) const;

    /// @brief 记录一帧被归属栅栏拦截的过期轮持仓回包（错误回包路径调用）。
    void note_stale_position_frame();

    /// @brief 累计被归属栅栏拦截丢弃的过期轮持仓回包帧数（进程级观测，单调不清零）。
    uint64_t stale_position_frame_dropped() const;

    /// @brief 累计 owner 已绑定但 request_id<=0 无法比对而 fail-open 接受的帧数（进程级观测）。
    uint64_t stale_position_frame_unfenceable() const;

    /// @brief 推进资金阶段：恢复模式查询委托，非恢复模式直接 ready。
    void on_account_done(bool bIsLast);
    /// @brief 推进委托阶段：查询成交；若成交已先完成则收尾 ready。
    void on_order_done(bool bIsLast);
    /// @brief 推进成交阶段：若委托已完成则收尾 ready，否则等待委托阶段。
    void on_trade_done(bool bIsLast);

private:
    /// @brief on_position 阶段的持仓聚合中间状态。
    struct SideBucketAgg {
        double total          = 0.0;
        double today_by_date  = 0.0;  ///< PositionDate=今 行累加的今量
        double today_by_field = 0.0;  ///< TodayPosition 字段累加的今量
        double yd_sum         = 0.0;  ///< YdPosition 字段累加的昨量
        bool   saw_pos_date    = false;  ///< 是否见过 PositionDate 口径
        bool   saw_today_field = false;  ///< 是否见过 TodayPosition 字段
        bool   saw_yd_field    = false;  ///< 是否见过 YdPosition 字段
    };
    struct PositionBucketAgg {
        SideBucketAgg long_side;
        SideBucketAgg short_side;
    };

    /// @brief 聚合单行 CTP 持仓数据到缓存。
    /// @pre 调用方持有 mu_。
    void merge_position_row(const CThostFtdcInvestorPositionField* row);

    /// @brief 收敛今仓口径，构建快照列表并清空缓存。
    /// @pre 调用方持有 mu_。
    std::vector<PositionSnapshot> collect_snapshots();

    /// @brief 聚合单侧持仓数量到 bucket。
    /// @pre qty / today_field / yd 已 clamp 为非负值。
    static void accumulate_side(SideBucketAgg& side,
                                double qty, double today_field,
                                double yd, char pos_date);

    /// @brief 按 TodayPosition > 总量-YdPosition > PositionDate=今 推算今仓量。
    static double resolve_today_qty(const SideBucketAgg& side);

    /// @brief 幂等发射 ready，并用 reset generation guard 阻断旧 session 发布。
    void emit_ready();

    // ── 恢复状态 ──────────────────────────────────────────────
    bool recovery_enabled_    = false;
    bool recovery_done_       = false;
    bool recovered_positions_ = false;
    bool recovered_account_   = false;
    bool recovered_orders_    = false;
    bool recovered_trades_    = false;
    bool ready_emitted_       = false;

    uint32_t reset_gen_ = 0;               ///< reset generation，防旧 ready 发布穿透
    std::atomic<bool> recovering_{false};  ///< is_recovering() 无锁镜像，risk 热路径读取

    mutable std::mutex mu_;  ///< 保护恢复状态、持仓聚合缓存和 ready generation
    std::unordered_map<std::string, PositionSnapshot>  pos_cache_;  ///< 按合约聚合后的持仓快照缓存
    std::unordered_map<std::string, PositionBucketAgg> pos_bucket_cache_;  ///< 今仓口径聚合缓存
    /// 当前持有持仓聚合缓存的查询轮 nRequestID；0=无绑定（含单测直调与本轮完成后释放）。
    /// mu_ 保护；on_position 按其过滤过期轮回包，防超时重发后旧轮 row 叠加/混轮。
    int pos_query_owner_req_ = 0;
    std::atomic<uint64_t> stale_position_frame_dropped_{0};  ///< 混轮拦截丢弃帧计数（进程级，单调）
    std::atomic<uint64_t> stale_position_frame_unfenceable_{0};  ///< owner 已绑定但 request_id<=0 fail-open 接受帧计数

    PositionsPublisher publish_positions_;      ///< ctor 绑定；持仓阶段完成后锁外发布快照
    QueryStarter       start_position_query_;   ///< ctor 绑定；on_login 恢复模式下锁外发起持仓查询
    QueryStarter       start_account_query_;    ///< ctor 绑定；持仓阶段后或非恢复模式锁外发起资金查询
    QueryStarter       start_order_query_;      ///< ctor 绑定；资金阶段完成后锁外发起委托查询
    QueryStarter       start_trade_query_;      ///< ctor 绑定；委托阶段完成后锁外发起成交查询
    ReadyPublisher     publish_ready_;          ///< ctor 绑定；ready generation 复查通过后锁外发布完成信号
};
