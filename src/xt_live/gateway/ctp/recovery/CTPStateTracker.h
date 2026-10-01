#pragma once
#include <atomic>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "ctp/common/CTPUtil.h"
#include "xt_core/order/XtOrderLifecycle.h"
#include "xt_core/order/XtOrderTypes.h"
#include "xt_core/order/XtTradeRecord.h"
#include "xt_live/gateway/common/XtExecDeliveryLedger.h"
#include "ThostFtdcTraderApi.h"

/// @brief CTP 回报的翻译与去重层：把 CTP 各种回报转成内部统一订单/成交事件。
///
/// CTP 的委托回报、成交回报、查询应答和撤单错误分散在多个 SPI 回调里，
/// 字段口径也不统一。CtpStateTracker 做三件事：
/// - 把这些回报翻译成内部 `XtOrderExecUpdate` / `XtTradeRecord`
/// - 记住每笔订单的 CTP 路由字段（OrderRef/OrderSysID/FrontID/SessionID），撤单时反查
/// - 对重复回报去重，防止同一事件被处理两次
///
/// 成交去重判据 = 交付投影（`XtExecDeliveryLedger`）的 fill 记账（查+记一体，
/// exactly-once：同 key 任一来源先到恰好记账一次；记忆按进程生命周期保留，
/// recovery 不清）。终态 payload 同步写入投影。fill 的 absorbed/delivered 语义
/// 由 caller 按来源规则决定（`TraderCTP::on_rtn_trade_`）。
///
/// 只做翻译和记录，不调 gateway callbacks、不记账、不做风控。
/// 内部由 mutex 保护，线程安全。
class CtpStateTracker {
public:
    /// @brief CTP 撤单请求所需的路由字段快照。
    ///
    /// 由 `get_cancel_target()` 返回给 `TraderCTP::cancel_order()`，用于填充
    /// `CThostFtdcInputOrderActionField`。`ref` 是最小必需字段；其余字段来自
    /// `OnRtnOrder` 回填，缺失时调用方可按当前 front/session 做 conservative fallback。
    struct CancelTarget {
        std::string ref;           ///< CTP `OrderRef`；撤单请求的本地订单引用。
        std::string code;          ///< `InstrumentID`；有值时填入撤单请求以提高柜台匹配精度。
        std::string exchange_id;   ///< `ExchangeID`；有值时与 `OrderSysID` 组成交易所侧定位键。
        std::string order_sys_id;  ///< `OrderSysID`；CTP 接收订单后回填的交易所订单号。
        int front_id = 0;          ///< `FrontID`；合法值可为负，0 表示尚未从回报回填，调用方可使用当前 session。
        int session_id = 0;        ///< `SessionID`；合法值可为负，0 表示尚未从回报回填，调用方可使用当前 session。
    };

    // ── 本地状态注册与映射 ──────────────────────────────────────────
    /// @brief 初始化本地单号对应的回报补齐状态。
    ///
    /// 保存 submit 阶段已知字段；`OnRtnOrder` 后续会补齐 `OrderSysID`、`FrontID`
    /// 和 `SessionID`。该函数不创建领域订单，只初始化 tracker 内部缓存。
    /// @param absorbed_watermark_fp reverse-sync rebuild 注册跨重启单时的 venue
    ///        已成交量封印（pre-restart 成交不再逐笔交付，量口径由此承接）；
    ///        submit 路径传 0。
    /// @note submit 路径在 `ReqOrderInsert` 发送前预注册（SDK 极端同步回调也能
    ///       查到元数据与投影记录），此时 `submit_end_us` 传 0、发送成功后经
    ///       `note_submit_end` 回填。
    void register_order(uint64_t local_id,
                        const char* order_ref,
                        const char* code,
                        bool is_buy,
                        double price,
                        int64_t qty_fp,
                        const char* user_tag,
                        bool is_close,
                        bool is_today,
                        uint64_t submit_begin_us,
                        uint64_t submit_end_us,
                        int64_t absorbed_watermark_fp = 0);

    /// @brief 预注册失败回滚：删除本地元数据与交付投影记录。
    /// @note 仅用于 `ReqOrderInsert` 发送失败路径（请求未上 wire，无任何回调在途）；
    ///       local_id 跨重启单调不复用 → 回滚无残留语义。记录不存在时 no-op。
    void rollback_register(uint64_t local_id);

    /// @brief 回填 submit_end 延迟锚（预注册时不可知，`ReqOrderInsert` 返回后补）。
    void note_submit_end(uint64_t local_id, uint64_t submit_end_us);

    /// @brief 查询本地单号对应的 CTP OrderRef。
    bool get_order_ref(uint64_t local_id, std::string& out_ref) const;
    /// @brief 判断给定 OrderRef 是否属于当前进程登记过的本地单。
    bool has_local_order_ref(const char* order_ref) const;
    /// @brief 查询撤单路由字段（order ref + 会话/交易所键）。
    bool get_cancel_target(uint64_t local_id, CancelTarget& out) const;
    /// @brief 查询 local_id 是否已在终态集（撤单自愈探测：撤单撞 no_cancel_target 时区分
    ///        「真幽灵—gateway 已终态但 book/策略仍 open」与「未注册 unknown id」）。
    bool is_local_terminal(uint64_t local_id) const;
    /// @brief 用 recovery 查询行暂存的路由字段回填撤单路由键（reverse-sync rebuild 用）。
    /// @note 与内部 OnRtnOrder 回填同覆盖语义：仅非空/非零字段写入；entry 不存在时 no-op。
    void backfill_cancel_routing(uint64_t local_id,
                                 const std::string& exchange_id,
                                 const std::string& order_sys_id,
                                 int front_id,
                                 int session_id);

    // ── 事件归一化 ──────────────────────────────────────────────
    /// @brief 将 CTP `OnRspOrderInsert` error 转为内部 Rejected 订单事件。
    ///
    /// 只处理拒单分支：填充 `XtOrderExecUpdate{status=Rejected}` 后返回给
    /// `TraderCTP` 调用 `on_order_update`。转换成功会补齐本地 submit 元数据、
    /// 标记 local_id 终态，并经过订单去重；重复拒单返回 false。
    bool on_order_insert_reject(const CThostFtdcInputOrderField* pInputOrder,
                                const CThostFtdcRspInfoField* pRspInfo,
                                uint64_t local_cb_us,
                                XtOrderExecUpdate& out);
    /// @brief 将 CTP `OnRtnOrder` / `OnRspQryOrder` 转为内部订单状态事件。
    ///
    /// 按 CTP `OrderStatus` / `OrderSubmitStatus` 填充 `XtOrderExecUpdate` 后返回给
    /// `TraderCTP` 调用 `on_order_update`。转换成功会回填撤单路由字段、补齐本地
    /// submit 元数据；终态回报会标记 local_id 终态，并经过订单去重。
    /// @param out_suppressed_terminal  非 null 时，若本次回报是首次终态但被去重器吞掉
    ///        （progress_regressed/rank 残余乱序），置 true 并回填 out，让 caller 在非-recovery
    ///        路径合成终态投递（防终态丢失致幽灵挂单）。
    bool on_order(const CThostFtdcOrderField* pOrder,
                  uint64_t local_cb_us,
                  XtOrderExecUpdate& out,
                  bool* out_suppressed_terminal = nullptr);
    /// @brief 将 CTP `OnRtnTrade` / `OnRspQryTrade` 转为内部成交事件。
    ///
    /// 填充 `XtTradeRecord` 后按 CTP 成交唯一键经交付投影 `record_fill` 查+记
    /// 一体判重（exactly-once：任一来源先到恰好记账一次；已记账返回 false）。
    /// 记账成功即补齐本地 submit 元数据并回填 out；absorbed 行同样记账（抑制
    /// 后续同 key 重放），是否下发由 caller 决定。
    /// @param fill_source  成交行来源标注（查询重放行由 caller 传 kQueryReplay）。
    /// @param absorbed     true = 记账但不下发（cold 历史行 / pre-restart 封印），
    ///        由 caller 按来源规则判定（§4.3）。
    /// @param query_gen    交付该行时的查询会话代际（provenance 列）。
    bool on_trade(const CThostFtdcTradeField* pTrade,
                  uint64_t local_cb_us,
                  XtTradeRecord& out,
                  xt_live::gateway::FillSource fill_source =
                      xt_live::gateway::FillSource::kLivePush,
                  bool absorbed = false,
                  uint32_t query_gen = 0);
    /// @brief 将 CTP 撤单 error(26) 转为内部 Filled/Canceled 订单事件。
    ///
    /// error(26) 表示目标单已全成或已撤，CTP 不再返回可撤状态。函数按交付投影
    /// 累计成交量（含 absorbed 水位口径）推断 `Filled` / `Canceled`，补齐本地
    /// submit 元数据，标记 local_id 终态，并经过订单去重；非 26 error 返回 false。
    bool on_cancel_action_error(const char* order_ref,
                                int error_id,
                                uint64_t local_cb_us,
                                XtOrderExecUpdate& out);
    /// @brief fill 记账后的 completion 判定：累计交付量（封印水位 + Σ 非 absorbed
    ///        fill）达到委托量且未终态时，派生 `status=Filled` 的收尾事件。
    ///
    /// 成功路径同步完成终态标记 + 投影 payload 写入（prov=kSynthCumFilled）并经
    /// 订单去重；caller 负责把 out 送入既有 on_order_update 事件管线（book/账户/
    /// 策略三方对重复终态幂等）。迟到的真实终态由去重器吸收；非 Filled 部分成交
    /// 终态迟到 → keep-Filled + `cum_filled_conflict_total` 计数（不降级重放）。
    /// @return true = out 已填充（累计交付量达到委托量）；false = 条件不满足 /
    ///         已终态 / 重复。
    bool make_cum_filled_completion(uint64_t local_id,
                                    uint64_t local_cb_us,
                                    XtOrderExecUpdate& out);

    // ── 交付投影观测 ──────────────────────────────────────────────
    /// @brief 交付投影只读视图（观测/测试）；写入仅发生在本类内部写点。
    const xt_live::gateway::XtExecDeliveryLedger& exec_ledger() const { return exec_ledger_; }

    /// @brief pre-restart 量水位 defense-in-depth 计数：交付 query 行时该记录的
    ///        absorbed 累计仍低于注册封印水位 → 疑似 pre-restart 行越过 TradeTime
    ///        封印（O7 备机制，仅观测不仲裁）。
    uint64_t watermark_breach_total() const {
        return watermark_breach_total_.load(std::memory_order_relaxed);
    }

    /// @brief cum>=qty completion 事件派生累计（进程内观测，不接 prometheus）。
    uint64_t synth_cum_filled_total() const {
        return synth_cum_filled_total_.load(std::memory_order_relaxed);
    }

    /// @brief keep-Filled 冲突计数：合成 Filled 终态后迟到非 Filled 部分成交终态
    ///        （observe before arbitrate，不降级重放）。
    uint64_t cum_filled_conflict_total() const {
        return cum_filled_conflict_total_.load(std::memory_order_relaxed);
    }

    /// @brief 从 CTP 成交行还原 venue fill 绝对时刻（UTC epoch µs，秒级分辨率）；
    ///        发射分区与封印判定的统一时间源。解析失败返回 0。
    static uint64_t fill_epoch_us(const CThostFtdcTradeField* pTrade);

private:
    // ── 本地单状态模型 ──────────────────────────────────────────────
    /// @brief 单个 local_id 在 CTP TD 生命周期内的回报补齐状态。
    ///
    /// 不是跨 venue 订单 DTO，也不参与账户/风控记账；只服务 CtpStateTracker 的
    /// 字段补齐、撤单路由和撤单 error(26) 终态收敛。
    ///
    /// 保存 submit 阶段已知的本地上下文，以及回报阶段补齐的 CTP 路由字段。
    /// 累计成交量不在此维护——error(26) 终态推断读交付投影（`ledger_effective_filled_fp_`）。
    struct LocalOrderState {
        // CTP 路由字段；`ref` 来自 submit，其余字段由 OnRtnOrder 回填。
        std::string ref;           ///< CTP OrderRef
        std::string code;          ///< 合约代码（InstrumentID）
        std::string exchange_id;   ///< 交易所代码（ExchangeID）
        std::string order_sys_id;  ///< 交易所订单号（OrderSysID）
        int front_id = 0;          ///< CTP FrontID
        int session_id = 0;        ///< CTP SessionID

        // submit 阶段的本地上下文。
        bool is_buy = true;
        double price = 0.0;
        int64_t qty_fp = 0;
        std::string user_tag;
        bool is_close = false;
        bool is_today = false;         ///< CloseToday 标记

        // 运行期锚点字段。
        uint64_t submit_begin_us = 0;  ///< submit 开始 steady us
        uint64_t submit_end_us = 0;    ///< ReqOrderInsert 返回 steady us
    };

    // ── 本地元数据回填 ──────────────────────────────────────────────
    /// @brief 查询本地单补齐状态（local order state lookup）。
    bool try_get_local(uint64_t local_id, LocalOrderState& out) const;
    /// @brief 将本地补齐状态合并到订单更新（apply local state -> order update）。
    static void apply_local_to_update(const LocalOrderState& state, XtOrderExecUpdate& upd);
    /// @brief 将本地补齐状态合并到成交记录（apply local state -> trade）。
    static void apply_local_to_trade(const LocalOrderState& state, XtTradeRecord& tr);
    /// @brief 用 OnRtnOrder 字段回填撤单所需路由键。
    void update_local_cancel_fields(uint64_t local_id, const CThostFtdcOrderField* pOrder);
    /// @brief 交付投影口径的累计成交量：注册封印水位 + Σ 非 absorbed fill。
    ///        absorbed 附着行属水位口径不重复计；无记录返回 0。
    int64_t ledger_effective_filled_fp_(uint64_t local_id) const;

    // ── 去重辅助 ──────────────────────────────────────────────
    /// @brief 判断订单更新是否应返回给调用方处理（deduplicate）。
    bool should_emit(uint64_t local_id, const XtOrderExecUpdate& upd);
    /// @brief 组装交易所终态键（exchange_id|order_sys_id）。
    static std::string build_terminal_order_key(const std::string& exchange_id,
                                                const std::string& order_sys_id);
    /// @brief 标记本地单号与交易所单键终态，阻断重复撤单。
    /// @return true=本次为该 local_id 首次终态标记；false=已终态（重复）。
    bool mark_terminal(uint64_t local_id, const LocalOrderState* state);
    /// @brief 终态本地单号集合删除（新单注册前清理旧痕迹）。
    void clear_terminal_local(uint64_t local_id);
    /// @brief 将交易所终态键写入有限窗口缓存。
    void remember_terminal_key_unlocked(const std::string& key);

    // ── 交付投影写点辅助 ──────────────────────────────────────────
    /// @brief 终态 payload 写入交付投影（首个终态生效，重复由投影幂等吸收）。
    void record_terminal_projection(uint64_t local_id,
                                    const XtOrderExecUpdate& upd,
                                    xt_live::gateway::TerminalProvenance prov);

    // ── 运行时状态 ──────────────────────────────────────────────
    mutable std::mutex mu_;  ///< 保护本地单状态、去重窗口和终态撤单保护窗口
    /// local_id -> CTP 回报补齐状态。终态后保留（迟到 fill 归因 / error-26 推断），
    /// 随终态窗 FIFO 淘汰或同 id 重注册清理，不在终态时刻即时删除。
    std::unordered_map<uint64_t, LocalOrderState> local_;
    order_lifecycle::XtOrderUpdateDeduper deduper_;  ///< 订单状态去重器，按 local_id 折叠重复/倒退回报

    std::unordered_set<uint64_t> terminal_local_ids_;  ///< 已终态 local_id 集合；阻断重复撤单
    std::deque<uint64_t> terminal_local_fifo_;         ///< local_id 终态窗口 FIFO；按插入顺序淘汰到 200K 上限
    std::unordered_set<std::string> terminal_order_keys_;  ///< 已终态 exchange_id|order_sys_id 集合
    std::deque<std::string> terminal_order_key_fifo_;  ///< 交易所终态键 FIFO；按插入顺序淘汰到 200K 上限

    // ── 交付投影（fill exactly-once 判据 + 终态 payload 承载）────────
    xt_live::gateway::XtExecDeliveryLedger exec_ledger_;  ///< per-order 交付投影
    std::atomic<uint64_t> watermark_breach_total_{0};  ///< pre-restart 封印疑似泄漏计数（O7 备机制，仅观测）
    std::atomic<uint64_t> synth_cum_filled_total_{0};    ///< cum>=qty completion 派生计数
    std::atomic<uint64_t> cum_filled_conflict_total_{0}; ///< keep-Filled 冲突计数
};
