#include "TraderBinanceUGatewayImpl.h"

namespace binance_u {

// ── Public PImpl forwarding ─────────────────────────────────────────────────

TraderBinanceUGateway::TraderBinanceUGateway()
    : impl_(std::make_unique<Impl>()) {}

TraderBinanceUGateway::~TraderBinanceUGateway() {
    if (impl_) impl_->stop();
}

void TraderBinanceUGateway::configure_binance(const BinanceTdCfg& cfg) {
    impl_->configure(cfg);
}

bool TraderBinanceUGateway::init(const Config& cfg) { return impl_->init(cfg); }
bool TraderBinanceUGateway::start() { return impl_->start(); }
void TraderBinanceUGateway::stop() { impl_->stop(); }

bool TraderBinanceUGateway::check_health(uint32_t now_ms) {
    return impl_->check_health(now_ms);
}

bool TraderBinanceUGateway::query_wallet_balance_sync(double& out_balance) {
    return impl_->query_wallet_balance_sync(out_balance);
}

uint64_t TraderBinanceUGateway::submit_order(const char* code, bool is_buy,
                                             int64_t price_fp, int64_t qty_fp,
                                             XtTimeInForce tif, const char* user_tag,
                                             XtOffset close_offset, ExecFlags exec_flags) {
    return impl_->submit_order_inq(code, is_buy, price_fp, qty_fp, tif, user_tag,
                                   close_offset, exec_flags, 0);
}

uint64_t TraderBinanceUGateway::submit_order_with_intent_trace(
    const char* code, bool is_buy,
    int64_t price_fp, int64_t qty_fp,
    XtTimeInForce tif, const char* user_tag,
    XtOffset close_offset, ExecFlags exec_flags,
    uint64_t intent_trace_id) {
    return impl_->submit_order_inq(code, is_buy, price_fp, qty_fp, tif, user_tag,
                                   close_offset, exec_flags, intent_trace_id);
}

bool TraderBinanceUGateway::cancel_order(uint64_t local_id) {
    return impl_->cancel_order_inq(local_id);
}

void TraderBinanceUGateway::bind_callbacks(Callbacks callbacks) {
    impl_->bind_callbacks(std::move(callbacks));
}

xt_live::gateway::TdReadinessReport TraderBinanceUGateway::td_readiness_report() const {
    return impl_->td_readiness_report();
}

// ── IReconcilableGateway forwarding ─────────────────────────────
std::optional<std::vector<XtPositionSnapshot>>
TraderBinanceUGateway::fetch_positions_for_reconcile() {
    return impl_->reconciler_.fetch_positions();
}

std::optional<std::size_t>
TraderBinanceUGateway::detect_and_resolve_ghost_orders_for_reconcile(uint32_t min_age_ms) {
    return impl_->reconciler_.detect_and_resolve_ghosts(min_age_ms);
}

void TraderBinanceUGateway::request_resnap_for_reconcile() {
    impl_->request_resnap();
}

// live channels 读取 TD state 时会走到这里；reconciler 在 resync emit 仓位期间短暂置位。
bool TraderBinanceUGateway::is_recovering() const {
    return impl_->is_recovering();
}

bool TraderBinanceUGateway::is_order_terminal(uint64_t local_id) const {
    return impl_->ledger_.is_order_terminal(local_id);
}

bool TraderBinanceUGateway::lookup_terminal(uint64_t local_id,
                                            XtOrderTerminalRecord& out) const {
    XtOrderStatus status = XtOrderStatus::Canceled;
    int64_t filled_fp = -1;
    if (!impl_->ledger_.lookup_terminal(local_id, status, filled_fp)) return false;
    out.status = status;
    out.filled_fp = filled_fp;
    return true;
}

bool TraderBinanceUGateway::collect_open_delivery_ids(std::vector<uint64_t>& out) const {
    for (const auto& brief : impl_->ledger_.inflight_snapshot()) {
        out.push_back(brief.local_id);
    }
    return true;
}

#ifdef XT_BUILD_TESTS
// 单测入口 (XT_BUILD_TESTS only)。
// 单测通过 trader_binance_test_* 自由函数驱动 PImpl 的私有路径，需要拿到 Impl
// 指针；用单一 void* 出口暴露，不把 Impl 类型 / private 字段泄漏到 header。
void* TraderBinanceUGateway::_test_impl_void() noexcept {
    return static_cast<void*>(impl_.get());
}

void trader_binance_test_inject_transport_ack(void* impl_void, const TransportAck& ack) {
    auto* impl = static_cast<TraderBinanceUGateway::Impl*>(impl_void);
    TransportLatencyTrace trace;
    trace.local_id = ack.local_id;
    trace.transport = "test";
    // t1 一律取真实到达时刻：transport error 也可能带真实 HTTP/WS 响应时刻（如
    // execution-unknown 5xx/-1007）。raw ACK 旁路的采样判据是 timing_origin（L-1.4），
    // 不再从 t1 推断——旧「transport error ⇒ t1=0」的掩盖形态修正（latency-terminal L-1.4）。
    trace.t1_us = lat_now_us();
    impl->test_inject_transport_ack(ack, trace);
}

void trader_binance_test_inject_uds_order(void* impl_void, const XtOrderExecUpdate& upd) {
    auto* impl = static_cast<TraderBinanceUGateway::Impl*>(impl_void);
    impl->test_inject_uds_order(upd);
}

void trader_binance_test_emit_reconcile_ghost(void* impl_void, const XtOrderExecUpdate& upd) {
    auto* impl = static_cast<TraderBinanceUGateway::Impl*>(impl_void);
    impl->test_emit_reconcile_ghost(upd);
}

bool trader_binance_test_emit_blank_sys_id_reject(void* impl_void, uint64_t local_id) {
    auto* impl = static_cast<TraderBinanceUGateway::Impl*>(impl_void);
    return impl->test_emit_blank_sys_id_reject(local_id);
}

// 订单账本只读观测 seam：断言经生产查询 API（is_inflight / sys_id_for / ...）
// 观测，不再为每个字段开 friend 转发。
const BinanceOrderLedger& trader_binance_test_ledger(void* impl_void) {
    return static_cast<TraderBinanceUGateway::Impl*>(impl_void)->ledger_;
}

std::size_t trader_binance_test_cold_worker_task_count(void* impl_void) {
    auto* impl = static_cast<TraderBinanceUGateway::Impl*>(impl_void);
    return impl->test_cold_worker_task_count();
}

uint64_t trader_binance_test_trace_id_for_local(void* impl_void, uint64_t local_id) {
    auto* impl = static_cast<TraderBinanceUGateway::Impl*>(impl_void);
    return impl->test_trace_id_for_local(local_id);
}

void trader_binance_test_rebuild_from_open_orders_body(
    void* impl_void, const std::string& body, bool is_cold_start) {
    auto* impl = static_cast<TraderBinanceUGateway::Impl*>(impl_void);
    impl->test_rebuild_from_open_orders_body(body, is_cold_start);
}

// 反向 sweep 单测入口：返回 resolved 数；-1 表示 openOrders 拉取失败（nullopt）。
long long trader_binance_test_detect_ghosts(void* impl_void, uint32_t min_age_ms) {
    auto r = static_cast<TraderBinanceUGateway::Impl*>(impl_void)
                 ->reconciler_.detect_and_resolve_ghosts(min_age_ms);
    return r.has_value() ? static_cast<long long>(*r) : -1LL;
}

// userTrades 分页触顶计数观测入口（per-symbol / per-order 两查询共用同一计数）。
uint64_t trader_binance_test_user_trades_pages_capped(void* impl_void) {
    return static_cast<TraderBinanceUGateway::Impl*>(impl_void)
        ->reconciler_.user_trades_pages_capped_total();
}

// reconcile resync 离线驱动：注入 Fake REST（脚本化 positionRisk/account/openOrders
// 响应）后直接跑 resync，验 emit 顺序（position → account）与 anchor 节流。
void trader_binance_test_set_rest(void* impl_void,
                                  std::unique_ptr<BinanceRestClient> rest) {
    auto* impl = static_cast<TraderBinanceUGateway::Impl*>(impl_void);
    impl->rest = std::move(rest);
}

void trader_binance_test_reconcile_resync(void* impl_void) {
    static_cast<TraderBinanceUGateway::Impl*>(impl_void)->reconciler_.reconcile_resync();
}

// tracked 集播种单测入口：驱动 per-symbol 拉取模式 / 解析坏行 tracked 分类。
void trader_binance_test_note_tracked_symbol(void* impl_void, const std::string& symbol) {
    static_cast<TraderBinanceUGateway::Impl*>(impl_void)->tracked_symbols_.note(symbol);
}

// resync 快照-流栅栏单测入口：模拟「并发」UDS fill（Fake REST 的 positionRisk
// 响应 hook 内调用 = fill 在 REST 读取窗口内到达）/ 驱动 bootstrap 路径。
void trader_binance_test_forward_uds_trade(void* impl_void, const XtTradeRecord& tr) {
    static_cast<TraderBinanceUGateway::Impl*>(impl_void)->test_forward_uds_trade(tr);
}

void trader_binance_test_bootstrap_snapshot(void* impl_void) {
    static_cast<TraderBinanceUGateway::Impl*>(impl_void)->test_bootstrap_snapshot();
}

// resnap 队列合并单测入口：验连续 request_resnap 只排队一个 kReconcileResync。
void trader_binance_test_request_resnap(void* impl_void) {
    static_cast<TraderBinanceUGateway::Impl*>(impl_void)->test_request_resnap();
}

// 栅栏单测入口：手动开/关 ResyncFillGate，验终态与 fill 同栅栏的 flush 顺序。
void* trader_binance_test_resync_gate_open(void* impl_void) {
    return static_cast<TraderBinanceUGateway::Impl*>(impl_void)->test_resync_gate_open();
}
void trader_binance_test_resync_gate_close(void* impl_void, void* gate) {
    static_cast<TraderBinanceUGateway::Impl*>(impl_void)->test_resync_gate_close(gate);
}

// keepalive 选择器单测入口：-1 = 静默（退避未到期），0 = kRefreshSnapshot，
// 1 = kBootstrapSnapshot（td 未 ready 的 bootstrap 重试）。
int trader_binance_test_keepalive_pick(void* impl_void, long long now_ms) {
    auto* impl = static_cast<TraderBinanceUGateway::Impl*>(impl_void);
    const auto pick = impl->test_keepalive_pick(static_cast<int64_t>(now_ms));
    if (!pick.has_value()) return -1;
    return *pick == TraderBinanceUGateway::Impl::ColdWorkerTask::kBootstrapSnapshot ? 1 : 0;
}

// reset_td_readiness 单测入口：复刻 start() 的 gate 初始化（Settlement not_required）。
void trader_binance_test_reset_td_readiness(void* impl_void) {
    static_cast<TraderBinanceUGateway::Impl*>(impl_void)->test_reset_td_readiness();
}

// stop-while-gap-fill-inflight 回归装配入口；语义见 Impl::test_boot_cold_worker_with_uds。
void trader_binance_test_boot_cold_worker_with_uds(
    void* impl_void, const std::string& tracked_symbol,
    std::function<void(const XtTradeRecord&, XtLatencyEligibility)> on_gap_fill_trade) {
    static_cast<TraderBinanceUGateway::Impl*>(impl_void)
        ->test_boot_cold_worker_with_uds(tracked_symbol, std::move(on_gap_fill_trade));
}

// ghost fill 补发回归装配入口；语义见 Impl::test_install_uds_prod_wired。
void trader_binance_test_install_uds_prod_wired(void* impl_void,
                                                const std::string& tracked_symbol) {
    static_cast<TraderBinanceUGateway::Impl*>(impl_void)
        ->test_install_uds_prod_wired(tracked_symbol);
}

#endif  // XT_BUILD_TESTS

}  // namespace binance_u
