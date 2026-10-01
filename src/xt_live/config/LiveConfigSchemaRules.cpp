#include "LiveConfigSchemaRules.h"

// schema 表是启动期 unknown-key gate：固定 YAML key 必须登记到 allowed_paths。
// parse_* 新增或删除读路径时同步维护这里，避免有效配置被误拒或废弃 key 被静默接受。
XtConfigSchemaRules build_live_schema_rules() {
    XtConfigSchemaRules rules;
    rules.allowed_paths = {
        // ── queue ──────────────────────────────────────────────────────────
        "queue.mode",
        "queue.capacity",
        "queue.event_capacity",
        // tick queue
        "queue.tick_high_watermark",
        "queue.tick_batch_size",
        "queue.tick_max_wait_us",
        "queue.tick_starvation_warn_loops",
        // td event queue
        "queue.td_event_mode",
        "queue.td_event_capacity",
        "queue.td_event_high_watermark",
        "queue.td_event_budget_per_loop",
        "queue.td_backlog_warn_threshold",
        "queue.td_event_batch_size",
        // flow event queue
        "queue.flow_event_mode",
        "queue.flow_event_capacity",
        "queue.flow_event_high_watermark",
        "queue.flow_event_batch_size",
        "queue.flow_event_budget_per_loop",

        // ── log ────────────────────────────────────────────────────────────
        "log.level",
        "log.console",
        "log.file",
        "log.max_size",
        "log.max_files",
        "log.queue_size",
        "log.threads",
        "log.modules.*",
        "log.throttle_ms.*",

        // ── commodities ────────────────────────────────────────────────────
        "commodities.*.price_tick",
        "commodities.*.vol_scale",
        "commodities.*.allow_short",
        // capability flags (venue-neutral)
        "commodities.*.has_open_interest",
        "commodities.*.has_settle_price",
        "commodities.*.is_t_plus_1",
        "commodities.*.min_lot",
        // 其他可选 catalog 字段（margin_rate / slippage 在已有 catalog parse 中）
        "commodities.*.margin_rate",
        "commodities.*.slippage",
        // 品种级开平路由（close_first | open_allowed，offset_router_b）
        "commodities.*.offset_policy",

        // ── fees ───────────────────────────────────────────────────────────
        "fees.*.open",
        "fees.*.close",
        "fees.*.close_today",
        "fees.*.by_volume",
        "fees.*.maker_rebate",
        // A 股专属(venue-neutral，期货/加密 yaml 不写则 default 0）
        "fees.*.stamp_duty_rate",
        "fees.*.transfer_fee_rate",
        "fees.*.min_commission",

        // ── runtime.debug：健康监控（health monitoring）────────────────────
        "runtime.debug.tick_count",
        "runtime.debug.heartbeat_ms",
        "runtime.debug.fail_on_critical_td_drop",
        "runtime.debug.consistency_warn_loops",
        "runtime.debug.consistency_block_loops",
        "runtime.debug.consistency_kill_loops",
        // runtime.debug：unmatched drop 预算（→ kill_switch 自动 trip，HftUnmatchedBudget.h）
        "runtime.debug.unmatched_budget.window_sec",
        "runtime.debug.unmatched_budget.max_drops",
        "runtime.debug.unmatched_budget.max_ratio",
        // runtime.debug：统一报告（unified report）
        "runtime.debug.report_contract_dir",
        "runtime.debug.report_contract_flush_ms",
        "runtime.debug.report_run_id",
        "runtime.debug.report_config_hash",
        "runtime.debug.report_code_version",
        "runtime.debug.strategy_metrics_flush_sec",
        // runtime.debug：进程调优（process tuning）
        "runtime.debug.mlockall",
        "runtime.debug.main_affinity",
        "runtime.debug.main_priority",
        // runtime.debug：延迟追踪（latency tracking）
        "runtime.debug.latency_enabled",
        "runtime.debug.latency_stale_ttl_us",
        "runtime.debug.latency_rolling_window_s",
        "runtime.debug.params_wait_timeout_ms",

        // ── runtime.control_grpc：控制面 gRPC server ──
        // mTLS gRPC server 现是唯一 panel→runner control plane transport
        "runtime.control_grpc.enabled",
        "runtime.control_grpc.listen_addr",
        "runtime.control_grpc.tls.mode",
        "runtime.control_grpc.tls.ca_path",
        "runtime.control_grpc.tls.cert_path",
        "runtime.control_grpc.tls.key_path",
        "runtime.control_grpc.audit_log_path",

        // ── runtime.instance_id：runner 实例身份（单一身份来源）────────────
        // safety / aggregator / baseline 三类 per-instance state 子目录全部派生自它。
        // 空=回退 md_provider。多盘组合盘 (transport=shm) 同 venue 多 runner 须显式 per-book
        // id，避免互覆写 kill_switch/peak_equity/today-PnL。
        "runtime.instance_id",

        // ── runtime.safety_state.save_interval_ms：safety.bin 写盘间隔──────
        // path 不再直接配置，由 instance_id 派生。
        "runtime.safety_state.save_interval_ms",

        // ── runtime.metrics：Prometheus /metrics exposer per-profile port────
        // 默认 9101。同 host 多 runner 需不同端口避免 bind 冲突。
        "runtime.metrics.exposer_port",
        "runtime.metrics.exposer_bind_addr",

        // ── runtime.md（行情网关 / MD gateway）────────────────────────────
        "runtime.md.provider",
        "runtime.md.md_front",
        "runtime.md.broker",
        "runtime.md.user",
        "runtime.md.password",
        "runtime.md.flow_dir",
        "runtime.md.log_stats",
        "runtime.md.log_interval_ms",
        "runtime.md.worker_affinity",
        "runtime.md.worker_priority",
        "runtime.md.md_worker_affinity",
        "runtime.md.md_worker_priority",
        "runtime.md.md_mlock",
        "runtime.md.mlock",
        "runtime.md.stale_reconnect_ms",
        // multibook transport：direct(默认,自连 venue) | shm(从 connector ring 读)
        "runtime.md.transport",
        "runtime.md.shm_ring",
        "runtime.md.stale_md_ms",
        // binance_u provider 专属
        "runtime.md.binance.base_ws_url",
        "runtime.md.binance.base_rest_url",
        "runtime.md.binance.testnet",
        "runtime.md.binance.tick_source",
        // provider-specific secret overlay. This lets one secrets.yaml carry CTP
        // and XTP credentials without both providers fighting over md.user/password.
        "runtime.md.ctp.user",
        "runtime.md.ctp.password",
        // xtp provider 专属
        "runtime.md.xtp.user",
        "runtime.md.xtp.password",
        "runtime.md.xtp.client_id",
        "runtime.md.xtp.software_key",
        "runtime.md.xtp.protocol",
        "runtime.md.xtp.heartbeat_interval_s",
        "runtime.md.xtp.buffer_size_mb",
        "runtime.md.xtp.log_dir",
        "runtime.md.xtp.log_level",

        // ── runtime.td（交易网关 / trade gateway）─────────────────────────
        "runtime.td.provider",
        "runtime.td.td_front",
        "runtime.td.broker",
        "runtime.td.user",
        "runtime.td.password",
        "runtime.td.flow_dir",
        "runtime.td.reconnect",
        "runtime.td.reconnect_interval_ms",
        "runtime.td.confirm_settlement",
        "runtime.td.settlement_confirm_timeout_ms",
        "runtime.td.recovery_query_timeout_ms",
        "runtime.td.recovery_query_max_timeouts",
        "runtime.td.recovery_rearm_backoff_ms",
        "runtime.td.recovery_rearm_alert_rounds",
        // CTP 查询回包 generation fence（nRequestID 归属；false=关闭回退全收）
        "runtime.td.query_generation_fence",
        // CTP query 重放成交行按来源规则交付（false=回滚：一律 absorbed 不下发）
        "runtime.td.exec_ledger_query_fill_delivery",
        "runtime.td.account_refresh_ms",
        "runtime.td.position_refresh_ms",
        "runtime.td.account_snapshot_max_abs",
        "runtime.td.stale_reconnect_ms",
        "runtime.td.recovery",
        // reconcile loop（venue-neutral，见 XtHftReconcileLoopCfg；阈值 0 = 禁用该级阶梯）
        "runtime.td.reconcile.enabled",
        "runtime.td.reconcile.cycle_ms",
        "runtime.td.reconcile.reconnect_after_n",
        "runtime.td.reconcile.kill_after_n",
        "runtime.td.reconcile.resync_throttle_ms",
        "runtime.td.reconcile.position_epsilon_fp",
        "runtime.td.reconcile.order_min_age_ms",
        // multibook identity 自证锚（非密；空 = 不校验，deploy contract gate 真金强制非空）
        "runtime.td.expected_investor_id",
        "runtime.td.expected_api_key_sha8",
        // binance_u provider 专属
        "runtime.td.binance.base_rest_url",
        "runtime.td.binance.base_ws_url",
        "runtime.td.binance.base_ws_trade_url",
        "runtime.td.binance.api_key",
        "runtime.td.binance.api_secret",
        "runtime.td.binance.listen_key_refresh_ms",
        "runtime.td.binance.testnet",
        "runtime.td.binance.recv_window_ms",
        "runtime.td.binance.time_sync_interval_ms",
        "runtime.td.binance.transport",
        "runtime.td.binance.ws_submit_timeout_ms",
        "runtime.td.binance.ws_stale_drop_ms",
        "runtime.td.binance.ws_send_queue_cap",
        "runtime.td.binance.ws_reconnect_min_ms",
        "runtime.td.binance.ws_reconnect_max_ms",
        "runtime.td.binance.uds_first_effective_ack",
        "runtime.td.binance.close_reject_plain_fallback",
        // provider-specific secret overlay. See runtime.md.ctp note above.
        "runtime.td.ctp.user",
        "runtime.td.ctp.password",
        // xtp provider 专属
        "runtime.td.xtp.user",
        "runtime.td.xtp.password",
        "runtime.td.xtp.client_id",
        "runtime.td.xtp.software_key",
        "runtime.td.xtp.software_version",
        "runtime.td.xtp.account_key",
        "runtime.td.xtp.heartbeat_interval_s",
        "runtime.td.xtp.buffer_size_mb",
        "runtime.td.xtp.log_dir",
        "runtime.td.xtp.log_level",
        "runtime.td.xtp.confirm_init_query",
        "runtime.td.xtp.init_query_timeout_ms",
        "runtime.td.xtp.recovery_query_timeout_ms",
        "runtime.td.xtp.recovery_query_max_timeouts",
        "runtime.td.xtp.position_refresh_ms",  // ready 后周期 requery 间隔（validator <=0 拒启）

        // ── runtime.account（账户 / account）──────────────────────────────
        "runtime.account.cash",
        "runtime.account.t1_enabled",
        "runtime.account.mode",
        "runtime.account.margin_rate",
        "runtime.account.margin_by_symbol.*",
        "runtime.account.reconcile_on_stop",  // 停机时 runtime vs broker 直比报告

        // ── runtime.risk（风控 / risk control）────────────────────────────
        "runtime.risk.max_order_qty",
        "runtime.risk.reject_on_position_unknown",
        "runtime.risk.max_order_notional",
        "runtime.risk.min_order_notional",  // venue MIN_NOTIONAL lower-bound (opens only; closes/reduce exempt)
        "runtime.risk.max_pos_qty",
        "runtime.risk.enforce_long_t_plus_1",  // A 股 T+1
        "runtime.risk.max_daily_loss",
        "runtime.risk.max_orders_per_sec",
        "runtime.risk.max_cancels_per_sec",
        "runtime.risk.max_rejects_per_min",
        "runtime.risk.cooldown_ms",
        "runtime.risk.enable_position_snapshot_stale_guard",
        "runtime.risk.position_snapshot_stale_ms",
        "runtime.risk.require_l2_orderflow",
        "runtime.risk.stp_enabled",
        "runtime.risk.stp_disabled_reason",
        "runtime.risk.max_order_count_per_symbol",
        "runtime.risk.max_order_rate_per_symbol",
        "runtime.risk.order_rate_window_ms",
        "runtime.risk.blacklist_persist_across_session",
        // 同 symbol 多策略 opt-in: B1 guard + 运行期 run_overlap_check 同步放行
        // (conformance multi 类压测档专用; release_scope 政策门由 deploy 侧
        //  check_release_scope.py 把守)
        "runtime.risk.allow_strategy_symbol_overlap",
        // per-strategy 主动强平（auto-liquidation，HFT-only；map 形式见下方开放前缀）
        "runtime.risk.per_strategy_auto_liquidate",
        "runtime.risk.per_strategy_auto_liquidate_dry_run",

        // ── runtime.risk_monitor（active risk monitor）─────────
        "runtime.risk_monitor.enabled",
        "runtime.risk_monitor.idd_scale_threshold_bp",
        "runtime.risk_monitor.idd_halt_threshold_bp",
        "runtime.risk_monitor.idd_scale_factor_fp",
        "runtime.risk_monitor.stale_quote_ms",

        // ── runtime.instrument / session / trading ─────────────────────────
        "runtime.instrument.symbol",
        "runtime.session.active_sections[].start",
        "runtime.session.active_sections[].end",
        // A 股集合竞价 IOC/FOK enforce 已在 risk 层（XtRiskEngine InCallAuction
        // reject，时段走 classify_phase 内置表）；本 sections 键 schema 预留，
        // 当前无消费者。
        "runtime.session.call_auction_sections[].start",
        "runtime.session.call_auction_sections[].end",
        "runtime.trading.allow_short",
        // venue 默认开平路由（品种 Inherit 时生效）
        "runtime.trading.offset_policy_default",
        // 本地条件单开关（engine tick 驱动 stop/trailing stop；默认关）
        "runtime.trading.conditional_orders_enabled",

        // ── runtime.cost（成本模型 / trade cost）──────────────────────────
        "runtime.cost.slippage",

        // ── runtime.roll（live 主力换月 / roll engine）───────────────────
        "runtime.roll.active",
        "runtime.roll.logical_symbol",
        "runtime.roll.window_before_days",
        "runtime.roll.window_after_days",
        "runtime.roll.dispatch_parallel_ticks",
        "runtime.roll.path",

        // ── runtime.closeout（强平窗口 / closeout）─────────────────────────
        "runtime.closeout.enabled",
        "runtime.closeout.no_open_hhmm",
        "runtime.closeout.force_flat_hhmm",
        "runtime.closeout.retry_ms",
        "runtime.closeout.escalate_after_ms",
        "runtime.closeout.aggressive_buffer_bps",
        "runtime.closeout.force_exit_latch_enabled",
        "runtime.closeout.force_exit_latch_ttl_ms",
        "runtime.closeout.windows[].no_open_hhmm",
        "runtime.closeout.windows[].force_flat_hhmm",
        "runtime.closeout.session_gate.enabled",

        // ── runtime.recovery（cross-restart 孤儿在途单回收 / orphan reaper）───
        "runtime.recovery.orphan_reap_enabled",
        "runtime.recovery.orphan_reap_grace_ms",
        "runtime.recovery.orphan_reap_interval_ms",
        "runtime.recovery.orphan_reap_max_per_sweep",
        "runtime.recovery.lost_terminal_reconcile_enabled",

        // ── runtime.recording（实盘事件录制 / live event recording）────────
        "runtime.recording.enabled",
        "runtime.recording.file_path",
        "runtime.recording.queue_capacity",
        "runtime.recording.rotate_bytes",
        "runtime.recording.rotate_interval_sec",
        "runtime.recording.intent_trace.enabled",
        "runtime.recording.intent_trace.sample_rate",
        "runtime.recording.intent_trace.include_wire_debug",
        // FBS event store sink
        "runtime.recording.event_store.enabled",
        "runtime.recording.event_store.root_path",

        // ── runtime.params_backend（参数后端 / params backend）────────────
        "runtime.params_backend.type",
        "runtime.params_backend.domain",

        // ── runtime.strategies / params（开放前缀，open prefix）──
        "runtime.strategies",
        // retired compatibility key；validator fail-closed，schema 仅用于给出明确错误。
        "runtime.strategies_allow_same_symbol",
        "runtime.params",

        // ── gateway 级 sidecar grace window override (默认 60s) ────────────
        "runtime.gateway.order_strategy_grace_us",

        // ── portfolio router (default ON; explicit false is validated) ──
        "runtime.portfolio_router",
        // per-strategy position quota (yaml string sid → uint32 via sid registry)
        "runtime.risk.per_strategy_max_pos_qty.*",
        "runtime.risk.per_strategy_daily_loss_pct.*",
        "runtime.risk.per_strategy_liquidate_pct.*",

        // SEL live 调度任务（顶层 selector.tasks[]；SEL runner 逐 task 注册进
        // SelTaskScheduler，task↔strategy 不匹配启动 fail-fast。backtest SEL 走
        // 独立 XtSelRunnerConfig，键名是 selector.task 单数，与此处无关）──
        "selector.tasks",                          // task list
        "selector.tasks[].name",                   // 可选标签（缺省派生 <strategy_id>_task）
        "selector.tasks[].strategy_id",
        "selector.tasks[].period",                 // daily / weekly / monthly / yearly / minute
        "selector.tasks[].day",                    // weekly: 1-7; monthly: 1-31; yearly: MMDD
        "selector.tasks[].time",                   // HHMM
        "selector.tasks[].trdtpl",                 // CHINA / SSE / etc.
        "selector.tasks[].session",
        "selector.tasks[].strict_time",
    };
    rules.open_prefixes = {
        "runtime.strategies",
        "runtime.params",
        // per-strategy quota map 是开放前缀 (key 是 strategy_id 字符串)
        "runtime.risk.per_strategy_max_pos_qty",
        "runtime.risk.per_strategy_daily_loss_pct",
        "runtime.risk.per_strategy_liquidate_pct",
    };
    return rules;
}
