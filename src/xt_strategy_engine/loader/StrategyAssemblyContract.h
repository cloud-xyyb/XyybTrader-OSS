#pragma once

/// @file StrategyAssemblyContract.h
/// @brief multi-strategy 装配的启动期 fail-fast 契约检查。
///
/// - capability gate：拒绝会静默丢失 exec sink 回调的部署形态
/// - 4-tuple alignment：校验 cfg / instance / exec sink / engine context 下标一致，
///   防止 sid routing 把订单、成交、账户回报分发给错误策略
///
/// header-only、不持运行状态；live / backtest 共用。

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

#include "StrategyExecCaps.h"
#include "StrategyInstance.h"
#include "xt_identity/StrategySidRegistry.h"
#include "xt_strategy_engine/IStrategyExecSink.h"

namespace xt_strategy_engine {

/// @brief multi-strategy 装配对齐结果。
struct AssemblyAlignmentReport {
    bool        aligned = true;
    std::string error;
};

template <typename StrategyIdAt, typename SinkAt>
inline AssemblyAlignmentReport check_multi_exec_sink_alignment(
    const std::vector<std::string>&      cfg_strategy_ids,
    const std::vector<StrategyInstance>& instances,
    const std::vector<IStrategyExecSink*>& sink_ptrs,
    std::size_t context_count,
    StrategyIdAt&& strategy_id_at,
    SinkAt&& sink_at,
    const xt_identity::StrategySidRegistry* sid_registry = nullptr) {
    AssemblyAlignmentReport rep;
    const std::size_t n = instances.size();

    if (n != cfg_strategy_ids.size()) {
        rep.aligned = false;
        rep.error = "alignment fail: instances.size=" + std::to_string(n) +
                    " != cfg_strategy_ids.size=" +
                    std::to_string(cfg_strategy_ids.size());
        return rep;
    }
    if (n != sink_ptrs.size()) {
        rep.aligned = false;
        rep.error = "alignment fail: instances.size=" + std::to_string(n) +
                    " != sink_ptrs.size=" + std::to_string(sink_ptrs.size());
        return rep;
    }
    if (n != context_count) {
        rep.aligned = false;
        rep.error = "alignment fail: instances.size=" + std::to_string(n) +
                    " != context_count=" + std::to_string(context_count);
        return rep;
    }

    for (std::size_t i = 0; i < n; ++i) {
        const std::string& cfg_id   = cfg_strategy_ids[i];
        const std::string& inst_id  = instances[i].id;
        const std::string  ctx_id   = strategy_id_at(i);
        const IStrategyExecSink* sink_p = sink_ptrs[i];
        const IStrategyExecSink* sink_a = sink_at(i);

        if (cfg_id != inst_id || cfg_id != ctx_id || sink_p == nullptr ||
            sink_a == nullptr || sink_p != sink_a) {
            rep.aligned = false;
            rep.error = "alignment fail @ idx=" + std::to_string(i) +
                        ": cfg.id='" + cfg_id +
                        "' instance.id='" + inst_id +
                        "' ctx.id='" + ctx_id +
                        "' sink_ptr_match=" +
                        (sink_p == sink_a ? "true" : "false") +
                        " (engine sid routing 将串扰 fill)";
            return rep;
        }
        if (sid_registry) {
            const uint32_t sid_id = sid_registry->lookup(cfg_id);
            if (sid_id != i + 1) {
                rep.aligned = false;
                rep.error = "alignment fail @ idx=" + std::to_string(i) +
                            ": cfg.id='" + cfg_id +
                            "' sid_registry.lookup=" + std::to_string(sid_id) +
                            " expected=" + std::to_string(i + 1);
                return rep;
            }
        }
    }
    return rep;
}

template <typename StrategyIdAt, typename SinkAt>
inline AssemblyAlignmentReport check_multi_exec_sink_alignment_no_instances(
    const std::vector<std::string>&        cfg_strategy_ids,
    const std::vector<IStrategyExecSink*>& sink_ptrs,
    std::size_t                            context_count,
    StrategyIdAt&&                         strategy_id_at,
    SinkAt&&                               sink_at,
    const xt_identity::StrategySidRegistry* sid_registry = nullptr) {
    AssemblyAlignmentReport rep;
    const std::size_t n = cfg_strategy_ids.size();

    if (n != sink_ptrs.size()) {
        rep.aligned = false;
        rep.error = "alignment fail: cfg_strategy_ids.size=" + std::to_string(n) +
                    " != sink_ptrs.size=" + std::to_string(sink_ptrs.size());
        return rep;
    }
    if (n != context_count) {
        rep.aligned = false;
        rep.error = "alignment fail: cfg_strategy_ids.size=" + std::to_string(n) +
                    " != context_count=" + std::to_string(context_count);
        return rep;
    }

    for (std::size_t i = 0; i < n; ++i) {
        const std::string& cfg_id = cfg_strategy_ids[i];
        const std::string  ctx_id = strategy_id_at(i);
        const IStrategyExecSink* sink_p = sink_ptrs[i];
        const IStrategyExecSink* sink_a = sink_at(i);

        if (cfg_id != ctx_id || sink_p == nullptr || sink_a == nullptr ||
            sink_p != sink_a) {
            rep.aligned = false;
            rep.error = "alignment fail @ idx=" + std::to_string(i) +
                        ": cfg.id='" + cfg_id +
                        "' ctx.id='" + ctx_id +
                        "' sink_ptr_match=" +
                        (sink_p == sink_a ? "true" : "false") +
                        " (engine sid routing 将串扰 fill)";
            return rep;
        }
        if (sid_registry) {
            const uint32_t sid_id = sid_registry->lookup(cfg_id);
            if (sid_id != i + 1) {
                rep.aligned = false;
                rep.error = "alignment fail @ idx=" + std::to_string(i) +
                            ": cfg.id='" + cfg_id +
                            "' sid_registry.lookup=" + std::to_string(sid_id) +
                            " expected=" + std::to_string(i + 1);
                return rep;
            }
        }
    }
    return rep;
}

template <typename Engine>
inline AssemblyAlignmentReport check_multi_exec_sink_alignment(
    const std::vector<std::string>&      cfg_strategy_ids,
    const std::vector<StrategyInstance>& instances,
    const std::vector<IStrategyExecSink*>& sink_ptrs,
    const Engine*                        engine,
    const xt_identity::StrategySidRegistry* sid_registry = nullptr) {
    if (!engine) {
        AssemblyAlignmentReport rep;
        rep.aligned = false;
        rep.error = "alignment fail: engine is null";
        return rep;
    }
    return check_multi_exec_sink_alignment(
        cfg_strategy_ids,
        instances,
        sink_ptrs,
        engine->context_count(),
        [engine](std::size_t i) {
            const auto* ctx = engine->context(i);
            return ctx ? ctx->strategy_id() : std::string();
        },
        [engine](std::size_t i) -> const IStrategyExecSink* {
            const auto* ctx = engine->context(i);
            return ctx ? static_cast<const IStrategyExecSink*>(ctx) : nullptr;
        },
        sid_registry);
}

}
