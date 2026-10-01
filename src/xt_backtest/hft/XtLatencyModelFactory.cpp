#include "XtLatencyModelFactory.h"

#include <cstdio>
#include <stdexcept>
#include <string>

#include "xt_engine/sim/XtLatencyArith.h"

#include "xt_util/path/PathUtil.h"
#include "xt_engine/sim/CdfLatencyModel.h"
#include "xt_engine/sim/ConstantLatencyModel.h"
#include "xt_engine/sim/ILatencyModel.h"
#include "xt_engine/sim/InterpolatedLatencyModel.h"
#include "xt_engine/sim/IQueueModel.h"
#include "xt_engine/sim/PowerProbQueueFunc.h"
#include "xt_engine/sim/ProbQueueModel.h"
#include "xt_engine/sim/RatioQueueModel.h"

namespace xt_bt::hft {

std::unique_ptr<ILatencyModel> make_latency_model(const LatencyModelCfg& cfg) {
    if (cfg.type == "constant") {
        // 构造期域校验；消费点（sample_*）二次校验保 custom/test model 不绕过。
        // checked_us_to_ns 先验乘法溢出（裸 us*1000 溢出会 wrap 成小正值绕过 validate）。
        return std::make_unique<ConstantLatencyModel>(
            xt_latency::checked_us_to_ns(cfg.entry_us, "constant entry_us"),
            xt_latency::checked_us_to_ns(cfg.response_us, "constant response_us"));
    }
    if (cfg.type == "cdf") {
        // CDF 输入使用 percentile YAML；先校验后缀，避免误读时间序列 CSV。
        if (cfg.source.empty()) {
            throw std::runtime_error(
                "backtest.latency_model.type='cdf' 必须指定 source 路径 (.yaml/.yml)");
        }
        const std::string ext = PathUtil::lower_ext(cfg.source);
        if (ext != "yaml" && ext != "yml") {
            throw std::runtime_error(
                "backtest.latency_model.type='cdf' 仅支持 .yaml/.yml source；"
                "实际后缀='." + ext + "' 路径=" + cfg.source +
                "（如要按时间索引 CSV 请改用 type:interpolated）");
        }
        return std::make_unique<CdfLatencyModel>(cfg.source);
    }
    if (cfg.type == "interpolated") {
        if (cfg.source.empty()) {
            throw std::runtime_error(
                "backtest.latency_model.type='interpolated' 必须指定 source 路径");
        }
        const std::string ext = PathUtil::lower_ext(cfg.source);
        if (ext != "csv") {
            throw std::runtime_error(
                "backtest.latency_model.type='interpolated' 仅支持 .csv source；"
                "实际后缀='." + ext + "' 路径=" + cfg.source +
                "（CDF YAML 请改用 type:cdf）");
        }
        // 时变维度已接线（event-axis 采样）；ctor 强制 v2 header + 非法值 fail-fast。
        return std::make_unique<InterpolatedLatencyModel>(cfg.source);
    }
    throw std::runtime_error(
        "backtest.latency_model.type='" + cfg.type +
        "' 不支持 (期望: constant | interpolated | cdf)");
}

std::unique_ptr<IQueueModel> make_queue_model(const QueueModelCfg& cfg) {
    if (cfg.type == "ratio") {
        return std::make_unique<RatioQueueModel>(cfg.ratio, cfg.cancel_resubmit_ratio);
    }
    if (cfg.type == "prob_power1") {
        return std::make_unique<ProbQueueModel<PowerProbQueueFunc1>>(
            PowerProbQueueFunc1{cfg.k});
    }
    if (cfg.type == "prob_power2") {
        return std::make_unique<ProbQueueModel<PowerProbQueueFunc2>>(
            PowerProbQueueFunc2{cfg.k});
    }
    throw std::runtime_error(
        "backtest.queue_model.type='" + cfg.type +
        "' 不支持 (期望: ratio | prob_power1 | prob_power2)");
}

} // namespace xt_bt::hft
