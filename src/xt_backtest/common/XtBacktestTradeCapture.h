#pragma once

#include <string>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "api/XtBtFillTruth.h"
#include "xt_backtest/common/XtBacktestReport.h"

/// @brief Backtest 成交采集器，把真值点的 `XtBtFillTruth` 转成 BarTradeRow。
///
/// 支持两种模式：single-bucket 将所有成交写入一个外部 vector；multi-bucket
/// 按 `strategy_id` 写入已注册 vector。所有 sink 均为 non-owning，调用方必须保证
/// 生命周期覆盖本对象。multi-bucket 下空或未知 `strategy_id` 会 fail-fast，避免
/// 成交流入错误策略结果。
///
/// @note 挂在引擎的 `XtBtFillSink`（account apply 之后）而非 gateway observer fan；
///       后者位于 apply 之前，据此采集就只能事后回填账户状态（BR-01 的成因）。
/// @note 本类不修改账户、订单或 frozen state；SEL frozen release 仍由 gateway fan 上的
///       独立 observer 处理，二者互不替代。
class XtBacktestTradeCapture final {
public:

    explicit XtBacktestTradeCapture(std::vector<BarTradeRow>& sink) noexcept
        : single_bucket_(&sink) {}

    XtBacktestTradeCapture() = default;

    void add_bucket(const std::string& strategy_id,
                    std::vector<BarTradeRow>& bucket) {
        if (single_bucket_) {
            throw std::runtime_error("backtest trade capture single-bucket mode 不能注册 strategy bucket");
        }
        if (strategy_id.empty()) {
            throw std::runtime_error("backtest trade capture strategy_id 不能为空");
        }
        const auto result = buckets_.emplace(strategy_id, &bucket);
        if (!result.second) {
            throw std::runtime_error("backtest trade capture strategy_id 重复: " + strategy_id);
        }
    }

    void operator()(const XtBtFillTruth& t) {
        const XtTradeRecord& tr = t.tr;
        std::vector<BarTradeRow>* target = single_bucket_;
        if (!buckets_.empty()) {
            if (tr.strategy_id.empty()) {
                throw std::runtime_error("backtest trade capture missing strategy_id");
            }
            auto it = buckets_.find(tr.strategy_id);
            if (it == buckets_.end()) {
                throw std::runtime_error("backtest trade capture unknown strategy_id: " + tr.strategy_id);
            }
            target = it->second;
        }
        if (!target) {
            throw std::runtime_error("backtest trade capture has no registered sink");
        }
        target->push_back(to_bar_trade_row(t));
    }

private:
    std::unordered_map<std::string, std::vector<BarTradeRow>*> buckets_;
    std::vector<BarTradeRow>* single_bucket_ = nullptr;
};
