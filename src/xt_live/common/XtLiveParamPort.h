#pragma once

#include <cstdint>
#include <string>

#include "XtLiveParamDomain.h"
#include "xt_strategy_engine/XtStrategyContext.h"

/// @brief 策略层 `IParamPort` 到实盘 `XtLiveParamDomain` 的适配器。
///
/// `param_domain` 可为空；为空时读取/写入失败，version 返回 0。
class XtLiveParamPort final : public XtStrategyContext::IParamPort {
public:
    explicit XtLiveParamPort(XtLiveParamDomain* param_domain)
        : param_domain_(param_domain) {}

    bool get(const std::string& key, std::string& out) const override {
        return param_domain_ ? param_domain_->get(key, out) : false;
    }

    bool set(const std::string& key, const std::string& value) override {
        return param_domain_ ? param_domain_->set(key, value) : false;
    }

    uint64_t version() const override {
        return param_domain_ ? param_domain_->version() : 0;
    }

private:
    XtLiveParamDomain* param_domain_ = nullptr;
};
