#include "XtStrategyFactory.h"

#include "xt_util/path/PathUtil.h"

#include <yaml-cpp/yaml.h>

XtStrategyFactory::Config parse_strategy_factory_config(const YAML::Node& node) {
    XtStrategyFactory::Config cfg;
    cfg.id = (node && node["id"]) ? node["id"].as<std::string>("strategy") : "strategy";
    cfg.so_path = (node && node["so"]) ? node["so"].as<std::string>("") : "";
    return cfg;
}

void XtStrategyFactoryCfg::validate(std::vector<std::string>& errors) const {
    if (id.empty()) {
        errors.push_back("strategy.id cannot be empty");
    }
    if (so_path.empty()) {
        errors.push_back("strategy.so cannot be empty");
    }
}

void XtStrategyFactory::set_base_path(const std::string& base_path) {
    base_path_ = base_path;
}

bool XtStrategyFactory::register_plugin(const std::string& id, const std::string& so_path) {
    if (entries_.find(id) != entries_.end()) {
        err_ = "strategy id '" + id + "' already registered";
        return false;
    }
    Entry e;
    e.so_path = PathUtil::resolve_relative(base_path_, so_path);
    if (e.so_path.empty()) {
        err_ = "empty so path";
        return false;
    }
    if (!e.loader.load(e.so_path.c_str())) {
        err_ = e.loader.last_error();
        return false;
    }

    entries_[id] = std::move(e);
    return true;
}

XtStrategyBase* XtStrategyFactory::create(const std::string& id) {
    auto it = entries_.find(id);
    if (it == entries_.end()) {
        err_ = "strategy id not registered";
        return nullptr;
    }
    XtStrategyBase* s = it->second.loader.create();
    if (!s) {
        err_ = "strategy create returned null";
    }
    return s;
}

XtStrategyFactory::UniqueStra XtStrategyFactory::create_managed(const std::string& id) {
    XtStrategyBase* s = create(id);
    if (!s) {
        return UniqueStra(nullptr, [](XtStrategyBase*) {});
    }

    // managed pointer 仍要求 factory/loader 生命周期覆盖策略实例；deleter 只负责选对 id。
    return UniqueStra(s, [this, id](XtStrategyBase* p) {
        this->destroy(id, p);
    });
}

void XtStrategyFactory::destroy(const std::string& id, XtStrategyBase* stra) {
    auto it = entries_.find(id);
    if (it == entries_.end()) {
        return;
    }
    it->second.loader.destroy(stra);
}
