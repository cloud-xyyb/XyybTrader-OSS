#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "api/IMarketDataReader.h"

namespace ct::data {
class IEventStore;
class IEventIterator;
}

/// @brief 从 FBS event store 查询 MdTick 并输出 `XtRawTick` 的回测 reader。
///
/// 按 `fbs_root`、trading day 范围和可选 symbol 过滤创建 event-store iterator，顺序遍历
/// MdTick 并转换 fixed-point 字段。FBS v1 只提供 L1-L5，`XtRawTick` 的 L6-L10 保持默认 0。
/// 本类 owns `IEventStore` / `IEventIterator` 游标；`reset()` 重建 iterator 并清空 per-symbol
/// cumulative volume 派生状态。单线程顺序读取。
class XtFbsTickReader : public IMarketDataReader {
public:
    /// @brief FBS event store reader 的查询配置。
    struct Options {
        std::string              fbs_root;
        uint32_t                 start_trading_day = 0;
        uint32_t                 end_trading_day   = 0;
        std::vector<std::string> symbols;
    };

    explicit XtFbsTickReader(Options opts);
    ~XtFbsTickReader() override;

    bool init() override;

    void reset() override;

    bool next_tick(XtRawTick& out) override;

private:
    Options opts_;
    std::unique_ptr<ct::data::IEventStore> store_;
    std::unique_ptr<ct::data::IEventIterator> iter_;
    bool initialized_ = false;

    std::unordered_map<std::string, double> prev_total_volume_;
};
