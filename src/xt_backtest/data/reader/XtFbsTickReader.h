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
/// MdTick 并转换 fixed-point 字段。盘口按 `depth_levels_real`（1..kEventStoreMdDepth）
/// 回放真实档数，更深槽位保持 `XtRawTick` 默认 0。
/// 本类 owns `IEventStore` / `IEventIterator` 游标；`reset()` 重建 iterator 并清空 per-symbol
/// cumulative volume 派生状态。单线程顺序读取。
class XtFbsTickReader : public IMarketDataReader {
public:
    /// @brief FBS event store reader 的查询配置。
    struct Options {
        std::string              fbs_root;              ///< `data/events/` 根目录。
        uint32_t                 start_trading_day = 0; ///< YYYYMMDD inclusive。
        uint32_t                 end_trading_day   = 0; ///< YYYYMMDD inclusive。
        std::vector<std::string> symbols;               ///< 空表示所有合约。
    };

    explicit XtFbsTickReader(Options opts);
    ~XtFbsTickReader() override;

    /// @return true 表示 event store 与 iterator 创建成功；false 表示配置或路径不可用。
    bool init() override;
    /// @brief 重新创建 iterator 并清空 per-symbol volume 派生状态。
    void reset() override;
    /// @return true 表示 `out` 已写入下一条 tick；false 表示 EOF、iterator 错误或事件非法。
    bool next_tick(XtRawTick& out) override;

private:
    Options opts_; ///< 构造期传入的 fbs 查询配置。
    std::unique_ptr<ct::data::IEventStore> store_;    ///< owning event store；init() 成功后创建。
    std::unique_ptr<ct::data::IEventIterator> iter_;  ///< 当前 query iterator；reset() 会重建。
    bool initialized_ = false; ///< true 表示 init() 已成功建立 store/iterator。

    std::unordered_map<std::string, double> prev_total_volume_; ///< per-symbol cumulative volume，用于派生 incremental volume。
};
