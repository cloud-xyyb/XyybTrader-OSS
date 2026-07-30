#pragma once

/// @file
/// @brief append-only 事件存储接口，覆盖 MdTick 分片与日级 EventExt 流。
///
/// live runner 要把每条行情 tick 和扩展事件落盘——既供回测「录制即数据集」直接回放，也供
/// 事后审计。本接口是这套落盘的抽象：写侧接收已序列化好的 FlatBuffers bytes，按 routing key
/// 持久化；读侧按交易日范围顺序取回 MdTick。append 路径按 single-writer 设计；需要多 writer
/// 时由调用方加锁或拆分 store 实例。query 只读取已经对后端可见的 record。

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "event_generated.h"

namespace ct::data {

/// @brief IEventIterator::next() 返回的单条 MdTick 视图。
///
/// `tick` 指向当前 record 的 FlatBuffers payload，`bytes_len` 为 payload 字节数。
/// 视图只在同一 iterator 下一次 `next()` 前有效；需要保留时由调用方复制。
struct MdTickView {
    const schema::MdTick* tick = nullptr;
    std::size_t bytes_len = 0;
};

/// @brief MdTick 查询结果的顺序读取游标。
///
/// 调用 `next()` 取得下一条 MdTick；返回 false 表示结果耗尽或读取出错。
/// 每次成功读取都会更新输出的 MdTickView，上一条 view 随之失效。
class IEventIterator {
public:
    virtual ~IEventIterator() = default;

    virtual bool next(MdTickView& out) = 0;

    virtual std::size_t bytes_read() const = 0;

    virtual bool has_error() const = 0;
};

/// @brief MdTick 与 EventExt 的 append-only 事件存储抽象。
///
/// `append()` 写入 MdTick record，`append_event_ext()` 写入日级扩展事件流。
/// `query()` 创建 MdTick iterator；查询结果不包含 EventExt payload。
///
/// 同一 store 的写入路径按 single-writer 使用。
class IEventStore {
public:
    /// @brief MdTick 查询范围；trading_day 使用闭区间 [start, end]。
    struct QueryRange {
        std::uint32_t start_trading_day = 0;
        std::uint32_t end_trading_day = 0;
        std::vector<std::string> exchanges;
        std::vector<std::string> symbols;
    };

    virtual ~IEventStore() = default;

    virtual bool append(std::uint32_t trading_day,
                        std::string_view exchange,
                        std::string_view symbol,
                        const void* fbs_buf,
                        std::size_t fbs_size) = 0;

    virtual std::unique_ptr<IEventIterator> query(const QueryRange& range) = 0;

    virtual bool flush() = 0;

    virtual bool append_event_ext(std::uint32_t trading_day,
                                  const void* fbs_buf,
                                  std::size_t fbs_size) = 0;

    virtual std::size_t pending_bytes() const = 0;

    virtual std::string rotate() { return {}; }
};

}
