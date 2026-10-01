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

#include "event_generated.h"  // 由 xt_data_schema 暴露的 flatc 生成头。

namespace ct::data {

/// @brief IEventIterator::next() 返回的单条 MdTick 视图。
///
/// `tick` 指向当前 record 的 FlatBuffers payload，`bytes_len` 为 payload 字节数。
/// 视图只在同一 iterator 下一次 `next()` 前有效；需要保留时由调用方复制。
struct MdTickView {
    const schema::MdTick* tick = nullptr; ///< next() 成功后指向当前 MdTick。
    std::size_t bytes_len = 0;            ///< 当前 payload 字节数，不含 4-byte size prefix。
};

/// @brief MdTick 查询结果的顺序读取游标。
///
/// 调用 `next()` 取得下一条 MdTick；返回 false 表示结果耗尽或读取出错。
/// 每次成功读取都会更新输出的 MdTickView，上一条 view 随之失效。
class IEventIterator {
public:
    virtual ~IEventIterator() = default;

    /// @brief 读取下一条 tick view。
    /// @return true 表示 `out` 已更新；false 表示耗尽或出错，调用 has_error() 区分。
    virtual bool next(MdTickView& out) = 0;

    /// @brief 返回累计读取字节数，包含每条 record 的 4-byte size prefix。
    virtual std::size_t bytes_read() const = 0;

    /// @brief 是否遇到「丢失已提交记录」的腐化：非法 size prefix、中段截断或
    ///        FlatBuffers verify fail。
    /// @note append-only 单写下尾部不完整记录（崩溃残留、从未提交）视为良性 EOF，
    ///       不置位；此时同一 shard 的 next() 返回 false 但 has_error() 仍为 false，
    ///       归并不受影响、继续读取其它 shard。
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
        std::uint32_t start_trading_day = 0; ///< 起始交易日 YYYYMMDD。
        std::uint32_t end_trading_day = 0;   ///< 结束交易日 YYYYMMDD。
        std::vector<std::string> exchanges;  ///< 空表示所有交易所。
        std::vector<std::string> symbols;    ///< 空表示所有合约。
    };

    virtual ~IEventStore() = default;

    /// @brief append 一条 MdTick FlatBuffers record。
    ///
    /// @param trading_day 交易日 YYYYMMDD；CTP 夜盘归次一交易日。
    /// @param exchange 交易所代码；空值非法。
    /// @param symbol 合约代码；空值非法。
    /// @param fbs_buf FlatBufferBuilder::Finish 后的 buffer 指针，不含 size prefix。
    /// @param fbs_size payload 字节数；实现可拒绝超过 record 上限的输入。
    /// @return true 表示 record 已交给后端缓冲；false 表示参数非法或后端写入失败。
    virtual bool append(std::uint32_t trading_day,
                        std::string_view exchange,
                        std::string_view symbol,
                        const void* fbs_buf,
                        std::size_t fbs_size) = 0;

    /// @brief 按 range 创建 iterator；缺失 shard 返回空 iterator，不视为错误。
    /// @note 只查询 MdTick shard，不读取日级 EventExt 事件流。
    virtual std::unique_ptr<IEventIterator> query(const QueryRange& range) = 0;

    /// @brief flush 当前写缓冲到后端持久可见状态；stop / 析构前必须调用。
    /// @return true 表示所有 shard 均成功落盘并关闭；false 表示至少一个 shard 的
    ///         fflush/fsync/fclose 失败（磁盘满 / IO 错误）——数据可能未持久化，
    ///         调用方应告警或走降级。失败时仍会完成对所有 shard 的关闭清理，不泄漏 FD。
    virtual bool flush() = 0;

    /// @brief append 一条 EventExt wrapper record 到日级扩展事件流。
    ///
    /// caller 负责构造 EventExt payload 与 union discriminant；store 只按
    /// trading_day 分文件。trading_day=0 表示未知交易日，必须拒绝并返回 false，
    /// 由调用方决定是否走其它审计通道。
    virtual bool append_event_ext(std::uint32_t trading_day,
                                  const void* fbs_buf,
                                  std::size_t fbs_size) = 0;

    /// @brief 已 append 但未 flush 的近似字节数，只用于诊断/telemetry。
    virtual std::size_t pending_bytes() const = 0;

    /// @brief 运行时滚动：flush 当前所有 shard，并把 root 改名为 `<root>.<epoch>`。
    ///
    /// 新 root 在下次 append 时 lazy 重建。
    /// @return 滚动后的目录路径；无数据可滚动、flush 未能持久化、或 rename 失败时返回空串。
    /// @note 与 append 同线程调用（single-writer 契约）；flush 失败或 rename 失败时实现应保持
    ///       root 原地可续写，返回空串表示降级（不报告成功），不抛异常升级停机。空串同时覆盖
    ///       「无数据」（良性）与「flush/rename 失败」（降级）两种结果，调用方无法区分。
    virtual std::string rotate() { return {}; }
};

} // namespace ct::data
