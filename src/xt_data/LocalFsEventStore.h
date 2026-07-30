#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "IEventStore.h"

namespace ct::data {

/// @brief MdTick / EventExt 的本地 append-only 文件存储。
///
/// `append()` 和 `append_event_ext()` 写入 size-prefixed FlatBuffers record；
/// `query()` 读取已经落到磁盘的 MdTick shard。
/// 同一实例的 append 路径要求 single writer
/// 打开的 FILE* 由本对象拥有，并在 `flush()` 或析构时关闭。
class LocalFsEventStore : public IEventStore {
public:

    explicit LocalFsEventStore(std::string root_path);

    ~LocalFsEventStore() override;

    LocalFsEventStore(const LocalFsEventStore&)            = delete;
    LocalFsEventStore& operator=(const LocalFsEventStore&) = delete;

    bool append(std::uint32_t trading_day,
                std::string_view exchange,
                std::string_view symbol,
                const void* fbs_buf,
                std::size_t fbs_size) override;

    bool append_event_ext(std::uint32_t trading_day,
                          const void* fbs_buf,
                          std::size_t fbs_size) override;

    std::unique_ptr<IEventIterator> query(const QueryRange& range) override;

    bool flush() override;

    std::string rotate() override;

    std::size_t pending_bytes() const override { return pending_bytes_; }

    [[nodiscard]] const std::string& root() const { return root_path_; }

    [[nodiscard]] std::uint64_t torn_frame_rollback_count() const {
        return torn_frame_rollback_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::uint64_t poisoned_shard_count() const {
        return poisoned_shard_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::uint64_t rotate_fail_count() const {
        return rotate_fail_count_.load(std::memory_order_relaxed);
    }

private:
    /// @brief 单个 shard 的打开态：owning FILE* 与已确认完整 frame 的累计字节。
    struct ShardHandle {
        std::FILE* fp = nullptr;
        std::size_t clean_size = 0;
    };

    std::filesystem::path shard_path(std::uint32_t day,
                                     std::string_view exch,
                                     std::string_view sym) const;

    ShardHandle* open_shard_(const std::string& key,
                             const std::filesystem::path& path);

    std::filesystem::path events_ext_path(std::uint32_t day) const;

    bool write_frame_(const std::string& key, const std::filesystem::path& path,
                      ShardHandle& sh, const void* fbs_buf, std::size_t fbs_size);

    void close_and_drop_shard_(const std::string& key);

    void poison_shard_(const std::string& key);

    std::string root_path_;

    std::unordered_map<std::string, ShardHandle> files_;

    std::unordered_set<std::string> poisoned_shards_;
    std::size_t pending_bytes_ = 0;
    std::vector<std::uint8_t> write_scratch_;
    std::atomic<std::uint64_t> torn_frame_rollback_count_{0};
    std::atomic<std::uint64_t> poisoned_shard_count_{0};
    std::atomic<std::uint64_t> rotate_fail_count_{0};
};

}
