#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>
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

    void flush() override;

    std::string rotate() override;

    std::size_t pending_bytes() const override { return pending_bytes_; }

    [[nodiscard]] const std::string& root() const { return root_path_; }

private:

    std::filesystem::path shard_path(std::uint32_t day,
                                     std::string_view exch,
                                     std::string_view sym) const;

    std::FILE* get_or_open_file(std::uint32_t day,
                                std::string_view exchange,
                                std::string_view symbol);

    std::filesystem::path events_ext_path(std::uint32_t day) const;

    bool write_record_(std::FILE* fp, const void* fbs_buf, std::size_t fbs_size);

    void close_and_drop_shard_(const std::string& key);

    std::string root_path_;

    std::unordered_map<std::string, std::FILE*> files_;
    std::size_t pending_bytes_ = 0;
    std::vector<std::uint8_t> write_scratch_;
};

}
