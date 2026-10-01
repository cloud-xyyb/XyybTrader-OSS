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
    /// @param root_path 数据根路径；构造时不创建目录，首次 append 时 lazy create。
    explicit LocalFsEventStore(std::string root_path);
    /// @brief 析构时调用 flush() 关闭所有已打开文件。
    ~LocalFsEventStore() override;

    LocalFsEventStore(const LocalFsEventStore&)            = delete;
    LocalFsEventStore& operator=(const LocalFsEventStore&) = delete;

    /// @brief append MdTick record 到 `<day>/<exchange>/<symbol>/tick.fbs`。
    bool append(std::uint32_t trading_day,
                std::string_view exchange,
                std::string_view symbol,
                const void* fbs_buf,
                std::size_t fbs_size) override;

    /// @brief append EventExt record 到 `<day>/_events/events_ext.fbs`。
    bool append_event_ext(std::uint32_t trading_day,
                          const void* fbs_buf,
                          std::size_t fbs_size) override;

    /// @brief 创建按 trading_day / action_time 归并的 forward-only MdTick iterator。
    std::unique_ptr<IEventIterator> query(const QueryRange& range) override;

    /// @brief fflush + fsync + fclose 所有打开文件，并重置 pending_bytes()。
    /// @return 全部 shard 落盘成功为 true；任一 fflush/fsync/fclose 失败为 false（仍完成关闭清理）。
    bool flush() override;

    /// @brief flush 所有 shard 后把 root 改名为 `<root>.<epoch>`；新 root 下次 append lazy 重建。
    /// @note flush 未能持久化或 rename 失败 → 不改名、返回空串（fail-closed，不报告成功），
    ///       root 原地保留供下次重试。
    std::string rotate() override;

    /// @brief 返回已写入 stdio 缓冲但未 flush 的近似字节数。
    std::size_t pending_bytes() const override { return pending_bytes_; }

    /// @brief 返回配置的 root 路径，供测试/诊断使用。
    [[nodiscard]] const std::string& root() const { return root_path_; }

    /// @brief 短写后成功截断回滚到上一完整 frame 边界的累计次数（诊断/测试用）。
    [[nodiscard]] std::uint64_t torn_frame_rollback_count() const {
        return torn_frame_rollback_count_.load(std::memory_order_relaxed);
    }
    /// @brief 因残帧无法回滚被闩死 fail-closed 的 shard 累计次数（诊断/测试用）。
    [[nodiscard]] std::uint64_t poisoned_shard_count() const {
        return poisoned_shard_count_.load(std::memory_order_relaxed);
    }
    /// @brief rotate 因 flush 失败或 rename 失败而降级（返回空串）的累计次数（诊断/测试用）；
    ///        不含「无数据可滚动」的良性空返回。
    [[nodiscard]] std::uint64_t rotate_fail_count() const {
        return rotate_fail_count_.load(std::memory_order_relaxed);
    }

private:
    /// @brief 单个 shard 的打开态：owning FILE* 与已确认完整 frame 的累计字节。
    struct ShardHandle {
        std::FILE* fp = nullptr;    ///< append-binary 句柄；关闭集中在 flush()/回滚路径。
        std::size_t clean_size = 0; ///< 已确认完整 frame 的累计字节（含 open 时既有大小），短写回滚锚点。
    };

    /// @brief 生成 MdTick shard 路径。
    std::filesystem::path shard_path(std::uint32_t day,
                                     std::string_view exch,
                                     std::string_view sym) const;

    /// @brief 打开或复用 shard；不存在时创建父目录并以 append-binary 打开。
    /// @return 就绪可写的 ShardHandle*；shard 已闩死 (poisoned)、目录/文件打开失败时返回 nullptr。
    ShardHandle* open_shard_(const std::string& key,
                             const std::filesystem::path& path);

    /// @brief 生成日级 EventExt 流路径 `<root>/<day>/_events/events_ext.fbs`。
    std::filesystem::path events_ext_path(std::uint32_t day) const;

    /// @brief 把 `[uint32 LE size][payload]` 拼成单帧后一次 fwrite 写出。
    /// @note 整帧一次 fwrite，避免 size prefix 与 payload 分裂。短写 = 残帧毒化 shard 尾部：
    ///       丢 FD（fclose 尽力刷出已确认的完整 frame）后重开 `path` 把尾部 ftruncate 回滚到上一
    ///       完整 frame 边界 (clean_size)；on-disk 已低于该边界（确认帧未落盘、干净边界不可定位）
    ///       或 ftruncate 失败 → 闩死 shard，禁止后续续写制造不可见后缀。clean_size /
    ///       pending_bytes_ 仅在整帧写成功后累加。复用 write_scratch_ 高水位 buffer（drain 单写独占）。
    bool write_frame_(const std::string& key, const std::filesystem::path& path,
                      ShardHandle& sh, const void* fbs_buf, std::size_t fbs_size);

    /// @brief 关闭并从 FD cache 移除指定 shard，使后续 append 重新 lazy open。
    void close_and_drop_shard_(const std::string& key);

    /// @brief 把 shard 闩死为 fail-closed（store 生命周期内不解除）；首次闩死时计数并限频告警。
    void poison_shard_(const std::string& key);

    std::string root_path_; ///< 数据根路径。

    // Shard key = "YYYYMMDD|EXCH|SYM"；EventExt 使用 "_events" 占位 key。
    // value 为 owning ShardHandle，关闭路径集中在 flush()/析构。
    std::unordered_map<std::string, ShardHandle> files_;
    // 回滚/flush 失败被闩死的 shard key；独立于 files_ 存活，flush()/rotate() 清 files_ 时不丢。
    std::unordered_set<std::string> poisoned_shards_;
    std::size_t pending_bytes_ = 0; ///< 已写入 stdio 缓冲但未 flush 的近似字节数。
    std::vector<std::uint8_t> write_scratch_; ///< append 单帧拼装 buffer，drain 单写独占复用。
    std::atomic<std::uint64_t> torn_frame_rollback_count_{0}; ///< 短写后成功回滚次数。
    std::atomic<std::uint64_t> poisoned_shard_count_{0};      ///< 闩死 shard 次数。
    std::atomic<std::uint64_t> rotate_fail_count_{0};         ///< rotate 因 flush/rename 失败降级次数。
};

} // namespace ct::data
