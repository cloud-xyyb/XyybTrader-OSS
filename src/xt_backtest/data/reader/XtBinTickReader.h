#pragma once

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "api/IMarketDataReader.h"
#include "xt_core/market/XtTickBinCodec.h"

/// @brief 从 `XtTickBinCodec` 格式 `.bin` 路径串接输出 `XtRawTick` 的回测 reader。
///
/// 按 `paths` 顺序打开文件并逐条解码，文件之间不排序、不过滤，也不执行日期/session
/// 准入。路径选择、simtick/bin-cache 来源判断和 replay 调度由 `XtMarketDataSource` /
/// `XtReplayer` 负责。单线程顺序读取；`reset()` 回到首个路径并清空当前 stream 与
/// header 探测状态。
class XtBinTickReader : public IMarketDataReader {
public:
    /// @brief 构造读取器并接管路径列表；读取顺序与 `paths` 顺序一致。
    explicit XtBinTickReader(std::vector<std::string> paths);

    /// @return true 表示至少有一个路径对应的文件存在；不会打开全部文件或预校验 header。
    bool init() override;
    /// @brief 回到第一个路径并清空当前 stream、格式状态和 sticky I/O 状态。
    void reset() override;
    /// @return true 表示 `out` 已写入下一条 tick；false 表示所有路径已耗尽或不可继续读取。
    /// @note header 无法识别的文件会被跳过；打开失败通过 has_io_error() 暴露。
    bool next_tick(XtRawTick& out) override;

private:
    /// @brief 打开下一个可打开的 bin 文件，并为该文件重置格式探测状态。
    /// @return true 表示 `in_` 已指向打开的 stream；false 表示路径已耗尽。
    bool open_next_file();

    static constexpr std::size_t kStreamBufBytes = 1u << 20;  ///< 1MB 流缓冲；默认 ~8KB 下 1KB/record 逐条读 syscall 密集。

    std::vector<std::string> paths_;  ///< 待读取文件列表；不排序、不去重。
    /// 流缓冲；声明在 in_ 之前保证析构晚于 stream（filebuf 持裸指针）。
    std::vector<char> stream_buf_;
    size_t idx_ = 0;                  ///< 下一次 open_next_file() 尝试打开的路径下标。
    std::unique_ptr<std::ifstream> in_; ///< 当前文件 stream；空表示需要切换文件。
    XtTickBinFormat fmt_ = XtTickBinFormat::DualV2; ///< 当前文件格式；header 探测后有效。
    bool fmt_inited_ = false; ///< true 表示当前文件已完成 header 探测。
};
