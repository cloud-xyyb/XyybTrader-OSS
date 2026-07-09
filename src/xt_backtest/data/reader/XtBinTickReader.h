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

    explicit XtBinTickReader(std::vector<std::string> paths);

    bool init() override;

    void reset() override;

    bool next_tick(XtRawTick& out) override;

private:

    bool open_next_file();

    static constexpr std::size_t kStreamBufBytes = 1u << 20;

    std::vector<std::string> paths_;

    std::vector<char> stream_buf_;
    size_t idx_ = 0;
    std::unique_ptr<std::ifstream> in_;
    XtTickBinFormat fmt_ = XtTickBinFormat::DualV2;
    bool fmt_inited_ = false;
};
