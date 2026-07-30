#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "api/IMarketDataReader.h"
#include "xt_core/market/XtRawShardCodecV3.h"

/// @brief 按交易日分片（RawShardV3）串接输出 `XtRawTick` 的回测 reader。
///
/// 按 `shard_paths` 顺序整片载入并逐条交付；分片之间不排序、不过滤，也不做日期/
/// session 准入（与 `XtBinTickReader` 同分工）。分片选择与窗口裁剪由
/// `XtMarketDataSource` 负责，调用方应按交易日升序传入路径。
///
/// 每片在交付第一条记录前完成 header + 记录数 + payload CRC 全量校验：校验不过即
/// 置 sticky I/O 错误并停止交付，绝不半片交付。整片载入使单片驻留 = 一个交易日的
/// tick 量（分片粒度即驻留上界）。
class XtRawShardTickReader : public IMarketDataReader {
public:

    explicit XtRawShardTickReader(std::vector<std::string> shard_paths);

    bool init() override;

    void reset() override;

    bool next_tick(XtRawTick& out) override;

    const std::string& failed_shard() const { return failed_shard_; }

    xt_raw_shard::XtRawShardStatus failure_status() const { return failure_status_; }

private:

    bool load_next_shard();

    std::vector<std::string> shard_paths_;
    std::vector<XtRawTick> records_;
    size_t path_idx_ = 0;
    size_t rec_idx_ = 0;
    std::string failed_shard_;
    xt_raw_shard::XtRawShardStatus failure_status_ = xt_raw_shard::XtRawShardStatus::Ok;
};
