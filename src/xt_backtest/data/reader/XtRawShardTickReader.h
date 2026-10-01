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
    /// @brief 构造并接管分片路径列表；读取顺序与 `shard_paths` 一致。
    explicit XtRawShardTickReader(std::vector<std::string> shard_paths);

    /// @return true 表示路径列表非空；不在此打开或校验分片。
    bool init() override;
    /// @brief 回到首片并释放当前分片缓存与 sticky I/O 状态。
    void reset() override;
    /// @return true 表示 `out` 已写入下一条 tick；false 表示分片耗尽或校验失败。
    /// @note 校验失败与正常 EOF 用 has_io_error() 区分：前者必须让上层判定 run 失败，
    ///       否则损坏分片会被当成提前 EOF 静默截断出假结果。
    bool next_tick(XtRawTick& out) override;

    /// @brief 校验失败的分片路径；无失败时为空串（诊断/告警用）。
    const std::string& failed_shard() const { return failed_shard_; }
    /// @brief 校验失败的原因码；未失败时为 `Ok`。
    xt_raw_shard::XtRawShardStatus failure_status() const { return failure_status_; }

private:
    /// @brief 载入下一个可用分片到 `records_`。
    /// @return false 表示路径耗尽，或某片校验失败（后者已置 io_error_）。
    bool load_next_shard();

    std::vector<std::string> shard_paths_;  ///< 待读取分片；不排序、不去重。
    std::vector<XtRawTick> records_;        ///< 当前分片的全部记录（校验通过后才填充）。
    size_t path_idx_ = 0;                   ///< 下一次 load_next_shard() 的路径下标。
    size_t rec_idx_ = 0;                    ///< records_ 的交付游标。
    std::string failed_shard_;              ///< 见 failed_shard()。
    xt_raw_shard::XtRawShardStatus failure_status_ = xt_raw_shard::XtRawShardStatus::Ok;
};
