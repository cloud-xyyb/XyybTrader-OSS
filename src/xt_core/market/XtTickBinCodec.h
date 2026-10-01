#pragma once

#include <cstdint>
#include <fstream>
#include <istream>
#include <ostream>
#include <string>

#include "XtTick.h"
#include "XtRawTick.h"

/// @file XtTickBinCodec.h
/// @brief 回测 tick 加速缓存的二进制编解码入口。
///
/// 回测要反复读同一批 tick，每次都解析 CSV 太慢；解析一次后把 tick 落成二进制缓存，下次
/// 直接顺序读回。本文件定义这个缓存格式（DualV2：header 之后每条记录同时存 double 版
/// `XtRawTick` 与 fixed-point 版 `XtTick`）并给出读写函数。调用方负责以 binary 模式打开
/// stream、决定文件路径与缓存生命周期。detect/read 系列用 `false` 表示打不开、短读、
/// header 不匹配或格式不支持；失败不抛异常。

enum class XtTickBinFormat : uint8_t {
    DualV2 = 2,  ///< 双记录 cache 格式家族：header + XtRawTick(double) + XtTick(fixed-point)。
                 ///< on-disk 修订号独立演进（见 XtTickBinCodec.cpp kTickBinVersion，
                 ///< 解析器语义变更时 bump 使旧 cache 自动失效重建）。
};

namespace xt_tick_bin {

/// @brief tick bin cache 的 on-disk 解析器修订号（SSOT）。
///
/// 解析器语义变更（如补列）时 bump：header 校验拒旧版 bin，且 manifest 签名
/// 混入本值使旧 cache 自动失效重建。bump 时同步 .cpp 的 magic 字符（'V','<n>'）。
inline constexpr uint16_t kTickBinCodecVersion = 4;

/// @brief 从 seekable stream 起始位置识别 tick bin 格式；失败时复位到文件开头。
///
/// 成功后 stream 停在 header 后；失败会 clear 状态并 seek 到开头。
bool detect_stream_format(std::istream& in, XtTickBinFormat& fmt);

/// @brief 打开文件并识别 tick bin 格式；打不开、短读或未知版本返回 false。
bool detect_file_format(const std::string& path, XtTickBinFormat& fmt);

/// @brief 写入 v2 header；调用方负责以 binary 模式打开 stream。
bool write_v2_header(std::ostream& out);

/// @brief 追加写入前校验既有文件格式；空文件会写入 v2 header。
///
/// `out` 必须由调用方以 binary/append 语义打开到同一路径；本函数不负责 open/seek。
/// 非空文件必须已有合法 DualV2 header，headerless 或尺寸不匹配的 cache 会被拒绝。
bool prepare_append_stream(const std::string& path, std::ofstream& out, XtTickBinFormat& fmt);

/// @brief 写入一条 DualV2 tick 记录；不支持的格式或写失败返回 false。
bool write_tick(std::ostream& out,
                XtTickBinFormat fmt,
                const XtRawTick& raw,
                const XtTick& core);

/// @brief 读取下一条 tick 记录。
///
/// 短读、EOF 或不支持的格式返回 false；`core_out=nullptr` 时只返回 raw tick。
bool read_next_tick(std::istream& in,
                    XtTickBinFormat fmt,
                    XtRawTick& raw_out,
                    XtTick* core_out);

}  // namespace xt_tick_bin
