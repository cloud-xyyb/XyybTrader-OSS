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
    DualV2 = 2,

};

namespace xt_tick_bin {

inline constexpr uint16_t kTickBinCodecVersion = 4;

bool detect_stream_format(std::istream& in, XtTickBinFormat& fmt);

bool detect_file_format(const std::string& path, XtTickBinFormat& fmt);

bool write_v2_header(std::ostream& out);

bool prepare_append_stream(const std::string& path, std::ofstream& out, XtTickBinFormat& fmt);

bool write_tick(std::ostream& out,
                XtTickBinFormat fmt,
                const XtRawTick& raw,
                const XtTick& core);

bool read_next_tick(std::istream& in,
                    XtTickBinFormat fmt,
                    XtRawTick& raw_out,
                    XtTick* core_out);

}
