#pragma once

#include <cstdint>
#include <fstream>
#include <istream>
#include <ostream>
#include <string>

#include "XtTick.h"
#include "XtRawTick.h"

/// @file XtTickBinCodec.h
/// @brief Tick binary cache 的 DualV2 编解码入口。
///
/// 本文件只定义回测加速缓存格式：header 后按记录写入 `XtRawTick` 与 `XtTick`。
/// 调用方负责以 binary 模式打开 stream、决定文件路径和 cache 生命周期。
/// detect/read 系列用 `false` 表示打不开、短读、header 不匹配或不支持的格式；失败不抛异常。

enum class XtTickBinFormat : uint8_t {
    DualV2 = 2,

};

namespace xt_tick_bin {

inline constexpr uint16_t kTickBinCodecVersion = 3;

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
