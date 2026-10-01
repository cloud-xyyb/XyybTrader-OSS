#include "XtTickBinCodec.h"

#include <array>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <type_traits>

namespace {

// Header 记录 magic/version 和两种 tick 结构大小；layout 或版本不匹配时拒绝读取 cache。
// v4：盘口深度 DEPTH 10→20（raw/core 512→832B，layout 变更 → 旧 cache 按
// header 尺寸校验 + manifest 签名双路失效重建）。
constexpr std::array<char, 8> kTickBinMagic = {'C', 'T', 'T', 'K', 'V', '4', '\0', '\0'};
constexpr uint16_t kTickBinVersion = xt_tick_bin::kTickBinCodecVersion;  // SSOT 见 header

#pragma pack(push, 1)
struct XtTickBinHeaderV2 {
    char magic[8];
    uint16_t version;
    uint16_t header_size;
    uint32_t raw_size;
    uint32_t core_size;
    uint32_t reserved;
};

/// @brief On-disk DualV2 record：原始 double tick 后接 fixed-point core tick。
///
/// raw 服务回放/诊断路径，core 让热路径 reader 避免重复 double→fp 转换。
struct XtTickBinRecordV2 {
    XtRawTick raw;
    XtTick core;
};
#pragma pack(pop)

// ── On-disk layout tripwire ──────────────────────────────────────────────
// DualV2 是 cache-only 格式，但 record 仍是 XtRawTick + XtTick 的裸 memcpy。
// header 尺寸校验负责拒绝不兼容 cache；static_assert 把 layout 漂移提前到编译期。
static_assert(std::is_trivially_copyable_v<XtRawTick>,
              "XtRawTick must stay trivially_copyable for memcpy codec");
static_assert(std::is_trivially_copyable_v<XtTick>,
              "XtTick must stay trivially_copyable for memcpy codec");
static_assert(sizeof(XtRawTick) == 832,
              "XtRawTick layout changed — only bump this if intentionally changing cache format");
static_assert(sizeof(XtTick) == 832,
              "XtTick layout changed — only bump this if intentionally changing cache format");
// record 无内部填充（pack(1) + 两个成员各自 8 字节对齐尺寸）：read_next_tick 的
// 逐成员 memcpy 偏移依赖这一点。
static_assert(sizeof(XtTickBinRecordV2) == sizeof(XtRawTick) + sizeof(XtTick),
              "DualV2 record must stay padding-free for the byte-buffer read path");
static_assert(offsetof(XtTickBinRecordV2, raw) == 0,
              "DualV2 record must start with the raw tick");
static_assert(offsetof(XtTickBinRecordV2, core) == sizeof(XtRawTick),
              "DualV2 core tick must directly follow the raw tick");

XtTickBinHeaderV2 make_v2_header() {
    XtTickBinHeaderV2 h{};
    std::memcpy(h.magic, kTickBinMagic.data(), kTickBinMagic.size());
    h.version = kTickBinVersion;
    h.header_size = static_cast<uint16_t>(sizeof(XtTickBinHeaderV2));
    h.raw_size = static_cast<uint32_t>(sizeof(XtRawTick));
    h.core_size = static_cast<uint32_t>(sizeof(XtTick));
    h.reserved = 0;
    return h;
}

bool is_valid_v2_header(const XtTickBinHeaderV2& h) {
    return std::memcmp(h.magic, kTickBinMagic.data(), kTickBinMagic.size()) == 0 &&
           h.version == kTickBinVersion &&
           h.header_size == sizeof(XtTickBinHeaderV2) &&
           h.raw_size == sizeof(XtRawTick) &&
           h.core_size == sizeof(XtTick);
}

}  // namespace

namespace xt_tick_bin {

bool detect_stream_format(std::istream& in, XtTickBinFormat& fmt) {
    auto reject = [&]() {
        // 失败不消费 stream，调用方可在同一 stream 上选择 fallback 或按损坏 cache 处理。
        in.clear();
        in.seekg(0, std::ios::beg);
        return false;
    };

    in.clear();
    in.seekg(0, std::ios::beg);
    XtTickBinHeaderV2 h{};
    if (!in.read(reinterpret_cast<char*>(&h), sizeof(h))) {
        return reject();
    }
    if (is_valid_v2_header(h)) {
        fmt = XtTickBinFormat::DualV2;
        return true;
    }
    return reject();
}

bool detect_file_format(const std::string& path, XtTickBinFormat& fmt) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return false;
    return detect_stream_format(in, fmt);
}

bool write_v2_header(std::ostream& out) {
    const XtTickBinHeaderV2 h = make_v2_header();
    out.write(reinterpret_cast<const char*>(&h), sizeof(h));
    return out.good();
}

bool prepare_append_stream(const std::string& path, std::ofstream& out, XtTickBinFormat& fmt) {
    std::error_code ec;
    const bool exists = std::filesystem::exists(path, ec);
    const uintmax_t sz = exists ? std::filesystem::file_size(path, ec) : 0;
    if (ec) return false;
    if (!exists || sz == 0) {
        fmt = XtTickBinFormat::DualV2;
        return write_v2_header(out);
    }
    // append-only 输出流不保证可读；既有文件必须通过独立输入流先完成 header 校验。
    if (!detect_file_format(path, fmt)) return false;
    return true;
}

bool write_tick(std::ostream& out,
                XtTickBinFormat fmt,
                const XtRawTick& raw,
                const XtTick& core) {
    if (fmt != XtTickBinFormat::DualV2) return false;
    XtTickBinRecordV2 rec{};
    rec.raw = raw;
    rec.core = core;
    out.write(reinterpret_cast<const char*>(&rec), sizeof(rec));
    return out.good();
}

bool read_next_tick(std::istream& in,
                    XtTickBinFormat fmt,
                    XtRawTick& raw_out,
                    XtTick* core_out) {
    if (fmt != XtTickBinFormat::DualV2) return false;
    // 读进未初始化字节缓冲，而非 `XtTickBinRecordV2 rec{}`：XtRawTick / XtTick 都带
    // 默认成员初始化器，值初始化与默认初始化都会先把整条 1664B record 清零，随后被
    // in.read 整段覆盖——装载热路径上每条 tick 白付一次清零。两者都 trivially_copyable，
    // memcpy 与成员赋值等价（上方 offsetof/size static_assert 钉住偏移）。
    // 失败语义不变：短读 / EOF 时 raw_out 与 core_out 均不被触碰。
    unsigned char buf[sizeof(XtTickBinRecordV2)];
    if (!in.read(reinterpret_cast<char*>(buf), sizeof(buf))) {
        return false;
    }
    std::memcpy(&raw_out, buf + offsetof(XtTickBinRecordV2, raw), sizeof(XtRawTick));
    if (core_out) {
        std::memcpy(core_out, buf + offsetof(XtTickBinRecordV2, core), sizeof(XtTick));
    }
    return true;
}

}  // namespace xt_tick_bin
