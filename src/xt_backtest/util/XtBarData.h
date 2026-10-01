/// @file
/// @brief XTBR V2 bar 二进制 cache 工具。
///
/// 提供 `data/bars/{contract}/{period}.bar.bin` 的读写、周期转换和细粒度 bar 聚合。
/// 读路径只接受 XTBR V2 header required 的文件；LZ4 支持由 `XT_HAS_LZ4` 控制，
/// 未启用时写入会降级为未压缩格式。
#pragma once
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

// 依赖 XtCtaBarLoader::TimedBar；通常由 XtCtaBarLoader.h 在该类型定义后 include。

#ifdef XT_HAS_LZ4
#include <lz4.h>
#endif

namespace xt_data {

// ── 压缩类型 ──────────────────────────────────────────────────────────────────
enum CompressType : uint8_t {
    kNone = 0,
    kLZ4  = 1,
};

// ── V2 wire header（32 字节） ─────────────────────────────────────────────────
//
// 格式说明：
//   magic[8]       = "CTBR V2\0"
//   version        = 2
//   bar_size       = sizeof(TimedBar)，用于版本兼容校验
//   bar_count      = bar 条数
//   period_s       = 周期秒数（300=5m, 900=15m, 1800=30m）
//   compress_type  = CompressType (0=none, 1=LZ4)
//   reserved[3]    = {0,0,0}
//
/// @brief XTBR V2 `.bar.bin` wire header，固定 32 字节。
struct BarBinHeaderV2 {
    char     magic[8];          // "CTBR V2\0"
    uint32_t version;           // 2
    uint32_t bar_size;          // sizeof(TimedBar)
    uint64_t bar_count;         // bar 条数
    uint32_t period_s;          // 周期秒数
    uint8_t  compress_type;     // CompressType
    uint8_t  reserved[3];       // 保留字段
};
static_assert(sizeof(BarBinHeaderV2) == 32, "BarBinHeaderV2 必须为 32 字节");

inline constexpr char kBarBinMagicV2[8] = {'C','T','B','R',' ','V','2','\0'};

// ── 周期转换工具 ──────────────────────────────────────────────────────────────

/// 周期字符串转秒数（"5m"→300, "15m"→900, "30m"→1800, "1h"→3600）
inline uint32_t period_str_to_seconds(const std::string& period) {
    if (period.empty()) return 900; // 默认 15m
    char* end_ptr = nullptr;
    long n = std::strtol(period.c_str(), &end_ptr, 10);
    if (!end_ptr || n <= 0) return 900;
    if (*end_ptr == 'h' || *end_ptr == 'H') return static_cast<uint32_t>(n * 3600);
    return static_cast<uint32_t>(n * 60); // 默认分钟
}

/// 秒数转周期字符串（300→"5m", 900→"15m", 3600→"1h"）
inline std::string period_seconds_to_str(uint32_t s) {
    if (s >= 3600 && s % 3600 == 0) return std::to_string(s / 3600) + "h";
    return std::to_string(s / 60) + "m";
}

/// 周期字符串转目录下的文件名（"5m"→"5m.bar.bin"）
inline std::string period_to_filename(const std::string& period) {
    return period + ".bar.bin";
}

// ── write_bars — 写入 XTBR V2 格式 ───────────────────────────────────────────

/// @brief 将 TimedBar 序列写入 XTBR V2 二进制文件
///
/// @param path        输出文件路径
/// @param bars        TimedBar 序列
/// @param period_s    周期秒数（300/900/1800）
/// @param compress    压缩类型；未启用 XT_HAS_LZ4 时 kLZ4 会降级为 kNone
/// @return true 表示文件完整写入；false 表示目录、文件或压缩写入失败。
inline bool write_bars(const std::filesystem::path& path,
                       const std::vector<XtCtaBarLoader::TimedBar>& bars,
                       uint32_t period_s,
                       CompressType compress = kLZ4) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec);

    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) return false;

#ifndef XT_HAS_LZ4
    compress = kNone;
#endif

    BarBinHeaderV2 hdr{};
    std::memcpy(hdr.magic, kBarBinMagicV2, 8);
    hdr.version       = 2;
    hdr.bar_size      = static_cast<uint32_t>(sizeof(XtCtaBarLoader::TimedBar));
    hdr.bar_count     = bars.size();
    hdr.period_s      = period_s;
    hdr.compress_type = static_cast<uint8_t>(compress);
    std::memset(hdr.reserved, 0, sizeof(hdr.reserved));

    out.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));

    if (bars.empty()) return out.good();

    const auto* raw_data = reinterpret_cast<const char*>(bars.data());
    // LZ4 API 使用 int 表示字节数；超过范围的输入应在调用侧拆分文件。
    const auto  raw_size = static_cast<int>(bars.size() * sizeof(XtCtaBarLoader::TimedBar));

    if (compress == kLZ4) {
#ifdef XT_HAS_LZ4
        // 压缩 payload 前置原始字节数，读路径据此恢复 TimedBar 数组。
        const int max_dst = LZ4_compressBound(raw_size);
        std::vector<char> buf(max_dst);
        const int compressed_size = LZ4_compress_default(raw_data, buf.data(), raw_size, max_dst);
        if (compressed_size <= 0) return false;

        const uint32_t raw_size_u32 = static_cast<uint32_t>(raw_size);
        out.write(reinterpret_cast<const char*>(&raw_size_u32), sizeof(raw_size_u32));
        out.write(buf.data(), compressed_size);
#endif
    } else {
        out.write(raw_data, raw_size);
    }

    return out.good();
}

// ── read_bars — 读取 XTBR V2 格式 ────────────────────────────────────────────

/// @brief 从 .bar.bin 文件读取 TimedBar 序列（XTBR V2 header required）
///
/// @param path          输入文件路径
/// @param out           输出 TimedBar 序列；成功读取时追加到现有内容后面
/// @param period_s_out  若非空，返回文件中记录的周期秒数
/// @return true 表示 header、bar_size、payload 均校验通过；false 不保证 out 不变。
inline bool read_bars(const std::filesystem::path& path,
                      std::vector<XtCtaBarLoader::TimedBar>& out,
                      uint32_t* period_s_out = nullptr) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;

    char magic[8] = {};
    in.read(magic, 8);
    if (!in.good()) return false;

    if (std::memcmp(magic, kBarBinMagicV2, 8) == 0) {
        in.seekg(0);
        BarBinHeaderV2 hdr{};
        in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
        if (!in.good()) return false;
        if (hdr.bar_size != sizeof(XtCtaBarLoader::TimedBar)) return false;

        if (period_s_out) *period_s_out = hdr.period_s;

        if (hdr.bar_count == 0) return true;

        if (hdr.compress_type == kLZ4) {
#ifdef XT_HAS_LZ4
            uint32_t raw_size = 0;
            in.read(reinterpret_cast<char*>(&raw_size), sizeof(raw_size));
            if (!in.good()) return false;

            const auto cur_pos = in.tellg();
            in.seekg(0, std::ios::end);
            const auto file_size = in.tellg();
            in.seekg(cur_pos);
            const auto compressed_size = static_cast<size_t>(file_size - cur_pos);

            std::vector<char> compressed(compressed_size);
            in.read(compressed.data(), static_cast<std::streamsize>(compressed_size));
            if (!in.good()) return false;

            const size_t prev = out.size();
            const size_t n_bars = raw_size / sizeof(XtCtaBarLoader::TimedBar);
            out.resize(prev + n_bars);
            const int decompressed = LZ4_decompress_safe(
                compressed.data(),
                reinterpret_cast<char*>(out.data() + prev),
                static_cast<int>(compressed_size),
                static_cast<int>(raw_size));
            if (decompressed < 0 || static_cast<uint32_t>(decompressed) != raw_size) {
                out.resize(prev);
                return false;
            }
            return true;
#else
            return false;
#endif
        } else {
            const size_t prev = out.size();
            out.resize(prev + hdr.bar_count);
            in.read(reinterpret_cast<char*>(out.data() + prev),
                    static_cast<std::streamsize>(hdr.bar_count * sizeof(XtCtaBarLoader::TimedBar)));
            if (!in.good()) { out.resize(prev); return false; }
            return true;
        }
    }

    return false;
}

// ── derive_bars — 从细粒度 bar 聚合为粗粒度 ──────────────────────────────────

/// @brief 将细粒度 bar 聚合为粗粒度 bar（如 5m → 15m）
///
/// 按目标周期的时间槽分组，合并 OHLCV：
///   open = 组内第一根 bar 的 open
///   high = max(highs), low = min(lows)
///   close = 组内最后一根 bar 的 close
///   volume/turnover = sum，open_interest = 组内最后一根 bar 的值
///   key = 组内第一根 bar 的 key
///
/// @param src           源 bar 序列（需已按时间排序）
/// @param src_period_s  源周期秒数（如 300）
/// @param dst_period_s  目标周期秒数（如 900），必须为 src 的整数倍
/// @return              聚合后的 bar 序列
inline std::vector<XtCtaBarLoader::TimedBar> derive_bars(
        const std::vector<XtCtaBarLoader::TimedBar>& src,
        uint32_t src_period_s,
        uint32_t dst_period_s) {
    std::vector<XtCtaBarLoader::TimedBar> result;
    if (src.empty() || dst_period_s <= src_period_s) return result;

    const uint32_t src_min = src_period_s / 60;
    const uint32_t dst_min = dst_period_s / 60;
    if (src_min == 0 || dst_min == 0 || dst_min % src_min != 0) return result;

    result.reserve(src.size() / (dst_min / src_min) + 1);

    // 聚合槽只看分钟，毫秒级输入会落入同一分钟槽。
    auto bar_slot = [&](const XtCtaBarLoader::TimedBar& tb) -> uint64_t {
        uint32_t date = tb.bar.action_date;
        uint32_t at = tb.bar.action_time;
        uint32_t t = at / 1000;
        uint32_t total_min = (t / 10000) * 60 + ((t / 100) % 100);
        uint32_t slot_min = (total_min / dst_min) * dst_min;
        return static_cast<uint64_t>(date) * 10000ULL + slot_min;
    };

    uint64_t cur_slot = UINT64_MAX;
    XtCtaBarLoader::TimedBar agg{};

    auto flush = [&]() {
        if (cur_slot != UINT64_MAX && agg.bar.close > 0.0)
            result.push_back(agg);
    };

    for (const auto& tb : src) {
        uint64_t s = bar_slot(tb);
        if (s != cur_slot) {
            flush();
            agg = tb;
            cur_slot = s;
        } else {
            if (tb.bar.high > agg.bar.high) agg.bar.high = tb.bar.high;
            if (tb.bar.low  < agg.bar.low)  agg.bar.low  = tb.bar.low;
            agg.bar.close         = tb.bar.close;
            agg.bar.volume       += tb.bar.volume;
            agg.bar.turnover     += tb.bar.turnover;
            agg.bar.open_interest = tb.bar.open_interest;
        }
    }
    flush();

    return result;
}

} // namespace xt_data
