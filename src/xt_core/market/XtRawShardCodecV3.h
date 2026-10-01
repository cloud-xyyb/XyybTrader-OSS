#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <istream>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include "XtRawTick.h"

/// @file XtRawShardCodecV3.h
/// @brief 按交易日切分的 raw-only tick 分片格式（RawShardV3）。
///
/// 一个文件 = 一个交易日的 tick 分片：header 之后是定长 `XtRawTick` 记录序列，
/// **不存** fixed-point 副本（载入方按需 `to_core` 现算）。header 自带记录数与
/// payload CRC32，截断、追加与位翻转都在载入时被拒；header 自身另有 CRC，元数据
/// 损坏与 payload 损坏可区分。
///
/// 与 `XtTickBinCodec.h`（DualV2）并列且互不读写：magic 不同、记录布局不同，两种
/// 格式的 detect 互相拒绝。本 header 只做字节层编解码——分片放哪个目录、何时可用、
/// 何时回落 CSV 由调用方（cache store）决定。
///
/// 读侧失败一律返回状态码/false，不抛异常；`load_shard` 失败时不触碰输出容器。

namespace xt_raw_shard {

/// @brief RawShardV3 的 on-disk 修订号（SSOT）。
///
/// 记录布局或 header 语义变更时 bump：header 校验拒旧版分片，调用方指纹混入本值
/// 使旧 generation 自然失效。bump 时同步 `kShardMagic` 的版本字符。
inline constexpr uint16_t kRawShardCodecVersion = 1;

#pragma pack(push, 1)
/// @brief 分片头；`header_crc32` 之前的全部字节参与 header CRC。
struct XtRawShardHeaderV3 {
    char magic[8];           ///< 'X','T','R','S','V','3',0,0
    uint16_t version;        ///< kRawShardCodecVersion
    uint16_t header_size;    ///< sizeof(XtRawShardHeaderV3)
    uint32_t record_size;    ///< sizeof(XtRawTick)
    uint32_t trading_day;    ///< 分片所属交易日 YYYYMMDD；0 非法（见 D4 CSV 兜底）
    uint32_t flags;          ///< 保留位，当前恒 0
    uint64_t record_count;   ///< payload 记录条数
    uint32_t payload_crc32;  ///< payload 全部字节的 CRC32-IEEE
    uint32_t header_crc32;   ///< 本结构前 36 字节的 CRC32-IEEE
};
#pragma pack(pop)

/// @brief 分片头承载的元信息（读侧视图）。
struct XtRawShardInfo {
    uint32_t trading_day = 0;
    uint64_t record_count = 0;
    uint32_t payload_crc32 = 0;
};

/// @brief 分片载入结果；除 `Ok` 外调用方都应视为该分片不可用。
enum class XtRawShardStatus : uint8_t {
    Ok = 0,        ///< header + 记录数 + payload CRC 全部通过。
    OpenFailed,    ///< 文件打不开。
    BadHeader,     ///< magic/版本/尺寸不符，或 header CRC 不匹配。
    Truncated,     ///< 实际记录数少于 header 声明。
    TrailingBytes, ///< header 声明的记录之后仍有字节。
    CrcMismatch,   ///< payload CRC 与 header 不符（位翻转/内容被改写）。
};

/// @brief 供日志与断言使用的稳定短名。
inline const char* to_string(XtRawShardStatus s) {
    switch (s) {
        case XtRawShardStatus::Ok:            return "ok";
        case XtRawShardStatus::OpenFailed:    return "open_failed";
        case XtRawShardStatus::BadHeader:     return "bad_header";
        case XtRawShardStatus::Truncated:     return "truncated";
        case XtRawShardStatus::TrailingBytes: return "trailing_bytes";
        case XtRawShardStatus::CrcMismatch:   return "crc_mismatch";
    }
    return "unknown";
}

// ── CRC32-IEEE（多项式 0xEDB88320，反射实现）─────────────────────────────

inline constexpr uint32_t kCrc32Init = 0xFFFFFFFFu;

/// @brief 首次调用构建查表；C++11 magic static 保证线程安全。
inline const uint32_t* crc32_table() noexcept {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            t[i] = c;
        }
        return t;
    }();
    return table.data();
}

/// @brief 逐字节推进 CRC（参考实现；`crc` 为中间态，非最终值）。
inline uint32_t crc32_update_bytewise(uint32_t crc, const void* data, size_t n) noexcept {
    const uint32_t* table = crc32_table();
    const auto* p = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < n; ++i) {
        crc = table[(crc ^ p[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc;
}

/// @brief slice-by-8 查表（8 张 256 项，table[0] 即逐字节表）。
inline const uint32_t* crc32_slice_table() noexcept {
    static const std::array<uint32_t, 8 * 256> table = [] {
        std::array<uint32_t, 8 * 256> t{};
        const uint32_t* base = crc32_table();
        for (uint32_t i = 0; i < 256; ++i) t[i] = base[i];
        for (uint32_t k = 1; k < 8; ++k) {
            for (uint32_t i = 0; i < 256; ++i) {
                const uint32_t prev = t[(k - 1) * 256 + i];
                t[k * 256 + i] = (prev >> 8) ^ base[prev & 0xFFu];
            }
        }
        return t;
    }();
    return table.data();
}

/// @brief 流式推进 CRC；`crc` 为中间态（初值 `kCrc32Init`），非最终值。
///
/// 主体走 slice-by-8（每轮吃 8 字节），结果与 `crc32_update_bytewise` 逐位相同——
/// 同一多项式的等价重排，不是新校验算法。整片校验是命中路径上唯一的全量扫描，
/// 逐字节实现在 GB 级分片上是主要耗时项。
inline uint32_t crc32_update(uint32_t crc, const void* data, size_t n) noexcept {
    const uint32_t* t = crc32_slice_table();
    const auto* p = static_cast<const unsigned char*>(data);
    while (n >= 8) {
        // 显式按小端组装 32 位字：不依赖 host 字节序，也不做未对齐 load。
        const uint32_t lo = crc ^ (static_cast<uint32_t>(p[0]) |
                                   (static_cast<uint32_t>(p[1]) << 8) |
                                   (static_cast<uint32_t>(p[2]) << 16) |
                                   (static_cast<uint32_t>(p[3]) << 24));
        const uint32_t hi = static_cast<uint32_t>(p[4]) |
                            (static_cast<uint32_t>(p[5]) << 8) |
                            (static_cast<uint32_t>(p[6]) << 16) |
                            (static_cast<uint32_t>(p[7]) << 24);
        crc = t[7 * 256 + (lo & 0xFFu)] ^
              t[6 * 256 + ((lo >> 8) & 0xFFu)] ^
              t[5 * 256 + ((lo >> 16) & 0xFFu)] ^
              t[4 * 256 + ((lo >> 24) & 0xFFu)] ^
              t[3 * 256 + (hi & 0xFFu)] ^
              t[2 * 256 + ((hi >> 8) & 0xFFu)] ^
              t[1 * 256 + ((hi >> 16) & 0xFFu)] ^
              t[0 * 256 + ((hi >> 24) & 0xFFu)];
        p += 8;
        n -= 8;
    }
    return crc32_update_bytewise(crc, p, n);
}

/// @brief 把中间态收尾为可落盘的 CRC 值。
inline uint32_t crc32_final(uint32_t crc) noexcept { return crc ^ 0xFFFFFFFFu; }

/// @brief 一次性计算 CRC32-IEEE。
inline uint32_t crc32_ieee(const void* data, size_t n) noexcept {
    return crc32_final(crc32_update(kCrc32Init, data, n));
}

// ── On-disk layout tripwire ──────────────────────────────────────────────
// 分片是 memcpy 落盘的裸记录：header 尺寸/记录尺寸校验负责拒绝不兼容分片，
// static_assert 把 layout 漂移提前到编译期。
static_assert(std::is_trivially_copyable_v<XtRawTick>,
              "XtRawTick must stay trivially_copyable for the RawShardV3 memcpy codec");
static_assert(sizeof(XtRawTick) == 832,
              "XtRawTick layout changed — bump kRawShardCodecVersion deliberately");
static_assert(sizeof(XtRawShardHeaderV3) == 40, "RawShardV3 header must stay 40 bytes");
static_assert(offsetof(XtRawShardHeaderV3, header_crc32) == 36,
              "header_crc32 must be the trailing field — CRC covers everything before it");

namespace detail {

inline constexpr std::array<char, 8> kShardMagic = {'X', 'T', 'R', 'S', 'V', '3', '\0', '\0'};

/// @brief header CRC 覆盖范围：结构体去掉尾部 `header_crc32` 字段。
inline constexpr size_t kHeaderCrcSpan = offsetof(XtRawShardHeaderV3, header_crc32);

inline XtRawShardHeaderV3 make_header(uint32_t trading_day, uint64_t record_count,
                                      uint32_t payload_crc) {
    XtRawShardHeaderV3 h{};
    std::memcpy(h.magic, kShardMagic.data(), kShardMagic.size());
    h.version = kRawShardCodecVersion;
    h.header_size = static_cast<uint16_t>(sizeof(XtRawShardHeaderV3));
    h.record_size = static_cast<uint32_t>(sizeof(XtRawTick));
    h.trading_day = trading_day;
    h.flags = 0;
    h.record_count = record_count;
    h.payload_crc32 = payload_crc;
    h.header_crc32 = crc32_ieee(&h, kHeaderCrcSpan);
    return h;
}

inline bool header_is_valid(const XtRawShardHeaderV3& h) {
    if (std::memcmp(h.magic, kShardMagic.data(), kShardMagic.size()) != 0) return false;
    if (h.version != kRawShardCodecVersion) return false;
    if (h.header_size != sizeof(XtRawShardHeaderV3)) return false;
    if (h.record_size != sizeof(XtRawTick)) return false;
    // 交易日是分片身份的一部分：closure index 与 CURRENT 指针都按它寻址，缺失即无法定位。
    if (h.trading_day == 0) return false;
    if (h.header_crc32 != crc32_ieee(&h, kHeaderCrcSpan)) return false;
    return true;
}

}  // namespace detail

// ── 写 ───────────────────────────────────────────────────────────────────

/// @brief 写出完整分片（header + payload）。
///
/// header 携带记录数与 payload CRC，故要求调用方先备齐整日记录；`n == 0` 合法
/// （空交易日），`trading_day == 0` 被拒。
/// @pre `out` 由调用方以 binary 模式打开；本函数不负责 open/commit。
/// @return false 表示参数非法或流写失败；调用方应放弃该分片（勿提交）。
inline bool write_shard(std::ostream& out, uint32_t trading_day,
                        const XtRawTick* recs, size_t n) {
    if (trading_day == 0) return false;
    if (n != 0 && recs == nullptr) return false;

    uint32_t crc = kCrc32Init;
    if (n != 0) crc = crc32_update(crc, recs, n * sizeof(XtRawTick));
    const XtRawShardHeaderV3 h =
        detail::make_header(trading_day, static_cast<uint64_t>(n), crc32_final(crc));

    out.write(reinterpret_cast<const char*>(&h), sizeof(h));
    if (!out.good()) return false;
    if (n != 0) {
        out.write(reinterpret_cast<const char*>(recs),
                  static_cast<std::streamsize>(n * sizeof(XtRawTick)));
    }
    return out.good();
}

/// @brief `write_shard` 的 vector 重载。
inline bool write_shard(std::ostream& out, uint32_t trading_day,
                        const std::vector<XtRawTick>& recs) {
    return write_shard(out, trading_day, recs.data(), recs.size());
}

// ── 读 ───────────────────────────────────────────────────────────────────

/// @brief 从流起始位置读 header 并校验；失败时 clear 状态并 seek 回开头。
///
/// 成功后流停在第一条记录处，可继续 `read_next_record`。
inline bool read_header(std::istream& in, XtRawShardInfo& out_info) {
    auto reject = [&]() {
        in.clear();
        in.seekg(0, std::ios::beg);
        return false;
    };
    in.clear();
    in.seekg(0, std::ios::beg);
    XtRawShardHeaderV3 h{};
    if (!in.read(reinterpret_cast<char*>(&h), sizeof(h))) return reject();
    if (!detail::header_is_valid(h)) return reject();
    out_info.trading_day = h.trading_day;
    out_info.record_count = h.record_count;
    out_info.payload_crc32 = h.payload_crc32;
    return true;
}

/// @brief 读下一条记录；短读或 EOF 返回 false 且**不触碰** `out`。
///
/// 装载循环靠「失败不写出参」这一点：读到尾部那一轮不能让半条脏数据覆盖上一条。
inline bool read_next_record(std::istream& in, XtRawTick& out) {
    unsigned char buf[sizeof(XtRawTick)];
    if (!in.read(reinterpret_cast<char*>(buf), sizeof(buf))) return false;
    std::memcpy(&out, buf, sizeof(XtRawTick));
    return true;
}

/// @brief 整分片载入：header + 记录数 + 尾字节 + payload CRC 四重校验。
///
/// 任一校验不过都返回对应状态且 **不触碰** `out`/`info`（fail-closed：调用方不会
/// 拿到半个分片，只能整体回落 CSV 兜底）。
inline XtRawShardStatus load_shard(const std::string& path,
                                   std::vector<XtRawTick>& out,
                                   XtRawShardInfo& info) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return XtRawShardStatus::OpenFailed;

    in.seekg(0, std::ios::end);
    const auto end_pos = in.tellg();
    if (end_pos < 0) return XtRawShardStatus::BadHeader;
    const auto file_bytes = static_cast<uint64_t>(end_pos);
    in.seekg(0, std::ios::beg);

    XtRawShardInfo local{};
    if (!read_header(in, local)) return XtRawShardStatus::BadHeader;

    // 先用文件尺寸判定记录数是否自洽，再逐条读：既区分截断与追加，也让下方 reserve
    // 的上界由真实文件长度而非 header 声明决定（伪造的巨大 record_count 不致巨量预留）。
    const uint64_t payload_bytes = file_bytes - sizeof(XtRawShardHeaderV3);
    if (local.record_count > payload_bytes / sizeof(XtRawTick)) {
        return XtRawShardStatus::Truncated;
    }
    if (payload_bytes != local.record_count * sizeof(XtRawTick)) {
        return XtRawShardStatus::TrailingBytes;
    }

    // 整段一次读入：记录数已由文件尺寸自洽校验定死，逐条 read 在百万级记录上是纯开销
    // （每条一次 filebuf 调用 + 一次中转 memcpy）。CRC 随后对整段算，与逐条推进等价。
    std::vector<XtRawTick> recs(static_cast<size_t>(local.record_count));
    if (local.record_count != 0) {
        const auto payload_span = static_cast<std::streamsize>(
            local.record_count * sizeof(XtRawTick));
        in.read(reinterpret_cast<char*>(recs.data()), payload_span);
        if (in.gcount() != payload_span) return XtRawShardStatus::Truncated;
        if (crc32_ieee(recs.data(), static_cast<size_t>(payload_span)) != local.payload_crc32) {
            return XtRawShardStatus::CrcMismatch;
        }
    } else if (crc32_final(kCrc32Init) != local.payload_crc32) {
        return XtRawShardStatus::CrcMismatch;
    }

    out.swap(recs);
    info = local;
    return XtRawShardStatus::Ok;
}

}  // namespace xt_raw_shard
