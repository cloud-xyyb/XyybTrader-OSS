#include "SafetyStateStore.h"

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace xt_risk::state {

namespace {

// ── On-disk format ────────────────────────────────────────────────────────
//
// Common scalar body [0x00, 0x38):
//   0x00 magic "CTSS"
//   0x04 format_version
//   0x08 schema_version
//   0x0C current_year
//   0x10 kill_switch_active   uint8 0/1
//   0x11 open_halted          uint8 0/1
//   0x12 annual_cutoff_active uint8 0/1
//   0x13 reserved             uint8
//   0x14 reserved             int32
//   0x18 halt_trigger_ms      int64
//   0x20 peak_equity_fp       int64
//   0x28 year_start_equity_fp int64
//   0x30 frozen_cash_fp       int64
//
// v1: common body + CRC32 at 0x38, total 0x3C bytes.
// v2: common body + uint32 vector_len + N uint8 per-sid kill flags + CRC32.
// v3: v2 + risk_fund_balance_fp + year_start_net_pnl_fp before CRC32.
// v4: v3 + kill_switch_source (uint32, low byte) before CRC32.
// v5: v4 + uint32 al_len + al_len uint8 per-sid auto-liquidation flags
//        + uint32 src_len + src_len uint8 per-sid liquidating-source before CRC32.
//
// Writer emits v5 only. Loader accepts v1/v2/v3/v4/v5 after exact-size and CRC checks.
// Fields are encoded by width with memcpy; no C++ struct padding/alignment enters disk.
// Older layouts leave fields introduced by later layouts at SafetyState{} defaults.

constexpr std::array<char, 4> kMagic = {'C', 'T', 'S', 'S'};
constexpr int32_t kFormatVersionV1 = 1;
constexpr int32_t kFormatVersionV2 = 2;
constexpr int32_t kFormatVersionV3 = 3;
constexpr int32_t kFormatVersionV4 = 4;
constexpr int32_t kFormatVersionV5 = 5;
constexpr int32_t kFormatVersion = kFormatVersionV5;  // writer format
constexpr int32_t kSchemaVersionWriter = 5;           // writer schema（与 decode_v5 校验一致）
constexpr size_t  kFileSizeV1 = 0x3C;  // v1 fixed-size file
constexpr size_t  kV1HeaderEnd = 0x38;  // v1 CRC offset (also v2 vector_len offset)
constexpr size_t  kV2VectorLenOffset = 0x38;  // v2 vector_len 起点
constexpr size_t  kV2VectorDataOffset = 0x3C;  // v2 vector data 起点
constexpr uint32_t kMaxVectorLen = 4096;  // per-sid array sanity bound

// CRC32 table is process-local; C++11 magic-static initialization is thread-safe.
const uint32_t* crc32_table() noexcept {
    static const auto table = []() {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) {
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            t[i] = c;
        }
        return t;
    }();
    return table.data();
}

uint32_t crc32_ieee(const uint8_t* data, size_t len) noexcept {
    const uint32_t* table = crc32_table();
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

void put_i32(uint8_t* buf, int32_t v) noexcept {
    std::memcpy(buf, &v, sizeof(v));
}
void put_u32(uint8_t* buf, uint32_t v) noexcept {
    std::memcpy(buf, &v, sizeof(v));
}
void put_i64(uint8_t* buf, int64_t v) noexcept {
    std::memcpy(buf, &v, sizeof(v));
}
int32_t get_i32(const uint8_t* buf) noexcept {
    int32_t v;
    std::memcpy(&v, buf, sizeof(v));
    return v;
}
uint32_t get_u32(const uint8_t* buf) noexcept {
    uint32_t v;
    std::memcpy(&v, buf, sizeof(v));
    return v;
}
int64_t get_i64(const uint8_t* buf) noexcept {
    int64_t v;
    std::memcpy(&v, buf, sizeof(v));
    return v;
}

// 编码 common scalar body；caller 负责追加 layout-specific tail 和 CRC32。
void encode_v1_body(const SafetyState& s, uint8_t* buf, int32_t format_version) noexcept {
    std::memcpy(&buf[0x00], kMagic.data(), 4);
    put_i32(&buf[0x04], format_version);
    put_i32(&buf[0x08], s.schema_version);
    put_i32(&buf[0x0C], s.current_year);
    buf[0x10] = s.kill_switch_active ? 1 : 0;
    buf[0x11] = s.open_halted ? 1 : 0;
    buf[0x12] = s.annual_cutoff_active ? 1 : 0;
    buf[0x13] = 0;  // reserved
    put_i32(&buf[0x14], 0);  // reserved align
    put_i64(&buf[0x18], s.halt_trigger_ms);
    put_i64(&buf[0x20], s.peak_equity_fp);
    put_i64(&buf[0x28], s.year_start_equity_fp);
    put_i64(&buf[0x30], s.frozen_cash_fp);
}

// Writer format: common body + per-sid kill flags + annual net-PnL anchors + kill_switch_source + CRC32。
size_t encode_v4(const SafetyState& s, std::vector<uint8_t>& buf) {
    const uint32_t n = static_cast<uint32_t>(s.per_strategy_kill_switch.size());
    const size_t fields_off = kV2VectorDataOffset + n;   // 2 int64 起点 (vector 之后)
    const size_t total = fields_off + 16 + 4 + 4;        // anchors(16) + kill_switch_source u32(4) + crc(4)
    buf.assign(total, 0);
    encode_v1_body(s, buf.data(), kFormatVersion);
    put_u32(&buf[kV2VectorLenOffset], n);
    if (n > 0) {
        std::memcpy(&buf[kV2VectorDataOffset], s.per_strategy_kill_switch.data(), n);
    }
    put_i64(&buf[fields_off], s.risk_fund_balance_fp);
    put_i64(&buf[fields_off + 8], s.year_start_net_pnl_fp);
    put_u32(&buf[fields_off + 16], static_cast<uint32_t>(s.kill_switch_source));  // 低字节有效
    const uint32_t crc = crc32_ieee(buf.data(), total - 4);
    put_u32(&buf[total - 4], crc);
    return total;
}

// Writer format v5: v4 layout + per-sid auto-liquidation flags + per-sid liquidating-source。
// 两个 vector 各自带 uint32 长度前缀（自描述，与 per_strategy_kill_switch 同模式）。
size_t encode_v5(const SafetyState& s, std::vector<uint8_t>& buf) {
    const uint32_t n     = static_cast<uint32_t>(s.per_strategy_kill_switch.size());
    const uint32_t n_al  = static_cast<uint32_t>(s.per_strategy_auto_liquidating.size());
    const uint32_t n_src = static_cast<uint32_t>(s.per_strategy_liquidating_source.size());
    const size_t fields_off = kV2VectorDataOffset + n;       // v3 anchors 起点
    const size_t v5_off     = fields_off + 16 + 4;           // v4 source field 之后（anchors16 + source4）
    const size_t total      = v5_off + 4 + n_al + 4 + n_src + 4;  // al_len + al + src_len + src + crc
    buf.assign(total, 0);
    encode_v1_body(s, buf.data(), kFormatVersion);
    // schema_version 归一到 writer schema：不信任 s.schema_version（可能是 load 进来的旧值），
    // 否则会写出 format=5/schema=4，被当前 reader 按 schema mismatch 拒读。
    put_i32(&buf[0x08], kSchemaVersionWriter);
    put_u32(&buf[kV2VectorLenOffset], n);
    if (n > 0) {
        std::memcpy(&buf[kV2VectorDataOffset], s.per_strategy_kill_switch.data(), n);
    }
    put_i64(&buf[fields_off], s.risk_fund_balance_fp);
    put_i64(&buf[fields_off + 8], s.year_start_net_pnl_fp);
    put_u32(&buf[fields_off + 16], static_cast<uint32_t>(s.kill_switch_source));
    put_u32(&buf[v5_off], n_al);
    if (n_al > 0) {
        std::memcpy(&buf[v5_off + 4], s.per_strategy_auto_liquidating.data(), n_al);
    }
    const size_t src_len_off = v5_off + 4 + n_al;
    put_u32(&buf[src_len_off], n_src);
    if (n_src > 0) {
        std::memcpy(&buf[src_len_off + 4], s.per_strategy_liquidating_source.data(), n_src);
    }
    const uint32_t crc = crc32_ieee(buf.data(), total - 4);
    put_u32(&buf[total - 4], crc);
    return total;
}

// v1 没有 per-sid kill flags 和 v3 annual net-PnL anchors；缺失字段保持默认值。
std::optional<SafetyState> decode_v1(const uint8_t* buf) noexcept {
    if (std::memcmp(&buf[0x00], kMagic.data(), 4) != 0) return std::nullopt;
    if (get_i32(&buf[0x04]) != kFormatVersionV1) return std::nullopt;
    const int32_t schema = get_i32(&buf[0x08]);
    if (schema != 1) return std::nullopt;  // v1 file must have schema=1
    const uint32_t stored_crc = get_u32(&buf[kV1HeaderEnd]);
    const uint32_t actual_crc = crc32_ieee(buf, kV1HeaderEnd);
    if (stored_crc != actual_crc) return std::nullopt;
    SafetyState s;
    s.schema_version       = schema;
    s.current_year         = get_i32(&buf[0x0C]);
    s.kill_switch_active   = buf[0x10] != 0;
    s.open_halted          = buf[0x11] != 0;
    s.annual_cutoff_active = buf[0x12] != 0;
    s.halt_trigger_ms      = get_i64(&buf[0x18]);
    s.peak_equity_fp       = get_i64(&buf[0x20]);
    s.year_start_equity_fp = get_i64(&buf[0x28]);
    s.frozen_cash_fp       = get_i64(&buf[0x30]);
    return s;
}

// v2 增加 per-sid kill flags；v3 annual net-PnL anchors 保持默认值。
std::optional<SafetyState> decode_v2(const uint8_t* buf, size_t buf_size) noexcept {
    if (buf_size < kV2VectorDataOffset + 4) return std::nullopt;  // 不够 header + len + crc
    if (std::memcmp(&buf[0x00], kMagic.data(), 4) != 0) return std::nullopt;
    if (get_i32(&buf[0x04]) != kFormatVersionV2) return std::nullopt;
    const int32_t schema = get_i32(&buf[0x08]);
    if (schema != 2) return std::nullopt;
    const uint32_t n = get_u32(&buf[kV2VectorLenOffset]);
    if (n > kMaxVectorLen) return std::nullopt;  // sanity check
    const size_t expected_size = kV2VectorDataOffset + n + 4;
    if (buf_size != expected_size) return std::nullopt;
    const uint32_t stored_crc = get_u32(&buf[expected_size - 4]);
    const uint32_t actual_crc = crc32_ieee(buf, expected_size - 4);
    if (stored_crc != actual_crc) return std::nullopt;
    SafetyState s;
    s.schema_version       = schema;
    s.current_year         = get_i32(&buf[0x0C]);
    s.kill_switch_active   = buf[0x10] != 0;
    s.open_halted          = buf[0x11] != 0;
    s.annual_cutoff_active = buf[0x12] != 0;
    s.halt_trigger_ms      = get_i64(&buf[0x18]);
    s.peak_equity_fp       = get_i64(&buf[0x20]);
    s.year_start_equity_fp = get_i64(&buf[0x28]);
    s.frozen_cash_fp       = get_i64(&buf[0x30]);
    s.per_strategy_kill_switch.resize(n);
    if (n > 0) {
        std::memcpy(s.per_strategy_kill_switch.data(), &buf[kV2VectorDataOffset], n);
    }
    return s;
}

// v3 decoder 校验 per-sid vector 和尾部 anchor 的 variable-size 边界。
std::optional<SafetyState> decode_v3(const uint8_t* buf, size_t buf_size) noexcept {
    if (buf_size < kV2VectorDataOffset + 4) return std::nullopt;
    if (std::memcmp(&buf[0x00], kMagic.data(), 4) != 0) return std::nullopt;
    if (get_i32(&buf[0x04]) != kFormatVersionV3) return std::nullopt;
    const int32_t schema = get_i32(&buf[0x08]);
    if (schema != 3) return std::nullopt;
    const uint32_t n = get_u32(&buf[kV2VectorLenOffset]);
    if (n > kMaxVectorLen) return std::nullopt;
    const size_t fields_off = kV2VectorDataOffset + n;
    const size_t expected_size = fields_off + 16 + 4;
    if (buf_size != expected_size) return std::nullopt;
    const uint32_t stored_crc = get_u32(&buf[expected_size - 4]);
    const uint32_t actual_crc = crc32_ieee(buf, expected_size - 4);
    if (stored_crc != actual_crc) return std::nullopt;
    SafetyState s;
    s.schema_version       = schema;
    s.current_year         = get_i32(&buf[0x0C]);
    s.kill_switch_active   = buf[0x10] != 0;
    s.open_halted          = buf[0x11] != 0;
    s.annual_cutoff_active = buf[0x12] != 0;
    s.halt_trigger_ms      = get_i64(&buf[0x18]);
    s.peak_equity_fp       = get_i64(&buf[0x20]);
    s.year_start_equity_fp = get_i64(&buf[0x28]);
    s.frozen_cash_fp       = get_i64(&buf[0x30]);
    s.per_strategy_kill_switch.resize(n);
    if (n > 0) {
        std::memcpy(s.per_strategy_kill_switch.data(), &buf[kV2VectorDataOffset], n);
    }
    s.risk_fund_balance_fp  = get_i64(&buf[fields_off]);
    s.year_start_net_pnl_fp = get_i64(&buf[fields_off + 8]);
    return s;
}

// v4 在 v3 尾部追加 kill_switch_source（uint32 低字节）；旧 v3 文件走 decode_v3，source 缺省 0。
std::optional<SafetyState> decode_v4(const uint8_t* buf, size_t buf_size) noexcept {
    if (buf_size < kV2VectorDataOffset + 4) return std::nullopt;
    if (std::memcmp(&buf[0x00], kMagic.data(), 4) != 0) return std::nullopt;
    if (get_i32(&buf[0x04]) != kFormatVersionV4) return std::nullopt;
    const int32_t schema = get_i32(&buf[0x08]);
    if (schema != 4) return std::nullopt;
    const uint32_t n = get_u32(&buf[kV2VectorLenOffset]);
    if (n > kMaxVectorLen) return std::nullopt;
    const size_t fields_off = kV2VectorDataOffset + n;
    const size_t expected_size = fields_off + 16 + 4 + 4;  // anchors + source u32 + crc
    if (buf_size != expected_size) return std::nullopt;
    const uint32_t stored_crc = get_u32(&buf[expected_size - 4]);
    const uint32_t actual_crc = crc32_ieee(buf, expected_size - 4);
    if (stored_crc != actual_crc) return std::nullopt;
    SafetyState s;
    s.schema_version       = schema;
    s.current_year         = get_i32(&buf[0x0C]);
    s.kill_switch_active   = buf[0x10] != 0;
    s.open_halted          = buf[0x11] != 0;
    s.annual_cutoff_active = buf[0x12] != 0;
    s.halt_trigger_ms      = get_i64(&buf[0x18]);
    s.peak_equity_fp       = get_i64(&buf[0x20]);
    s.year_start_equity_fp = get_i64(&buf[0x28]);
    s.frozen_cash_fp       = get_i64(&buf[0x30]);
    s.per_strategy_kill_switch.resize(n);
    if (n > 0) {
        std::memcpy(s.per_strategy_kill_switch.data(), &buf[kV2VectorDataOffset], n);
    }
    s.risk_fund_balance_fp  = get_i64(&buf[fields_off]);
    s.year_start_net_pnl_fp = get_i64(&buf[fields_off + 8]);
    s.kill_switch_source    = static_cast<uint8_t>(get_u32(&buf[fields_off + 16]) & 0xFFu);
    return s;
}

// v5 在 v4 尾部追加两段 per-sid vector（auto-liquidation flags + liquidating-source）。
// 旧 v4 文件走 decode_v4，两段缺省为空（cold-start false）。所有从盘读出的长度都先
// 做 sanity bound + 逐段 buf_size 边界校验，再读对应字节，最后 exact-size + CRC。
std::optional<SafetyState> decode_v5(const uint8_t* buf, size_t buf_size) noexcept {
    if (buf_size < kV2VectorDataOffset + 4) return std::nullopt;
    if (std::memcmp(&buf[0x00], kMagic.data(), 4) != 0) return std::nullopt;
    if (get_i32(&buf[0x04]) != kFormatVersionV5) return std::nullopt;
    const int32_t schema = get_i32(&buf[0x08]);
    if (schema != 5) return std::nullopt;
    const uint32_t n = get_u32(&buf[kV2VectorLenOffset]);
    if (n > kMaxVectorLen) return std::nullopt;
    const size_t fields_off = kV2VectorDataOffset + n;
    const size_t v5_off = fields_off + 16 + 4;          // v4 anchors(16) + source(4) 之后
    if (buf_size < v5_off + 4) return std::nullopt;      // 够读 al_len
    const uint32_t n_al = get_u32(&buf[v5_off]);
    if (n_al > kMaxVectorLen) return std::nullopt;
    const size_t src_len_off = v5_off + 4 + n_al;
    if (buf_size < src_len_off + 4) return std::nullopt; // 够读 src_len
    const uint32_t n_src = get_u32(&buf[src_len_off]);
    if (n_src > kMaxVectorLen) return std::nullopt;
    const size_t expected_size = src_len_off + 4 + n_src + 4;  // src + crc
    if (buf_size != expected_size) return std::nullopt;
    const uint32_t stored_crc = get_u32(&buf[expected_size - 4]);
    const uint32_t actual_crc = crc32_ieee(buf, expected_size - 4);
    if (stored_crc != actual_crc) return std::nullopt;
    SafetyState s;
    s.schema_version       = schema;
    s.current_year         = get_i32(&buf[0x0C]);
    s.kill_switch_active   = buf[0x10] != 0;
    s.open_halted          = buf[0x11] != 0;
    s.annual_cutoff_active = buf[0x12] != 0;
    s.halt_trigger_ms      = get_i64(&buf[0x18]);
    s.peak_equity_fp       = get_i64(&buf[0x20]);
    s.year_start_equity_fp = get_i64(&buf[0x28]);
    s.frozen_cash_fp       = get_i64(&buf[0x30]);
    s.per_strategy_kill_switch.resize(n);
    if (n > 0) {
        std::memcpy(s.per_strategy_kill_switch.data(), &buf[kV2VectorDataOffset], n);
    }
    s.risk_fund_balance_fp  = get_i64(&buf[fields_off]);
    s.year_start_net_pnl_fp = get_i64(&buf[fields_off + 8]);
    s.kill_switch_source    = static_cast<uint8_t>(get_u32(&buf[fields_off + 16]) & 0xFFu);
    s.per_strategy_auto_liquidating.resize(n_al);
    if (n_al > 0) {
        std::memcpy(s.per_strategy_auto_liquidating.data(), &buf[v5_off + 4], n_al);
    }
    s.per_strategy_liquidating_source.resize(n_src);
    if (n_src > 0) {
        std::memcpy(s.per_strategy_liquidating_source.data(), &buf[src_len_off + 4], n_src);
    }
    return s;
}

// 拼 tmp 文件名：<target>.tmp.<pid>。
// pid 用于在罕见的并发写场景下避免覆盖（同一进程序列化由 caller 保证）。
std::string make_tmp_path(const std::filesystem::path& target) {
    std::string s = target.string();
    s += ".tmp.";
    s += std::to_string(::getpid());
    return s;
}

// 父目录创建失败不在此处吞掉写流程；后续 open 会返回可观测的 I/O failure。
void try_create_parent_dir(const std::filesystem::path& target) noexcept {
    std::error_code ec;
    auto parent = target.parent_path();
    if (parent.empty()) return;
    std::filesystem::create_directories(parent, ec);
}

// POSIX rename 只保证 namespace 原子替换；父目录 fsync 才持久化 dir entry。
bool fsync_parent_dir(const std::filesystem::path& target) noexcept {
    auto parent = target.parent_path();
    if (parent.empty()) return true;
    int dir_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) return false;
    const int rc = ::fsync(dir_fd);
    ::close(dir_fd);
    return rc == 0;
}

}  // namespace

bool SafetyStateStore::save(const std::filesystem::path& target, const SafetyState& state) {
    try_create_parent_dir(target);

    // 写路径只产出 writer layout；旧 layout 仅由 load() 兼容读取。
    std::vector<uint8_t> buf;
    const size_t total = encode_v5(state, buf);

    const std::string tmp = make_tmp_path(target);

    // 0600 限制非 owner 用户读取 latched risk state。
    int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
    if (fd < 0) return false;

    // 普通文件 write() 可能 partial；这里循环到完整 payload 落入 fd。
    ssize_t written = 0;
    while (written < static_cast<ssize_t>(total)) {
        const ssize_t n = ::write(fd, buf.data() + written, total - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            ::unlink(tmp.c_str());
            return false;
        }
        if (n == 0) {
            ::close(fd);
            ::unlink(tmp.c_str());
            return false;
        }
        written += n;
    }

    // 先 fsync file content，再 close；rename 只在完整 tmp 文件持久化后执行。
    if (::fsync(fd) != 0) {
        ::close(fd);
        ::unlink(tmp.c_str());
        return false;
    }
    if (::close(fd) != 0) {
        ::unlink(tmp.c_str());
        return false;
    }

    // 同一目录内 rename 原子替换，避免 crash 后暴露半写文件。
    if (::rename(tmp.c_str(), target.c_str()) != 0) {
        ::unlink(tmp.c_str());
        return false;
    }

    // rename 已成功时不回滚 target；父目录 fsync 失败只降低 crash-recovery 强度。
    (void)fsync_parent_dir(target);

    return true;
}

LoadResult SafetyStateStore::load_checked(const std::filesystem::path& source) {
    std::error_code ec;
    const bool present = std::filesystem::exists(source, ec);
    // exists() 本身出错（如 path 遍历权限）→ 无法确认缺失，保守按 Invalid（fail-closed）。
    if (ec) return {LoadStatus::Invalid, std::nullopt};
    if (!present) return {LoadStatus::Missing, std::nullopt};

    // 文件已确认存在：此后任何失败都是 Invalid（损坏），绝不降级为 Missing。
    const auto fsz_raw = std::filesystem::file_size(source, ec);
    if (ec) return {LoadStatus::Invalid, std::nullopt};
    if (fsz_raw < kFileSizeV1) return {LoadStatus::Invalid, std::nullopt};
    const size_t fsz = static_cast<size_t>(fsz_raw);

    std::ifstream in(source, std::ios::binary);
    if (!in.is_open()) return {LoadStatus::Invalid, std::nullopt};

    std::vector<uint8_t> buf(fsz);
    in.read(reinterpret_cast<char*>(buf.data()), fsz);
    if (static_cast<size_t>(in.gcount()) != fsz) return {LoadStatus::Invalid, std::nullopt};

    // format_version dispatch；各 decoder 负责 schema、exact-size 和 CRC 校验。
    if (fsz < 8) return {LoadStatus::Invalid, std::nullopt};
    const int32_t format = get_i32(&buf[0x04]);
    std::optional<SafetyState> decoded;
    if (format == kFormatVersionV1 && fsz == kFileSizeV1) {
        decoded = decode_v1(buf.data());
    } else if (format == kFormatVersionV2) {
        decoded = decode_v2(buf.data(), fsz);
    } else if (format == kFormatVersionV3) {
        decoded = decode_v3(buf.data(), fsz);
    } else if (format == kFormatVersionV4) {
        decoded = decode_v4(buf.data(), fsz);
    } else if (format == kFormatVersionV5) {
        decoded = decode_v5(buf.data(), fsz);
    }
    if (!decoded.has_value()) return {LoadStatus::Invalid, std::nullopt};
    return {LoadStatus::Ok, std::move(decoded)};
}

std::optional<SafetyState> SafetyStateStore::load(const std::filesystem::path& source) {
    return load_checked(source).state;
}

}  // namespace xt_risk::state
