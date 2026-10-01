#pragma once

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <ios>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include <stdexcept>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include "xt_core/sync/XtTransparentStringHash.h"
#include "xt_util/path/PathUtil.h"

namespace xt_core::local_id {

/// @brief 单个交易 venue 的本地递增 order id 生成器。
///
/// 每笔报单都要有一个本地唯一 id，好在回报、撤单、恢复时把委托认回来。调用方为每个 venue
/// 持有独立实例，submit 路径通过 next() 取号。
/// state_path 非空时启用 high-water 持久化，重启后继承时钟起点和已持久化上界。
/// 空 state_path 只提供当前进程内唯一递增语义，适合测试或无状态场景。
/// next() 支持多个 submit 线程并发调用；构造、析构和 test inspect 接口不提供并发同步。
///
/// ── 唯一性域（uniqueness domain）──
/// local_id 的唯一性域是「同一 venue 的**全部**运行实例（multibook 跨进程）」，
/// 不只是单进程：panel working-order → book 归属以 local_id 单键跨 book join，
/// binance leaked-order sweep / reverse-sync 以 `ct_<local_id>_<nonce>` 格式反解
/// 判定「本进程委托」，而 nonce 是进程内 counter，不含实例区分段。
///
/// 跨实例不相交机制（InstanceIdentity 构造时启用）：
///   id = (instance_tag << 53) | counter，counter 沿用「时钟播种 + high-water
///   持久化」逻辑且 < 2^53（时间位 now_ms<<10 到 ~2248 年才触顶，触顶后 clamp
///   不越 tag 位）。instance_tag ∈ [1,255] 按实例分配 → 不同 tag 的 id 域高位
///   不同，counter 无论如何推进都不会跨域；legacy 无 tag 域（< 2^53）与所有
///   tag 域也不相交。tag 分配 = FNV-1a(venue, instance) 折 8-bit 作首选槽 +
///   state_dir 下 claim file（O_CREAT|O_EXCL 原子抢占 + 线性探测）持久化，
///   同名实例跨重启稳定拿回原槽，同 host 同 venue 的实例间 hash 碰撞由探测
///   消解。**claim 无法完成（目录不可用 / owner 读损 / IO 错 / 255 槽耗尽）
///   一律 throw fail-fast**——发号器绝不在跨实例唯一性未经验证的状态下发号
///   （fail-open 会让同 ms boot race 完整复活）。同名双活由 per-instance
///   state lock file 上的 flock(LOCK_EX|LOCK_NB) 互斥（持有至对象析构/进程
///   退出），冲突同样 throw 拒起。
///
/// 覆盖边界：
///   - 保证仅覆盖共享同一 state_dir 的实例（部署现实：multibook 同 venue 各
///     book 共置一台 host）。跨 host 同 venue 实例不共享 claim 目录，无跨
///     host 互斥保证，部署层需避免该形态。
///   - XTP client32 域（local_id 截断为 uint32）不受 tag 保护（tag 在 bit
///     53+，wrap 护栏是进程内的）——多实例 XTP 非支持形态。
///
/// 部署前置不变量（deploy preconditions；违反 = money-path 碰撞或误拒起）：
///   - claim `.owner` 与 `.lock` 文件在实例存活期间**不可被外部删除**：删
///     owner → 异名实例可 O_EXCL 抢同槽 → 两 live 实例同域；删 `.lock` → 同名
///     实例可在新 inode 上 flock 绕过双活保护 → 同域并发。
///   - 跨 host 同 venue 多实例**不受 flock/claim 保护**（claim 目录与 flock
///     均为单机语义）——部署层必须保证同 venue 实例共享 claim 目录或不跨 host。
///   - 近域顶（own 水位落在 domain_end 的 reserve_block-1 内）干净停机会在重启
///     时**假拒起**（把预留段末端当域耗尽）：方向 fail-closed 正确，且仅 ~2248
///     年或单实例发满 2^53 个 id 后可达。
///
/// @warning 不要在不同 venue 间复用实例或 state file，否则本地订单号域会混在一起。
class PerVenueLocalIdGenerator {
public:
    static constexpr uint64_t kDefaultReserveBlockSize = 1ULL << 16;

    // ── per-instance id 域划分 ──
    static constexpr int      kInstanceTagShift = 53;  ///< tag 左移位数；时间位上限 2^53。
    static constexpr uint64_t kInstanceTagMax   = 255; ///< 8-bit tag，取值 1..255；0 = legacy 无 tag 域。

    /// @brief per-instance 构造身份；venue + resolved runtime.instance_id。
    struct InstanceIdentity {
        std::string venue;      ///< venue 名，如 "binance_u" / "xtp"。
        std::string instance;   ///< resolved instance id，如 "binance_pf_a"；空 = 退化为 legacy 共享域。
        std::string state_dir = "/var/lib/xyybtrader";  ///< state/claim 文件目录（test seam）。
    };

    /// @brief venue 共享 legacy state path；venue 为空时使用共享 fallback 名。
    /// @return `/var/lib/xyybtrader/local_id_<venue>.state`。
    /// @note multibook live 应改用 InstanceIdentity 构造（per-instance 域）；
    ///       本路径保留给未配 instance 的直连调用方与迁移播种。
    static std::string default_state_path(std::string_view venue) {
        return legacy_state_path("/var/lib/xyybtrader", venue);
    }

    /// @brief per-instance state path：`<dir>/local_id_<venue>__<instance>.state`。
    static std::string instance_state_path(const InstanceIdentity& ident) {
        return ident.state_dir + "/local_id_" + sanitize_component(ident.venue) +
               "__" + sanitize_component(ident.instance) + ".state";
    }

    /// @brief 无持久化构造；仅适合测试或明确接受进程重启后不保留 high-water 的调用方。
    PerVenueLocalIdGenerator()
        : PerVenueLocalIdGenerator(/*state_path=*/std::string{}) {}

    /// @brief 持久化构造；读取旧 high-water 后预留第一段可用 id 区间。
    ///
    /// @param state_path 状态文件路径；空字符串表示不持久化。
    /// @note legacy venue 共享域（无 instance tag）。multibook live 用 InstanceIdentity 构造。
    explicit PerVenueLocalIdGenerator(std::string state_path)
        : state_path_(std::move(state_path)) {
        initialize_from_raw_start(now_ms_shifted_start());
    }

    /// @brief per-instance 构造：claim instance tag + 双活互斥后在专属 tag 域发号并持久化。
    ///
    /// 迁移语义：instance-scoped state file 尚无水位而 legacy 共享文件
    /// `local_id_<venue>.state` 存在 → 折入其 saved_high 防回退（tag 域数值上
    /// 恒高于 legacy 域，此播种是 defense-in-depth）；legacy 文件不删不写——
    /// 滚动部署期间旧版本进程可能仍在读写它。
    ///
    /// @param ident venue + resolved instance id；instance 为空时退化为 legacy
    ///              共享域构造（等价 default_state_path(venue)，tag=0，不 claim
    ///              不 flock，test/直连路径旧行为）。
    /// @throws std::runtime_error claim 无法完成（state_dir 不可用 / owner 读损 /
    ///         IO 错 / 落盘不耐久 / 255 槽耗尽）、同名实例已活跃（flock 冲突）、
    ///         或本域已耗尽（own 水位 >= domain_end，reseed 会重发已发 id）——
    ///         caller（gateway configure/init）必须让 runner 拒起，绝不进入可发号状态。
    explicit PerVenueLocalIdGenerator(const InstanceIdentity& ident) {
        if (ident.instance.empty()) {
            state_path_ = legacy_state_path(ident.state_dir, ident.venue);
            initialize_from_raw_start(now_ms_shifted_start());
            return;
        }
        init_instance_identity(ident, now_ms_shifted_start());
    }

    /// @brief 测试入口：直接指定 counter 起点，不读时钟、不读 state file。
    struct ExplicitStartTag {};
    PerVenueLocalIdGenerator(ExplicitStartTag, uint64_t start_value) {
        counter_.store(start_value, std::memory_order_relaxed);
    }

#ifdef XT_BUILD_TESTS
    /// @brief 测试入口：注入 raw_start、state path 和预留块大小。
    struct RawStartStateTag {};
    PerVenueLocalIdGenerator(RawStartStateTag, std::string state_path,
                             uint64_t raw_start,
                             uint64_t reserve_block = kDefaultReserveBlockSize)
        : state_path_(std::move(state_path)),
          reserve_block_(reserve_block == 0 ? 1 : reserve_block) {
        initialize_from_raw_start(raw_start);
    }

    /// @brief 测试入口：per-instance 构造 + 注入时间位（替代 now_ms<<10），
    ///        用于确定性复现「同 ms 播种」的跨实例 boot race。
    PerVenueLocalIdGenerator(RawStartStateTag, const InstanceIdentity& ident,
                             uint64_t raw_time_part,
                             uint64_t reserve_block = kDefaultReserveBlockSize)
        : reserve_block_(reserve_block == 0 ? 1 : reserve_block) {
        init_instance_identity(ident, raw_time_part);
    }

    /// @brief 测试观察：instance 的首选 hash 槽（未经 claim 探测）。
    static uint64_t tag_base_for_test(std::string_view venue,
                                      std::string_view instance) noexcept {
        return tag_base_from_hash(venue, instance);
    }

#define XT_LOCAL_ID_CLAIM_SYNC_SEAM 1
    /// @brief 测试 seam：置 true 让 claim 落盘按 fsync 失败处理（注入耐久性故障）。
    static bool& claim_sync_fail_for_test() noexcept {
        static bool v = false;
        return v;
    }
#endif

    ~PerVenueLocalIdGenerator() {
        // clean-stop 时收紧 high-water，减少下次启动跳过未使用预留段；失败不影响析构。
        // 水位封顶 domain_end：发完最后一个 id 后 counter=domain_end+1，写越域值
        // 会让重启侧无法区分「域耗尽」与「越域垃圾」（legacy 域 domain_end=max，无效果）。
        if (!state_path_.empty()) {
            const uint64_t high = std::min(
                counter_.load(std::memory_order_relaxed), domain_end_);
            if (!persist_state_file_atomic(state_path_, high)) {
                persist_fail_count_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        if (lock_fd_ >= 0) ::close(lock_fd_);  // 释放同名双活 flock。
    }

    PerVenueLocalIdGenerator(const PerVenueLocalIdGenerator&) = delete;
    PerVenueLocalIdGenerator& operator=(const PerVenueLocalIdGenerator&) = delete;

    /// @brief 返回一个进程内唯一且严格递增的 local id。
    ///
    /// 非空 state_path 模式下，生成超过已持久化上界的 id 前会尝试扩展 high-water。
    /// 如果持久化失败，本次 id 仍会返回；同进程唯一性保持，crash 后防重依赖旧 high-water。
    /// tag 域触顶（id > domain_end）→ 硬失败 abort：时间位 ~2248 年才自然触顶，
    /// 且 watermark 校验 + reservation 封顶下实际仅 state 篡改可达；越域发号会
    /// 侵入他人实例域，宁停不发。
    uint64_t next() noexcept {
        const uint64_t id = counter_.fetch_add(1, std::memory_order_relaxed);
        if (id > domain_end_) {
            std::fprintf(stderr,
                         "[FATAL] local_id domain exhausted: id=%llu > domain_end=%llu tag=%llu\n",
                         static_cast<unsigned long long>(id),
                         static_cast<unsigned long long>(domain_end_),
                         static_cast<unsigned long long>(instance_tag_));
            std::abort();
        }
        reserve_until_includes(id);
        return id;
    }

    /// @brief 测试观察：返回下一次 next() 将取出的 counter 值，不是最后已发出的 id。
    uint64_t current_high_for_test() const noexcept {
        return counter_.load(std::memory_order_relaxed);
    }

    /// @brief 测试观察：返回已成功持久化的 high-water 上界。
    uint64_t reserved_until_for_test() const noexcept {
        return reserved_until_.load(std::memory_order_relaxed);
    }

    /// @brief persist_state_file_atomic 失败累计；非 0 = 存在 crash 后 id reissue 风险。
    /// @note leaf-safe 观测点：xt_core 不依赖 xt_metrics，由 gateway/runner 层读取并 emit prom。
    uint64_t persist_fail_count() const noexcept {
        return persist_fail_count_.load(std::memory_order_relaxed);
    }

    /// @brief 当前实例的 id 域 tag；0 = legacy 共享域（非 InstanceIdentity 构造）。
    ///        InstanceIdentity 构造成功即隐含 claim 已验证（否则构造 throw）。
    uint64_t instance_tag() const noexcept { return instance_tag_; }

private:
    static uint64_t now_ms_shifted_start() noexcept {
        const uint64_t now_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        return now_ms << 10;
    }

    /// @brief InstanceIdentity 构造公共体：flock 双活互斥 → claim tag → 播种。
    ///
    /// flock 必须先于 claim：claim 协议（O_EXCL 抢占 / 耐久失败 unlink 归还）
    /// 全程在锁内执行。若 claim 在锁外先行，同名双启存在窗口——A 建 claim 未
    /// 耐久时 A' 读到 owner 匹配接受 tag 并拿锁起跑，A 耐久失败 unlink 释放
    /// 槽位，第三实例 O_EXCL 抢走同 tag → A' 与它域重叠。flock-first 让同名
    /// 并发 boot 直接在锁上拒起（双活守卫本职），该窗口结构性消失。
    ///
    /// @throws std::runtime_error 见 InstanceIdentity ctor 契约。
    void init_instance_identity(const InstanceIdentity& ident,
                                uint64_t raw_time_part) {
        validate_instance_or_throw(ident);
        state_path_       = instance_state_path(ident);
        legacy_seed_path_ = legacy_state_path(ident.state_dir, ident.venue);
        if (!PathUtil::ensure_directory(ident.state_dir)) {
            throw std::runtime_error(
                "local_id state dir unavailable: " + ident.state_dir);
        }
        acquire_instance_lock_or_throw(ident);
        try {
            instance_tag_ = claim_instance_tag_or_throw(ident);
            domain_end_   = ((instance_tag_ + 1) << kInstanceTagShift) - 1;
            initialize_from_raw_start(
                tagged_raw_start(instance_tag_, raw_time_part));
        } catch (...) {
            // ctor 未完成不会跑析构：显式释放 flock（覆盖 claim 与播种两段
            // throw 路径），否则同进程内重试构造（或 gateway re-configure）
            // 会撞自己泄漏的锁。
            ::close(lock_fd_);
            lock_fd_ = -1;
            throw;
        }
    }

    /// @brief 同名双活互斥：per-instance lock file 上 flock(LOCK_EX|LOCK_NB)，
    ///        持有至析构/进程退出。锁在独立 `.lock` 文件而非 state file 本体——
    ///        state file 走 tmp+rename 原子写会换 inode，flock 跟 inode 走，
    ///        rename 后锁就失效了。
    void acquire_instance_lock_or_throw(const InstanceIdentity& ident) {
        const std::string lock_path = state_path_ + ".lock";
        const int fd = ::open(lock_path.c_str(),
                              O_CREAT | O_RDWR | O_CLOEXEC, 0644);
        if (fd < 0) {
            throw std::runtime_error(
                "local_id instance lock open failed: " + lock_path +
                " errno=" + std::to_string(errno));
        }
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            const int e = errno;
            ::close(fd);
            throw std::runtime_error(
                "local_id instance '" + ident.instance +
                "' already active (flock busy on " + lock_path +
                ", errno=" + std::to_string(e) +
                ") — two runners misconfigured with the same runtime.instance_id?");
        }
        lock_fd_ = fd;
    }

    void initialize_from_raw_start(uint64_t raw_start) {
        uint64_t start = raw_start;
        if (!state_path_.empty()) {
            uint64_t saved_high = read_state_file_or_zero(state_path_);
            if (instance_tag_ != 0) {
                if (saved_high >= domain_end_) {
                    // domain_end 是合法的 inclusive 水位：>= 域顶说明本域已发到
                    // 最后一个 id（或水位越域/被篡改）。重播种会重发已发 id，
                    // 与 next() 触顶 abort 同语义——启动失败，不 reseed。
                    throw std::runtime_error(
                        "local_id domain exhausted (saved_high=" +
                        std::to_string(saved_high) + " >= domain_end=" +
                        std::to_string(domain_end_) + ", tag=" +
                        std::to_string(instance_tag_) +
                        ") — refusing to reseed and reissue ids");
                }
                if ((saved_high >> kInstanceTagShift) != instance_tag_) {
                    // 低于本域的跨域水位（tag 重分配 / 文件挪用 / legacy 量级
                    // 残值）：不折入，归零重播种——本域时间播种数值上恒高于
                    // 任何低域水位，不会重发。
                    saved_high = 0;
                }
            }
            if (saved_high == 0 && !legacy_seed_path_.empty()) {
                // 迁移：per-instance 水位尚不存在 → 折入 legacy venue 共享文件
                // saved_high 防回退。只接受真 legacy 域值（< 2^53）：tag 域数值
                // 恒高于 legacy 域 → 数值上恒为 no-op（结构性防回退已由域划分
                // 保证），带 tag 的值出现在 legacy 文件属污染，折入会把本实例
                // 拖进他人域，故拒收。
                const uint64_t legacy_high =
                    read_state_file_or_zero(legacy_seed_path_);
                if ((legacy_high >> kInstanceTagShift) == 0) {
                    saved_high = legacy_high;
                }
            }
            const uint64_t saved_next =
                saved_high == std::numeric_limits<uint64_t>::max()
                    ? saved_high
                    : saved_high + 1;
            start = std::max(start, saved_next);
        }
        counter_.store(start, std::memory_order_relaxed);
        if (state_path_.empty()) {
            reserved_until_.store(std::numeric_limits<uint64_t>::max(),
                                  std::memory_order_relaxed);
        } else {
            reserve_until_includes(start);
        }
    }

    // ── per-instance tag 域 helpers ──────────────────────────────────────

    static std::string legacy_state_path(const std::string& dir,
                                         std::string_view venue) {
        std::string v = venue.empty() ? std::string("default")
                                      : sanitize_component(venue);
        return dir + "/local_id_" + v + ".state";
    }

    /// @brief 文件名安全化：非 [A-Za-z0-9_-] 一律替换为 '_'。
    static std::string sanitize_component(std::string_view s) {
        std::string out(s);
        for (char& c : out) {
            const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                            (c >= '0' && c <= '9') || c == '_' || c == '-';
            if (!ok) c = '_';
        }
        return out;
    }

    /// @return tag 左移拼时间位；时间位 clamp 到 2^53-1（now_ms<<10 于 2^43 ms
    ///         ≈ 278.7 年 ≈ ~2248 年触顶，触顶后跨重启防重完全由 high-water
    ///         持久化承担，不越 tag 位）。
    static uint64_t tagged_raw_start(uint64_t tag, uint64_t time_part) noexcept {
        const uint64_t cap = (1ULL << kInstanceTagShift) - 1;
        return (tag << kInstanceTagShift) | std::min(time_part, cap);
    }

    /// @return FNV-1a(venue, 0x1f, instance) 折 8-bit 的首选槽，1..kInstanceTagMax。
    static uint64_t tag_base_from_hash(std::string_view venue,
                                       std::string_view instance) noexcept {
        uint64_t h = sync::fnv1a_update(sync::kFnv1a64OffsetBasis,
                                        venue.data(), venue.size());
        const char sep = '\x1f';
        h = sync::fnv1a_update(h, &sep, 1);
        h = sync::fnv1a_update(h, instance.data(), instance.size());
        return (h % kInstanceTagMax) + 1;
    }

    static std::string claim_file_path(const InstanceIdentity& ident,
                                       uint64_t tag) {
        return ident.state_dir + "/local_id_tag_" +
               sanitize_component(ident.venue) + "_" + std::to_string(tag) +
               ".owner";
    }

    /// @brief claim 内容耐久落盘：全量 write（循环处理 short-write / EINTR）+
    ///        fsync(file) + close 检查 + fsync(parent dir)（新建文件的目录项也要
    ///        落盘）。接管 fd 的关闭。
    static bool claim_write_durable(int fd, const std::string& line,
                                    const std::string& dir) noexcept {
        const char* buf = line.data();
        size_t remaining = line.size();
        bool ok = true;
        while (remaining > 0) {
            const ssize_t n = ::write(fd, buf, remaining);
            if (n > 0) {
                buf += n;
                remaining -= static_cast<size_t>(n);
                continue;  // short-write：续写剩余。
            }
            if (n < 0 && errno == EINTR) continue;  // 被信号打断：重试。
            ok = false;  // 真错（n<0 && errno≠EINTR）或写 0 字节（非预期）。
            break;
        }
        if (ok) ok = (::fsync(fd) == 0);
        if (::close(fd) != 0) ok = false;
        if (ok) ok = fsync_dir(dir);
#ifdef XT_BUILD_TESTS
        if (claim_sync_fail_for_test()) ok = false;
#endif
        return ok;
    }

    /// @return 目录 fsync 成功与否；打不开目录、fsync 失败或 close 失败均视为失败。
    static bool fsync_dir(const std::string& dir) noexcept {
        const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd < 0) return false;
        bool ok = (::fsync(dfd) == 0);
        if (::close(dfd) != 0) ok = false;
        return ok;
    }

    /// claim owner 三态读取结果。
    enum class OwnerRead { NotPresent, Incomplete, Complete };

    /// @brief claim owner 三态读取（守 owner = 恰好单行 + 换行终止不变量）。
    /// @return Complete   = 文件**恰好等于 `<owner>\n`**：首行以 '\n' 终止且紧接
    ///                      EOF（owner 内容完整可信，out=owner）；
    ///         Incomplete = 文件存在但非恰好 `<owner>\n`（空文件 / 半写 / 前缀 /
    ///                      多行 / 尾随字节，归属未知）；
    ///         NotPresent = 打不开（不存在 / 权限）。
    /// @note 完整性判据两条：(1) 换行终止——写者先写内容后写 '\n'，异名 hash 撞
    ///       同槽时 probe 读者可能只见到 prefix（`book_53` 是 `book_53_x` 的前缀，
    ///       尚无 '\n'），未终止 = 写者在写或 crash 半途；(2) 换行后紧接 EOF——
    ///       否则多行 owner（首行恰是某合法名 + 第二行垃圾，来自旧二进制或外部
    ///       写入的 `name\nGARBAGE\n`）会被同名实例误采纳 → 两 live 实例同域。
    ///       任一不满足 → Incomplete，绝不采纳。写入方恒写单行 `instance + "\n"`，
    ///       合法文件恒满足恰好 `<name>\n`。内嵌 '\n' 的自命名注入子 case 另由
    ///       validate_instance_or_throw 在构造期挡下（管不住外部写入的文件）。
    static OwnerRead read_owner(const std::string& path,
                               std::string& out) noexcept {
        std::ifstream f(path, std::ios::in);
        if (!f.is_open()) return OwnerRead::NotPresent;
        out.clear();
        std::getline(f, out);
        // 触 eofbit（空文件 / prefix 半写，无换行终止）或 fail/badbit（零抽取 /
        // 读 I/O 错）→ Incomplete。
        if (f.eof() || f.fail()) return OwnerRead::Incomplete;
        // getline 消费了 '\n' 但未触 eof：确认换行后紧接 EOF（peek 到任何字节 =
        // 多行 / 尾随字节污染）→ Incomplete。
        if (f.peek() != std::char_traits<char>::eof()) {
            return OwnerRead::Incomplete;
        }
        return OwnerRead::Complete;
    }

    /// @brief 校验 instance 不含破坏单行 owner 不变量的控制字符（'\n'/'\r'/'\0'）。
    /// @throws std::runtime_error 含控制字符即拒起——换行注入（owner 内嵌 '\n'
    ///         让 probe 读者把换行前缀当自己 owner 采纳）单靠完整性校验挡不住，
    ///         因为首行本身是换行终止的完整值。
    static void validate_instance_or_throw(const InstanceIdentity& ident) {
        for (const char c : ident.instance) {
            if (c == '\n' || c == '\r' || c == '\0') {
                throw std::runtime_error(
                    "local_id instance id contains control char (\\n/\\r/\\0), "
                    "breaks single-line owner invariant: '" + ident.instance +
                    "'");
            }
        }
    }

    /// @brief owner-match 复用既有 claim 前重建耐久性（best-effort，返回值不参与
    ///        决策）。上一次 boot 的 claim 可能来自「耐久失败 + unlink 失败」残留
    ///        （内容完整但未落盘），复用前 re-fsync 文件 + 目录项，否则掉电消失
    ///        → 槽位可被重抢。正常路径 claim 本就耐久，re-fsync 冗余无害；boot
    ///        非热路径。
    static void refsync_reused_claim(const std::string& claim,
                                     const std::string& dir) noexcept {
        const int fd = ::open(claim.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd >= 0) {
            (void)::fsync(fd);
            ::close(fd);
        }
        (void)fsync_dir(dir);
    }

    /// @return dir 的父目录路径（去尾斜杠后取末组件之前）；无父层返回 "."/"/".
    static std::string parent_dir_of(const std::string& dir) noexcept {
        if (dir.empty()) return ".";
        size_t end = dir.size();
        while (end > 1 && dir[end - 1] == '/') --end;  // 去尾斜杠（保留根 "/"）。
        const size_t slash = dir.find_last_of('/', end - 1);
        if (slash == std::string::npos) return ".";  // 相对单组件。
        if (slash == 0) return "/";                  // 父层是根。
        return dir.substr(0, slash);
    }

    /// @brief 在 state_dir 下 claim 一个 venue 内唯一的 instance tag；失败即 throw。
    ///
    /// 首选槽 = hash(venue, instance)；被他人持有 → 线性探测下一槽。抢占用
    /// O_CREAT|O_EXCL 原子创建（同 host 并发 boot 只有一方成功），owner 文件
    /// 记 instance 名 → 同名实例跨重启稳定拿回原槽。owner 读取走 read_owner 三态
    /// （NotPresent/Incomplete/Complete）：**Incomplete（空 / 半写 / 前缀，无换行
    /// 终止）= 归属未知，一律跳过探测下一槽**——原主重启会探测拿新槽（安全：
    /// 新槽 = 新域，与旧域不相交），孤儿/半写槽永不复用。
    ///
    /// fail-fast（throw std::runtime_error，caller 拒起）：state_dir 不可用 /
    /// claim create 撞 EEXIST 后 owner 变 NotPresent（竞态被删，归属不可判，可能
    /// 正是本实例历史槽，不可猜）/ 其它 IO 错 / 255 槽耗尽。fail-open 会让同 ms
    /// boot race 完整复活（两实例退回同一 hash 槽 = 同一 id 域），correctness
    /// 优先于 availability。
    static uint64_t claim_instance_tag_or_throw(const InstanceIdentity& ident) {
        const uint64_t base = tag_base_from_hash(ident.venue, ident.instance);
        if (!PathUtil::ensure_directory(ident.state_dir)) {
            throw std::runtime_error(
                "local_id claim dir unavailable: " + ident.state_dir);
        }
        // best-effort：让 state_dir 目录项落盘（首启新建 state_dir + 掉电时
        // 目录项可能未落盘 → claim/state 文件"消失"，槽位可被重抢）。
        // PathUtil::ensure_directory 已对每级新建目录的父目录做过 fsync，此处
        // 对 state_dir 自身父目录再 fsync 一次是冗余但无害的双保险。
        (void)fsync_dir(parent_dir_of(ident.state_dir));
        for (uint64_t k = 0; k < kInstanceTagMax; ++k) {
            const uint64_t tag = ((base - 1 + k) % kInstanceTagMax) + 1;
            const std::string claim = claim_file_path(ident, tag);
            std::string owner;
            const OwnerRead r = read_owner(claim, owner);
            if (r == OwnerRead::Complete) {
                if (owner == ident.instance) {
                    refsync_reused_claim(claim, ident.state_dir);  // F2 复用前重建耐久。
                    return tag;
                }
                continue;  // 他人持有 → 探测下一槽。
            }
            if (r == OwnerRead::Incomplete) {
                continue;  // 空 / 半写 / 前缀（归属未知）→ 探测，不 O_EXCL create。
            }
            // r == NotPresent → O_EXCL create 抢占。
            const int fd = ::open(claim.c_str(),
                                  O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0644);
            if (fd >= 0) {
                // 落盘必须耐久（file + parent dir 双 fsync 全检查）：write 成功但
                // fsync/close 报 ENOSPC/EIO 时若照常起跑，crash 后 claim 消失 →
                // tag 可被重分配 → 域合并。失败 unlink 归还槽位后 throw
                //（磁盘已在出错，继续探测只会连环毒化槽位）。
                if (!claim_write_durable(fd, ident.instance + "\n",
                                         ident.state_dir)) {
                    // flock 已前移到 claim 之前（P1-a），claim 全程持锁 → 无同名
                    // 读者窗口，以下清理残差都不破唯一性：
                    //   - unlink 失败 / crash 回滚留下的**完整 owner claim** =
                    //     槽位继续归本实例，下次 boot owner 匹配复用（正确）；
                    //   - 半写内容 = 空 owner，异名读者跳槽探测下一个（孤儿槽不复用）。
                    // unlink 失败时并入 errno（既非耐久又没清干净，磁盘已在出错）。
                    if (::unlink(claim.c_str()) != 0) {
                        throw std::runtime_error(
                            "local_id claim not durable AND unlink failed: " +
                            claim + " errno=" + std::to_string(errno));
                    }
                    // best-effort：让目录项删除落盘；其返回值不改 throw 决策。
                    (void)fsync_dir(ident.state_dir);
                    throw std::runtime_error(
                        "local_id claim not durable (write/fsync failed): " +
                        claim);
                }
                return tag;
            }
            if (errno == EEXIST) {
                // 抢占输给并发 boot 的另一实例；重读确认归属。
                const OwnerRead r2 = read_owner(claim, owner);
                if (r2 == OwnerRead::Complete && owner == ident.instance) {
                    refsync_reused_claim(claim, ident.state_dir);  // F2。
                    return tag;
                }
                if (r2 != OwnerRead::NotPresent) {
                    continue;  // 他人持有 / 写者半途（Incomplete）→ 探测下一槽。
                }
                // NotPresent：EEXIST 后又读不出（竞态被删 / 权限异常），槽位归属
                // 不可判（可能正是本实例历史槽）→ 不可猜，fail-fast。
                throw std::runtime_error(
                    "local_id claim owner unreadable: " + claim);
            }
            throw std::runtime_error(
                "local_id claim create failed: " + claim +
                " errno=" + std::to_string(errno));
        }
        throw std::runtime_error(
            "local_id claim slots exhausted for venue '" + ident.venue +
            "' (255 instances per venue is not a supported topology)");
    }

    /// @return state file 中的 high-water；空文件、不存在或解析失败返回 0。
    static uint64_t read_state_file_or_zero(const std::string& path) noexcept {
        std::ifstream f(path, std::ios::in);
        if (!f.is_open()) return 0;
        std::string line;
        if (!std::getline(f, line)) return 0;
        try {
            return std::stoull(line);
        } catch (...) {
            return 0;
        }
    }

    /// @brief 饱和加法，避免高水位扩展接近 uint64 max 时 wrap。
    static uint64_t add_saturating(uint64_t value, uint64_t delta) noexcept {
        const uint64_t max = std::numeric_limits<uint64_t>::max();
        return value > max - delta ? max : value + delta;
    }

    void reserve_until_includes(uint64_t id) noexcept {
        if (state_path_.empty()) return;
        if (id <= reserved_until_.load(std::memory_order_acquire)) return;

        std::lock_guard<std::mutex> guard(reserve_mu_);
        const uint64_t current = reserved_until_.load(std::memory_order_relaxed);
        if (id <= current) return;
        const uint64_t reserve_delta = reserve_block_ > 0 ? reserve_block_ - 1 : 0;
        uint64_t next_reserved = add_saturating(id, reserve_delta);
        // tag 域下 reservation 封顶不越 domain_end（legacy 域 domain_end_=max，无效果）。
        if (next_reserved > domain_end_) next_reserved = domain_end_;
        if (persist_state_file_atomic(state_path_, next_reserved)) {
            reserved_until_.store(next_reserved, std::memory_order_release);
        } else {
            // 持久化失败：high-water 未推进，crash 后可能 reissue 已发 id（reserve block
            // 内有界）。leaf 不能直连 xt_metrics，累计 atomic 供 gateway/runner 层 emit prom。
            persist_fail_count_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    /// @brief 原子写 state file；PathUtil 负责 tmp + fsync(fd) + rename + fsync(parent)。
    static bool persist_state_file_atomic(const std::string& path,
                                          uint64_t value) noexcept {
        return PathUtil::write_atomic(path, std::to_string(value));
    }

    std::string state_path_;        ///< 空字符串表示禁用持久化。
    std::string legacy_seed_path_;  ///< 非空 = 首启迁移时可折入的 legacy venue 共享水位文件。
    uint64_t instance_tag_ = 0;     ///< id 域 tag（bit 53..60）；0 = legacy 无 tag 域。
    uint64_t domain_end_ =          ///< 本域最大合法 id；legacy 域无界（uint64 max）。
        std::numeric_limits<uint64_t>::max();
    int lock_fd_ = -1;              ///< 同名双活互斥 flock fd；-1 = 未持锁（legacy 域）。
    std::atomic<uint64_t> counter_{};         ///< fetch_add 源；保存下一次将发出的 id。
    std::atomic<uint64_t> reserved_until_{0}; ///< 最近成功写入 state file 的 high-water。
    std::atomic<uint64_t> persist_fail_count_{0};  ///< persist 失败累计（leaf-safe 观测点）。
    uint64_t reserve_block_ = kDefaultReserveBlockSize;  ///< 每次持久化预留的 id 数量。
    std::mutex reserve_mu_;  ///< 串行化 high-water 扩展和 state file 写入。
};

}  // namespace xt_core::local_id
