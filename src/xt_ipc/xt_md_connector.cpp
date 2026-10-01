// xt_md_connector 是 MD fan-out 共享内存 ring 的 owner/writer 进程。
// 本文件只暴露可执行入口；CLI 解析、shm owner 初始化、live/replay source 运行都留在
// anonymous namespace。运行约束：单 writer、满则覆盖不反压、运行中不 truncate。
// 退出由 owner shm_unlink 清段；本进程只发布行情 tick，不接订单、账户或风控状态。

#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "api/IMdGateway.h"
#include "GatewayFactory.h"       // per-venue connector target 将 src/xt_live/gateway 放入 include path
#include "ctp/md/CTPMdCfg.h"
#include "ctp/md/CtpMdSessionGate.h"  // expected-tick 窗解析/eval（gauge + CTPMdCfg 共用）
#include "binance/md/BinanceMdCfg.h"  // POD（仅 WS/REST URL）；GatewayFactory.h 已无条件引入，显式列出供 binance 分支
#include "xt_backtest/data/reader/XtBinTickReader.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_ipc/XtMdSidecarPath.h"
#include "xt_ipc/XtMdWireContract.h"
#include "xt_ipc/XtShmRing.h"
#include "xt_metrics/XtMdConnectorExporter.h"

namespace {

using Ring = xt_ipc::XtShmRing<xt_ipc::XtMdWireSlot>;

// capture 时戳：CLOCK_MONOTONIC steady ns。**必须与 reader 进程 XtSteadyClock 同时钟域**
// —— XtSteadyClock 基于 std::chrono::steady_clock（Linux == CLOCK_MONOTONIC），故本
// connector 直用 std::chrono::steady_clock（与 connector_epoch 同源）→ 跨进程可比。
// 前提：writer/reader 同一 monotonic time namespace（bare-proc 同 host 部署天然成立；
// 容器化分离 time namespace 会引入不可检测的 capture 偏移）。
int64_t capture_now_ns() {
    // duration_cast：标准不保证 steady_clock period 为 ns，显式转换消除实现依赖。
    return static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

std::atomic<bool> g_stop{false};
std::string g_shm_name;  // 供信号处理器与 die() unlink
int g_lock_fd = -1;      // 单实例 flock fd；进程生命周期内持有（退出由 OS 释放）

struct ConnectorOptions {
    std::string ring_name = "/xt_md_shadow";
    uint64_t capacity = 4096;  // 默认容量覆盖 live fan-out 的短时 reader 抖动
    std::string source = "synthetic";
    std::string provider = "ctp";
    std::vector<std::string> bin_paths;
    std::string dump_source;
    int64_t count = 10000;
    int64_t interval_us = 0;     // 0 = 全速
    int64_t start_delay_ms = 0;  // 建段后延迟发布，给 reader attach 到 head=0
    int64_t hold_sec = 5;        // replay publish 完后保段存活，给 reader 读完

    // live 通用：MD 静默超时自愈阈值，0 表示禁用 gateway 内部 stale reconnect。
    uint32_t stale_reconnect_ms = 30000;
    uint16_t metrics_port = 0;               // 0 = 禁用 Prometheus exporter
    std::string metrics_addr = "127.0.0.1";  // prom-agent 本机 scrape 地址
    std::string lock_dir;  ///< 单实例 flock 目录 override；空 = /run/xyybtrader（回退 /tmp）

    std::string ctp_md_front;
    std::string broker;
    std::string flow_dir = "CTP_MD_FLOW";
    std::vector<std::string> symbols;
    std::string ctp_user;
    std::string ctp_password;

    // expected-tick window gate（provider=ctp 生效）。默认（未传参）用内置 CTP 窗；
    // --trading-windows 显式覆盖；--disable-trading-window-gate 回退现行行为。
    std::string trading_windows_raw;      // 显式 CLI 值（空 = 用内置默认）
    bool trading_windows_explicit = false;
    bool disable_trading_window_gate = false;
    ctp_md_session::TradingWindowGate window_gate;  // resolve 后填入

    std::string base_ws_url = "wss://fstream.binance.com";
    std::string base_rest_url = "https://fapi.binance.com";
    bool binance_testnet = false;
    bool ws_url_explicit = false;
    bool rest_url_explicit = false;
};

struct OwnerRingMapping {
    void* base = nullptr;
    uint64_t bytes = 0;
    uint64_t connector_epoch = 0;
};

void on_signal(int) { g_stop.store(true); }

int64_t arg_i64(const char* s) { return std::strtoll(s, nullptr, 10); }

[[noreturn]] void die(const char* msg) {
    std::fprintf(stderr, "xt_md_connector: %s: %s\n", msg, std::strerror(errno));
    if (!g_shm_name.empty()) shm_unlink(g_shm_name.c_str());
    std::exit(1);
}

// 逗号分隔的合约列表解析（"ag2606,ag2612"）；空白与空项忽略。
std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        // 去首尾空白
        size_t b = tok.find_first_not_of(" \t");
        size_t e = tok.find_last_not_of(" \t");
        if (b == std::string::npos) continue;
        out.push_back(tok.substr(b, e - b + 1));
    }
    return out;
}

// 解析 sidecar 目录（.lock 单实例锁 + .subs 订阅集共用）。
//
// 默认 /run/xyybtrader 而非 /tmp：systemd-tmpfiles 会按龄清 /tmp（默认 10d），清掉打开中的
// 锁文件后新 connector 开新 inode → flock 假成功 → 同 ring 双写者；/run 为 boot-scoped tmpfs
// 不被 age-clean，且保持全局命名空间（per-ring 互斥跨 unit 仍成立）。--lock-dir 可覆盖
// （dev/测试）；/run 不可写（非 root dev 运行）时回退 /tmp 并 warn。
std::string resolve_sidecar_dir(const std::string& lock_dir_opt) {
    if (!lock_dir_opt.empty()) return lock_dir_opt;
    std::string dir = "/run/xyybtrader";
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        std::fprintf(stderr,
                     "xt_md_connector: mkdir %s 失败 (%s)，sidecar 目录回退 /tmp（注意："
                     "/tmp 可能被 tmpfiles 按龄清理，长跑单实例互斥可能失效）\n",
                     dir.c_str(), std::strerror(errno));
        dir = "/tmp";
    }
    return dir;
}

// 抢占单实例锁：<sidecar_dir>/<规整名>.lock 上 flock(LOCK_EX|LOCK_NB)。抢到返 true（fd 存
// g_lock_fd，进程退出由 OS 释放）；抢不到（另一 connector 持有）返 false。规整名与 reader
// 侧同源（xt_ipc::md_sidecar_path，'/'→'_'）。
bool acquire_single_instance_lock(const std::string& ring_name, const std::string& sidecar_dir) {
    const std::string lock_path = xt_ipc::md_sidecar_path(sidecar_dir, ring_name, ".lock");
    int fd = open(lock_path.c_str(), O_CREAT | O_RDWR, 0600);
    if (fd < 0) {
        std::fprintf(stderr, "xt_md_connector: 打不开锁文件 %s: %s\n",
                     lock_path.c_str(), std::strerror(errno));
        return false;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        std::fprintf(stderr,
                     "xt_md_connector: another connector owns this ring (%s)；"
                     "flock %s 失败: %s\n",
                     ring_name.c_str(), lock_path.c_str(), std::strerror(errno));
        close(fd);
        return false;
    }
    g_lock_fd = fd;  // 进程生命周期内持有，不显式 close
    return true;
}

// 把本 connector 的订阅集写到 <sidecar_dir>/<规整名>.subs（每行一合约，原子 tmp+rename）。
// reader（XtShmMdGateway）attach 后据此做 declared⊆subs 诊断（缺失合约的 tick 永不到达）。
// 非致命：写失败仅 warn，reader 侧降级为 warn（不阻塞行情）。live 用 opt.symbols；replay 为空。
void write_subs_sidecar(const std::string& sidecar_dir, const std::string& ring_name,
                        const std::vector<std::string>& symbols) {
    const std::string path = xt_ipc::md_sidecar_path(sidecar_dir, ring_name, ".subs");
    const std::string tmp = path + ".tmp";
    int fd = open(tmp.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        std::fprintf(stderr, "xt_md_connector: 写 subs sidecar 失败（open %s: %s），"
                     "reader 侧 declared⊆subs 校验将降级\n", tmp.c_str(), std::strerror(errno));
        return;
    }
    std::string body;
    for (const auto& s : symbols) { body += s; body += '\n'; }
    const ssize_t wn = write(fd, body.data(), body.size());
    close(fd);
    if (wn != static_cast<ssize_t>(body.size())) {
        std::fprintf(stderr, "xt_md_connector: 写 subs sidecar %s 短写，放弃 rename\n", tmp.c_str());
        unlink(tmp.c_str());
        return;
    }
    if (rename(tmp.c_str(), path.c_str()) != 0) {
        std::fprintf(stderr, "xt_md_connector: rename subs sidecar %s→%s 失败: %s\n",
                     tmp.c_str(), path.c_str(), std::strerror(errno));
        unlink(tmp.c_str());
    }
}

bool parse_options(int argc, char** argv, ConnectorOptions& opt) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : ""; };
        if (a == "--ring-name") opt.ring_name = next();
        else if (a == "--capacity") opt.capacity = static_cast<uint64_t>(arg_i64(next()));
        else if (a == "--source") opt.source = next();
        else if (a == "--provider") opt.provider = next();
        else if (a == "--path") opt.bin_paths.emplace_back(next());
        else if (a == "--dump-source") opt.dump_source = next();
        else if (a == "--count") opt.count = arg_i64(next());
        else if (a == "--interval-us") opt.interval_us = arg_i64(next());
        else if (a == "--start-delay-ms") opt.start_delay_ms = arg_i64(next());
        else if (a == "--hold-sec") opt.hold_sec = arg_i64(next());
        else if (a == "--lock-dir") opt.lock_dir = next();
        else if (a == "--ctp-md-front") opt.ctp_md_front = next();
        else if (a == "--ctp-user") opt.ctp_user = next();
        else if (a == "--ctp-password") opt.ctp_password = next();
        else if (a == "--broker") opt.broker = next();
        else if (a == "--flow-dir") opt.flow_dir = next();
        else if (a == "--base-ws-url") { opt.base_ws_url = next(); opt.ws_url_explicit = true; }
        else if (a == "--base-rest-url") { opt.base_rest_url = next(); opt.rest_url_explicit = true; }
        else if (a == "--testnet") opt.binance_testnet = true;
        else if (a == "--stale-reconnect-ms")
            opt.stale_reconnect_ms = static_cast<uint32_t>(arg_i64(next()));
        else if (a == "--metrics-port") opt.metrics_port = static_cast<uint16_t>(arg_i64(next()));
        else if (a == "--metrics-addr") opt.metrics_addr = next();
        else if (a == "--trading-windows") {
            // 缺值 = fatal parse error（非零退出），与「显式空/非法业务值 → loud warn disable」
            // 显式分开：回滚有独立 --disable-trading-window-gate flag。
            if (i + 1 >= argc) {
                std::fprintf(stderr, "--trading-windows 需一个值 \"HHMM-HHMM[,...]\"\n");
                return false;
            }
            opt.trading_windows_raw = argv[++i];
            opt.trading_windows_explicit = true;
        }
        else if (a == "--disable-trading-window-gate") opt.disable_trading_window_gate = true;
        else if (a == "--symbols") {
            for (auto& s : split_csv(next())) opt.symbols.push_back(s);
        }
        else {
            std::fprintf(stderr, "unknown arg: %s\n", a.c_str());
            return false;
        }
    }
    return true;
}

void normalize_provider(ConnectorOptions& opt) {
    size_t b = opt.provider.find_first_not_of(" \t");
    size_t e = opt.provider.find_last_not_of(" \t");
    opt.provider = (b == std::string::npos) ? std::string() : opt.provider.substr(b, e - b + 1);
    for (char& c : opt.provider)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
}

void apply_binance_testnet_defaults(ConnectorOptions& opt) {
    // --testnet 且用户未显式覆盖 URL → 自动切 binance futures testnet 端点；
    // 显式 --base-ws-url / --base-rest-url 始终优先。
    if (opt.binance_testnet) {
        if (!opt.ws_url_explicit) opt.base_ws_url = "wss://stream.binancefuture.com";
        if (!opt.rest_url_explicit) opt.base_rest_url = "https://testnet.binancefuture.com";
    }
}

bool validate_options(ConnectorOptions& opt) {
    normalize_provider(opt);
    apply_binance_testnet_defaults(opt);

    if (opt.capacity == 0 || (opt.capacity & (opt.capacity - 1)) != 0) {
        std::fprintf(stderr, "capacity 必须是 2 的幂: %llu\n",
                     (unsigned long long)opt.capacity);
        return false;
    }
    if (opt.source != "synthetic" && opt.source != "bin" && opt.source != "live") {
        std::fprintf(stderr, "source '%s' 暂未支持（synthetic | bin | live）\n",
                     opt.source.c_str());
        return false;
    }
    if (opt.source == "bin" && opt.bin_paths.empty()) {
        std::fprintf(stderr, "--source bin 需至少一个 --path <file.bin>\n");
        return false;
    }
    if (opt.source == "live") {
        // provider 由 per-venue build 决定可用集；选了本 target 没编进的 venue 要 fail-fast。
        bool provider_ok = false;
#ifndef XT_NO_CTP
        if (opt.provider == "ctp") {
            provider_ok = true;
            if (opt.ctp_md_front.empty()) {
                std::fprintf(stderr, "--source live ctp 需 --ctp-md-front <tcp://...>\n");
                return false;
            }
            // CTP MD 是鉴权会话（ReqUserLogin），不同 binance public 匿名 MD。CLI 未给则回退
            // env（部署侧走 systemd EnvironmentFile，避免 password 进 ps/unit 明文）。
            if (opt.ctp_user.empty()) {
                if (const char* e = std::getenv("XT_CTP_MD_USER")) opt.ctp_user = e;
            }
            if (opt.ctp_password.empty()) {
                if (const char* e = std::getenv("XT_CTP_MD_PASSWORD")) opt.ctp_password = e;
            }
            // 提前到 connector 层 fail-fast，避免 create_md_gateway 只给出 opaque init failure。
            if (opt.ctp_user.empty() || opt.ctp_password.empty()) {
                std::fprintf(stderr, "--source live ctp 需 --ctp-user/--ctp-password"
                             "（或 env XT_CTP_MD_USER/XT_CTP_MD_PASSWORD）；CTP MD 需登录\n");
                return false;
            }
        }
#endif
#ifndef XT_NO_BINANCE
        if (opt.provider == "binance_u") {
            provider_ok = true;
            if (opt.base_ws_url.empty()) {
                std::fprintf(stderr, "--source live binance_u 需 --base-ws-url <wss://...>\n");
                return false;
            }
            if (opt.base_rest_url.empty()) {
                // depth snapshot 是 Binance MD 初始化的必要输入，空 URL 直接拒绝。
                std::fprintf(stderr, "--source live binance_u 需 --base-rest-url <https://...>（depth snapshot）\n");
                return false;
            }
        }
#endif
        if (!provider_ok) {
            std::fprintf(stderr, "--source live: 本 build 不支持 --provider %s\n",
                         opt.provider.c_str());
            return false;
        }
        if (opt.symbols.empty()) {
            std::fprintf(stderr, "--source live 需 --symbols <code[,code...]>\n");
            return false;
        }
    }
    return true;
}

OwnerRingMapping create_owner_ring_segment(const ConnectorOptions& opt) {
    const uint64_t bytes = xt_ipc::segment_bytes(opt.capacity, Ring::kSlotStride);
    // owner-create：O_EXCL 独占创建；只有在已持单实例锁后才把残留旧段视为 stale 并重建。
    g_shm_name = opt.ring_name;
    int fd = shm_open(opt.ring_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0 && errno == EEXIST) {
        std::fprintf(stderr, "xt_md_connector: 段 %s 已存在（持锁后判定为 stale），unlink 重建\n",
                     opt.ring_name.c_str());
        shm_unlink(opt.ring_name.c_str());
        fd = shm_open(opt.ring_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    }
    if (fd < 0) die("shm_open");
    if (ftruncate(fd, static_cast<off_t>(bytes)) != 0) die("ftruncate");

    void* base = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);  // mmap 后 fd 可关
    if (base == MAP_FAILED) die("mmap");

    Ring::init_ctrl(base, opt.capacity);

    // connector epoch 写 RingControlBlock._pad1 前 8 字节：每次 create 单调递增，
    // 供 reader re-attach 检测换代。已映射页的 magic 可能仍合法，不能只靠 magic。
    const uint64_t connector_epoch = static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    auto* ctrl = reinterpret_cast<xt_ipc::RingControlBlock*>(base);
    std::memcpy(ctrl->_pad1, &connector_epoch, sizeof(connector_epoch));
    return {base, bytes, connector_epoch};
}

void cleanup_owner_segment(void* base, uint64_t bytes, const std::string& ring_name) {
    munmap(base, bytes);
    shm_unlink(ring_name.c_str());
}

// expected-tick window gate 解析（仅 provider=ctp）。两态 {gate_enabled, sections}：
//   --disable-trading-window-gate → gate_enabled=false（回滚现行行为）
//   --trading-windows <csv>      → 解析；任一非法 token all-or-nothing fail-open disabled + loud warn
//   未传参                        → 内置默认 CTP 窗（gate_enabled=true）
void resolve_trading_window_gate(ConnectorOptions& opt) {
    if (opt.provider != "ctp") {
        opt.window_gate.gate_enabled = false;  // binance 等 24/7 provider 不门控
        return;
    }
    if (opt.disable_trading_window_gate) {
        opt.window_gate.gate_enabled = false;
        std::printf("xt_md_connector: trading-window gate disabled（--disable-trading-window-gate），"
                    "stale watchdog 全时段运行（现行行为）\n");
        return;
    }
    if (opt.trading_windows_explicit) {
        const auto r = ctp_md_session::parse_trading_windows(opt.trading_windows_raw);
        if (!r.ok) {
            // fail-open：非法业务配置 → 不门控 + loud warn（绝不部分生效/空窗喂 in_session）
            opt.window_gate.gate_enabled = false;
            std::fprintf(stderr,
                         "xt_md_connector: --trading-windows 非法（%s）→ fail-open：gate disabled、"
                         "stale watchdog 全时段运行\n",
                         r.error.c_str());
            return;
        }
        opt.window_gate = r.gate;
        std::printf("xt_md_connector: trading-window gate enabled（%zu 窗，来自 --trading-windows）\n",
                    opt.window_gate.windows.size());
        return;
    }
    opt.window_gate = ctp_md_session::built_in_ctp_gate();
    std::printf("xt_md_connector: trading-window gate enabled（内置 CTP 默认窗 "
                "0900-1015,1030-1130,1330-1500,2100-0230）\n");
}

void build_live_gateway_config(const ConnectorOptions& opt,
                               IMdGateway::Config& common,
                               gateway_factory::MdProviderCfg& cfg) {
    (void)cfg;
    common.subs = opt.symbols;  // venue MD 登录后订阅集
    common.stale_reconnect_ms = opt.stale_reconnect_ms;

#ifndef XT_NO_CTP
    if (opt.provider == "ctp") {
        common.front = opt.ctp_md_front;
        common.user = opt.ctp_user;          // ParserCTP::init 读 cfg.user/password
        common.password = opt.ctp_password;  // CTPMdCfg 只承载 broker/flow_dir
        CTPMdCfg ctp;
        ctp.broker = opt.broker;
        ctp.flow_dir = opt.flow_dir;
        ctp.window_gate = opt.window_gate;  // ParserCTP::check_health 窗门控用
        cfg.extra = ctp;
    }
#endif
#ifndef XT_NO_BINANCE
    if (opt.provider == "binance_u") {
        common.front = opt.base_ws_url;  // BinanceMdCfg：front 同 base_ws_url
        BinanceMdCfg bn;
        bn.base_ws_url = opt.base_ws_url;
        bn.base_rest_url = opt.base_rest_url;
        bn.testnet = opt.binance_testnet;
        cfg.extra = bn;
        // 打实际端点便于部署核对：真正决定连哪的是 URL，而不是 testnet flag。
        std::printf("xt_md_connector: binance_u ws=%s rest=%s testnet=%d\n",
                    opt.base_ws_url.c_str(), opt.base_rest_url.c_str(),
                    opt.binance_testnet ? 1 : 0);
    }
#endif
}

uint32_t steady_ms() {
    return static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

// wall-clock epoch ms（system_clock, UTC）；expected-tick 窗按固定 UTC+8 折算，不依赖 host TZ。
int64_t wall_epoch_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

int run_live_source(const ConnectorOptions& opt,
                    Ring& ring,
                    void* base,
                    uint64_t bytes,
                    uint64_t connector_epoch) {
    IMdGateway::Config common;
    gateway_factory::MdProviderCfg cfg;
    build_live_gateway_config(opt, common, cfg);

    std::string err;
    std::unique_ptr<IMdGateway> md =
        gateway_factory::create_md_gateway(opt.provider, common, cfg, &err);
    if (!md) {
        std::fprintf(stderr, "xt_md_connector: create_md_gateway(%s) 失败: %s\n",
                     opt.provider.c_str(), err.c_str());
        cleanup_owner_segment(base, bytes, opt.ring_name);
        return 1;
    }

    IMdGateway::Callbacks cbs;
    // live capture stamp：venue provider 无 capture 传 0，connector 是 SHM 真值必经
    // 通道，在 publish 前 stamp 自己的 CLOCK_MONOTONIC steady 时刻（跨进程可比）→ reader 端
    // 上溯 md_ingress→submit 起点。捕获进程 provider 拿不到发布时刻，故忽略入参 0 自行 stamp。
    cbs.on_tick = [&ring](const XtRawTick& tk, uint64_t /*provider_capture_ns=0*/) {
        xt_ipc::XtMdWireSlot slot;
        slot.capture_ns = capture_now_ns();
        slot.tick = tk;
        ring.publish(slot);
    };
    cbs.on_ready = []() { std::fprintf(stderr, "xt_md_connector: md ready\n"); };
    cbs.on_front = [](bool connected, int reason) {
        std::fprintf(stderr, "xt_md_connector: md front connected=%d reason=%d\n",
                     connected ? 1 : 0, reason);
    };
    md->bind_callbacks(std::move(cbs));

    if (!md->start()) {
        std::fprintf(stderr, "xt_md_connector: md start 失败\n");
        cleanup_owner_segment(base, bytes, opt.ring_name);
        return 1;
    }
    std::printf("xt_md_connector: %s md started（stale_reconnect_ms=%u），等 SIGINT/SIGTERM\n",
                opt.provider.c_str(), opt.stale_reconnect_ms);
    std::fflush(stdout);

    // 等信号；行情在 gateway provider-owned 线程回调 on_tick → publish。
    // connector 自身必须 ~1Hz 驱动 check_health：某些 venue 会保持连接但停止推送，
    // 自愈逻辑在 gateway 内部，connector 负责触发并在 false 时打 warn。
    ct::metrics::XtMdConnectorExporter exporter;
    if (opt.metrics_port > 0) {
        if (exporter.start(opt.metrics_port, opt.ring_name, opt.metrics_addr)) {
            exporter.set_epoch(connector_epoch);
            std::printf("xt_md_connector: metrics exposer on %s:%u（ring=%s）\n",
                        opt.metrics_addr.c_str(), opt.metrics_port, opt.ring_name.c_str());
            std::fflush(stdout);
        } else {
            std::fprintf(stderr, "xt_md_connector: metrics exposer bind %s:%u 失败（降级无 metrics）\n",
                         opt.metrics_addr.c_str(), opt.metrics_port);
        }
    }

    const uint32_t start_ms = steady_ms();
    uint32_t last_health_ms = start_ms;
    // stale watchdog armed 状态跟踪：仅在窗内/外切换时打一条 rate-limited transition log。
    // 初值 = 首帧计算前的补集，保证首帧必打一条基线（-1 = 未知）。
    int prev_watchdog = -1;
    while (!g_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        const uint32_t now_ms = steady_ms();
        if (now_ms - last_health_ms >= 1000u) {  // unsigned 差 → wraparound 安全
            last_health_ms = now_ms;
            const bool healthy = md->check_health(now_ms);
            if (!healthy)
                std::fprintf(stderr, "xt_md_connector: %s md check_health=false（stale/重连中）\n",
                             opt.provider.c_str());
            // expected-tick 窗 armed 状态（gate 未启用恒 true）；connector 侧独立计算供 gauge。
            const bool watchdog_active =
                ctp_md_session::watchdog_active(opt.window_gate, wall_epoch_ms());
            if (static_cast<int>(watchdog_active) != prev_watchdog) {
                std::fprintf(stderr,
                             "xt_md_connector: %s stale watchdog %s（expected-tick 窗%s）\n",
                             opt.provider.c_str(),
                             watchdog_active ? "ARMED" : "SUPPRESSED",
                             watchdog_active ? "内" : "外");
                prev_watchdog = static_cast<int>(watchdog_active);
            }
            if (exporter.is_running()) {
                exporter.update_ring_head(ring.head());
                exporter.set_check_health(healthy);
                exporter.set_stale_watchdog_active(watchdog_active);
                exporter.set_uptime_seconds(static_cast<double>(now_ms - start_ms) / 1000.0);
            }
        }
    }

    if (exporter.is_running()) exporter.stop();
    md->stop();
    cleanup_owner_segment(base, bytes, opt.ring_name);
    std::printf("xt_md_connector: %s md stopped + shm_unlink %s，退出\n",
                opt.provider.c_str(), opt.ring_name.c_str());
    return 0;
}

int run_replay_source(const ConnectorOptions& opt, Ring& ring, void* base, uint64_t bytes) {
    // ── Synthetic / bin source：publish 完保段存活给 reader 读完 ──────────────
    // 建段后延迟发布：给 reader 时间 attach 在 head=0（fast oracle 见全 N）。
    if (opt.start_delay_ms > 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(opt.start_delay_ms));

    // 可选：把 publish 的 tick 落 CSV，用于与 reader --dump-recv 逐字段对账。
    std::FILE* dump = nullptr;
    if (!opt.dump_source.empty()) {
        dump = std::fopen(opt.dump_source.c_str(), "w");
        if (!dump) die("open --dump-source");
        std::fprintf(dump, "price,action_time,bid0,ask0\n");
    }
    auto emit = [&](const XtRawTick& tk) {
        // replay capture stamp：stamp **读盘/回放时刻**（playback ingress），**非**原始录制时
        // 的 capture —— synthetic/bin source 无真实采集时戳，此值仅表征本次回放的 SHM 入队
        // 时刻，供 reader 端起点上溯口径一致（见 F 反证：语义是 playback ingress）。
        xt_ipc::XtMdWireSlot slot;
        slot.capture_ns = capture_now_ns();
        slot.tick = tk;
        ring.publish(slot);
        if (dump) std::fprintf(dump, "%.6f,%u,%.6f,%.6f\n", tk.price, tk.action_time,
                               tk.bid_prices[0], tk.ask_prices[0]);
        if (opt.interval_us > 0)
            std::this_thread::sleep_for(std::chrono::microseconds(opt.interval_us));
    };

    int64_t n = 0;
    if (opt.source == "synthetic") {
        // price/action_time 编码逻辑 index，reader 可逐笔自校验。
        XtRawTick t{};
        std::memcpy(t.exchg, "SHADOW", 6);
        std::memcpy(t.code, "TEST", 4);
        for (int64_t i = 0; i < opt.count && !g_stop.load(std::memory_order_relaxed); ++i) {
            t.price = static_cast<double>(i);
            t.action_time = static_cast<uint32_t>(i);
            t.bid_prices[0] = static_cast<double>(i) - 0.5;
            t.ask_prices[0] = static_cast<double>(i) + 0.5;
            emit(t);
            ++n;
        }
    } else {  // bin：复用 IMarketDataReader 读真录制 tick（XtBinTickReader）
        XtBinTickReader rdr(opt.bin_paths);
        if (!rdr.init()) die("XtBinTickReader::init");
        XtRawTick t{};
        while (!g_stop.load(std::memory_order_relaxed) && rdr.next_tick(t)) {
            emit(t);
            ++n;
        }
        if (rdr.has_io_error())
            std::fprintf(stderr, "xt_md_connector: bin reader io_error（部分文件打不开）\n");
    }
    if (dump) std::fclose(dump);
    std::printf("xt_md_connector: source=%s published=%lld head=%llu，hold %llds 给 reader 读完\n",
                opt.source.c_str(), (long long)n, (unsigned long long)ring.head(),
                (long long)opt.hold_sec);
    std::fflush(stdout);

    // 保段存活让 reader 读完（或被信号打断）
    for (int64_t s = 0; s < opt.hold_sec && !g_stop.load(std::memory_order_relaxed); ++s)
        std::this_thread::sleep_for(std::chrono::seconds(1));

    cleanup_owner_segment(base, bytes, opt.ring_name);
    std::printf("xt_md_connector: shm_unlink %s，退出\n", opt.ring_name.c_str());
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    ConnectorOptions opt;
    if (!parse_options(argc, argv, opt)) return 2;
    if (!validate_options(opt)) return 2;
    // validate_options 已 normalize provider；此后解析 expected-tick 窗 gate（仅 ctp）。
    resolve_trading_window_gate(opt);

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    // 单实例保护：抢到 flock 才允许碰段；抢不到说明同 ring 已有 writer，直接 fail-fast。
    const std::string sidecar_dir = resolve_sidecar_dir(opt.lock_dir);
    if (!acquire_single_instance_lock(opt.ring_name, sidecar_dir)) return 1;

    const OwnerRingMapping mapping = create_owner_ring_segment(opt);
    // 段建好后写订阅集 sidecar，供 reader 做 declared⊆subs 诊断（与 .lock 同目录同源规整名）。
    write_subs_sidecar(sidecar_dir, opt.ring_name, opt.symbols);
    Ring ring(mapping.base);

    std::printf("xt_md_connector: ring=%s capacity=%llu segment=%llu bytes slot_stride=%llu epoch=%llu\n",
                opt.ring_name.c_str(), (unsigned long long)opt.capacity,
                (unsigned long long)mapping.bytes, (unsigned long long)Ring::kSlotStride,
                (unsigned long long)mapping.connector_epoch);
    std::printf("xt_md_connector: source=%s provider=%s count=%lld interval_us=%lld\n",
                opt.source.c_str(), opt.provider.c_str(), (long long)opt.count,
                (long long)opt.interval_us);
    std::fflush(stdout);

    if (opt.source == "live")
        return run_live_source(opt, ring, mapping.base, mapping.bytes, mapping.connector_epoch);
    return run_replay_source(opt, ring, mapping.base, mapping.bytes);
}
