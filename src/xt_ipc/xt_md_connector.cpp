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
#include "GatewayFactory.h"
#include "ctp/md/CTPMdCfg.h"
#include "binance/md/BinanceMdCfg.h"
#include "xt_backtest/data/reader/XtBinTickReader.h"
#include "xt_core/market/XtRawTick.h"
#include "xt_ipc/XtMdWireContract.h"
#include "xt_ipc/XtShmRing.h"
#include "xt_metrics/XtMdConnectorExporter.h"

namespace {

using Ring = xt_ipc::XtShmRing<XtRawTick>;

std::atomic<bool> g_stop{false};
std::string g_shm_name;
int g_lock_fd = -1;

struct ConnectorOptions {
    std::string ring_name = "/xt_md_shadow";
    uint64_t capacity = 4096;
    std::string source = "synthetic";
    std::string provider = "ctp";
    std::vector<std::string> bin_paths;
    std::string dump_source;
    int64_t count = 10000;
    int64_t interval_us = 0;
    int64_t start_delay_ms = 0;
    int64_t hold_sec = 5;

    uint32_t stale_reconnect_ms = 30000;
    uint16_t metrics_port = 0;
    std::string metrics_addr = "127.0.0.1";
    std::string lock_dir;

    std::string ctp_md_front;
    std::string broker;
    std::string flow_dir = "CTP_MD_FLOW";
    std::vector<std::string> symbols;
    std::string ctp_user;
    std::string ctp_password;

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

std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {

        size_t b = tok.find_first_not_of(" \t");
        size_t e = tok.find_last_not_of(" \t");
        if (b == std::string::npos) continue;
        out.push_back(tok.substr(b, e - b + 1));
    }
    return out;
}

bool acquire_single_instance_lock(const std::string& ring_name,
                                  const std::string& lock_dir_opt) {
    std::string dir = lock_dir_opt;
    if (dir.empty()) {
        dir = "/run/xyybtrader";
        if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
            std::fprintf(stderr,
                         "xt_md_connector: mkdir %s 失败 (%s)，锁目录回退 /tmp（注意："
                         "/tmp 可能被 tmpfiles 按龄清理，长跑单实例互斥可能失效）\n",
                         dir.c_str(), std::strerror(errno));
            dir = "/tmp";
        }
    }
    std::string lock_path = dir + "/xt_md_connector";
    for (char c : ring_name) lock_path += (c == '/') ? '_' : c;
    lock_path += ".lock";
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
    g_lock_fd = fd;
    return true;
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

        bool provider_ok = false;
#ifndef XT_NO_CTP
        if (opt.provider == "ctp") {
            provider_ok = true;
            if (opt.ctp_md_front.empty()) {
                std::fprintf(stderr, "--source live ctp 需 --ctp-md-front <tcp://...>\n");
                return false;
            }

            if (opt.ctp_user.empty()) {
                if (const char* e = std::getenv("XT_CTP_MD_USER")) opt.ctp_user = e;
            }
            if (opt.ctp_password.empty()) {
                if (const char* e = std::getenv("XT_CTP_MD_PASSWORD")) opt.ctp_password = e;
            }

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
    close(fd);
    if (base == MAP_FAILED) die("mmap");

    Ring::init_ctrl(base, opt.capacity);

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

void build_live_gateway_config(const ConnectorOptions& opt,
                               IMdGateway::Config& common,
                               gateway_factory::MdProviderCfg& cfg) {
    (void)cfg;
    common.subs = opt.symbols;
    common.stale_reconnect_ms = opt.stale_reconnect_ms;

#ifndef XT_NO_CTP
    if (opt.provider == "ctp") {
        common.front = opt.ctp_md_front;
        common.user = opt.ctp_user;
        common.password = opt.ctp_password;
        CTPMdCfg ctp;
        ctp.broker = opt.broker;
        ctp.flow_dir = opt.flow_dir;
        cfg.extra = ctp;
    }
#endif
#ifndef XT_NO_BINANCE
    if (opt.provider == "binance_u") {
        common.front = opt.base_ws_url;
        BinanceMdCfg bn;
        bn.base_ws_url = opt.base_ws_url;
        bn.base_rest_url = opt.base_rest_url;
        bn.testnet = opt.binance_testnet;
        cfg.extra = bn;

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
    cbs.on_tick = [&ring](const XtRawTick& tk) { ring.publish(tk); };
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
    while (!g_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        const uint32_t now_ms = steady_ms();
        if (now_ms - last_health_ms >= 1000u) {
            last_health_ms = now_ms;
            const bool healthy = md->check_health(now_ms);
            if (!healthy)
                std::fprintf(stderr, "xt_md_connector: %s md check_health=false（stale/重连中）\n",
                             opt.provider.c_str());
            if (exporter.is_running()) {
                exporter.update_ring_head(ring.head());
                exporter.set_check_health(healthy);
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

    if (opt.start_delay_ms > 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(opt.start_delay_ms));

    std::FILE* dump = nullptr;
    if (!opt.dump_source.empty()) {
        dump = std::fopen(opt.dump_source.c_str(), "w");
        if (!dump) die("open --dump-source");
        std::fprintf(dump, "price,action_time,bid0,ask0\n");
    }
    auto emit = [&](const XtRawTick& tk) {
        ring.publish(tk);
        if (dump) std::fprintf(dump, "%.6f,%u,%.6f,%.6f\n", tk.price, tk.action_time,
                               tk.bid_prices[0], tk.ask_prices[0]);
        if (opt.interval_us > 0)
            std::this_thread::sleep_for(std::chrono::microseconds(opt.interval_us));
    };

    int64_t n = 0;
    if (opt.source == "synthetic") {

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
    } else {
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

    for (int64_t s = 0; s < opt.hold_sec && !g_stop.load(std::memory_order_relaxed); ++s)
        std::this_thread::sleep_for(std::chrono::seconds(1));

    cleanup_owner_segment(base, bytes, opt.ring_name);
    std::printf("xt_md_connector: shm_unlink %s，退出\n", opt.ring_name.c_str());
    return 0;
}

}

int main(int argc, char** argv) {
    ConnectorOptions opt;
    if (!parse_options(argc, argv, opt)) return 2;
    if (!validate_options(opt)) return 2;

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    if (!acquire_single_instance_lock(opt.ring_name, opt.lock_dir)) return 1;

    const OwnerRingMapping mapping = create_owner_ring_segment(opt);
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
