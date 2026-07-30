#pragma once

/// @file
/// @brief force-flat submit 的同步 per-symbol fence（over-close 缝挡板）。
///
/// `force_exit_panel_request` submit 后经 `record_force_flat_order` 标记
/// genuine-force-flat，但该标记从 control/gRPC 线程发起时走 book worker
/// marshal 异步投递，要等 worker 应用 + snapshot publish 才对 stage (1) 的
/// async genuine 检查可见——盲窗内：① 同帧多 caller（PerSidLiquidator /
/// PanelForceExitLatch / RPC）对同一 symbol 各交一次全量 close；② ForceClosed
/// 后 terminal update 先于 trade 落账时，下帧按 stale position 重复提交。
/// 后果 = 超仓位 close、CTP over-close reject 风暴。
///
/// 本 fence 是 held 窗口内的同步侧挡板：submit 前 `try_acquire` 原子 claim
/// `until = now + hold`（单锁段内 check+set，并发 caller 恰一胜出），返回本次
/// claim 的 generation；窗口内同 symbol 的后续 submit 被闸（撤单轮不受影响，
/// 仅 (4a) submit 闸）；submit 失败方按 generation 归属 `release` 让出——claim
/// 过期后被他方重新 claim 时，先前持有者的 release 不得误删新窗口。到期后
/// async genuine 检查接管（正常路径此时标记早已可见）。固定护栏语义，
/// 不做 config。
///
/// 线程契约：control frame 线程与 gRPC RPC 线程并发调用；单 mutex 序列化。
/// 条目按 symbol 记（不分 sid）——盲窗内 sibling sid 的 force-exit 一并推迟
/// 到下帧（latch Keep 重试），保守方向。
///
/// 已接受残留（不做更重的收敛闸——那会在丢失一条回报时永久卡死平仓）：
/// 重复 submit 速率受 hold 限制；存在 retry
/// driver（PanelForceExitLatch / PerSidLiquidator）时由其续帧收敛到 flat，
/// 直接 RPC 的 ForceClosed 不 arm latch、latch 亦可 TTL Expired——此类情形
/// 由 operator/驱动方后续触发兜底（Close-only 语义下无翻仓风险）：
/// ① trade/marshal 延迟 > hold：fence 过期 → 按 stale position 重发 → 若被
///    venue 受理，helper 先返 ForceClosed，超额部分的业务拒单随后以异步终态
///    回报到达。
/// ② acquire 与外部 submit 之间 caller 被阻塞 > hold：他方可重新 claim 并
///    submit，阻塞方苏醒后的 submit 构成同款受限速率重试；generation 归属
///    保证阻塞方失败时不误删他方窗口。hold=3000 维持不加大。

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <mutex>
#include <string>
#include <unordered_map>

namespace xt_live {
namespace control {

inline constexpr int64_t kForceFlatFenceHoldMs = 3000;

/// @brief per-symbol force-flat submit fence（见文件头）。
class ForceFlatSubmitFence {
public:

    explicit ForceFlatSubmitFence(int64_t hold_ms = kForceFlatFenceHoldMs)
        : hold_ms_(hold_ms) {}

    bool held(const std::string& symbol, int64_t now_ms) {
        std::lock_guard<std::mutex> lk(mu_);
        return held_locked_(symbol, now_ms);
    }

    uint64_t try_acquire(const std::string& symbol, int64_t now_ms) {
        std::lock_guard<std::mutex> lk(mu_);
        if (held_locked_(symbol, now_ms)) return 0;

        if (entries_.size() >= kSweepSize) {
            for (auto it = entries_.begin(); it != entries_.end();) {
                it = (now_ms >= it->second.until_ms) ? entries_.erase(it) : std::next(it);
            }
        }
        uint64_t gen = ++next_gen_;
        if (gen == 0) gen = ++next_gen_;
        entries_[symbol] = Entry{now_ms + hold_ms_, gen};
        return gen;
    }

    void release(const std::string& symbol, uint64_t gen) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = entries_.find(symbol);
        if (it != entries_.end() && it->second.gen == gen) entries_.erase(it);
    }

private:
    static constexpr std::size_t kSweepSize = 32;
    struct Entry {
        int64_t until_ms = 0;
        uint64_t gen = 0;
    };

    bool held_locked_(const std::string& symbol, int64_t now_ms) {
        auto it = entries_.find(symbol);
        if (it == entries_.end()) return false;
        if (now_ms >= it->second.until_ms) {
            entries_.erase(it);
            return false;
        }
        return true;
    }

    std::mutex mu_;
    std::unordered_map<std::string, Entry> entries_;
    uint64_t next_gen_ = 0;
    int64_t hold_ms_ = kForceFlatFenceHoldMs;
};

}
}
