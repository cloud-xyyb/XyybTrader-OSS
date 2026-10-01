// ============================================================================
// XtClockSkewEstimator — state machine 与 metrics 集成
// ============================================================================

#include "XtClockSkewEstimator.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "xt_metrics/XtMetricRegistry.h"
#include "xt_util/time/XtSteadyClock.h"

namespace xt_live::gateway {

namespace {

// singleton key 为 "<venue>/<region>"；adapter attach 后缓存引用，热路径不查表。
constexpr std::size_t kOffsetUpdateSampleInterval = 128;

std::mutex g_singleton_mu;
std::unordered_map<std::string, std::unique_ptr<XtClockSkewEstimator>>
    g_singletons;

}  // namespace

// ── Singleton ──────────────────────────────────────────────────────────

XtClockSkewEstimator& XtClockSkewEstimator::shared(std::string_view venue,
                                                std::string_view region) {
    std::string key;
    key.reserve(venue.size() + 1 + region.size());
    key.append(venue);
    key.push_back('/');
    key.append(region);

    std::lock_guard<std::mutex> lk(g_singleton_mu);
    auto it = g_singletons.find(key);
    if (it == g_singletons.end()) {
        auto est = std::make_unique<XtClockSkewEstimator>(
            std::string(venue), std::string(region));
        auto* ptr = est.get();
        g_singletons.emplace(std::move(key), std::move(est));
        return *ptr;
    }
    return *it->second;
}

// ── Lifecycle ──────────────────────────────────────────────────────────

XtClockSkewEstimator::XtClockSkewEstimator(std::string venue,
                                         std::string region,
                                         ClockSkewConfig cfg)
    : venue_(std::move(venue)),
      region_(std::move(region)),
      cfg_(cfg) {
    // 当前 bootstrap 为 best-effort；失败时进入 WARMING，用 live 样本恢复 offset。
    try_bootstrap(/*preserve_nan_on_failure=*/false);
}

// ── Hot path: record_sample ────────────────────────────────────────────

void XtClockSkewEstimator::record_sample(uint64_t recv_mono_us,
                                        uint64_t server_event_us) {
    // 热路径只短暂持有 window_mu_ 做 append/evict；排序重算按样本间隔节流。
    Sample s{
        .mono_us = recv_mono_us,
        .offset_sample_us = static_cast<int64_t>(server_event_us)
                            - static_cast<int64_t>(recv_mono_us),
    };
    {
        std::lock_guard<std::mutex> lk(window_mu_);
        window_.push_back(s);
        // 使用 monotonic 时间淘汰窗口，避免 wall clock step 影响窗口年龄。
        const uint64_t window_us =
            static_cast<uint64_t>(cfg_.window_duration.count()) * 1'000'000ULL;
        while (!window_.empty()
               && (recv_mono_us - window_.front().mono_us) > window_us) {
            window_.pop_front();
        }
    }
    const std::size_t samples =
        sample_count_.fetch_add(1, std::memory_order_relaxed) + 1;

    if (samples == 1 || samples % kOffsetUpdateSampleInterval == 0) {
        update_offset_from_window();
    }
    maybe_promote_to_locked();
}

// ── Reset / transitions ────────────────────────────────────────────────

void XtClockSkewEstimator::on_ws_reconnect(std::string_view channel) {
    reset_window(/*preserve_bootstrap=*/true);
    state_.store(ClockSkewState::Warming, std::memory_order_release);
    ct::metrics::XtMetricRegistry::instance().inc_md_ws_reconnect(channel);
    publish_state(channel);
}

void XtClockSkewEstimator::on_clock_step_detected(std::string_view channel) {
    reset_window(/*preserve_bootstrap=*/false);
    state_.store(ClockSkewState::Stepped, std::memory_order_release);
    offset_us_.store(std::nan(""), std::memory_order_release);
    ct::metrics::XtMetricRegistry::instance().inc_md_clock_step(channel);
    publish_state(channel);
    // REST bootstrap 不可用时保留 NaN 发布，但允许 live 样本继续恢复窗口。
    try_bootstrap(/*preserve_nan_on_failure=*/true);
}

void XtClockSkewEstimator::reset_window(bool preserve_bootstrap) {
    {
        std::lock_guard<std::mutex> lk(window_mu_);
        window_.clear();
    }
    sample_count_.store(0, std::memory_order_release);
    if (!preserve_bootstrap) {
        std::lock_guard<std::mutex> lk(bootstrap_mu_);
        bootstrap_done_ = false;
        bootstrap_offset_us_ = 0.0;
    }
}

// ── Bootstrap ──────────────────────────────────────────────────────────

bool XtClockSkewEstimator::try_bootstrap(bool preserve_nan_on_failure) {
    // REST bootstrap 尚未接入。cold start 使用 offset=0 进入 WARMING；STEPPED 后
    // 保持 NaN，直到 live 样本重新形成 offset，避免发布看似可信的 0 offset。
    std::lock_guard<std::mutex> lk(bootstrap_mu_);
    bootstrap_done_ = false;
    bootstrap_offset_us_ = 0.0;
    if (preserve_nan_on_failure) {
        state_.store(ClockSkewState::Warming, std::memory_order_release);
        return false;
    }
    offset_us_.store(bootstrap_offset_us_, std::memory_order_release);
    state_.store(ClockSkewState::Warming, std::memory_order_release);
    return false;
}

// ── Offset estimation ──────────────────────────────────────────────────

void XtClockSkewEstimator::update_offset_from_window() {
    if (state_.load(std::memory_order_acquire) == ClockSkewState::Stepped) {
        return;
    }

    // 单样本 diff = clock_offset - network_latency；最大值最接近真实 offset。
    // 先裁掉最高尾部样本，再取最大值，降低异常样本影响。
    std::vector<int64_t> diffs;
    {
        std::lock_guard<std::mutex> lk(window_mu_);
        if (window_.empty()) return;
        diffs.reserve(window_.size());
        for (const auto& s : window_) {
            diffs.push_back(s.offset_sample_us);
        }
    }

    std::sort(diffs.begin(), diffs.end());
    const std::size_t last_idx = diffs.size() - 1;
    const std::size_t trim_count = std::min(
        static_cast<std::size_t>(diffs.size() * cfg_.trim_ratio),
        last_idx);
    const double trimmed_max = static_cast<double>(diffs[last_idx - trim_count]);

    // 直接发布 trimmed maximum，避免平滑把 offset 压低并制造负 corrected latency。
    offset_us_.store(trimmed_max, std::memory_order_release);
}

void XtClockSkewEstimator::maybe_promote_to_locked() {
    if (state_.load(std::memory_order_acquire) != ClockSkewState::Warming) {
        return;
    }
    if (sample_count_.load(std::memory_order_acquire)
        >= cfg_.locked_sample_threshold) {
        state_.store(ClockSkewState::Locked, std::memory_order_release);
    }
}

// ── Accessors ──────────────────────────────────────────────────────────

double XtClockSkewEstimator::offset_us() const noexcept {
    return offset_us_.load(std::memory_order_acquire);
}

ClockSkewState XtClockSkewEstimator::state() const noexcept {
    return state_.load(std::memory_order_acquire);
}

std::size_t XtClockSkewEstimator::window_samples() const noexcept {
    std::lock_guard<std::mutex> lk(window_mu_);
    return window_.size();
}

double XtClockSkewEstimator::window_min_age_seconds() const noexcept {
    std::lock_guard<std::mutex> lk(window_mu_);
    if (window_.empty()) return 0.0;
    // 最老样本的 monotonic 时间与当前 monotonic 时间差。
    const auto now = XtSteadyClock::steady_now_us();
    const int64_t age_us = now - static_cast<int64_t>(window_.front().mono_us);
    return static_cast<double>(age_us) / 1'000'000.0;
}

// ── Publish to XtMetricRegistry ────────────────────────────────────────

void XtClockSkewEstimator::publish_state(std::string_view channel) {
    auto& reg = ct::metrics::XtMetricRegistry::instance();
    reg.set_md_clock_offset_us(channel, offset_us());
    reg.set_md_clock_offset_confidence(
        channel, state() == ClockSkewState::Locked);
    reg.set_md_clock_offset_window_samples(channel, window_samples());
    reg.set_md_clock_offset_window_min_age_seconds(
        channel, window_min_age_seconds());
}

}  // namespace xt_live::gateway
