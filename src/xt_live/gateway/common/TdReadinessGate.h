#pragma once

/// @file
/// @brief 交易通道的 ready 检查清单。
///
/// - TD gateway 启动时需要满足以下条件才能放行交易：结算确认、账户快照、持仓快照、
///   在途单查询、reverse-sync 回灌。
/// - TdReadinessGate 确保所有检查项通过后 ready 才为 true。
/// - Gate 本身只记状态，不发起查询、不触发 ready callback；那些归 gateway。

#include <array>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>

namespace xt_live::gateway {

enum class TdReadinessStage : uint8_t {
    Settlement = 0,
    Account,
    Position,
    OpenOrders,
    ReverseSync,
    Count
};

enum class TdReadinessStatus : uint8_t {
    Pending = 0,
    NotRequired,
    Satisfied,
    Failed,
    Stale
};

inline constexpr std::size_t td_readiness_stage_count() noexcept {
    return static_cast<std::size_t>(TdReadinessStage::Count);
}

inline constexpr std::size_t td_readiness_index(TdReadinessStage stage) noexcept {
    return static_cast<std::size_t>(stage);
}

inline const char* td_readiness_stage_name(TdReadinessStage stage) noexcept {
    switch (stage) {
        case TdReadinessStage::Settlement:  return "settlement";
        case TdReadinessStage::Account:     return "account";
        case TdReadinessStage::Position:    return "position";
        case TdReadinessStage::OpenOrders:  return "open_orders";
        case TdReadinessStage::ReverseSync: return "reverse_sync";
        case TdReadinessStage::Count:       return "count";
    }
    return "unknown";
}

inline const char* td_readiness_status_name(TdReadinessStatus status) noexcept {
    switch (status) {
        case TdReadinessStatus::Pending:     return "pending";
        case TdReadinessStatus::NotRequired: return "not_required";
        case TdReadinessStatus::Satisfied:   return "satisfied";
        case TdReadinessStatus::Failed:      return "failed";
        case TdReadinessStatus::Stale:       return "stale";
    }
    return "unknown";
}

/// @brief 单个 TD readiness stage 的状态快照。
struct TdReadinessStageReport {
    TdReadinessStage  stage = TdReadinessStage::Settlement;
    TdReadinessStatus status = TdReadinessStatus::Pending;
    std::string       reason;
    uint64_t          updated_ms = 0;
};

/// @brief TD readiness gate 的完整报告。
///
/// `ready=true` 表示所有 required stage 均为 Satisfied/NotRequired。
/// `blocking_stage/reason` 指向第一个阻塞 stage；`stages` 保留全量状态供诊断。
struct TdReadinessReport {
    std::string venue;
    bool ready = false;
    std::string blocking_stage;
    std::string blocking_reason;
    std::array<TdReadinessStageReport, td_readiness_stage_count()> stages{};

    const TdReadinessStageReport& stage(TdReadinessStage s) const noexcept {
        return stages[td_readiness_index(s)];
    }

    bool satisfied(TdReadinessStage s) const noexcept {
        const auto status = stage(s).status;
        return status == TdReadinessStatus::Satisfied ||
               status == TdReadinessStatus::NotRequired;
    }
};

/// @brief 线程安全的 readiness 状态表；report() 汇总当前全貌。
class TdReadinessGate {
public:
    explicit TdReadinessGate(std::string venue = {}) {
        reset(std::move(venue));
    }

    void reset(std::string venue) {
        std::lock_guard<std::mutex> guard(mu_);
        venue_ = std::move(venue);
        for (std::size_t i = 0; i < stages_.size(); ++i) {
            stages_[i].stage = static_cast<TdReadinessStage>(i);
            stages_[i].status = TdReadinessStatus::Pending;
            stages_[i].reason.clear();
            stages_[i].updated_ms = 0;
        }
    }

    void set_pending(TdReadinessStage stage, std::string reason = {}, uint64_t now_ms = 0) {
        set_status(stage, TdReadinessStatus::Pending, std::move(reason), now_ms);
    }

    void set_not_required(TdReadinessStage stage, std::string reason = {}, uint64_t now_ms = 0) {
        set_status(stage, TdReadinessStatus::NotRequired, std::move(reason), now_ms);
    }

    void satisfy(TdReadinessStage stage, std::string reason = {}, uint64_t now_ms = 0) {
        set_status(stage, TdReadinessStatus::Satisfied, std::move(reason), now_ms);
    }

    void fail(TdReadinessStage stage, std::string reason, uint64_t now_ms = 0) {
        set_status(stage, TdReadinessStatus::Failed, std::move(reason), now_ms);
    }

    void mark_stale(TdReadinessStage stage, std::string reason, uint64_t now_ms = 0) {
        set_status(stage, TdReadinessStatus::Stale, std::move(reason), now_ms);
    }

    TdReadinessReport report() const {
        std::lock_guard<std::mutex> guard(mu_);
        TdReadinessReport out;
        out.venue = venue_;
        out.stages = stages_;
        out.ready = true;
        for (const auto& stage : out.stages) {
            if (stage.status == TdReadinessStatus::Satisfied ||
                stage.status == TdReadinessStatus::NotRequired) {
                continue;
            }
            out.ready = false;
            if (out.blocking_stage.empty()) {
                out.blocking_stage = td_readiness_stage_name(stage.stage);
                out.blocking_reason = stage.reason.empty()
                    ? td_readiness_status_name(stage.status)
                    : stage.reason;
            }
        }
        return out;
    }

    bool ready() const {
        return report().ready;
    }

private:
    void set_status(TdReadinessStage stage,
                    TdReadinessStatus status,
                    std::string reason,
                    uint64_t now_ms) {
        std::lock_guard<std::mutex> guard(mu_);
        auto& slot = stages_[td_readiness_index(stage)];
        slot.stage = stage;
        slot.status = status;
        slot.reason = std::move(reason);
        slot.updated_ms = now_ms;
    }

    mutable std::mutex mu_;
    std::string venue_;
    std::array<TdReadinessStageReport, td_readiness_stage_count()> stages_{};
};

}
