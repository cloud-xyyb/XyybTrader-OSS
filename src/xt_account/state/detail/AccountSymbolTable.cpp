#include "xt_account/state/detail/AccountSymbolTable.h"

#include <algorithm>

namespace xt_account {

void SymbolTable::initialize() {
    std::lock_guard<std::mutex> lk(append_mu_);
    if (gen_.load(std::memory_order_relaxed)) {
        return;
    }
    publish_generation_locked_();
}

std::size_t SymbolTable::ensure_slot(std::string_view code) {
    if (code.empty()) {
        return kInvalidSymbolIndex;
    }
    // 无锁快路径：活世代已有该 code ⇒ slot 下标跨世代稳定，直接返回，不进锁。
    if (const std::size_t hit = find_slot(code); hit != kInvalidSymbolIndex) {
        return hit;
    }

    std::lock_guard<std::mutex> lk(append_mu_);
    const auto* g = gen_.load(std::memory_order_relaxed);
    if (!g) {
        // 未 initialize：slot 语义尚未启用（symbol 集合未冻结）。
        return kInvalidSymbolIndex;
    }
    // 锁内重查：快路径与取锁之间可能有另一追加者发布了同一 code。
    if (const std::size_t hit = g->find_slot(code); hit != kInvalidSymbolIndex) {
        return hit;
    }
    if (codes_.size() >= kMaxPositionSlots) {
        return kInvalidSymbolIndex;
    }
    codes_.emplace_back(code);
    publish_generation_locked_();
    return codes_.size() - 1;
}

std::size_t SymbolTable::ensure_slots(const std::vector<std::string_view>& codes,
                                      std::vector<std::string>* out_rejected) {
    std::lock_guard<std::mutex> lk(append_mu_);
    const auto* g = gen_.load(std::memory_order_relaxed);
    if (!g) {
        if (out_rejected) {
            for (const auto code : codes) {
                if (!code.empty()) out_rejected->emplace_back(code);
            }
        }
        return 0;
    }

    const std::size_t before = codes_.size();
    for (const auto code : codes) {
        if (code.empty()) continue;
        if (g->find_slot(code) != kInvalidSymbolIndex) continue;
        // 批内重复防御：调用方去重后不可达，但重复项若漏进来会分到两个 slot，
        // 破坏「slot ↔ distinct code」不变量。扫描面 = 本批已追加的尾部（≤1024）。
        bool dup = false;
        for (std::size_t i = before; i < codes_.size(); ++i) {
            if (codes_[i] == code) {
                dup = true;
                break;
            }
        }
        if (dup) continue;
        if (codes_.size() >= kMaxPositionSlots) {
            if (out_rejected) out_rejected->emplace_back(code);
            continue;
        }
        codes_.emplace_back(code);
    }

    const std::size_t added = codes_.size() - before;
    if (added > 0) {
        // 整批只造一个新世代：世代常驻，逐 code 发布会把内存上界推到 Σ(1..n)。
        publish_generation_locked_();
    }
    return added;
}

void SymbolTable::publish_generation_locked_() {
    auto fresh = std::make_unique<SymbolGeneration>();
    const std::size_t n = codes_.size();
    fresh->n = static_cast<uint32_t>(n);
    fresh->by_slot.reserve(n);
    fresh->sorted_by_code.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        const std::string* code = &codes_[i];
        fresh->by_slot.push_back(code);
        fresh->sorted_by_code.push_back(SymbolGeneration::Entry{code, static_cast<uint32_t>(i)});
    }
    std::sort(fresh->sorted_by_code.begin(), fresh->sorted_by_code.end(),
              [](const SymbolGeneration::Entry& lhs, const SymbolGeneration::Entry& rhs) {
                  return *lhs.code < *rhs.code;
              });

    const SymbolGeneration* published = fresh.get();
    retained_.push_back(std::move(fresh));
    // 世代对象在 store 之前完成构造，release 保证读者经 acquire 看到完整内容。
    gen_.store(published, std::memory_order_release);
    // 计数排在发布之后（release）：读者 acquire 读到 K 时，第 K 代必已对 active() 可见。
    // 反序（计数先行 + relaxed）与读侧 acquire 无配对对象，会让「已发布世代数」领先
    // 真实发布点，诊断与 metric 自相矛盾。
    generations_.store(retained_.size(), std::memory_order_release);
}

}  // namespace xt_account
