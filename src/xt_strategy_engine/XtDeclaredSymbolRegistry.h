#pragma once

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace xt_strategy_engine {

/// @brief 策略 id -> 关注合约的登记表
///
/// 内容为 `strategy_id -> declared codes`，由 runner 装配期配置 seed 和策略
/// on_init 里的 declare_position_interest 共同写入
///
/// 线程语义：写可能来自 runner main 与策略初始化线程，读来自 engine/control/
/// 轮询线程，读写均经 shared_mutex 保护。
class XtDeclaredSymbolRegistry {
public:
    /// @brief 声明 strategy `sid` 关注 `code`；重复声明幂等。
    ///
    /// 空 sid/code 被忽略，调用方无需为可选配置额外分支。
    void declare(const std::string& sid, const std::string& code) {
        if (sid.empty() || code.empty()) return;
        std::unique_lock lk(mu_);
        sid_to_codes_[sid].insert(code);
    }

    /// @brief 返回 sid 已声明的 code 集合；未知 sid 返回空集合。
    std::unordered_set<std::string> declared_symbols(const std::string& sid) const {
        std::shared_lock lk(mu_);
        auto it = sid_to_codes_.find(sid);
        if (it == sid_to_codes_.end()) return {};
        return it->second;
    }

    /// @brief 返回至少声明过一个 code 的全部 sid。
    std::vector<std::string> registered_sids() const {
        std::shared_lock lk(mu_);
        std::vector<std::string> out;
        out.reserve(sid_to_codes_.size());
        for (const auto& [sid, codes] : sid_to_codes_) {
            (void)codes;
            out.push_back(sid);
        }
        return out;
    }

private:
    mutable std::shared_mutex mu_; ///< 保护 sid_to_codes_；读接口只返回 copy。
    std::unordered_map<std::string, std::unordered_set<std::string>> sid_to_codes_;
};

}  // namespace xt_strategy_engine
