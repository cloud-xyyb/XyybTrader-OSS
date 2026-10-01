#pragma once

#include "api/IAccountView.h"
#include "xt_account/state/AccountStateStore.h"

namespace xt_account {

/// @brief 以 AccountStateStore 为后端的只读账户视图。
///
/// 不拥有 store；调用方必须保证 store 生命周期覆盖本视图。
/// 所有读接口都转到 store 的 hazard snapshot API，返回按值快照。
class AccountStoreView final : public xt_runtime::IAccountView {
public:
    explicit AccountStoreView(AccountStateStore& store) : store_(store) {}

    [[nodiscard]] AccountSnapshot account() const override {
        return store_.snapshot_account();
    }

    [[nodiscard]] PositionSnapshot position(std::string_view code) const override {
        return store_.snapshot_position(code);
    }

    [[nodiscard]] std::vector<std::string> tracked_codes() const override {
        return store_.snapshot_tracked_codes();
    }

private:
    AccountStateStore& store_;
};

}  // namespace xt_account
