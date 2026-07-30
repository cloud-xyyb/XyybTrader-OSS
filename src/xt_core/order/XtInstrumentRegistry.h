#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "xt_core/sync/XtTransparentStringHash.h"

namespace xt_core {

using XtInstrumentId = uint32_t;

static constexpr XtInstrumentId kInvalidInstrumentId = 0;
static constexpr uint32_t kMaxInstruments = 4096;

/// @brief 合约代码字符串与整数 ID 的双向注册表。
///
/// 在热路径上到处传 `std::string` 合约代码（比较、hash、拷贝）太贵，本注册表在启动/持仓
/// 初始化阶段用 `register_or_get()` 把每个 code intern 成一个稳定的 `XtInstrumentId`
/// （从 1 起分配），此后热路径只传这个整数，需要时再用 `code_of()` O(1) 反查回代码。
/// ID 0 保留给无效/未注册。
///
/// @note 写入不是线程安全的；完成注册后，多线程只读访问由调用方发布/冻结生命周期保证。
class XtInstrumentRegistry {
public:

    using TransparentHash  = xt_core::sync::TransparentStringHash;
    using TransparentEqual = xt_core::sync::TransparentStringEqual;

    XtInstrumentId register_or_get(const char* code) {
        if (!code || code[0] == '\0') return kInvalidInstrumentId;
        auto it = code_to_id_.find(code);
        if (it != code_to_id_.end()) return it->second;
        if (next_id_ > kMaxInstruments) return kInvalidInstrumentId;

        const XtInstrumentId id = next_id_++;
        code_to_id_.emplace(std::string(code), id);
        std::strncpy(id_to_code_[id], code, kMaxCodeLen - 1);
        id_to_code_[id][kMaxCodeLen - 1] = '\0';
        return id;
    }

    XtInstrumentId id_of(const char* code) const noexcept {
        if (!code || code[0] == '\0') return kInvalidInstrumentId;
        auto it = code_to_id_.find(code);
        return it == code_to_id_.end() ? kInvalidInstrumentId : it->second;
    }

    const char* code_of(XtInstrumentId id) const noexcept {
        if (id == kInvalidInstrumentId || id >= next_id_) return "";
        return id_to_code_[id];
    }

    void reset() {
        code_to_id_.clear();

        for (uint32_t i = 1; i < next_id_; ++i) id_to_code_[i][0] = '\0';
        next_id_ = 1;
    }

private:

    static constexpr size_t kMaxCodeLen = 32;

    std::unordered_map<std::string, XtInstrumentId, TransparentHash, TransparentEqual> code_to_id_;

    std::array<char[kMaxCodeLen], kMaxInstruments + 1> id_to_code_{};
    uint32_t next_id_ = 1;
};

static_assert(std::is_empty_v<XtInstrumentRegistry::TransparentHash>,
              "TransparentHash must be empty struct (EBO assumption)");
static_assert(std::is_empty_v<XtInstrumentRegistry::TransparentEqual>,
              "TransparentEqual must be empty struct (EBO assumption)");
static_assert(sizeof(XtInstrumentRegistry::TransparentHash) == 1,
              "TransparentHash sizeof must be 1 (empty struct, no member)");
static_assert(sizeof(XtInstrumentRegistry::TransparentEqual) == 1,
              "TransparentEqual sizeof must be 1 (empty struct, no member)");

}
